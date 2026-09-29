"""Transactional upserts into separate Phase 4 schemas; Phase 3 stays immutable.

The landing rows, applied-event journal, batch ledger, and sequence watermark
commit together. dbt runs AFTER that transaction, in separate transactions. A
failed dbt build is recoverable but is never described as an atomic end-to-end
publication. Downstream consumers must check pipeline_inc.state.status.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from decimal import Decimal
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time
from uuid import uuid4

from src.common import PROJECT_ROOT, PipelineError, read_json
from src.load.warehouse_contracts import CONTRACTS, KEYS, TABLES, AMOUNTS, Schemas, assess_dbt_results
from src.load.warehouse import (analytics_summary, database_digests, fetch_state as phase3_state,
                                fingerprint as phase3_fingerprint, relation_exists, validate_landing)
from src.incremental.contracts import ensure_disjoint, event_digest, check_update

LOG = logging.getLogger(__name__)


def code_fingerprint() -> str:
    h = hashlib.sha256()
    for name in ('contracts.py', 'feed.py', 'warehouse.py'):
        p = PROJECT_ROOT / 'src/incremental' / name
        h.update(name.encode())
        h.update(p.read_bytes().replace(b'\r\n', b'\n'))
    h.update(phase3_fingerprint('loader').encode())
    return h.hexdigest()


@contextmanager
def warehouse_lock(conn, schemas: Schemas):
    key = int.from_bytes(hashlib.sha256(schemas.meta.encode()).digest()[:8], 'big', signed=True)
    if not conn.execute('SELECT pg_try_advisory_lock(%s)', (key,)).fetchone()[0]:
        raise PipelineError('Another process owns this warehouse lock. Retry when it finishes.')
    try:
        yield
    finally:
        conn.execute('SELECT pg_advisory_unlock(%s)', (key,))


def fetch_state(conn, schemas: Schemas, for_update=False) -> dict | None:
    from psycopg import sql
    from psycopg.rows import dict_row
    if not relation_exists(conn, schemas.meta, 'state'):
        return None
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql.SQL('SELECT * FROM {}.state WHERE slot=1' + (' FOR UPDATE' if for_update else '')).format(
            sql.Identifier(schemas.meta)))
        return cur.fetchone()


def require_state(conn, schemas: Schemas) -> dict:
    state = fetch_state(conn, schemas)
    if state is None:
        raise PipelineError('No Phase 4 bootstrap. Run run_phase4.cmd --bootstrap first.')
    if state['code_fingerprint'] != code_fingerprint():
        raise PipelineError('Phase 4 loader code changed since bootstrap. Do not overwrite or reset the data; review the change first.')
    return state


def current_quality(conn, schemas: Schemas) -> dict:
    from psycopg import sql
    s = sql.Identifier(schemas.landing)
    counts = {t: conn.execute(sql.SQL('SELECT count(*) FROM {}.{}').format(s, sql.Identifier(t))).fetchone()[0]
              for t in TABLES}
    totals = conn.execute(sql.SQL('''SELECT coalesce(sum(order_total),0),
        coalesce(sum(order_total) FILTER (WHERE status='delivered'),0) FROM {}.orders''').format(s)).fetchone()
    return {'status': 'passed', 'row_counts': counts,
            **{name: format(amount, '.2f') for name, amount in zip(AMOUNTS, totals)}}


def validate_current(conn, schemas: Schemas, state: dict) -> None:
    from psycopg import sql
    validate_landing(conn, schemas, {'quality': state['quality']}, state['row_digests'])
    count, first, last = conn.execute(sql.SQL(
        'SELECT count(*),min(change_seq),max(change_seq) FROM {}.events').format(sql.Identifier(schemas.meta))).fetchone()
    expected = state['last_change_seq']
    if count != expected or (expected and (first != 1 or last != expected)):
        raise PipelineError('Applied-event journal is not contiguous through the saved watermark.')


def assert_baseline_unchanged(conn, state: dict) -> dict:
    baseline = Schemas(**state['baseline_schemas'])
    saved = phase3_state(conn, baseline)
    if saved is None or saved['status'] != 'complete':
        raise PipelineError('The original Phase 3 baseline is missing or incomplete.')
    if (saved['source_manifest_sha256'] != state['baseline_manifest_sha256'] or
            saved['batch_id'] != state['baseline_batch_id'] or
            saved['row_digests'] != state['baseline_row_digests']):
        raise PipelineError('The Phase 3 baseline identity changed.')
    validate_landing(conn, baseline, saved['source_manifest'], state['baseline_row_digests'])
    return {'status': 'unchanged', 'batch_id': saved['batch_id'],
            'row_counts': saved['source_manifest']['quality']['row_counts']}


def _create_tables(conn, schemas: Schemas) -> None:
    from psycopg import sql
    for name in asdict(schemas).values():
        if conn.execute('SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=%s)', (name,)).fetchone()[0]:
            raise PipelineError(f'Schema {name} already exists without valid Phase 4 state. Nothing overwritten.')
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(name)))
    extra = {
        'products': ['CHECK (product_id>0)', 'CHECK (catalog_price>=0)'],
        'customers': ['CHECK (updated_at>=created_at)'],
        'orders': ["CHECK (currency='USD')", 'CHECK (order_total>=0)', 'CHECK (updated_at>=order_date)',
                   "CHECK (status IN ('placed','shipped','delivered','cancelled'))", 'CHECK (order_month BETWEEN 1 AND 12)'],
        'order_items': ['UNIQUE (order_id,line_number)', 'CHECK (quantity>0 AND line_number>0)',
                        'CHECK (unit_price>=0 AND line_amount=quantity*unit_price)', "CHECK (currency='USD')",
                        'CHECK (updated_at>=order_date)', 'CHECK (order_month BETWEEN 1 AND 12)',
                        "CHECK (order_status IN ('placed','shipped','delivered','cancelled'))"]}
    refs = {'orders': [('customer_id','customers','customer_id')],
            'order_items': [('order_id','orders','order_id'), ('customer_id','customers','customer_id'),
                            ('product_id','products','product_id')]}
    for table, columns in CONTRACTS.items():
        fields = [sql.SQL('{} {} NOT NULL').format(sql.Identifier(c),sql.SQL(pg)) for c, _, pg in columns]
        fields.append(sql.SQL('PRIMARY KEY ({})').format(sql.Identifier(KEYS[table])))
        fields += [sql.SQL('CHECK (length(btrim({}))>0)').format(sql.Identifier(c)) for c, kind, _ in columns if kind=='string']
        fields += [sql.SQL(x) for x in extra[table]]
        for c, parent, key in refs.get(table, []):
            fields.append(sql.SQL('FOREIGN KEY ({}) REFERENCES {}.{} ({})').format(
                sql.Identifier(c),sql.Identifier(schemas.landing),sql.Identifier(parent),sql.Identifier(key)))
        conn.execute(sql.SQL('CREATE TABLE {}.{} ({})').format(sql.Identifier(schemas.landing),
                     sql.Identifier(table),sql.SQL(', ').join(fields)))
    conn.execute(sql.SQL('CREATE INDEX order_items_order_idx ON {}.order_items (order_id)').format(sql.Identifier(schemas.landing)))
    m = sql.Identifier(schemas.meta)
    conn.execute(sql.SQL('''CREATE TABLE {}.state (
        slot integer PRIMARY KEY CHECK(slot=1), baseline_batch_id text NOT NULL,
        baseline_manifest_sha256 text NOT NULL, baseline_schemas jsonb NOT NULL,
        baseline_row_digests jsonb NOT NULL, source_provenance jsonb NOT NULL,
        code_fingerprint text NOT NULL, last_change_seq bigint NOT NULL CHECK(last_change_seq>=0),
        models_for_seq bigint, model_fingerprint text,
        status text NOT NULL CHECK(status IN ('loaded','complete')),
        row_digests jsonb NOT NULL, quality jsonb NOT NULL,
        bootstrapped_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
    )''').format(m))
    conn.execute(sql.SQL('''CREATE TABLE {}.batches (
        batch_id text PRIMARY KEY, feed_sha256 text NOT NULL,
        first_seq bigint NOT NULL, last_seq bigint NOT NULL,
        stats jsonb NOT NULL, applied_at timestamptz NOT NULL DEFAULT now(),
        CHECK(first_seq>0 AND last_seq>=first_seq)
    )''').format(m))
    conn.execute(sql.SQL('''CREATE TABLE {}.events (
        change_seq bigint PRIMARY KEY, event_id text NOT NULL UNIQUE,
        batch_id text NOT NULL REFERENCES {}.batches(batch_id),
        payload_sha256 text NOT NULL, order_id text NOT NULL,
        source_updated_at timestamptz NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
    )''').format(m,m))


def bootstrap_storage(conn, baseline: Schemas, target: Schemas, batch_id: str) -> tuple[str, dict]:
    from psycopg import sql
    from psycopg.types.json import Jsonb
    ensure_disjoint(baseline, target)
    with conn.transaction():
        existing = fetch_state(conn, target)
        if existing is not None:
            existing = require_state(conn, target)
            if existing['baseline_batch_id'] != batch_id or existing['baseline_schemas'] != asdict(baseline):
                raise PipelineError('Phase 4 already belongs to a different baseline. Nothing overwritten.')
            assert_baseline_unchanged(conn, existing)
            validate_current(conn, target, existing)
            return 'already_bootstrapped', existing
        saved = phase3_state(conn, baseline)
        if saved is None or saved['status'] != 'complete' or saved['batch_id'] != batch_id:
            raise PipelineError('A completed Phase 3 baseline with the requested batch ID is required.')
        if saved['loader_fingerprint'] != phase3_fingerprint('loader') or saved['model_fingerprint'] != phase3_fingerprint('models'):
            raise PipelineError('Baseline code differs from its completed Phase 3 run. Verify Phase 3 first.')
        validate_landing(conn, baseline, saved['source_manifest'], saved['row_digests'])
        analytics_summary(conn, baseline, saved['source_manifest'])
        _create_tables(conn, target)
        for table, columns in CONTRACTS.items():
            names = sql.SQL(', ').join(sql.Identifier(c) for c, _, _ in columns)
            conn.execute(sql.SQL('INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}').format(
                sql.Identifier(target.landing),sql.Identifier(table),names,names,
                sql.Identifier(baseline.landing),sql.Identifier(table)))
        validate_landing(conn, target, saved['source_manifest'], saved['row_digests'])
        conn.execute(sql.SQL('''INSERT INTO {}.state
            (slot,baseline_batch_id,baseline_manifest_sha256,baseline_schemas,baseline_row_digests,
             source_provenance,code_fingerprint,last_change_seq,status,row_digests,quality)
            VALUES(1,%s,%s,%s,%s,%s,%s,0,'loaded',%s,%s)''').format(sql.Identifier(target.meta)),
            (batch_id,saved['source_manifest_sha256'],Jsonb(asdict(baseline)),Jsonb(saved['row_digests']),
             Jsonb(saved['source_manifest'].get('data_provenance',{})),code_fingerprint(),Jsonb(saved['row_digests']),
             Jsonb(saved['source_manifest']['quality'])))
        return 'bootstrapped', fetch_state(conn,target)


def _rows(conn, schemas: Schemas, table: str, condition: str='', params=()) -> list[dict]:
    from psycopg import sql
    from psycopg.rows import dict_row
    columns = sql.SQL(', ').join(sql.Identifier(c) for c, _, _ in CONTRACTS[table])
    # condition is INTERNAL fixed SQL, never a CLI/user value.
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql.SQL('SELECT {} FROM {}.{} ' + condition).format(columns,
            sql.Identifier(schemas.landing),sql.Identifier(table)),params)
        return cur.fetchall()


def get_batch(conn, schemas: Schemas, batch_id: str) -> dict | None:
    from psycopg import sql
    from psycopg.rows import dict_row
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql.SQL('SELECT * FROM {}.batches WHERE batch_id=%s').format(sql.Identifier(schemas.meta)),(batch_id,))
        return cur.fetchone()


def verify_applied_feed(conn, schemas: Schemas, feed: dict, events: list[dict], feed_sha: str) -> dict:
    from psycopg import sql
    saved = get_batch(conn, schemas, feed['batch_id'])
    if saved is None:
        raise PipelineError('This feed has not been applied. Run the ordinary delta command first.')
    if (saved['feed_sha256'] != feed_sha or saved['first_seq'] != feed['first_seq'] or saved['last_seq'] != feed['last_seq']):
        raise PipelineError('Batch ID reused for a different feed. No overwrite or replay permitted.')
    actual = conn.execute(sql.SQL('''SELECT change_seq,event_id,payload_sha256 FROM {}.events
        WHERE batch_id=%s ORDER BY change_seq''').format(sql.Identifier(schemas.meta)),(feed['batch_id'],)).fetchall()
    expected = [(e['change_seq'],e['event_id'],event_digest(e)) for e in events]
    if actual != expected:
        raise PipelineError('The applied-event journal differs from the immutable source feed.')
    return saved


def _upsert(conn, schemas: Schemas, table: str, row: dict) -> None:
    from psycopg import sql
    cols = [c for c, _, _ in CONTRACTS[table]]
    key = KEYS[table]
    updates = sql.SQL(', ').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(c),sql.Identifier(c))
                                for c in cols if c != key)
    query = sql.SQL('''INSERT INTO {}.{} AS target ({}) VALUES ({})
        ON CONFLICT ({}) DO UPDATE SET {}
        WHERE EXCLUDED.updated_at > target.updated_at RETURNING {}''').format(
        sql.Identifier(schemas.landing),sql.Identifier(table),sql.SQL(', ').join(map(sql.Identifier,cols)),
        sql.SQL(', ').join(sql.Placeholder() for _ in cols),sql.Identifier(key),updates,sql.Identifier(key))
    if conn.execute(query,tuple(row[c] for c in cols)).fetchone() is None:
        raise PipelineError('Upsert rejected a stale/equal source version; transaction rolled back.')


def apply_feed(conn, schemas: Schemas, feed: dict, events: list[dict], feed_sha: str,
               fail_after_events: int | None=None) -> tuple[str, dict]:
    """Storage transaction only. Caller builds/tests dbt separately after commit.

    fail_after_events is a test-only failure injection, not exposed in the CLI.
    """
    from psycopg import sql
    from psycopg.types.json import Jsonb
    with conn.transaction():
        state = require_state(conn, schemas)
        state = fetch_state(conn, schemas, for_update=True)
        validate_current(conn, schemas, state)
        saved = get_batch(conn, schemas, feed['batch_id'])
        if saved is not None:
            saved = verify_applied_feed(conn, schemas, feed, events, feed_sha)
            if state['last_change_seq'] < saved['last_seq']:
                raise PipelineError('Watermark is behind its own committed event journal.')
            return 'already_applied', saved['stats']
        if (feed['baseline_manifest_sha256'] != state['baseline_manifest_sha256'] or
                feed['parent_last_seq'] != state['last_change_seq'] or
                feed['parent_row_digests'] != state['row_digests']):
            raise PipelineError('Feed is not based on the current committed state/watermark; gap or stale parent.')
        if (not events or feed['first_seq'] != state['last_change_seq'] + 1 or
                [e['change_seq'] for e in events] != list(range(feed['first_seq'],feed['last_seq']+1))):
            raise PipelineError('Missing or noncontiguous events. Watermark not advanced.')
        before = state['quality']['row_counts']
        stats = dict(events_applied=len(events),orders_inserted=0,orders_updated=0,
                     order_items_inserted=0,order_items_updated=0,watermark_before=state['last_change_seq'],
                     watermark_after=feed['last_seq'],row_counts_before=before)
        m = sql.Identifier(schemas.meta)
        conn.execute(sql.SQL('''INSERT INTO {}.batches (batch_id,feed_sha256,first_seq,last_seq,stats)
            VALUES(%s,%s,%s,%s,%s)''').format(m),
            (feed['batch_id'],feed_sha,feed['first_seq'],feed['last_seq'],Jsonb({})))
        for n,event in enumerate(events,1):
            order = event['order']
            old = _rows(conn,schemas,'orders','WHERE order_id=%s',(order['order_id'],))
            old_items = _rows(conn,schemas,'order_items','WHERE order_id=%s ORDER BY line_number',(order['order_id'],))
            check_update(event,old[0] if old else None,old_items)
            ids = [i['order_item_id'] for i in event['items']]
            collisions = conn.execute(sql.SQL('''SELECT order_item_id FROM {}.order_items
                WHERE order_item_id=ANY(%s) AND order_id<>%s LIMIT 1''').format(sql.Identifier(schemas.landing)),
                (ids,order['order_id'])).fetchone()
            if collisions:
                raise PipelineError('Order-item ID collides with another order.')
            _upsert(conn,schemas,'orders',order)
            for item in event['items']:
                _upsert(conn,schemas,'order_items',item)
            suffix = 'updated' if old else 'inserted'
            stats['orders_'+suffix] += 1
            stats['order_items_'+suffix] += len(event['items'])
            conn.execute(sql.SQL('''INSERT INTO {}.events
                (change_seq,event_id,batch_id,payload_sha256,order_id,source_updated_at)
                VALUES(%s,%s,%s,%s,%s,%s)''').format(m),
                (event['change_seq'],event['event_id'],feed['batch_id'],event_digest(event),order['order_id'],order['updated_at']))
            if fail_after_events is not None and n == fail_after_events:
                raise PipelineError(f'Injected test failure after {n} event(s); all changes must roll back.')
        quality = current_quality(conn,schemas)
        expected = dict(before,orders=before['orders']+stats['orders_inserted'],
                        order_items=before['order_items']+stats['order_items_inserted'])
        if quality['row_counts'] != expected:
            raise PipelineError('Unexpected row-count movement in incremental load.')
        digests = database_digests(conn,schemas)
        validate_landing(conn,schemas,{'quality':quality},digests)
        stats['row_counts_after'] = quality['row_counts']
        conn.execute(sql.SQL('UPDATE {}.batches SET stats=%s WHERE batch_id=%s').format(m),
                     (Jsonb(stats),feed['batch_id']))
        conn.execute(sql.SQL('''UPDATE {}.state SET last_change_seq=%s,row_digests=%s,quality=%s,
            status='loaded',updated_at=now() WHERE slot=1''').format(m),(feed['last_seq'],Jsonb(digests),Jsonb(quality)))
        return 'applied',stats


def run_dbt(command: str, schemas: Schemas, reports: Path, logs: Path) -> dict:
    executable = shutil.which('dbt')
    if not executable:
        raise PipelineError('Use the existing Phase 3 Docker runner; dbt is missing here.')
    run_id = time.strftime('%Y%m%d_%H%M%S')+'_'+uuid4().hex[:8]
    target = (reports/'phase4/dbt'/run_id).resolve()
    log_dir = (logs/'phase4/dbt'/run_id).resolve()
    target.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    env = dict(os.environ,DBT_SCHEMA=schemas.analytics,DBT_TARGET_PATH=str(target),DBT_LOG_PATH=str(log_dir),
               DBT_PARTIAL_PARSE='false',DBT_USE_COLORS='false',DBT_SEND_ANONYMOUS_USAGE_STATS='false',
               PGOPTIONS='-c timezone=UTC')
    project = PROJECT_ROOT/'dbt/ecommerce'
    args = [executable,command,'--project-dir',str(project),'--profiles-dir',str(project),
            '--vars',json.dumps({'landing_schema':schemas.landing})]
    LOG.info('dbt %s into %s; artifacts: %s',command,schemas.analytics,target)
    done = subprocess.run(args,env=env,cwd=project,timeout=900,check=False)
    if done.returncode:
        raise PipelineError(f'dbt {command} failed. Landing commit is preserved; models are not marked complete. Logs: {log_dir}')
    result = assess_dbt_results(read_json(target/'run_results.json'),read_json(target/'manifest.json'),command)
    return dict(result,run_id=run_id,artifacts_directory=str(target),logs_directory=str(log_dir),
                materialization_scope='Landing is incremental; dbt table models rebuild in full after a new delta.')


def sync_models(conn, schemas: Schemas, reports: Path, logs: Path, verify_only=False,
                rebuild_models=False) -> tuple[dict, dict]:
    from psycopg import sql
    state = require_state(conn,schemas)
    model_hash = phase3_fingerprint('models')
    ready = (state['status']=='complete' and state['models_for_seq']==state['last_change_seq']
             and state['model_fingerprint']==model_hash)
    if verify_only and not ready:
        raise PipelineError('Models are behind the committed watermark. Rerun the ordinary bootstrap/delta command first.')
    if state['model_fingerprint'] not in (None,model_hash) and not rebuild_models:
        raise PipelineError('dbt model files changed. Review them, then run --rebuild-models.')
    command = 'test' if ready and not rebuild_models else 'build'
    if command == 'build':
        conn.execute(sql.SQL("UPDATE {}.state SET status='loaded' WHERE slot=1").format(sql.Identifier(schemas.meta)))
    dbt = run_dbt(command,schemas,reports,logs)
    with conn.transaction():
        latest = require_state(conn,schemas)
        if latest['last_change_seq'] != state['last_change_seq']:
            raise PipelineError('Watermark changed during model building. No completion marker written.')
        validate_current(conn,schemas,latest)
        metrics = analytics_summary(conn,schemas,{'quality':latest['quality']})
        if command == 'build':
            conn.execute(sql.SQL('''UPDATE {}.state SET status='complete',models_for_seq=last_change_seq,
                model_fingerprint=%s,updated_at=now() WHERE slot=1''').format(sql.Identifier(schemas.meta)),(model_hash,))
    return dbt,metrics
