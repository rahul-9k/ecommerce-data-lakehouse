"""Create/read immutable local change feeds. No API polling or log-based CDC."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from uuid import uuid4

from src.common import PipelineError, inventory, sha256_file, validate_batch_id, verify_snapshot, write_json
from src.incremental.contracts import canonical, make_demo_events, validate_events
from src.incremental.warehouse import _rows, require_state, validate_current
from src.load.warehouse_contracts import Schemas


def read_feed(changes: Path, batch_id: str) -> tuple[dict, list[dict], str]:
    directory = changes / validate_batch_id(batch_id)
    manifest = verify_snapshot(directory)
    if (manifest.get('phase') != 4 or manifest.get('stage') != 'simulated_order_change_feed'
            or manifest.get('batch_id') != batch_id):
        raise PipelineError('Expected a Phase 4 simulated order-change feed with matching batch ID.')
    path = directory/'events.jsonl'
    if path.stat().st_size > 64*1024*1024:
        raise PipelineError('Demo feeds are limited to 64 MiB. This is not a large-source ingestion connector.')
    try:
        with path.open(encoding='utf-8') as handle:
            values = [json.loads(line) for line in handle if line.strip()]
        events = validate_events(values,batch_id,manifest['parent_last_seq'])
    except (KeyError, ValueError, TypeError) as exc:
        raise PipelineError('Malformed change-feed data/manifest.') from exc
    if (manifest.get('event_count') != len(events) or manifest.get('first_seq') != events[0]['change_seq']
            or manifest.get('last_seq') != events[-1]['change_seq']):
        raise PipelineError('Feed counts or sequence boundaries differ from its contents.')
    return manifest,events,sha256_file(directory/'manifest.json')


def write_feed(changes: Path, batch_id: str, state: dict, events: list[dict], parameters: dict) -> dict:
    validate_batch_id(batch_id)
    events = validate_events(events,batch_id,state['last_change_seq'])
    final = changes/batch_id
    if final.exists():
        raise PipelineError('Feed directory already exists; refusing to overwrite it.')
    changes.mkdir(parents=True,exist_ok=True)
    temporary = changes/('.building_'+batch_id+'_'+uuid4().hex[:8])
    temporary.mkdir()
    try:
        with (temporary/'events.jsonl').open('w',encoding='utf-8',newline='\n') as handle:
            for event in events:
                handle.write(canonical(event)+'\n')
        manifest = dict(format_version=1,status='success',phase=4,stage='simulated_order_change_feed',
            batch_id=batch_id,baseline_manifest_sha256=state['baseline_manifest_sha256'],
            parent_last_seq=state['last_change_seq'],parent_row_digests=state['row_digests'],
            first_seq=events[0]['change_seq'],last_seq=events[-1]['change_seq'],event_count=len(events),
            parameters=parameters,created_at=datetime.now(timezone.utc).isoformat(),
            change_kinds={kind:sum(e['kind']==kind for e in events) for kind in ('insert','status_update','quantity_correction')},
            source_provenance='Deterministic synthetic order changes based on the local snapshot; not real transactions or database-log CDC.',
            sequence_contract='Source-assigned contiguous change_seq; immutable complete order envelopes; one change per order per batch.',
            files=inventory(temporary))
        write_json(temporary/'manifest.json',manifest)
        temporary.rename(final)
    finally:
        if temporary.exists(): shutil.rmtree(temporary)
    return manifest


def generate_demo(conn, schemas: Schemas, changes: Path, batch_id: str,
                  new_orders: int=100, status_updates: int=20, quantity_corrections: int=10) -> dict:
    from psycopg import sql
    state = require_state(conn,schemas)
    parameters = dict(new_orders=new_orders,status_updates=status_updates,quantity_corrections=quantity_corrections)
    if (changes/validate_batch_id(batch_id)).exists():
        manifest,_,_ = read_feed(changes,batch_id)
        if manifest['parameters'] != parameters or manifest['baseline_manifest_sha256'] != state['baseline_manifest_sha256']:
            raise PipelineError('Existing feed has different parameters/baseline; nothing overwritten.')
        return dict(action='already_generated',manifest=manifest)
    if state['status'] != 'complete' or state['models_for_seq'] != state['last_change_seq']:
        raise PipelineError('Finish bootstrap/the previous delta model build before generating the next feed.')
    for name,value in parameters.items():
        if type(value) is not int or not 0 <= value <= 1000:
            raise PipelineError(f'{name} must be between 0 and 1000.')
    with conn.transaction():
        validate_current(conn,schemas,state)
        products = _rows(conn,schemas,'products','ORDER BY product_id')
        customers = _rows(conn,schemas,'customers','ORDER BY customer_id')
        updates = _rows(conn,schemas,'orders',"WHERE status IN ('placed','shipped') ORDER BY order_id LIMIT %s",(status_updates,))
        corrections = _rows(conn,schemas,'orders','WHERE order_id <> ALL(%s::text[]) ORDER BY order_id LIMIT %s',
                            ([r['order_id'] for r in updates],quantity_corrections))
        if len(updates) != status_updates or len(corrections) != quantity_corrections:
            raise PipelineError('Not enough distinct eligible orders for the requested demo changes. Reduce the update counts.')
        items = {r['order_id']:_rows(conn,schemas,'order_items','WHERE order_id=%s ORDER BY line_number',(r['order_id'],))
                 for r in updates+corrections}
        source_time = conn.execute(sql.SQL('''SELECT greatest(
            (SELECT max(updated_at) FROM {s}.orders),
            (SELECT max(updated_at) FROM {s}.order_items),
            (SELECT max(created_at) FROM {s}.customers))''').format(s=sql.Identifier(schemas.landing))).fetchone()[0]
        events = make_demo_events(batch_id,state['last_change_seq'],products,customers,updates,corrections,items,
                                  new_orders,source_time,datetime.now(timezone.utc))
        # Reject accidental ID collision before writing a completed source file.
        new_ids = [e['order']['order_id'] for e in events if e['kind']=='insert']
        if conn.execute(sql.SQL('SELECT order_id FROM {}.orders WHERE order_id=ANY(%s) LIMIT 1').format(
                sql.Identifier(schemas.landing)),(new_ids,)).fetchone():
            raise PipelineError('Generated order IDs already exist. Choose a new batch ID.')
        manifest = write_feed(changes,batch_id,state,events,parameters)
    return dict(action='generated',manifest=manifest)
