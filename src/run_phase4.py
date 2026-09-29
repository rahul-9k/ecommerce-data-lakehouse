"""Phase 4: bootstrap a separate warehouse, then apply a synthetic change feed."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import logging
import os
from pathlib import Path
import sys
import time
import traceback
import unittest
from uuid import uuid4

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from src.common import PROJECT_ROOT, PipelineError, utc_now, validate_batch_id, write_json
from src.load.warehouse import connect, preview
from src.load.warehouse_contracts import Schemas
from src.incremental.contracts import INCREMENTAL_SCHEMAS
from src.incremental.feed import generate_demo, read_feed
from src.incremental.warehouse import (apply_feed, assert_baseline_unchanged, bootstrap_storage, fetch_state,
    get_batch, require_state, sync_models, validate_current, verify_applied_feed, warehouse_lock)


def execute(mode: str, batch_id: str, reports: Path, logs: Path, changes: Path,
            baseline_id='initial_001', target: Schemas=INCREMENTAL_SCHEMAS, baseline: Schemas=Schemas(),
            new_orders=100, status_updates=20, quantity_corrections=10) -> dict:
    started = time.perf_counter()
    validate_batch_id(batch_id)
    with connect() as conn, warehouse_lock(conn,target):
        if mode == 'bootstrap':
            with warehouse_lock(conn,baseline):
                action,state = bootstrap_storage(conn,baseline,target,baseline_id)
            dbt,metrics = sync_models(conn,target,reports,logs)
            stats = None
        elif mode == 'generate':
            result = generate_demo(conn,target,changes,batch_id,new_orders,status_updates,quantity_corrections)
            state = require_state(conn,target)
            with conn.transaction():
                preserved = assert_baseline_unchanged(conn,state)
            return dict(status='success',phase=4,action=result['action'],batch_id=batch_id,
                feed=result['manifest'],feed_directory=str(changes/batch_id),baseline=preserved,
                scope='Only generated a synthetic source feed; no incremental warehouse load has run yet.')
        else:
            state = require_state(conn,target)
            if mode == 'rebuild':
                stats = None
                action = 'rebuilt_models'
                dbt,metrics = sync_models(conn,target,reports,logs,rebuild_models=True)
            else:
                feed,events,feed_sha = read_feed(changes,batch_id)
                if mode in ('verify','preview'):
                    with conn.transaction():
                        validate_current(conn,target,state)
                        saved = verify_applied_feed(conn,target,feed,events,feed_sha)
                        stats = saved['stats']
                    action = 'verified' if mode=='verify' else 'inspected'
                    dbt,metrics = sync_models(conn,target,reports,logs,verify_only=True)
                else:
                    saved = get_batch(conn,target,batch_id)
                    if saved is None and state['status'] != 'complete':
                        raise PipelineError('Finish the previous bootstrap/delta model build before applying another feed.')
                    load_action,stats = apply_feed(conn,target,feed,events,feed_sha)
                    dbt,metrics = sync_models(conn,target,reports,logs)
                    action = ('applied' if load_action=='applied' else
                              'already_complete' if dbt['command']=='test' else 'resumed_models')
        state = require_state(conn,target)
        with conn.transaction():
            preserved = assert_baseline_unchanged(conn,state)
            validate_current(conn,target,state)
        if mode == 'preview':
            preview(conn,target)
        return dict(status='success',phase=4,action=action,
            batch_id=baseline_id if mode=='bootstrap' else batch_id,schemas=asdict(target),baseline=preserved,
            watermark=dict(kind='source_change_sequence',last_change_seq=state['last_change_seq'],
                           models_for_seq=state['models_for_seq']),
            original_batch_effects=stats,landing_row_counts=state['quality']['row_counts'],analytics=metrics,
            dbt=dbt,quality_status='passed',completed_at=utc_now(),elapsed_seconds=round(time.perf_counter()-started,3),
            source_provenance=dict(baseline=state['source_provenance'],changes='Simulated synthetic order envelopes, not real sales or log-based CDC.'),
            scope='Incremental PostgreSQL landing upserts and transactional sequence checkpoint; dbt models rebuild in full. No incremental Spark/Parquet update, deletions, customer/product changes, SCD2, cloud or Airflow.')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-id',default='delta_001')
    parser.add_argument('--baseline-id',default='initial_001')
    parser.add_argument('--changes-dir',type=Path,default=PROJECT_ROOT/'data/changes')
    parser.add_argument('--reports-dir',type=Path,default=PROJECT_ROOT/'reports')
    parser.add_argument('--new-orders',type=int,default=100)
    parser.add_argument('--status-updates',type=int,default=20)
    parser.add_argument('--quantity-corrections',type=int,default=10)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--self-test',action='store_true')
    modes.add_argument('--bootstrap',action='store_true')
    modes.add_argument('--generate-demo',action='store_true')
    modes.add_argument('--verify-only',action='store_true')
    modes.add_argument('--preview',action='store_true')
    modes.add_argument('--rebuild-models',action='store_true')
    args = parser.parse_args()
    logs = PROJECT_ROOT/'logs'
    logs.mkdir(parents=True,exist_ok=True)
    log_path = logs/('phase4_'+time.strftime('%Y%m%d_%H%M%S')+'_'+uuid4().hex[:6]+'.log')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.StreamHandler(sys.stdout),logging.FileHandler(log_path,encoding='utf-8')])
    report = args.reports_dir/('phase4_selftest.json' if args.self_test else 'phase4_latest.json')
    try:
        if args.self_test:
            import psycopg  # noqa: F401
            import pyarrow  # noqa: F401
            os.environ['PHASE4_RUN_INTEGRATION']='1'
            suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT/'tests'),pattern='test_phase4_*.py')
            outcome = unittest.TextTestRunner(verbosity=2).run(suite)
            passed = outcome.wasSuccessful() and not outcome.skipped and outcome.testsRun>0
            write_json(report,dict(status='passed' if passed else 'failed',tests_run=outcome.testsRun,
                failures=len(outcome.failures),errors=len(outcome.errors),skipped=len(outcome.skipped),
                scope='Temporary synthetic feeds, p4test_* schemas, real PostgreSQL upserts/rollback and dbt builds. Does not validate initial_001/delta_001.'))
            return 0 if passed else 1
        mode = ('bootstrap' if args.bootstrap else 'generate' if args.generate_demo else 'verify' if args.verify_only
                else 'preview' if args.preview else 'rebuild' if args.rebuild_models else 'apply')
        result = execute(mode,args.batch_id,args.reports_dir,logs,args.changes_dir,baseline_id=args.baseline_id,
                         new_orders=args.new_orders,status_updates=args.status_updates,quantity_corrections=args.quantity_corrections)
        write_json(report,result)
        print('\nPHASE 4 COMPLETE\nAction:',result['action'])
        if 'feed' in result:
            print('Change events:',result['feed']['event_count'])
            print('Kinds:',result['feed']['change_kinds'])
            print('Source created only; run the ordinary delta command next.')
        else:
            print('Watermark:',result['watermark'])
            print('Landing:',result['landing_row_counts'])
            if result['original_batch_effects']:
                print('ORIGINAL batch effects (not new inserts on replay):',result['original_batch_effects'])
            print('dbt tests passed:',result['dbt']['tests_passed'])
            print('Quality: PASSED')
        print('Original Phase 3 baseline:',result['baseline']['status'])
        print('Report: reports/phase4_latest.json')
        return 0
    except KeyboardInterrupt:
        write_json(report,dict(status='interrupted',phase=4,batch_id=args.batch_id))
        logging.error('Interrupted. Do not reset the database. Rerun the same command to inspect/resume.')
        return 130
    except Exception as exc:
        logging.error('PHASE 4 FAILED: %s',exc)
        with log_path.open('a',encoding='utf-8') as handle: handle.write(traceback.format_exc())
        write_json(report,dict(status='failed',phase=4,batch_id=args.batch_id,error=str(exc),log=str(log_path)))
        return 1


if __name__=='__main__':
    raise SystemExit(main())
