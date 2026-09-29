"""Real PostgreSQL/dbt tests. Enable through run_phase4.cmd --self-test only.

Only generated p4test_* schemas and temporary source files are used/removed.
"""
from copy import deepcopy
from decimal import Decimal
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from phase3_fixture import make_snapshot
from phase4_fixture import demo_events, set_demo_statuses
from src.common import PipelineError
from src.load.warehouse import connect, run_pipeline as phase3_pipeline
from src.load.warehouse_contracts import Schemas
from src.incremental.feed import read_feed,write_feed
from src.incremental.warehouse import (apply_feed,assert_baseline_unchanged,bootstrap_storage,current_quality,
    fetch_state,get_batch,validate_current,verify_applied_feed,warehouse_lock)
from src.run_phase4 import execute


@unittest.skipUnless(os.environ.get('PHASE4_RUN_INTEGRATION')=='1','Run the Docker Phase 4 self-test for real integration tests')
class Phase4IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from psycopg import sql
        cls.sql=sql
        cls.base_temp=tempfile.TemporaryDirectory()
        cls.base_root=Path(cls.base_temp.name)
        prefix='p4test_'+uuid4().hex[:10]
        cls.baseline=Schemas(prefix+'_base',prefix+'_basea',prefix+'_basem')
        cls.addClassCleanup(cls.clean_baseline)
        make_snapshot(cls.base_root/'data','baseline_fixture',change=set_demo_statuses)
        phase3_pipeline(cls.base_root/'data',cls.base_root/'reports','baseline_fixture',schemas=cls.baseline,
                        log_root=cls.base_root/'logs')

    @classmethod
    def clean_baseline(cls):
        try:
            with connect() as conn:
                for s in (cls.baseline.analytics,cls.baseline.landing,cls.baseline.meta):
                    if not s.startswith('p4test_'): raise RuntimeError('Unsafe test cleanup')
                    conn.execute(cls.sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(cls.sql.Identifier(s)))
        finally:
            cls.base_temp.cleanup()

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.conn=connect()
        prefix='p4test_'+uuid4().hex[:10]
        self.target=Schemas(prefix+'_inc',prefix+'_inca',prefix+'_incm')
        self.addCleanup(self.cleanup_target)
        _,self.state=bootstrap_storage(self.conn,self.baseline,self.target,'baseline_fixture')

    def cleanup_target(self):
        try:
            for s in (self.target.analytics,self.target.landing,self.target.meta):
                if not s.startswith('p4test_'): raise RuntimeError('Unsafe test cleanup')
                self.conn.execute(self.sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(self.sql.Identifier(s)))
        finally:
            self.conn.close(); self.temp.cleanup()

    def feed(self):
        write_feed(self.root/'changes','delta_test',self.state,demo_events(),{})
        return read_feed(self.root/'changes','delta_test')

    def apply(self,feed=None,**kw):
        return apply_feed(self.conn,self.target,*(feed or self.feed()),**kw)

    def state_now(self):
        return fetch_state(self.conn,self.target)

    def run_all(self,mode):
        return execute(mode,'delta_test',self.root/'reports',self.root/'logs',self.root/'changes',
            baseline_id='baseline_fixture',target=self.target,baseline=self.baseline,
            new_orders=2,status_updates=1,quantity_corrections=1)

    def assert_not_applied(self):
        now=self.state_now()
        self.assertEqual(now['last_change_seq'],0)
        self.assertEqual(now['row_digests'],self.state['row_digests'])
        self.assertIsNone(get_batch(self.conn,self.target,'delta_test'))
        with self.conn.transaction(): validate_current(self.conn,self.target,now)

    def test_01_bootstrap_copy_and_replay_preserve_baseline(self):
        action,state=bootstrap_storage(self.conn,self.baseline,self.target,'baseline_fixture')
        self.assertEqual(action,'already_bootstrapped')
        self.assertEqual(state['row_digests'],state['baseline_row_digests'])
        with self.conn.transaction():
            self.assertEqual(assert_baseline_unchanged(self.conn,state)['status'],'unchanged')

    def test_02_insert_update_and_financial_totals(self):
        action,stats=self.apply()
        self.assertEqual(action,'applied')
        self.assertEqual((stats['orders_inserted'],stats['orders_updated']),(2,2))
        self.assertEqual((stats['order_items_inserted'],stats['order_items_updated']),(4,4))
        q=current_quality(self.conn,self.target)
        self.assertEqual(q['row_counts']['orders'],4)
        self.assertEqual(q['row_counts']['order_items'],8)
        self.assertEqual(q['gross_order_value_usd_all_statuses'],'80.00')
        self.assertEqual(q['delivered_order_value_usd'],'32.50')
        self.assertEqual(self.state_now()['last_change_seq'],4)

    def test_03_same_feed_replay_is_idempotent(self):
        feed=self.feed(); self.apply(feed)
        first=self.state_now()
        action,stats=self.apply(feed)
        self.assertEqual(action,'already_applied')
        self.assertEqual(first['row_digests'],self.state_now()['row_digests'])
        self.assertEqual(self.state_now()['last_change_seq'],4)
        self.assertEqual(stats['events_applied'],4)

    def test_04_failure_mid_batch_rolls_back_rows_journal_and_watermark(self):
        feed=self.feed()
        with self.assertRaisesRegex(PipelineError,'Injected test failure'):
            self.apply(feed,fail_after_events=2)
        self.assert_not_applied()
        self.assertEqual(self.apply(feed)[0],'applied')

    def test_05_orphan_reference_rolls_back_entire_delta(self):
        import psycopg
        feed,events,digest=self.feed()
        events[-1]['items'][0]['product_id']=999999
        with self.assertRaises(psycopg.IntegrityError): self.apply((feed,events,digest))
        self.assert_not_applied()

    def test_06_same_batch_different_manifest_rejected(self):
        feed=self.feed(); self.apply(feed)
        manifest,events,_=feed
        with self.assertRaisesRegex(PipelineError,'Batch ID reused'):
            self.apply((manifest,events,'f'*64))
        self.assertEqual(self.state_now()['last_change_seq'],4)

    def test_07_stale_parent_state_rejected(self):
        feed,events,digest=self.feed(); feed['parent_row_digests']={}
        with self.assertRaisesRegex(PipelineError,'current committed state'): self.apply((feed,events,digest))
        self.assert_not_applied()

    def test_08_sequence_gap_rejected(self):
        feed,events,digest=self.feed(); feed['first_seq']=2
        with self.assertRaisesRegex(PipelineError,'noncontiguous'): self.apply((feed,events,digest))
        self.assert_not_applied()

    def test_09_old_version_rejected_without_advancing_watermark(self):
        feed,events,digest=self.feed()
        old=self.conn.execute(self.sql.SQL("SELECT updated_at FROM {}.orders WHERE order_id='O1'").format(
            self.sql.Identifier(self.target.landing))).fetchone()[0]
        events[0]['order']['updated_at']=old
        for item in events[0]['items']: item['updated_at']=old
        with self.assertRaisesRegex(PipelineError,'Stale'): self.apply((feed,events,digest))
        self.assert_not_applied()

    def test_10_second_writer_cannot_take_pipeline_lock(self):
        with connect() as other, warehouse_lock(self.conn,self.target):
            with self.assertRaisesRegex(PipelineError,'owns this warehouse lock'):
                with warehouse_lock(other,self.target): pass
        with warehouse_lock(self.conn,self.target): pass

    def test_11_untracked_landing_edit_detected(self):
        feed=self.feed()
        self.conn.execute(self.sql.SQL("UPDATE {}.products SET product_name='untracked edit' WHERE product_id=1").format(
            self.sql.Identifier(self.target.landing)))
        with self.assertRaisesRegex(PipelineError,'row values differ'): self.apply(feed)
        self.assertEqual(self.state_now()['last_change_seq'],0)

    def test_12_journal_checkpoint_disagreement_detected(self):
        self.conn.execute(self.sql.SQL('UPDATE {}.state SET last_change_seq=1 WHERE slot=1').format(
            self.sql.Identifier(self.target.meta)))
        with self.conn.transaction(), self.assertRaisesRegex(PipelineError,'not contiguous'):
            validate_current(self.conn,self.target,self.state_now())

    def test_13_changed_event_journal_blocks_replay(self):
        feed=self.feed(); self.apply(feed)
        self.conn.execute(self.sql.SQL("UPDATE {}.events SET payload_sha256='altered' WHERE change_seq=1").format(
            self.sql.Identifier(self.target.meta)))
        with self.assertRaisesRegex(PipelineError,'journal differs'): self.apply(feed)

    def test_14_real_dbt_and_recovery_after_committed_load(self):
        self.assertEqual(self.run_all('bootstrap')['dbt']['models_built'],9)
        generated=self.run_all('generate')
        self.assertEqual(generated['feed']['event_count'],4)
        self.assertEqual(self.run_all('generate')['action'],'already_generated')
        # Inject failure between landing commit and dbt. This is NOT a simulated successful dbt run.
        with patch('src.incremental.warehouse.run_dbt',side_effect=PipelineError('Injected model-stage failure')):
            with self.assertRaisesRegex(PipelineError,'Injected model-stage'): self.run_all('apply')
        pending=self.state_now()
        self.assertEqual(pending['last_change_seq'],4)
        self.assertEqual(pending['status'],'loaded')
        recovered=self.run_all('apply')
        self.assertEqual(recovered['action'],'resumed_models')
        self.assertEqual(recovered['dbt']['models_built'],9)
        self.assertEqual(recovered['dbt']['tests_passed'],46)
        self.assertEqual(recovered['analytics']['row_counts']['fact_order_items'],8)
        self.assertEqual(recovered['analytics']['gross_order_value_usd_all_statuses'],'80.00')
        self.assertEqual(self.run_all('verify')['action'],'verified')
        replay=self.run_all('apply')
        self.assertEqual(replay['action'],'already_complete')
        self.assertEqual(replay['dbt']['command'],'test')
        self.assertEqual(replay['baseline']['status'],'unchanged')
        self.assertEqual(self.state_now()['row_digests'],pending['row_digests'])
        self.assertEqual(self.state_now()['models_for_seq'],4)


if __name__=='__main__': unittest.main()
