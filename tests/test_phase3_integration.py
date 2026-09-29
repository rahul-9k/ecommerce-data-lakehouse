"""Real Parquet/PostgreSQL/dbt tests, enabled only by run_phase3.cmd --self-test.

Unique p3test_* schemas are removed after each test. User landing/analytics
schemas and initial_001 files are never the test target.
"""
from __future__ import annotations
from copy import deepcopy
from decimal import Decimal
import os
from pathlib import Path
import sys
import tempfile
import unittest
from uuid import uuid4
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from phase3_fixture import make_snapshot
from src.common import PipelineError
from src.load.warehouse_contracts import Schemas, read_processed
from src.load.warehouse import (connect, load_snapshot, fetch_state, parquet_rows,
                                run_pipeline, run_dbt)


@unittest.skipUnless(os.environ.get('PHASE3_RUN_INTEGRATION')=='1', 'Use the Docker Phase 3 self-test for database integration tests')
class Phase3IntegrationTests(unittest.TestCase):
    def setUp(self):
        from psycopg import sql
        self.sql=sql
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.data=self.root/'data'
        prefix='p3test_'+uuid4().hex[:10]
        self.schemas=Schemas(prefix+'_landing',prefix+'_analytics',prefix+'_meta')
        self.conn=connect()

    def tearDown(self):
        # Only generated test schemas, never public/landing/analytics/pipeline_meta.
        try:
            for schema in (self.schemas.analytics,self.schemas.landing,self.schemas.meta):
                if not schema.startswith('p3test_'):
                    raise RuntimeError('Refusing unsafe test cleanup')
                self.conn.execute(self.sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(self.sql.Identifier(schema)))
        finally:
            self.conn.close()
            self.temp.cleanup()

    def load(self, data=None):
        directory,manifest,digest=read_processed(data or self.data,'fixture_001')
        return load_snapshot(self.conn,directory,manifest,digest,self.schemas)

    def run_all(self, **kwargs):
        return run_pipeline(self.data,self.root/'reports','fixture_001',schemas=self.schemas,
                            log_root=self.root/'logs',**kwargs)

    def test_01_parquet_types_partitioning_decimal_and_int96(self):
        output=make_snapshot(self.data)
        items=list(parquet_rows(output,'order_items'))
        self.assertEqual(len(items),4)
        from src.load.warehouse_contracts import CONTRACTS
        columns=[c for c,_,_ in CONTRACTS['order_items']]
        item=dict(zip(columns,next(row for row in items if row[0]=='I3')))
        self.assertEqual(item['unit_price'],Decimal('1.25'))
        self.assertEqual(item['line_amount'],Decimal('3.75'))
        self.assertEqual(item['order_month'],2)
        self.assertIsNotNone(item['order_date'].tzinfo)

    def test_02_copy_roundtrip_replay_and_multiline_text(self):
        make_snapshot(self.data)
        action,state=self.load()
        self.assertEqual(action,'created')
        self.assertEqual(state['row_digests']['order_items']['rows'],4)
        self.assertEqual(self.load()[0],'already_loaded')
        value=self.conn.execute(self.sql.SQL("SELECT customer_name FROM {}.customers WHERE customer_id='C2'").format(
            self.sql.Identifier(self.schemas.landing))).fetchone()[0]
        self.assertEqual(value,'Demo\nTwo')

    def test_03_duplicate_key_rolls_back_entire_load(self):
        import psycopg
        make_snapshot(self.data,change=lambda rows:rows['products'].append(deepcopy(rows['products'][0])))
        with self.assertRaises(psycopg.IntegrityError): self.load()
        self.assertIsNone(fetch_state(self.conn,self.schemas))
        self.assertFalse(self.conn.execute('SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=%s)',
                                          (self.schemas.landing,)).fetchone()[0])

    def test_04_orphan_product_rolls_back(self):
        import psycopg
        make_snapshot(self.data,change=lambda rows:rows['order_items'][0].update(product_id=999))
        with self.assertRaises(psycopg.IntegrityError): self.load()
        self.assertIsNone(fetch_state(self.conn,self.schemas))

    def test_05_wrong_order_total_rolls_back(self):
        make_snapshot(self.data,change=lambda rows:rows['orders'][0].update(order_total=Decimal('26.00')))
        with self.assertRaisesRegex(PipelineError,'order totals'): self.load()
        self.assertIsNone(fetch_state(self.conn,self.schemas))

    def test_06_mutated_database_rejected_without_overwrite(self):
        make_snapshot(self.data)
        self.load()
        self.conn.execute(self.sql.SQL("UPDATE {}.products SET product_name='changed' WHERE product_id=1").format(
            self.sql.Identifier(self.schemas.landing)))
        with self.assertRaisesRegex(PipelineError,'row values differ'): self.load()

    def test_07_different_snapshot_rejected(self):
        make_snapshot(self.data)
        self.load()
        changed=self.root/'other_data'
        make_snapshot(changed,change=lambda rows:rows['products'][0].update(product_name='different'))
        with self.assertRaisesRegex(PipelineError,'different snapshot'): self.load(changed)
        self.assertEqual(self.load()[0],'already_loaded')

    def test_08_order_context_mismatch_rejected(self):
        make_snapshot(self.data,change=lambda rows:rows['order_items'][0].update(order_status='cancelled'))
        with self.assertRaisesRegex(PipelineError,'enriched order context'): self.load()

    def test_09_real_dbt_build_replay_failure_and_repair(self):
        make_snapshot(self.data)
        first=self.run_all()
        self.assertEqual(first['action'],'created')
        self.assertEqual(first['analytics']['row_counts']['fact_order_items'],4)
        self.assertEqual(first['analytics']['row_counts']['dim_date'],32)
        self.assertEqual(first['analytics']['gross_order_value_usd_all_statuses'],'31.25')
        self.assertEqual(first['analytics']['delivered_order_value_usd'],'25.00')
        self.assertEqual(first['dbt']['models_built'],9)
        self.assertGreater(first['dbt']['tests_passed'],20)
        replay=self.run_all()
        self.assertEqual(replay['action'],'already_complete')
        self.assertEqual(replay['dbt']['command'],'test')
        # Expected negative test: dbt must flag the broken line, not report success.
        self.conn.execute(self.sql.SQL("UPDATE {}.fact_order_items SET line_amount=line_amount+1 WHERE order_item_id='I1'").format(
            self.sql.Identifier(self.schemas.analytics)))
        with self.assertRaisesRegex(PipelineError,'dbt test failed'):
            self.run_all(verify_only=True)
        fixed=self.run_all(rebuild_models=True)
        self.assertEqual(fixed['action'],'rebuilt_models')
        self.assertEqual(fixed['analytics']['gross_order_value_usd_all_statuses'],'31.25')
        self.assertEqual(self.run_all(verify_only=True)['action'],'verified')


if __name__=='__main__': unittest.main()
