"""Dependency-free Phase 4 contracts and feed integrity tests."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from phase4_fixture import demo_rows, demo_events
from src.common import PipelineError
from src.incremental.contracts import (canonical,check_update,ensure_disjoint,event_digest,parse_event,parse_row,
                                      timestamp,validate_events,INCREMENTAL_SCHEMAS)
from src.incremental.feed import read_feed, write_feed
from src.load.warehouse_contracts import Schemas


class Phase4PureTests(unittest.TestCase):
    def setUp(self):
        self.events=demo_events()
        self.rows=demo_rows()

    def bad(self,event,pattern=None):
        with self.assertRaisesRegex(PipelineError,pattern or '.'):
            parse_event(event,'delta_test')

    def test_01_demo_has_expected_event_grain(self):
        self.assertEqual([e['kind'] for e in self.events],['status_update','quantity_correction','insert','insert'])
        self.assertEqual([e['change_seq'] for e in self.events],[1,2,3,4])
        self.assertEqual(sum(len(e['items']) for e in self.events),8)

    def test_02_tied_timestamps_are_distinguished_by_sequence(self):
        self.assertEqual(len({e['order']['updated_at'] for e in self.events}),1)
        self.assertEqual(len(validate_events(self.events,'delta_test',0)),4)

    def test_03_serialization_roundtrip_exact_decimal(self):
        restored=parse_event(json.loads(canonical(self.events[2])),'delta_test')
        self.assertEqual(restored,self.events[2])
        self.assertEqual(restored['order']['order_total'],Decimal('15.00'))

    def test_04_missing_column_rejected(self):
        event=deepcopy(self.events[0]); del event['order']['currency']; self.bad(event,'fields differ')

    def test_05_extra_column_rejected(self):
        event=deepcopy(self.events[0]); event['items'][0]['extra']=1; self.bad(event,'fields differ')

    def test_06_float_money_rejected(self):
        event=deepcopy(self.events[0]); event['items'][0]['unit_price']=10.0; self.bad(event,'decimal strings')

    def test_07_nan_and_extra_precision_rejected(self):
        for amount in ('NaN','Infinity','1.001','-1.00'):
            event=deepcopy(self.events[0]); event['order']['order_total']=amount
            with self.subTest(amount=amount): self.bad(event,'two-decimal')

    def test_08_naive_timestamp_rejected(self):
        with self.assertRaises(PipelineError): timestamp('2026-01-01T00:00:00')

    def test_09_timezone_normalized_to_utc(self):
        self.assertEqual(timestamp('2026-01-01T05:30:00+05:30'),timestamp('2026-01-01T00:00:00Z'))

    def test_10_bad_integer_and_quantity_rejected(self):
        for value in (True,'1',0):
            event=deepcopy(self.events[0]); event['items'][0]['quantity']=value
            with self.subTest(value=value): self.bad(event)

    def test_11_order_total_mismatch_rejected(self):
        event=deepcopy(self.events[0]); event['order']['order_total']+=Decimal('1.00'); self.bad(event,'Order total')

    def test_12_line_amount_mismatch_rejected(self):
        event=deepcopy(self.events[0]); event['items'][0]['line_amount']+=Decimal('1.00'); self.bad(event,'line amount')

    def test_13_order_context_mismatch_rejected(self):
        event=deepcopy(self.events[0]); event['items'][0]['customer_id']='WRONG'; self.bad(event,'context')

    def test_14_duplicate_item_key_rejected(self):
        event=deepcopy(self.events[0]); event['items'][1]['order_item_id']=event['items'][0]['order_item_id']; self.bad(event,'Duplicate')

    def test_15_duplicate_line_number_rejected(self):
        event=deepcopy(self.events[0]); event['items'][1]['line_number']=1; self.bad(event,'line numbers')

    def test_16_wrong_batch_provenance_rejected(self):
        event=deepcopy(self.events[0]); event['order']['_batch_id']='wrong'; self.bad(event,'provenance')

    def test_17_sequence_gap_and_duplicate_rejected(self):
        for values in (self.events[1:],self.events[:1]+self.events,self.events[::-1]):
            with self.subTest(size=len(values)), self.assertRaises(PipelineError): validate_events(values,'delta_test',0)

    def test_18_second_change_same_order_rejected(self):
        events=deepcopy(self.events); events[1]['order']=deepcopy(events[0]['order']); events[1]['items']=deepcopy(events[0]['items'])
        with self.assertRaisesRegex(PipelineError,'one change per order'): validate_events(events,'delta_test',0)

    def test_19_sequence_cannot_be_boolean(self):
        event=deepcopy(self.events[0]); event['change_seq']=True; self.bad(event,'positive')

    def test_20_stale_and_equal_versions_rejected(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        for t in (old['updated_at'],old['updated_at']-timedelta(days=1)):
            event=deepcopy(self.events[0]); event['order']['updated_at']=t
            with self.subTest(t=t),self.assertRaisesRegex(PipelineError,'Stale'): check_update(event,old,items)

    def test_21_complete_newer_update_allowed(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        check_update(self.events[0],old,items)

    def test_22_missing_existing_line_rejected(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        event=deepcopy(self.events[0]); event['items'].pop()
        with self.assertRaisesRegex(PipelineError,'cannot be omitted'): check_update(event,old,items)

    def test_23_immutable_header_key_rejected(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        event=deepcopy(self.events[0]); event['order']['customer_id']='OTHER'
        with self.assertRaisesRegex(PipelineError,'customer_id'): check_update(event,old,items)

    def test_24_product_rekey_rejected(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        event=deepcopy(self.events[0]); event['items'][0]['product_id']=999
        with self.assertRaisesRegex(PipelineError,'product_id'): check_update(event,old,items)

    def test_25_wrong_insert_update_kind_rejected(self):
        with self.assertRaises(PipelineError): check_update(self.events[0],None,[])
        with self.assertRaises(PipelineError): check_update(self.events[2],self.rows['orders'][0],[])

    def test_26_baseline_and_target_must_be_disjoint(self):
        ensure_disjoint(Schemas(),INCREMENTAL_SCHEMAS)
        with self.assertRaises(PipelineError): ensure_disjoint(Schemas(),Schemas('landing','other','meta2'))

    def test_27_event_digest_tracks_changes(self):
        changed=deepcopy(self.events[2]); changed['order']['status']='shipped'
        self.assertNotEqual(event_digest(changed),event_digest(self.events[2]))
        self.assertEqual(event_digest(self.events[2]),event_digest(json.loads(canonical(self.events[2]))))

    def test_28_feed_file_roundtrip_and_no_overwrite(self):
        state=dict(last_change_seq=0,baseline_manifest_sha256='a'*64,row_digests={'fixture':{'rows':8}})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            write_feed(root,'delta_test',state,self.events,{})
            manifest,events,digest=read_feed(root,'delta_test')
            self.assertEqual(events,self.events); self.assertEqual(manifest['event_count'],4); self.assertEqual(len(digest),64)
            with self.assertRaisesRegex(PipelineError,'overwrite'): write_feed(root,'delta_test',state,self.events,{})

    def test_29_feed_tampering_detected(self):
        state=dict(last_change_seq=0,baseline_manifest_sha256='a'*64,row_digests={})
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); write_feed(root,'delta_test',state,self.events,{})
            with (root/'delta_test/events.jsonl').open('a') as handle: handle.write('{}\n')
            with self.assertRaisesRegex(PipelineError,'changed after publication'): read_feed(root,'delta_test')

    def test_30_invalid_batch_path_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(PipelineError): read_feed(Path(tmp),'../escape')

    def test_31_old_order_date_with_new_change_version_is_valid(self):
        event=self.events[0]
        self.assertLess(event['order']['order_date'],event['order']['updated_at'])
        self.assertEqual(parse_event(event,'delta_test'),event)

    def test_32_status_update_cannot_also_change_quantity(self):
        old=self.rows['orders'][0]; items=[i for i in self.rows['order_items'] if i['order_id']==old['order_id']]
        event=deepcopy(self.events[0]); event['items'][0]['quantity']+=1
        with self.assertRaisesRegex(PipelineError,'cannot change line'): check_update(event,old,items)


if __name__=='__main__': unittest.main()
