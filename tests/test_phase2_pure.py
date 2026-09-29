"""Phase 2 contract and filesystem tests, runnable without Spark or Docker."""
from __future__ import annotations
import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from phase2_fixture import write_fixture
from src.common import PipelineError, read_json, verify_snapshot, write_json
from src.run_phase2 import compare_amounts, ensure_compatible, fingerprint, read_source
from src.transform.curate import check_header


class Phase2PureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.data = Path(self.temp.name)
        self.raw = write_fixture(self.data)

    def tearDown(self):
        self.temp.cleanup()

    def test_source_manifest_accepted(self):
        path, manifest, digest = read_source(self.data, "fixture_001")
        self.assertEqual(path, self.raw)
        self.assertEqual(manifest["quality"]["row_counts"]["order_items"], 4)
        self.assertEqual(len(digest), 64)

    def test_raw_tampering_rejected(self):
        with (self.raw / "orders" / "orders.csv").open("a") as f:
            f.write("tampered\n")
        with self.assertRaises(PipelineError):
            read_source(self.data, "fixture_001")

    def test_extra_raw_file_rejected(self):
        (self.raw / "extra.txt").write_text("not in manifest")
        with self.assertRaises(PipelineError):
            read_source(self.data, "fixture_001")

    def test_missing_snapshot_rejected(self):
        with self.assertRaises(PipelineError):
            read_source(self.data, "missing")

    def test_path_traversal_rejected(self):
        with self.assertRaises(PipelineError):
            read_source(self.data, "../outside")

    def test_header_correct(self):
        check_header(self.raw / "customers" / "customers.csv", "customers")

    def test_wrong_header_rejected(self):
        path = self.data / "wrong.csv"
        path.write_text("customer_id,name\nC1,Demo\n")
        with self.assertRaises(PipelineError):
            check_header(path, "customers")

    def test_fixture_has_valid_quoted_csv(self):
        with (self.raw / "customers" / "customers.csv").open(newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(rows[0]["customer_name"], 'Demo, "One"')
        self.assertEqual(rows[1]["customer_name"], "Demo\nTwo")

    def test_amount_comparison_is_decimal_not_string(self):
        compare_amounts({"gross_order_value_usd_all_statuses": "31.250", "delivered_order_value_usd": "25.0"},
                        {"gross_order_value_usd_all_statuses": "31.25", "delivered_order_value_usd": "25.00"})

    def test_amount_mismatch_rejected(self):
        with self.assertRaises(PipelineError):
            compare_amounts({"gross_order_value_usd_all_statuses": "31.26", "delivered_order_value_usd": "25.00"},
                            {"gross_order_value_usd_all_statuses": "31.25", "delivered_order_value_usd": "25.00"})

    def test_compatible_replay(self):
        obj = {"phase": 2, "batch_id": "a", "source_manifest_sha256": "b", "transform_fingerprint": "c", "transform_version": 1}
        ensure_compatible(obj, "b", "a", "c")
        with self.assertRaises(PipelineError):
            ensure_compatible(obj, "changed", "a", "c")
        with self.assertRaises(PipelineError):
            ensure_compatible(obj, "b", "a", "changed")

    def test_failed_source_quality_rejected(self):
        manifest = read_json(self.raw / "manifest.json")
        manifest["quality"]["status"] = "failed"
        write_json(self.raw / "manifest.json", manifest)
        with self.assertRaises(PipelineError):
            read_source(self.data, "fixture_001")

    def test_fingerprint_is_stable(self):
        self.assertEqual(fingerprint(), fingerprint())
        self.assertEqual(len(fingerprint()), 64)


if __name__ == "__main__":
    unittest.main()
