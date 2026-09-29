"""Dependency-free Phase 3 tests: run with Windows Python or in the container."""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import PipelineError, inventory, read_json, write_json
from src.load.warehouse_contracts import (MODELS, RowDigest, Schemas, assess_dbt_results,
    ensure_compatible, expected_schemas, normalize_value, read_processed)
from src.load.warehouse import fingerprint


class Phase3PureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.data = Path(self.temp.name)
        self.output = self.data / "processed" / "fixture_001"
        for t in expected_schemas():
            p = self.output / t / "part.parquet"
            p.parent.mkdir(parents=True)
            p.write_bytes(b"placeholder for manifest tests; not real Parquet")
        self.manifest = {"format_version": 1, "status": "success", "phase": 2,
            "batch_id": "fixture_001", "output_id": "fixture_001", "source_mode": "offline_fixture",
            "quality": {"status": "passed", "row_counts": {t: 1 for t in expected_schemas()},
                "gross_order_value_usd_all_statuses": "12.00", "delivered_order_value_usd": "12.00"},
            "schemas": expected_schemas(), "files": inventory(self.output)}
        self.save()

    def save(self):
        write_json(self.output / "manifest.json", self.manifest)

    def tearDown(self):
        self.temp.cleanup()

    def test_valid_manifest(self):
        path, _, digest = read_processed(self.data, "fixture_001")
        self.assertEqual(path, self.output)
        self.assertEqual(len(digest), 64)

    def test_tampered_file_rejected(self):
        (self.output / "products/part.parquet").write_bytes(b"changed")
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_missing_file_rejected(self):
        (self.output / "products/part.parquet").unlink()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_wrong_phase_rejected(self):
        self.manifest["phase"] = 1
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_wrong_output_id_rejected(self):
        self.manifest["output_id"] = "other"
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_failed_quality_rejected(self):
        self.manifest["quality"]["status"] = "failed"
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_schema_drift_rejected(self):
        self.manifest["schemas"]["products"]["catalog_price"] = "double"
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_empty_counts_rejected(self):
        self.manifest["quality"]["row_counts"]["orders"] = 0
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_invalid_amount_rejected(self):
        self.manifest["quality"]["gross_order_value_usd_all_statuses"] = "NaN"
        self.save()
        with self.assertRaises(PipelineError): read_processed(self.data, "fixture_001")

    def test_path_traversal_rejected(self):
        with self.assertRaises(PipelineError): read_processed(self.data, "../outside")

    def test_schema_identifiers_guarded(self):
        for bad in ("public", "pg_catalog", "bad; DROP TABLE x", "UPPERCASE", "x"*64):
            with self.subTest(bad=bad), self.assertRaises(PipelineError): Schemas(landing=bad)
        with self.assertRaises(PipelineError): Schemas("same", "same", "different")
        Schemas("p3test_a_landing", "p3test_a_analytics", "p3test_a_meta")

    def test_utc_normalization(self):
        naive = datetime(2026, 1, 1, 0, 0)
        east = datetime(2026, 1, 1, 5, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        self.assertEqual(normalize_value(naive), normalize_value(east))
        self.assertEqual(normalize_value(naive).tzinfo, timezone.utc)

    def test_invalid_values_rejected(self):
        for value in (None, "", " \t", "a\x00b", 0.1, True, Decimal("NaN"), Decimal("1.001")):
            with self.subTest(value=value), self.assertRaises(PipelineError): normalize_value(value)

    def test_row_digest_order_independent(self):
        rows = [(1, "C1", Decimal("1.20")), (2, "C2", Decimal("3.40"))]
        a,b = RowDigest(), RowDigest()
        for row in rows: a.add(row)
        for row in reversed(rows): b.add(row)
        self.assertEqual(a.result(), b.result())

    def test_row_digest_detects_attribute_change(self):
        a,b = RowDigest(), RowDigest()
        a.add((1, "C1", Decimal("1.20")))
        b.add((1, "C2", Decimal("1.20")))
        self.assertNotEqual(a.result(), b.result())

    def test_row_digest_decimal_and_timestamp_stable(self):
        a,b = RowDigest(), RowDigest()
        a.add((datetime(2026,1,1), Decimal("1.2")))
        b.add((datetime(2026,1,1,tzinfo=timezone.utc), Decimal("1.20")))
        self.assertEqual(a.result(), b.result())

    def test_replay_different_inputs_rejected(self):
        state = {"batch_id":"a", "output_id":"a", "source_manifest_sha256":"b", "loader_fingerprint":"c"}
        ensure_compatible(state, "b", "c", "a", "a")
        with self.assertRaises(PipelineError): ensure_compatible(state, "changed", "c", "a", "a")
        with self.assertRaises(PipelineError): ensure_compatible(state, "b", "changed", "a", "a")

    def test_dbt_results_require_all_models_and_tests(self):
        nodes = {f"test.ecommerce.t{i}": {"resource_type": "test"} for i in range(20)}
        result = {"results": [{"unique_id": k,"status":"pass"} for k in nodes] +
                  [{"unique_id":"model.ecommerce."+m,"status":"success"} for m in MODELS]}
        self.assertEqual(assess_dbt_results(result, {"nodes":nodes}, "build")["models_built"],9)
        result["results"].pop()
        with self.assertRaises(PipelineError): assess_dbt_results(result, {"nodes":nodes}, "build")

    def test_dbt_results_reject_skip_warning_empty(self):
        nodes = {f"test.ecommerce.t{i}": {"resource_type":"test"} for i in range(20)}
        good = {"results":[{"unique_id":k,"status":"pass"} for k in nodes]}
        self.assertEqual(assess_dbt_results(good,{"nodes":nodes},"test")["tests_passed"],20)
        for status in ("skipped", "warn", "fail", "error"):
            bad = deepcopy(good)
            bad["results"][0]["status"] = status
            with self.subTest(status=status), self.assertRaises(PipelineError): assess_dbt_results(bad,{"nodes":nodes},"test")
        with self.assertRaises(PipelineError): assess_dbt_results({"results":[]},{"nodes":nodes},"test")

    def test_fingerprints_stable(self):
        for kind in ("loader", "models"):
            self.assertEqual(fingerprint(kind), fingerprint(kind))
            self.assertEqual(len(fingerprint(kind)),64)


if __name__ == "__main__": unittest.main()
