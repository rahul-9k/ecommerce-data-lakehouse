from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.orchestration.phase5 import finalize, preflight


def sample_report(batch="delta_001", action="already_complete"):
    return {
        "status": "success",
        "phase": 4,
        "action": action,
        "batch_id": batch,
        "baseline": {"status": "unchanged", "batch_id": "initial_001", "row_counts": {"orders": 10000}},
        "watermark": {"kind": "source_change_sequence", "last_change_seq": 130, "models_for_seq": 130},
        "landing_row_counts": {"orders": 10100, "products": 194, "customers": 1000, "order_items": 25269},
        "analytics": {"distinct_orders_in_fact": 10100},
        "dbt": {"status": "passed", "tests_passed": 46, "failures": 0, "errors": 0, "skipped": 0},
        "quality_status": "passed",
    }


class Phase5PureTests(unittest.TestCase):
    def write(self, directory: Path, value: dict):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "phase4_latest.json").write_text(json.dumps(value), encoding="utf-8")

    def test_preflight_accepts_clean_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            self.write(path, sample_report())
            result = preflight(path)
            self.assertEqual(result["status"], "passed")
            self.assertTrue((path / "phase5_preflight.json").is_file())

    def test_preflight_rejects_watermark_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            value = sample_report()
            value["watermark"]["models_for_seq"] = 129
            self.write(path, value)
            with self.assertRaises(Exception):
                preflight(path)

    def test_preflight_rejects_dbt_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            value = sample_report()
            value["dbt"]["failures"] = 1
            self.write(path, value)
            with self.assertRaises(Exception):
                preflight(path)

    def test_finalize_requires_matching_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            self.write(path, sample_report())
            with self.assertRaises(Exception):
                finalize("other_001", path)

    def test_finalize_requires_verified_or_replay_action(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            self.write(path, sample_report(action="applied"))
            with self.assertRaises(Exception):
                finalize("delta_001", path)

    def test_finalize_writes_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            self.write(path, sample_report(action="verified"))
            result = finalize("delta_001", path)
            self.assertEqual(result["orchestrator"], "Apache Airflow")
            self.assertTrue((path / "phase5_latest.json").is_file())


if __name__ == "__main__":
    unittest.main()
