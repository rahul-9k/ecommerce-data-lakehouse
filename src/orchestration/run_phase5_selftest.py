"""Run Phase 5 unit/DAG-import tests and publish a small JSON report."""
from __future__ import annotations

from pathlib import Path
import sys
import unittest

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common import PROJECT_ROOT, utc_now, write_json


def main() -> int:
    suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT / "tests"), pattern="test_phase5_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    passed = result.wasSuccessful() and not result.skipped and result.testsRun > 0
    report = {
        "status": "passed" if passed else "failed",
        "phase": 5,
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "completed_at": utc_now(),
        "scope": "Phase 5 helper tests plus real Airflow 3 DAG import/structure checks. Does not mutate initial_001/delta_001.",
    }
    write_json(PROJECT_ROOT / "reports" / "phase5_selftest.json", report)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
