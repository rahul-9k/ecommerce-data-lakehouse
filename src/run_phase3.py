"""Windows CMD: run_phase3.cmd --self-test, then --batch-id initial_001."""
from __future__ import annotations
import argparse
import logging
import os
from pathlib import Path
import sys
import time
import traceback
import unittest
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common import PROJECT_ROOT, write_json
from src.load.warehouse import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", default="initial_001")
    parser.add_argument("--output-id", help="Phase 2 output directory ID, default: same as batch ID")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    mode.add_argument("--preview", action="store_true")
    mode.add_argument("--rebuild-models", action="store_true", help="Rebuild derived dbt tables, never replace landing data")
    args = parser.parse_args()
    logs = PROJECT_ROOT / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    log = logs / ("phase3_" + time.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:6] + ".log")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log, encoding="utf-8")])
    report = args.reports_dir / ("phase3_selftest.json" if args.self_test else "phase3_latest.json")
    try:
        if args.self_test:
            import pyarrow  # noqa: F401
            import psycopg  # noqa: F401
            os.environ["PHASE3_RUN_INTEGRATION"] = "1"
            suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT / "tests"), pattern="test_phase3_*.py")
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            passed = result.wasSuccessful() and not result.skipped and result.testsRun > 0
            write_json(report, {"status": "passed" if passed else "failed", "tests_run": result.testsRun,
                "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
                "scope": "Temporary synthetic Parquet and isolated p3test_* database schemas, including a real dbt build. Does not validate initial_001."})
            return 0 if passed else 1
        result = run_pipeline(args.data_dir, args.reports_dir, args.batch_id, args.output_id,
                              args.verify_only, args.preview, args.rebuild_models, log_root=logs)
        write_json(report, result)
        print("\nPHASE 3 COMPLETE")
        print(f"Batch: {result['batch_id']}\nAction: {result['action']}")
        print("PostgreSQL landing:", result["landing_row_counts"])
        print("Analytics:", result["analytics"]["row_counts"])
        print(f"dbt tests passed: {result['dbt']['tests_passed']}")
        print("Quality: PASSED\nReport: reports/phase3_latest.json")
        print("Scope: single local snapshot, not incremental loading.")
        return 0
    except KeyboardInterrupt:
        write_json(report, {"status": "interrupted", "phase": 3, "batch_id": args.batch_id})
        logging.error("Interrupted. Completed landing load (if any) is preserved. Rerun after checking the logs.")
        return 130
    except Exception as exc:
        logging.error("PHASE 3 FAILED: %s", exc)
        with log.open("a", encoding="utf-8") as handle:
            handle.write(traceback.format_exc())
        write_json(report, {"status": "failed", "phase": 3, "batch_id": args.batch_id,
                            "error": str(exc), "log": str(log)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
