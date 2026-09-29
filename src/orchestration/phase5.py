"""Small Airflow-facing checks/reporting around the already-tested Phase 4 runner."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common import PROJECT_ROOT, PipelineError, read_json, utc_now, validate_batch_id, write_json


def _require_successful_phase4(report_path: Path) -> dict:
    if not report_path.is_file():
        raise PipelineError("Missing reports/phase4_latest.json. Finish Phase 4 before Airflow orchestration.")
    report = read_json(report_path)
    if report.get("status") != "success" or report.get("quality_status") != "passed":
        raise PipelineError("Latest Phase 4 report is not a successful, quality-passed run.")
    if report.get("baseline", {}).get("status") != "unchanged":
        raise PipelineError("Phase 4 baseline preservation check is not passing.")
    watermark = report.get("watermark", {})
    if watermark.get("last_change_seq") != watermark.get("models_for_seq"):
        raise PipelineError("Incremental landing and dbt models are not at the same watermark.")
    dbt = report.get("dbt", {})
    if dbt.get("status") != "passed" or dbt.get("failures") or dbt.get("errors") or dbt.get("skipped"):
        raise PipelineError("Latest Phase 4 dbt verification is not clean.")
    return report


def preflight(report_dir: Path) -> dict:
    report = _require_successful_phase4(report_dir / "phase4_latest.json")
    result = {
        "status": "passed",
        "phase": 5,
        "check": "preflight",
        "phase4_batch": report.get("batch_id"),
        "watermark": report["watermark"],
        "landing_row_counts": report.get("landing_row_counts"),
        "completed_at": utc_now(),
        "scope": "Checks the saved successful Phase 4 state before Airflow submits another idempotent delta.",
    }
    write_json(report_dir / "phase5_preflight.json", result)
    return result


def finalize(batch_id: str, report_dir: Path) -> dict:
    batch_id = validate_batch_id(batch_id)
    report = _require_successful_phase4(report_dir / "phase4_latest.json")
    if report.get("batch_id") != batch_id:
        raise PipelineError(f"Phase 4 latest report is for {report.get('batch_id')!r}, not {batch_id!r}.")
    if report.get("action") not in {"verified", "already_complete"}:
        raise PipelineError("Airflow finalize expects the verify task to finish before publication.")
    result = {
        "status": "success",
        "phase": 5,
        "orchestrator": "Apache Airflow",
        "airflow_version_target": "3.3.2",
        "dag_id": "ecommerce_incremental_pipeline",
        "batch_id": batch_id,
        "phase4_action": report.get("action"),
        "watermark": report.get("watermark"),
        "landing_row_counts": report.get("landing_row_counts"),
        "analytics": report.get("analytics"),
        "dbt": report.get("dbt"),
        "quality_status": report.get("quality_status"),
        "baseline": report.get("baseline"),
        "completed_at": utc_now(),
        "scope": "Airflow scheduled/monitored the existing idempotent Phase 4 delta workflow. It did not replace the transactional loader or dbt validations.",
    }
    write_json(report_dir / "phase5_latest.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--finalize", action="store_true")
    parser.add_argument("--batch-id")
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports")
    args = parser.parse_args()
    try:
        if args.preflight:
            result = preflight(args.reports_dir)
            print("PHASE 5 PREFLIGHT PASSED", result["watermark"])
        else:
            if not args.batch_id:
                parser.error("--finalize requires --batch-id")
            result = finalize(args.batch_id, args.reports_dir)
            print("PHASE 5 COMPLETE")
            print("Batch:", result["batch_id"])
            print("Watermark:", result["watermark"])
            print("Landing:", result["landing_row_counts"])
            print("dbt tests passed:", result["dbt"].get("tests_passed"))
            print("Report: reports/phase5_latest.json")
        return 0
    except Exception as exc:
        print(f"PHASE 5 FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
