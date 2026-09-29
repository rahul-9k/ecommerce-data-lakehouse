"""Transform an existing Phase 1 snapshot into validated local Parquet.

Windows CMD: run_phase2.cmd --batch-id initial_001
Run a separate fixture test first: run_phase2.cmd --self-test
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import shutil
import sys
import time
import traceback
import unittest
from pathlib import Path
from typing import Any
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common import (PROJECT_ROOT, PipelineError, inventory, read_json, sha256_file,
                        utc_now, validate_batch_id, verify_snapshot, write_json)
from src.transform.curate import (TABLES, enrich_items, exact_deduplicate, read_and_type,
                                  read_parquet, schema_types, start_spark, validate_tables, write_parquet)

LOG = logging.getLogger(__name__)
TRANSFORM_VERSION = 1


def fingerprint() -> str:
    """Reject silent reuse after transformation or contract code changes."""
    digest = hashlib.sha256()
    for relative in ("src/run_phase2.py", "src/transform/curate.py", "src/common.py", "src/contracts.py"):
        digest.update(relative.encode())
        # Windows Git checkout may translate line endings; don't call that a logic change.
        digest.update((PROJECT_ROOT / relative).read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


def read_source(data_dir: Path, batch_id: str) -> tuple[Path, dict[str, Any], str]:
    validate_batch_id(batch_id)
    raw_dir = data_dir / "raw" / batch_id
    manifest = verify_snapshot(raw_dir)
    if manifest.get("batch_id") != batch_id or manifest.get("quality", {}).get("status") != "passed":
        raise PipelineError("Phase 1 manifest has a different batch ID or did not pass data quality.")
    counts = manifest.get("quality", {}).get("row_counts", {})
    if set(counts) != set(TABLES) or any(type(n) is not int or n < 1 for n in counts.values()):
        raise PipelineError("Phase 1 manifest is missing positive counts for the four source tables.")
    return raw_dir, manifest, sha256_file(raw_dir / "manifest.json")


def compare_amounts(quality: dict, source_quality: dict) -> None:
    from decimal import Decimal
    for key in ("gross_order_value_usd_all_statuses", "delivered_order_value_usd"):
        if Decimal(quality[key]) != Decimal(source_quality[key]):
            raise PipelineError(f"Amount reconciliation failed against Phase 1: {key}.")


def ensure_compatible(manifest: dict, source_hash: str, batch_id: str, code_hash: str) -> None:
    if (manifest.get("phase") != 2 or manifest.get("batch_id") != batch_id or
            manifest.get("source_manifest_sha256") != source_hash or
            manifest.get("transform_fingerprint") != code_hash or
            manifest.get("transform_version") != TRANSFORM_VERSION):
        raise PipelineError("Existing output belongs to different input/code. Keep it and choose a new --output-id.")


def summary_for(manifest: dict, output: Path, action: str, elapsed: float) -> dict:
    return {
        "status": "success", "phase": 2, "action": action,
        "batch_id": manifest["batch_id"], "output_id": manifest["output_id"],
        "source_mode": manifest["source_mode"], "spark_version": manifest["spark_version"],
        "row_counts": manifest["quality"]["row_counts"], "deduplication": manifest["deduplication"],
        "quality_status": manifest["quality"]["status"], "output_directory": str(output),
        "project_relative_output_directory": f"data/processed/{manifest['output_id']}",
        "elapsed_seconds_this_command": round(elapsed, 3),
        "original_build_seconds": manifest["build_seconds"],
        "data_provenance": manifest["data_provenance"],
        "scope": "Local complete-snapshot transformation, not incremental processing or an ACID lakehouse.",
    }


def run_pipeline(data_dir: Path, reports_dir: Path, batch_id: str, output_id: str | None = None,
                 verify_only: bool = False, preview: bool = False, spark_session=None) -> dict:
    started = time.perf_counter()
    output_id = validate_batch_id(output_id or batch_id)
    data_dir, reports_dir = data_dir.resolve(), reports_dir.resolve()
    LOG.info("[1/6] Verifying immutable Phase 1 files and manifest")
    raw_dir, source, source_hash = read_source(data_dir, batch_id)
    code_hash = fingerprint()
    output = data_dir / "processed" / output_id
    lock = data_dir / "processed" / ".locks" / output_id
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock.mkdir()
    except FileExistsError as exc:
        raise PipelineError("Phase 2 output is locked. Check for an active run before inspecting a stale lock.") from exc
    staging: Path | None = None
    spark = spark_session
    own_session = False
    cached = []
    try:
        if output.exists():
            manifest = verify_snapshot(output)
            ensure_compatible(manifest, source_hash, batch_id, code_hash)
            if verify_only or preview:
                if spark is None:
                    spark, own_session = start_spark(), True
                frames = read_parquet(spark, output)
                quality = validate_tables(frames, batch_id)
                if quality != manifest["quality"]:
                    raise PipelineError("Parquet revalidation disagrees with the saved quality report.")
                for table, frame in frames.items():
                    if schema_types(frame) != manifest["schemas"][table]:
                        raise PipelineError(f"Parquet schema changed for {table}.")
                compare_amounts(quality, source["quality"])
                if preview:
                    show_preview(frames)
            action = "verified" if verify_only else ("inspected" if preview else "already_complete")
            result = summary_for(manifest, output, action, time.perf_counter() - started)
            write_json(reports_dir / "phase2_latest.json", result)
            return result
        if verify_only or preview:
            raise PipelineError("No completed Phase 2 output. Run without --verify-only/--preview first.")
        if spark is None:
            spark, own_session = start_spark(), True
        LOG.info("[2/6] Reading explicit schemas and converting strings to typed columns")
        typed = read_and_type(spark, raw_dir)
        tables = {}
        dedup = {}
        for table in TABLES:
            frame = typed[table].cache()
            cached.append(frame)
            cleaned, stats = exact_deduplicate(frame)
            if stats["input_rows"] != source["quality"]["row_counts"][table]:
                raise PipelineError(f"Spark input count differs from Phase 1 for {table}.")
            tables[table] = cleaned.cache()
            cached.append(tables[table])
            dedup[table] = stats
        LOG.info("[3/6] Validating keys, values, relationships and order totals")
        quality = validate_tables(tables, batch_id)
        compare_amounts(quality, source["quality"])
        tables = enrich_items(tables)
        tables["order_items"] = tables["order_items"].cache()
        cached.append(tables["order_items"])
        if tables["order_items"].count() != quality["row_counts"]["order_items"]:
            raise PipelineError("Order-context join changed the order-item grain.")
        schemas_before = {name: schema_types(frame) for name, frame in tables.items()}
        LOG.info("[4/6] Writing Snappy Parquet; orders and items partitioned by order year/month")
        staging = data_dir / "processed" / ".staging" / (output_id + "_" + uuid4().hex[:8])
        staging.mkdir(parents=True)
        write_parquet(tables, staging)
        LOG.info("[5/6] Reading Parquet back and rechecking schemas, values and row counts")
        readback = read_parquet(spark, staging)
        readback_quality = validate_tables(readback, batch_id)
        if readback_quality != quality:
            raise PipelineError("Parquet round trip changed validation results.")
        for table, frame in readback.items():
            if schema_types(frame) != schemas_before[table]:
                raise PipelineError(f"Parquet round trip changed column types for {table}.")
        # Recheck the source signature after processing. The Docker source mount is read-only.
        verify_snapshot(raw_dir)
        if sha256_file(raw_dir / "manifest.json") != source_hash:
            raise PipelineError("Raw manifest changed while Phase 2 was running.")
        write_json(staging / "quality_report.json", {**readback_quality, "deduplication": dedup})
        manifest = {
            "format_version": 1, "phase": 2, "status": "success", "batch_id": batch_id, "output_id": output_id,
            "transform_version": TRANSFORM_VERSION, "transform_fingerprint": code_hash,
            "source_manifest_sha256": source_hash, "source_mode": source["parameters"]["source_mode"],
            "data_provenance": source["data_provenance"], "spark_version": spark.version,
            "completed_at": utc_now(), "build_seconds": round(time.perf_counter() - started, 3),
            "quality": readback_quality, "deduplication": dedup,
            "schemas": {name: schema_types(frame) for name, frame in readback.items()},
            "partition_columns": {"orders": ["order_year", "order_month"], "order_items": ["order_year", "order_month"]},
            "files": inventory(staging),
            "scope": "Full local snapshot. No CDC, cross-batch merge, SQL warehouse, dbt, S3, or Airflow yet.",
        }
        write_json(staging / "manifest.json", manifest)
        LOG.info("[6/6] Publishing completed output; existing outputs are never overwritten")
        # Release Spark caches/readback references before moving the directory.
        for frame in cached:
            frame.unpersist()
        cached.clear()
        staging.rename(output)
        staging = None
        result = summary_for(manifest, output, "created", time.perf_counter() - started)
        write_json(reports_dir / "phase2_latest.json", result)
        return result
    finally:
        for frame in cached:
            try:
                frame.unpersist()
            except Exception:
                pass
        if own_session and spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
        lock.rmdir()


def show_preview(frames: dict) -> None:
    items = frames["order_items"]
    print("\nORDER-ITEM SCHEMA")
    items.printSchema()
    print("\nFIVE ORDER ITEMS (unit_price is the transaction price, not today's catalogue price)")
    (items.select("order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_amount",
                  "order_status", "order_year", "order_month").orderBy("order_item_id").show(5, truncate=False))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-id", default="initial_001")
    parser.add_argument("--output-id", help="Default: same as batch ID. Choose a NEW ID for a new transform version.")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--preview", action="store_true", help="Recheck the saved output and print five line items.")
    parser.add_argument("--self-test", action="store_true", help="Run Phase 2 tests on temporary fixtures, not your batch.")
    args = parser.parse_args()
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / ("phase2_" + time.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:6] + ".log")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")])
    try:
        if args.self_test:
            # Fail rather than reporting success with all Spark integration tests skipped.
            import pyspark  # noqa: F401
            suite = unittest.defaultTestLoader.discover(str(PROJECT_ROOT / "tests"), pattern="test_phase2_*.py")
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            passed = result.wasSuccessful() and not result.skipped and result.testsRun > 0
            report = {"status": "passed" if passed else "failed", "tests_run": result.testsRun,
                      "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
                      "scope": "Synthetic temporary test fixtures only; does not validate initial_001."}
            write_json(args.reports_dir / "phase2_selftest.json", report)
            return 0 if passed else 1
        result = run_pipeline(args.data_dir, args.reports_dir, args.batch_id, args.output_id,
                              args.verify_only, args.preview)
        print("\nPHASE 2 COMPLETE")
        print(f"Batch:   {result['batch_id']}\nAction:  {result['action']}\nSpark:   {result['spark_version']}")
        for table, count in result["row_counts"].items():
            print(f"{table:12s}: {count:,}")
        print(f"Quality: {result['quality_status'].upper()}")
        print(f"Output:  {result['project_relative_output_directory']}")
        print("Report:  reports/phase2_latest.json")
        print("Scope:   Local snapshot transformation; no incremental loads yet.")
        return 0
    except KeyboardInterrupt:
        LOG.error("Phase 2 interrupted; incomplete staging output is not a successful batch.")
        return 130
    except Exception as exc:
        LOG.error("PHASE 2 FAILED: %s", exc)
        LOG.debug("Traceback", exc_info=True)
        # The stack trace also goes in the log file for debugging Spark environment errors.
        with log_path.open("a", encoding="utf-8") as handle:
            traceback.print_exc(file=handle)
        if not args.self_test:
            write_json(args.reports_dir / "phase2_latest.json", {"status": "failed", "phase": 2,
                       "batch_id": args.batch_id, "error": str(exc), "log_file": str(log_path)})
        print(f"Details: {log_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
