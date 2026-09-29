r"""Run Phase 1 from Windows CMD: python src\run_phase1.py --batch-id initial_001.

This is a plain Python coordinator, not Airflow. It performs complete local
snapshots, not source-level incremental extraction or warehouse upserts.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

# Support both `python src\run_phase1.py` and `python -m src.run_phase1`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common import (
    PROJECT_ROOT, PipelineError, inventory, new_batch_id, read_json,
    sha256_file, utc_now, validate_batch_id, verify_snapshot, write_json,
)
from src.contracts import CSV_CONTRACTS
from src.extract.extract_csv import ingest_csv
from src.extract.extract_products_api import API_URL, extract_products
from src.extract.generate_csv import generate_csv_sources
from src.quality import validate_raw

LOG = logging.getLogger(__name__)
FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "products.json"


def make_summary(manifest: dict[str, Any], raw_dir: Path, action: str) -> dict[str, Any]:
    return {
        "status": "success", "action": action, "batch_id": manifest["batch_id"],
        "source_mode": manifest["parameters"]["source_mode"],
        "row_counts": manifest["quality"]["row_counts"],
        "quality_status": manifest["quality"]["status"],
        "raw_directory": str(raw_dir.resolve()),
        "manifest": str((raw_dir / "manifest.json").resolve()),
        "elapsed_seconds": manifest["elapsed_seconds"],
        "data_provenance": manifest["data_provenance"],
    }


def run_pipeline(
    data_dir: Path,
    reports_dir: Path,
    batch_id: str,
    customers: int = 1000,
    orders: int = 10000,
    seed: int = 42,
    offline: bool = False,
    page_size: int = 100,
) -> dict[str, Any]:
    validate_batch_id(batch_id)
    if customers < 1 or orders < 1:
        raise PipelineError("Customer/order counts must be positive.")
    if not 1 <= page_size <= 100:
        raise PipelineError("Page size must be between 1 and 100.")
    data_dir = data_dir.resolve()
    reports_dir = reports_dir.resolve()
    parameters = {
        "customers": customers, "orders": orders, "seed": seed,
        "page_size": page_size, "source_mode": "offline_fixture" if offline else "live_api",
        "api_url": None if offline else API_URL,
        "fixture_sha256": sha256_file(FIXTURE) if offline else None,
        "generator_version": 1,
    }
    raw_dir = data_dir / "raw" / batch_id
    source_dir = data_dir / "source" / batch_id
    lock_dir = data_dir / ".locks" / batch_id
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_dir.mkdir()
    except FileExistsError as exc:
        raise PipelineError(
            f"Batch {batch_id} is locked. Another run may be active. After a hard crash, "
            "use a NEW batch ID or inspect the lock before removing it manually."
        ) from exc
    staging: Path | None = None
    source_published = False
    raw_published = False
    started = time.perf_counter()
    try:
        if raw_dir.exists():
            manifest = verify_snapshot(raw_dir)
            if manifest["parameters"] != parameters:
                raise PipelineError("Batch ID already exists with different parameters. Use a new --batch-id.")
            LOG.info("Existing completed batch verified: no API calls, generation, or rewriting.")
            summary = make_summary(manifest, raw_dir, "already_complete")
            write_json(reports_dir / "phase1_latest.json", summary)
            return summary
        if source_dir.exists():
            raise PipelineError("Source folder exists without a completed raw batch. Use a new batch ID.")
        staging = data_dir / ".staging" / f"{batch_id}_{uuid4().hex[:8]}"
        staged_raw = staging / "raw"
        staged_source = staging / "source"
        staged_raw.mkdir(parents=True)
        ingested_at = utc_now()
        LOG.info("[1/5] Extracting product catalogue (%s)", parameters["source_mode"])
        products, product_metadata = extract_products(
            staged_raw / "products", batch_id, ingested_at,
            page_size=page_size, offline_fixture=FIXTURE if offline else None,
        )
        LOG.info("[2/5] Generating synthetic customer, order and order-item CSVs")
        generated_counts = generate_csv_sources(staged_source, products, customers, orders, seed)
        LOG.info("[3/5] Ingesting CSV files with batch/timestamp/source metadata")
        ingestion: dict[str, Any] = {"products": product_metadata}
        for table in CSV_CONTRACTS:
            ingestion[table] = ingest_csv(
                staged_source / f"{table}.csv", staged_raw / table / f"{table}.csv",
                table, batch_id, ingested_at,
            )
            if ingestion[table]["rows"] != generated_counts[table]:
                raise PipelineError(f"Row count changed during {table} ingestion.")
        LOG.info("[4/5] Checking schemas, keys, foreign keys, timestamps and order totals")
        quality = validate_raw(staged_raw, batch_id)
        for table, count in quality["row_counts"].items():
            if count != ingestion[table]["rows"]:
                raise PipelineError(f"Validation count mismatch for {table}.")
        write_json(staged_raw / "quality_report.json", quality)
        provenance = {
            "products": "Local handcrafted test fixture" if offline else "DummyJSON public test-data REST API",
            "customers_orders_order_items": "Deterministically generated synthetic data, not real business transactions",
            "prices": "API/fixture price numbers treated as synthetic USD values; not verified source currency",
            "time_range": "Synthetic orders dated within the first 180 days of 2026",
        }
        manifest = {
            "format_version": 1, "status": "success", "batch_id": batch_id,
            "ingested_at": ingested_at, "completed_at": utc_now(),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "parameters": parameters, "data_provenance": provenance,
            "ingestion": ingestion, "quality": quality, "files": inventory(staged_raw),
            "scope": "Local complete snapshot. No Spark, SQL warehouse, dbt, S3, Airflow, or incremental loads yet.",
        }
        write_json(staged_raw / "manifest.json", manifest)
        LOG.info("[5/5] Publishing validated snapshot and manifest")
        source_dir.parent.mkdir(parents=True, exist_ok=True)
        raw_dir.parent.mkdir(parents=True, exist_ok=True)
        staged_source.rename(source_dir)
        source_published = True
        # Both directories are under data_dir. The final raw directory appears
        # only after all its files and the success manifest are ready.
        staged_raw.rename(raw_dir)
        raw_published = True
        summary = make_summary(manifest, raw_dir, "created")
        write_json(reports_dir / "phase1_latest.json", summary)
        return summary
    finally:
        if source_published and not raw_published:
            shutil.rmtree(source_dir, ignore_errors=True)
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
        lock_dir.rmdir()


def print_summary(summary: dict[str, Any]) -> None:
    print("\nPHASE 1 COMPLETE")
    print(f"Batch:   {summary['batch_id']}")
    print(f"Action:  {summary['action']}")
    print(f"Source:  {summary['source_mode']}")
    for table, count in summary["row_counts"].items():
        print(f"{table:12s}: {count:,}")
    print(f"Quality: {summary['quality_status'].upper()}")
    print(f"Raw:     {summary['raw_directory']}")
    print("Note: Customers and transactions are synthetic. This is a local batch pipeline.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-id", help="Explicit snapshot ID. Same ID + same parameters is a verified no-op.")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--offline", action="store_true", help="Use a labelled test fixture; does NOT exercise the real API.")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data")
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports")
    parser.add_argument("--verify-only", action="store_true", help="Recheck file hashes AND data rules; do not fetch or generate.")
    args = parser.parse_args()
    if sys.version_info < (3, 10):
        parser.error("Python 3.10+ is required; Python 3.12 is the recommended project baseline.")
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"phase1_{new_batch_id()}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")],
    )
    try:
        if args.verify_only:
            if args.batch_id:
                batch_id = validate_batch_id(args.batch_id)
            else:
                latest_path = args.reports_dir / "phase1_latest.json"
                if not latest_path.is_file():
                    raise PipelineError("No latest report. Supply --batch-id for an existing completed run.")
                batch_id = validate_batch_id(read_json(latest_path)["batch_id"])
            raw_dir = args.data_dir / "raw" / batch_id
            manifest = verify_snapshot(raw_dir)
            quality = validate_raw(raw_dir, batch_id)
            if quality != manifest["quality"]:
                raise PipelineError("Data checks disagree with the committed manifest.")
            summary = make_summary(manifest, raw_dir, "verified")
        else:
            summary = run_pipeline(
                args.data_dir, args.reports_dir, args.batch_id or new_batch_id(),
                args.customers, args.orders, args.seed, args.offline, args.page_size,
            )
        print_summary(summary)
        print(f"Log:     {log_path}")
        return 0
    except (PipelineError, OSError, ValueError, csv.Error) as exc:
        LOG.error("PHASE 1 FAILED: %s", exc)
        print(f"Details: {log_path}")
        return 1
    except KeyboardInterrupt:
        LOG.error("Run interrupted. Incomplete staging files were not published as a successful batch.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
