"""Explicit Phase 2 -> PostgreSQL contracts and dependency-free validation helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable

from src.common import PipelineError, sha256_file, validate_batch_id, verify_snapshot

# Each tuple is (column name, Spark logical type, PostgreSQL type).
META = (("_batch_id", "string", "text"), ("_ingested_at", "timestamp", "timestamptz"), ("_source", "string", "text"))
PARTS = (("order_year", "int", "integer"), ("order_month", "int", "integer"))
CONTRACTS = {
    "products": (
        ("product_id", "bigint", "bigint"), ("product_name", "string", "text"),
        ("category", "string", "text"), ("catalog_price", "decimal(18,2)", "numeric(18,2)"), *META),
    "customers": (
        ("customer_id", "string", "text"), ("customer_name", "string", "text"),
        ("city", "string", "text"), ("country", "string", "text"),
        ("created_at", "timestamp", "timestamptz"), ("updated_at", "timestamp", "timestamptz"), *META),
    "orders": (
        ("order_id", "string", "text"), ("customer_id", "string", "text"),
        ("order_date", "timestamp", "timestamptz"), ("status", "string", "text"),
        ("currency", "string", "text"), ("order_total", "decimal(28,2)", "numeric(28,2)"),
        ("updated_at", "timestamp", "timestamptz"), *META, *PARTS),
    "order_items": (
        ("order_item_id", "string", "text"), ("order_id", "string", "text"),
        ("line_number", "int", "integer"), ("product_id", "bigint", "bigint"),
        ("quantity", "int", "integer"), ("unit_price", "decimal(18,2)", "numeric(18,2)"),
        ("updated_at", "timestamp", "timestamptz"), *META,
        ("line_amount", "decimal(28,2)", "numeric(28,2)"),
        ("customer_id", "string", "text"), ("order_date", "timestamp", "timestamptz"),
        ("order_status", "string", "text"), ("currency", "string", "text"), *PARTS),
}
TABLES = tuple(CONTRACTS)
KEYS = dict(zip(TABLES, ("product_id", "customer_id", "order_id", "order_item_id")))
AMOUNTS = ("gross_order_value_usd_all_statuses", "delivered_order_value_usd")
MODELS = {"stg_products", "stg_customers", "stg_orders", "stg_order_items", "dim_product", "dim_customer", "dim_date", "fact_order_items", "mart_daily_sales"}


@dataclass(frozen=True)
class Schemas:
    landing: str = "landing"
    analytics: str = "analytics"
    meta: str = "pipeline_meta"

    def __post_init__(self):
        for name in (self.landing, self.analytics, self.meta):
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) or name.startswith("pg_") or name in ("public", "information_schema"):
                raise PipelineError("Unsafe/reserved PostgreSQL schema name.")
        if len({self.landing, self.analytics, self.meta}) != 3:
            raise PipelineError("Landing, analytics and metadata schemas must be different.")


def expected_schemas() -> dict:
    return {t: {c: spark for c, spark, _ in fields} for t, fields in CONTRACTS.items()}


def read_processed(data_dir: Path, batch_id: str, output_id: str | None = None) -> tuple[Path, dict, str]:
    validate_batch_id(batch_id)
    output_id = validate_batch_id(output_id or batch_id)
    directory = data_dir.resolve() / "processed" / output_id
    manifest = verify_snapshot(directory)
    if (manifest.get("phase") != 2 or manifest.get("batch_id") != batch_id or
            manifest.get("output_id") != output_id or manifest.get("quality", {}).get("status") != "passed"):
        raise PipelineError("Expected a successful Phase 2 snapshot with matching batch/output IDs.")
    counts = manifest["quality"].get("row_counts", {})
    if set(counts) != set(TABLES) or any(type(n) is not int or n < 1 for n in counts.values()):
        raise PipelineError("Phase 2 counts must contain four nonempty datasets.")
    if manifest.get("schemas") != expected_schemas():
        raise PipelineError("Phase 2 column/type contract differs from this loader. Do not edit the manifest to bypass it.")
    for field in AMOUNTS:
        try:
            amount = Decimal(str(manifest["quality"][field]))
        except Exception as exc:
            raise PipelineError("Missing/invalid Phase 2 amount totals.") from exc
        if not amount.is_finite() or amount < 0:
            raise PipelineError("Invalid Phase 2 financial total.")
    for table in TABLES:
        if not list((directory / table).rglob("*.parquet")):
            raise PipelineError(f"No Parquet files for {table}.")
    return directory, manifest, sha256_file(directory / "manifest.json")


def normalize_value(value: Any) -> Any:
    if value is None:
        raise PipelineError("Null field in curated data; all Phase 2 contract fields are required.")
    if isinstance(value, datetime):
        # Spark emitted UTC instants; INT96 readback can be timezone-naive.
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.quantize(Decimal("0.01")):
            raise PipelineError("Amounts must be finite, exact two-decimal values.")
        return value
    if isinstance(value, str):
        if not value.strip() or "\x00" in value:
            raise PipelineError("Empty text or NUL character in curated data.")
        return value
    if type(value) is int:
        return value
    raise PipelineError(f"Unexpected curated Python type: {type(value).__name__}.")


class RowDigest:
    """Order-independent checksum of typed rows, for accidental mutation detection.

    Sum SHA-256 row hashes modulo 2**256, plus row count. Not a signature or an
    authentication mechanism. Avoids collecting/sorting the entire dataset.
    """
    def __init__(self):
        self.rows = 0
        self.total = 0

    def add(self, row: Iterable[Any]) -> None:
        serial = []
        for v in row:
            v = normalize_value(v)
            if isinstance(v, datetime):
                v = ["timestamp", v.isoformat(timespec="microseconds")]
            elif isinstance(v, Decimal):
                v = ["decimal", format(v, ".2f")]
            serial.append(v)
        payload = json.dumps(serial, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.total = (self.total + int.from_bytes(hashlib.sha256(payload).digest(), "big")) % (1 << 256)
        self.rows += 1

    def result(self) -> dict:
        return {"rows": self.rows, "row_value_checksum": f"{self.total:064x}"}


def ensure_compatible(saved: dict, manifest_sha: str, loader_sha: str, batch_id: str, output_id: str) -> None:
    if (saved["source_manifest_sha256"] != manifest_sha or saved["loader_fingerprint"] != loader_sha or
            saved["batch_id"] != batch_id or saved["output_id"] != output_id):
        raise PipelineError("This warehouse already holds a different snapshot/loader version. Nothing overwritten. Cross-batch merging is a later phase.")


def assess_dbt_results(result: dict, manifest: dict, command: str) -> dict:
    rows = result.get("results", [])
    if not rows:
        raise PipelineError("dbt produced no executed resources.")
    identifiers = [r.get("unique_id", "") for r in rows]
    if len(identifiers) != len(set(identifiers)):
        raise PipelineError("Duplicate dbt result identifiers.")
    bad = [r for r in rows if r.get("status") not in ("success", "pass")]
    if bad:
        raise PipelineError("dbt reported a failed, errored, warned or skipped resource. Read the dbt log.")
    expected_tests = {k for k, v in manifest.get("nodes", {}).items() if v.get("resource_type") == "test"}
    actual_tests = {k for k in identifiers if k.startswith("test.")}
    if len(expected_tests) < 20 or actual_tests != expected_tests:
        raise PipelineError("dbt did not execute the complete expected data-test set.")
    model_names = {k.rsplit(".", 1)[-1] for k in identifiers if k.startswith("model.")}
    if command == "build" and model_names != MODELS:
        raise PipelineError("dbt did not build all nine warehouse models.")
    if command == "test" and model_names:
        raise PipelineError("Verification unexpectedly rebuilt a model.")
    return {"command": command, "status": "passed", "models_built": len(model_names),
            "tests_passed": len(actual_tests), "failures": 0, "errors": 0, "skipped": 0}
