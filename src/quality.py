"""Fail-fast validation of a complete raw snapshot, without modifying raw records.

CSV contents are streamed, but key indexes and per-order totals use RAM. This is
appropriate for this local demo; it is not a constant-memory distributed checker.
"""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from src.common import METADATA_COLUMNS, PipelineError
from src.contracts import CSV_CONTRACTS, ORDER_STATUSES
from src.extract.extract_products_api import validate_product


def parse_timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise ValueError("Expected UTC timezone")
        return parsed
    except (ValueError, TypeError) as exc:
        raise PipelineError(f"Invalid UTC timestamp for {label}: {value!r}") from exc


def parse_money(value: str, label: str) -> Decimal:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+\.[0-9]{2}", value):
        raise PipelineError(f"Expected nonnegative, two-decimal money for {label}: {value!r}")
    return Decimal(value)


def positive_int(value: str, label: str) -> int:
    if not re.fullmatch(r"[0-9]+", value) or int(value) <= 0:
        raise PipelineError(f"Expected positive integer for {label}: {value!r}")
    return int(value)


def check_metadata(row: dict[str, Any], batch_id: str, label: str) -> None:
    if row.get("_batch_id") != batch_id:
        raise PipelineError(f"{label}: ingestion batch ID mismatch.")
    if not row.get("_source"):
        raise PipelineError(f"{label}: missing source provenance.")
    parse_timestamp(row.get("_ingested_at", ""), label + "._ingested_at")


def table_rows(raw_dir: Path, table: str, batch_id: str) -> Iterator[dict[str, str]]:
    path = raw_dir / table / f"{table}.csv"
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, strict=True)
        if reader.fieldnames != [*CSV_CONTRACTS[table], *METADATA_COLUMNS]:
            raise PipelineError(f"Unexpected raw schema for {table}.")
        for record_number, row in enumerate(reader, start=1):
            if None in row or any(value is None or not value.strip() for value in row.values()):
                raise PipelineError(f"{table} record {record_number}: null/empty/malformed field.")
            check_metadata(row, batch_id, f"{table} record {record_number}")
            yield row


def validate_raw(raw_dir: Path, batch_id: str) -> dict[str, Any]:
    product_ids: set[int] = set()
    with (raw_dir / "products" / "products.jsonl").open("r", encoding="utf-8") as handle:
        for line in handle:
            product = json.loads(line)
            product_id = validate_product(product)
            check_metadata(product, batch_id, "products")
            if product_id in product_ids:
                raise PipelineError(f"Duplicate product_id: {product_id}")
            product_ids.add(product_id)
    if not product_ids:
        raise PipelineError("No products found in raw snapshot.")

    customers: dict[str, datetime] = {}
    for row in table_rows(raw_dir, "customers", batch_id):
        key = row["customer_id"]
        if key in customers:
            raise PipelineError(f"Duplicate customer_id: {key}")
        created = parse_timestamp(row["created_at"], "customer.created_at")
        updated = parse_timestamp(row["updated_at"], "customer.updated_at")
        if updated < created:
            raise PipelineError(f"Customer {key} was updated before being created.")
        customers[key] = created

    orders: dict[str, tuple[Decimal, datetime]] = {}
    line_totals: dict[str, Decimal] = {}
    line_counts: dict[str, int] = {}
    all_order_value = Decimal("0.00")
    delivered_order_value = Decimal("0.00")
    for row in table_rows(raw_dir, "orders", batch_id):
        key = row["order_id"]
        if key in orders:
            raise PipelineError(f"Duplicate order_id: {key}")
        if row["customer_id"] not in customers:
            raise PipelineError(f"Order {key} references unknown customer_id.")
        if row["status"] not in ORDER_STATUSES or row["currency"] != "USD":
            raise PipelineError(f"Order {key} has invalid status/currency for this demo.")
        ordered = parse_timestamp(row["order_date"], "order.order_date")
        updated = parse_timestamp(row["updated_at"], "order.updated_at")
        if ordered < customers[row["customer_id"]] or updated < ordered:
            raise PipelineError(f"Order {key} has inconsistent timestamps.")
        total = parse_money(row["order_total"], "order.order_total")
        orders[key] = (total, ordered)
        line_totals[key] = Decimal("0.00")
        line_counts[key] = 0
        all_order_value += total
        if row["status"] == "delivered":
            delivered_order_value += total

    item_ids: set[str] = set()
    line_keys: set[tuple[str, int]] = set()
    for row in table_rows(raw_dir, "order_items", batch_id):
        key = row["order_item_id"]
        order_id = row["order_id"]
        if key in item_ids:
            raise PipelineError(f"Duplicate order_item_id: {key}")
        if order_id not in orders:
            raise PipelineError(f"Item {key} references unknown order_id.")
        product_id = positive_int(row["product_id"], "item.product_id")
        if product_id not in product_ids:
            raise PipelineError(f"Item {key} references unknown product_id.")
        line_key = (order_id, positive_int(row["line_number"], "item.line_number"))
        if line_key in line_keys:
            raise PipelineError(f"Duplicate (order_id, line_number): {line_key}")
        line_keys.add(line_key)
        item_ids.add(key)
        quantity = positive_int(row["quantity"], "item.quantity")
        unit_price = parse_money(row["unit_price"], "item.unit_price")
        updated = parse_timestamp(row["updated_at"], "item.updated_at")
        if updated < orders[order_id][1]:
            raise PipelineError(f"Item {key} has an update before its order date.")
        line_totals[order_id] += quantity * unit_price
        line_counts[order_id] += 1
    for order_id, (declared_total, _) in orders.items():
        if line_counts[order_id] == 0 or declared_total != line_totals[order_id]:
            raise PipelineError(f"Order {order_id}: missing items or order/item total mismatch.")
    if not customers or not orders or not item_ids:
        raise PipelineError("Empty table in raw snapshot.")
    return {
        "status": "passed",
        "row_counts": {
            "products": len(product_ids), "customers": len(customers),
            "orders": len(orders), "order_items": len(item_ids),
        },
        "checks": [
            "Required CSV columns and nonempty field values",
            "Product payload types and nonnegative finite prices",
            "Unique primary keys in each table",
            "Unique (order_id, line_number) grain",
            "Orders reference existing customers",
            "Order items reference existing orders and products",
            "Positive integer quantities and two-decimal money",
            "UTC timestamps and valid timestamp ordering",
            "Valid status and project currency convention",
            "Every order has items and totals equal sum(quantity * unit_price)",
            "Batch ID, ingestion timestamp, and source metadata present",
        ],
        "gross_order_value_usd_all_statuses": str(all_order_value),
        "delivered_order_value_usd": str(delivered_order_value),
        "note": "Synthetic amounts; all-status gross order value is NOT recognized revenue.",
    }
