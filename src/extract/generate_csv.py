"""Deterministic synthetic source system: customers, orders, and order items.

All transactions and people are invented. Product price numbers are interpreted
as synthetic USD amounts for this exercise; the API does not establish currency.
Use integer cents for arithmetic, and write decimal strings to CSV.
"""
from __future__ import annotations

import csv
import logging
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from src.common import PipelineError
from src.contracts import CUSTOMER_COLUMNS, ORDER_COLUMNS, ORDER_ITEM_COLUMNS, ORDER_STATUSES
from src.extract.extract_products_api import validate_product

LOG = logging.getLogger(__name__)
CITIES = ("Austin", "Seattle", "Boston", "Denver", "Chicago", "Portland")


def timestamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def money(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def generate_csv_sources(
    destination: Path,
    products: list[dict[str, Any]],
    customers: int = 1000,
    orders: int = 10000,
    seed: int = 42,
) -> dict[str, int]:
    if customers < 1 or orders < 1 or not products:
        raise PipelineError("Need positive customer/order counts and a nonempty product catalogue.")
    catalogue = []
    for product in sorted(products, key=lambda record: record["id"]):
        validate_product(product)
        cents = int((Decimal(str(product["price"])) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        catalogue.append((product["id"], cents))
    if len({product_id for product_id, _ in catalogue}) != len(catalogue):
        raise PipelineError("Generator received duplicate product IDs.")
    destination.mkdir(parents=True, exist_ok=False)
    rng = random.Random(seed)
    customer_epoch = datetime(2025, 12, 1, tzinfo=timezone.utc)
    order_epoch = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with (destination / "customers.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CUSTOMER_COLUMNS)
        writer.writeheader()
        for index in range(1, customers + 1):
            created = customer_epoch + timedelta(seconds=rng.randrange(30 * 86400))
            writer.writerow({
                "customer_id": f"C{index:07d}", "customer_name": f"Customer {index:07d}",
                "city": rng.choice(CITIES), "country": "US",
                "created_at": timestamp(created), "updated_at": timestamp(created),
            })
    item_index = 0
    with (
        (destination / "orders.csv").open("w", newline="", encoding="utf-8") as order_handle,
        (destination / "order_items.csv").open("w", newline="", encoding="utf-8") as item_handle,
    ):
        order_writer = csv.DictWriter(order_handle, fieldnames=ORDER_COLUMNS)
        item_writer = csv.DictWriter(item_handle, fieldnames=ORDER_ITEM_COLUMNS)
        order_writer.writeheader()
        item_writer.writeheader()
        for index in range(1, orders + 1):
            order_id = f"O{index:09d}"
            customer_id = f"C{rng.randint(1, customers):07d}"
            ordered = order_epoch + timedelta(seconds=rng.randrange(180 * 86400))
            updated = ordered + timedelta(seconds=rng.randrange(14 * 86400))
            status = rng.choice(ORDER_STATUSES)
            selected = rng.sample(catalogue, rng.randint(1, min(4, len(catalogue))))
            total_cents = 0
            for line_number, (product_id, unit_price_cents) in enumerate(selected, start=1):
                item_index += 1
                quantity = rng.randint(1, 5)
                total_cents += quantity * unit_price_cents
                item_writer.writerow({
                    "order_item_id": f"OI{item_index:010d}", "order_id": order_id,
                    "line_number": line_number, "product_id": product_id,
                    "quantity": quantity, "unit_price": money(unit_price_cents),
                    "updated_at": timestamp(updated),
                })
            order_writer.writerow({
                "order_id": order_id, "customer_id": customer_id,
                "order_date": timestamp(ordered), "status": status,
                "currency": "USD", "order_total": money(total_cents),
                "updated_at": timestamp(updated),
            })
            if index % 25000 == 0:
                LOG.info("Generated %s orders and %s order items", f"{index:,}", f"{item_index:,}")
    return {"customers": customers, "orders": orders, "order_items": item_index}
