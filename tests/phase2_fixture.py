"""Tiny handcrafted Phase 1-format fixture; never a claim of real transaction data."""
from __future__ import annotations
import csv
import json
from pathlib import Path
from src.common import METADATA_COLUMNS, inventory, write_json
from src.contracts import CSV_CONTRACTS


def write_fixture(data_dir: Path, batch: str = "fixture_001") -> Path:
    raw = data_dir / "raw" / batch
    raw.mkdir(parents=True)
    meta = {"_batch_id": batch, "_ingested_at": "2026-09-29T10:00:00Z", "_source": "phase2_handcrafted_test_fixture"}
    products = [
        {"id": 1, "title": "  Keyboard  ", "category": " ELECTRONICS ", "price": 10.00},
        {"id": 2, "title": "Mouse", "category": "electronics", "price": 2.50},
        {"id": 3, "title": "Notebook", "category": "stationery", "price": 1.25},
    ]
    (raw / "products").mkdir()
    with (raw / "products" / "products.jsonl").open("w", encoding="utf-8") as handle:
        for row in products:
            handle.write(json.dumps({**row, **meta}) + "\n")
    tables = {
        "customers": [
            ["C1", 'Demo, "One"', " Austin ", "us", "2025-12-01T00:00:00Z", "2025-12-01T00:00:00Z"],
            ["C2", "Demo\nTwo", "Boston", "US", "2025-12-01T00:00:00Z", "2025-12-01T00:00:00Z"],
        ],
        "orders": [
            ["O1", "C1", "2026-01-05T10:00:00Z", "delivered", "USD", "25.00", "2026-01-06T10:00:00Z"],
            ["O2", "C2", "2026-02-05T10:00:00Z", "cancelled", "USD", "6.25", "2026-02-06T10:00:00Z"],
        ],
        "order_items": [
            ["I1", "O1", 1, 1, 2, "10.00", "2026-01-06T10:00:00Z"],
            ["I2", "O1", 2, 2, 2, "2.50", "2026-01-06T10:00:00Z"],
            ["I3", "O2", 1, 3, 3, "1.25", "2026-02-06T10:00:00Z"],
            ["I4", "O2", 2, 2, 1, "2.50", "2026-02-06T10:00:00Z"],
        ],
    }
    for table, rows in tables.items():
        (raw / table).mkdir()
        with (raw / table / f"{table}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([*CSV_CONTRACTS[table], *METADATA_COLUMNS])
            for row in rows:
                writer.writerow([*row, *meta.values()])
    quality = {"status": "passed", "row_counts": {"products": 3, "customers": 2, "orders": 2, "order_items": 4},
               "gross_order_value_usd_all_statuses": "31.25", "delivered_order_value_usd": "25.00"}
    write_json(raw / "quality_report.json", quality)
    write_json(raw / "manifest.json", {"format_version": 1, "status": "success", "batch_id": batch,
               "parameters": {"source_mode": "offline_fixture"}, "quality": quality,
               "data_provenance": {"all_records": "Handcrafted synthetic Phase 2 test fixture"}, "files": inventory(raw)})
    return raw
