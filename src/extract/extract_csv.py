"""Validate CSV structure and ingest row-by-row without changing source field values."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.common import METADATA_COLUMNS, PipelineError, sha256_file
from src.contracts import CSV_CONTRACTS


def ingest_csv(source: Path, destination: Path, table: str, batch_id: str, ingested_at: str) -> dict[str, Any]:
    expected = CSV_CONTRACTS[table]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("r", encoding="utf-8-sig", newline="") as source_handle:
        reader = csv.DictReader(source_handle, strict=True)
        header = reader.fieldnames or []
        if len(header) != len(set(header)) or set(header) != set(expected):
            raise PipelineError(f"{table}: expected columns {list(expected)}; found {header}.")
        count = 0
        with destination.open("x", encoding="utf-8", newline="") as raw_handle:
            writer = csv.DictWriter(raw_handle, fieldnames=(*expected, *METADATA_COLUMNS))
            writer.writeheader()
            for count, row in enumerate(reader, start=1):
                if None in row or any(value is None for value in row.values()):
                    raise PipelineError(f"{table}: wrong number of fields at CSV record {count}.")
                writer.writerow({
                    **row, "_batch_id": batch_id, "_ingested_at": ingested_at,
                    "_source": "synthetic_csv_generator_v1",
                })
        if count == 0:
            raise PipelineError(f"{table}: CSV has no data rows.")
    return {"rows": count, "source_file": source.name, "source_sha256": sha256_file(source)}
