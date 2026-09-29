"""Load one immutable snapshot with COPY; verify it; build/test dbt models.

The landing load is transactional. dbt uses separate transactions: derived
models are not advertised as complete until every test/reconciliation passes.
No upserts, CDC, historical dimensions, or cross-batch merges in this phase.
"""
from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any
from uuid import uuid4
from decimal import Decimal

from src.common import PROJECT_ROOT, PipelineError, read_json, sha256_file, utc_now, verify_snapshot
from src.load.warehouse_contracts import (AMOUNTS, CONTRACTS, KEYS, TABLES, RowDigest, Schemas,
    assess_dbt_results, ensure_compatible, normalize_value, read_processed)

LOG = logging.getLogger(__name__)


def fingerprint(kind: str) -> str:
    if kind == "loader":
        paths = [PROJECT_ROOT / p for p in ("src/load/warehouse.py", "src/load/warehouse_contracts.py", "src/common.py")]
    else:
        paths = [p for p in (PROJECT_ROOT / "dbt/ecommerce").rglob("*") if p.suffix in (".sql", ".yml")]
    result = hashlib.sha256()
    for path in sorted(paths):
        result.update(path.relative_to(PROJECT_ROOT).as_posix().encode())
        result.update(path.read_bytes().replace(b"\r\n", b"\n"))
    return result.hexdigest()


def connect():
    import psycopg
    # libpq PG* environment variables hold credentials; no password in arguments/logs.
    conn = psycopg.connect(autocommit=True, connect_timeout=10)
    conn.execute("SET TIME ZONE 'UTC'")
    conn.execute("SET statement_timeout = '120s'")
    conn.execute("SET lock_timeout = '10s'")
    return conn


def arrow_dataset(directory: Path, table: str):
    import pyarrow as pa
    import pyarrow.dataset as ds
    dataset = ds.dataset(str(directory / table), format="parquet", partitioning="hive")
    expected = {c: kind for c, kind, _ in CONTRACTS[table]}
    if set(dataset.schema.names) != set(expected) or len(dataset.schema.names) != len(expected):
        raise PipelineError(f"{table}: Parquet columns do not match the contract.")
    for field in dataset.schema:
        kind, typ = expected[field.name], field.type
        good = ((kind == "string" and pa.types.is_string(typ)) or
                (kind == "bigint" and pa.types.is_int64(typ)) or
                (kind == "int" and pa.types.is_int32(typ)) or
                (kind == "timestamp" and pa.types.is_timestamp(typ)) or
                (kind.startswith("decimal(") and pa.types.is_decimal(typ) and
                 (typ.precision, typ.scale) == (int(kind[8:].split(",")[0]), 2)))
        if not good:
            raise PipelineError(f"{table}.{field.name}: expected {kind}, found {typ}.")
    return dataset


def parquet_rows(directory: Path, table: str):
    columns = [c for c, _, _ in CONTRACTS[table]]
    scanner = arrow_dataset(directory, table).scanner(columns=columns, batch_size=4096, use_threads=False)
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            yield tuple(normalize_value(row[col]) for col in columns)


def relation_exists(conn, schema: str, table: str) -> bool:
    return conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s)",
                        (schema, table)).fetchone()[0]


def fetch_state(conn, schemas: Schemas) -> dict | None:
    from psycopg import sql
    from psycopg.rows import dict_row
    if not relation_exists(conn, schemas.meta, "snapshot"):
        return None
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql.SQL("SELECT * FROM {}.snapshot WHERE slot=1").format(sql.Identifier(schemas.meta)))
        return cur.fetchone()


def create_tables(conn, schemas: Schemas, batch_id: str) -> None:
    from psycopg import sql
    for name in (schemas.landing, schemas.analytics, schemas.meta):
        if conn.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=%s)", (name,)).fetchone()[0]:
            raise PipelineError(f"Schema {name} already exists without a valid snapshot record. Nothing overwritten.")
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    # Metadata, input rows and checks commit as one unit.
    conn.execute(sql.SQL('''CREATE TABLE {}.snapshot (
        slot integer PRIMARY KEY CHECK(slot=1), batch_id text NOT NULL, output_id text NOT NULL,
        source_manifest_sha256 text NOT NULL, loader_fingerprint text NOT NULL,
        model_fingerprint text, status text NOT NULL CHECK(status IN ('loaded','complete')),
        row_digests jsonb NOT NULL, source_manifest jsonb NOT NULL,
        loaded_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz
    )''').format(sql.Identifier(schemas.meta)))
    extra = {
        "products": ["CHECK (product_id > 0)", "CHECK (catalog_price >= 0)"],
        "customers": ["CHECK (updated_at >= created_at)"],
        "orders": ["CHECK (order_total >= 0)", "CHECK (currency = 'USD')",
                   "CHECK (status IN ('placed','shipped','delivered','cancelled'))",
                   "CHECK (updated_at >= order_date)", "CHECK (order_month BETWEEN 1 AND 12)"],
        "order_items": ["UNIQUE (order_id, line_number)", "CHECK (quantity > 0)", "CHECK (line_number > 0)",
                        "CHECK (unit_price >= 0)", "CHECK (line_amount = quantity * unit_price)",
                        "CHECK (currency='USD')", "CHECK (order_month BETWEEN 1 AND 12)",
                        "CHECK (order_status IN ('placed','shipped','delivered','cancelled'))",
                        "CHECK (updated_at >= order_date)"],
    }
    refs = {"orders": [("customer_id", "customers", "customer_id")],
            "order_items": [("order_id", "orders", "order_id"), ("product_id", "products", "product_id"),
                            ("customer_id", "customers", "customer_id")]}
    for table in TABLES:
        fields = [sql.SQL("{} {} NOT NULL").format(sql.Identifier(c), sql.SQL(pg)) for c, _, pg in CONTRACTS[table]]
        fields.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(KEYS[table])))
        fields.append(sql.SQL("CHECK (_batch_id = {})").format(sql.Literal(batch_id)))
        for col, kind, _ in CONTRACTS[table]:
            if kind == "string":
                fields.append(sql.SQL("CHECK (length(btrim({})) > 0)").format(sql.Identifier(col)))
        fields.extend(sql.SQL(check) for check in extra[table])
        for col, parent, key in refs.get(table, []):
            fields.append(sql.SQL("FOREIGN KEY ({}) REFERENCES {}.{} ({})").format(
                sql.Identifier(col), sql.Identifier(schemas.landing), sql.Identifier(parent), sql.Identifier(key)))
        conn.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schemas.landing), sql.Identifier(table), sql.SQL(", ").join(fields)))


def copy_table(conn, directory: Path, table: str, schemas: Schemas) -> dict:
    from psycopg import sql
    digest = RowDigest()
    columns = sql.SQL(", ").join(sql.Identifier(c) for c, _, _ in CONTRACTS[table])
    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(sql.Identifier(schemas.landing), sql.Identifier(table), columns)
    with conn.cursor() as cur:
        with cur.copy(statement) as copy:
            for row in parquet_rows(directory, table):
                copy.write_row(row)
                digest.add(row)
    return digest.result()


def database_digests(conn, schemas: Schemas) -> dict:
    from psycopg import sql
    results = {}
    # Caller supplies a transaction, so named cursors can stream bounded batches.
    for table in TABLES:
        digest = RowDigest()
        columns = sql.SQL(", ").join(sql.Identifier(c) for c, _, _ in CONTRACTS[table])
        with conn.cursor(name="check_" + uuid4().hex) as cur:
            cur.itersize = 4096
            cur.execute(sql.SQL("SELECT {} FROM {}.{}").format(columns, sql.Identifier(schemas.landing), sql.Identifier(table)))
            for row in cur:
                digest.add(row)
        results[table] = digest.result()
    return results


def validate_landing(conn, schemas: Schemas, manifest: dict, digests: dict) -> dict:
    from psycopg import sql
    actual = database_digests(conn, schemas)
    if actual != digests:
        raise PipelineError("PostgreSQL row values differ from the validated load. No automatic overwrite.")
    counts = {t: d["rows"] for t, d in actual.items()}
    if counts != manifest["quality"]["row_counts"]:
        raise PipelineError("PostgreSQL row counts differ from Phase 2.")
    s = sql.Identifier(schemas.landing)
    checks = [
        ('''SELECT o.order_id FROM {s}.orders o LEFT JOIN (
            SELECT order_id, sum(line_amount) AS total FROM {s}.order_items GROUP BY order_id
            ) i USING (order_id) WHERE o.order_total IS DISTINCT FROM i.total LIMIT 1''', "order totals"),
        ('''SELECT i.order_item_id FROM {s}.order_items i JOIN {s}.orders o USING(order_id)
            WHERE i.customer_id<>o.customer_id OR i.order_status<>o.status OR i.currency<>o.currency
               OR i.order_date<>o.order_date LIMIT 1''', "enriched order context"),
        ('''SELECT o.order_id FROM {s}.orders o JOIN {s}.customers c USING(customer_id)
            WHERE o.order_date<c.created_at LIMIT 1''', "customer creation time"),
    ]
    for table in ("orders", "order_items"):
        checks.append((f'''SELECT {KEYS[table]} FROM {{s}}.{table}
            WHERE order_year<>extract(year from order_date at time zone 'UTC')
               OR order_month<>extract(month from order_date at time zone 'UTC') LIMIT 1''', f"{table} partition dates"))
    for query, label in checks:
        if conn.execute(sql.SQL(query).format(s=s)).fetchone():
            raise PipelineError(f"PostgreSQL validation failed: {label}.")
    values = conn.execute(sql.SQL('''SELECT sum(order_total),
        coalesce(sum(order_total) FILTER(WHERE status='delivered'),0)
        FROM {}.orders''').format(s)).fetchone()
    for name, value in zip(AMOUNTS, values):
        if value != Decimal(str(manifest["quality"][name])):
            raise PipelineError(f"PostgreSQL amount mismatch against Parquet: {name}.")
    return counts


def load_snapshot(conn, directory: Path, manifest: dict, source_hash: str, schemas: Schemas,
                  require_existing: bool = False) -> tuple[str, dict]:
    from psycopg import sql
    from psycopg.types.json import Jsonb
    code_hash = fingerprint("loader")
    with conn.transaction():
        state = fetch_state(conn, schemas)
        if state is not None:
            ensure_compatible(state, source_hash, code_hash, manifest["batch_id"], manifest["output_id"])
            validate_landing(conn, schemas, manifest, state["row_digests"])
            return "already_loaded", state
        if require_existing:
            raise PipelineError("No completed warehouse load. Run the ordinary batch command first.")
        create_tables(conn, schemas, manifest["batch_id"])
        digests = {}
        for table in TABLES:
            LOG.info("COPY %s -> %s.%s", table, schemas.landing, table)
            digests[table] = copy_table(conn, directory, table, schemas)
            conn.execute(sql.SQL("ANALYZE {}.{}").format(sql.Identifier(schemas.landing), sql.Identifier(table)))
        validate_landing(conn, schemas, manifest, digests)
        # Source mounted read-only by Compose; this also detects edits during a direct/local run.
        verify_snapshot(directory)
        if sha256_file(directory / "manifest.json") != source_hash:
            raise PipelineError("Parquet manifest changed during loading; transaction rolled back.")
        conn.execute(sql.SQL('''INSERT INTO {}.snapshot
            (slot,batch_id,output_id,source_manifest_sha256,loader_fingerprint,status,row_digests,source_manifest)
            VALUES(1,%s,%s,%s,%s,'loaded',%s,%s)''').format(sql.Identifier(schemas.meta)),
            (manifest["batch_id"], manifest["output_id"], source_hash, code_hash, Jsonb(digests), Jsonb(manifest)))
        state = fetch_state(conn, schemas)
    return "created", state


def run_dbt(command: str, schemas: Schemas, reports: Path, log_root: Path) -> dict:
    import json
    executable = shutil.which("dbt")
    if not executable:
        raise PipelineError("dbt not installed in this runtime. Build/use the Phase 3 Docker image.")
    run_id = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
    target = (reports / "phase3/dbt" / run_id).resolve()
    logs = (log_root / "phase3/dbt" / run_id).resolve()
    target.mkdir(parents=True)
    logs.mkdir(parents=True)
    env = dict(os.environ, DBT_SCHEMA=schemas.analytics, DBT_TARGET_PATH=str(target),
               DBT_LOG_PATH=str(logs), DBT_PARTIAL_PARSE="false", DBT_USE_COLORS="false",
               DBT_SEND_ANONYMOUS_USAGE_STATS="false", PGOPTIONS="-c timezone=UTC")
    project = PROJECT_ROOT / "dbt/ecommerce"
    args = [executable, command, "--project-dir", str(project), "--profiles-dir", str(project),
            "--vars", json.dumps({"landing_schema": schemas.landing})]
    LOG.info("Running dbt %s (all models/tests, artifacts: %s)", command, target)
    completed = subprocess.run(args, env=env, cwd=project, timeout=900, check=False)
    if completed.returncode:
        raise PipelineError(f"dbt {command} failed (exit {completed.returncode}). Logs: {logs}. Landing data preserved; no success report issued.")
    result = assess_dbt_results(read_json(target / "run_results.json"), read_json(target / "manifest.json"), command)
    return {**result, "run_id": run_id, "artifacts_directory": str(target), "logs_directory": str(logs)}


def analytics_summary(conn, schemas: Schemas, manifest: dict) -> dict:
    from psycopg import sql
    schema = sql.Identifier(schemas.analytics)
    row_counts = {}
    for table in ("dim_customer", "dim_product", "dim_date", "fact_order_items", "mart_daily_sales"):
        row_counts[table] = conn.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(schema, sql.Identifier(table))).fetchone()[0]
    expected = manifest["quality"]["row_counts"]
    if (row_counts["dim_customer"] != expected["customers"] or row_counts["dim_product"] != expected["products"] or
            row_counts["fact_order_items"] != expected["order_items"]):
        raise PipelineError("Analytics row counts do not match the source.")
    values = conn.execute(sql.SQL('''SELECT count(distinct order_id), sum(line_amount),
        coalesce(sum(line_amount) FILTER(WHERE order_status='delivered'),0)
        FROM {}.fact_order_items''').format(schema)).fetchone()
    if values[0] != expected["orders"]:
        raise PipelineError("Order count changed in the fact table.")
    amounts = {name: format(value, ".2f") for name, value in zip(AMOUNTS, values[1:])}
    for key in AMOUNTS:
        if Decimal(amounts[key]) != Decimal(str(manifest["quality"][key])):
            raise PipelineError(f"Fact amount differs from Phase 2: {key}.")
    return {"row_counts": row_counts, "distinct_orders_in_fact": values[0], **amounts}


def preview(conn, schemas: Schemas) -> None:
    from psycopg import sql
    print("\nFIRST FIVE ORDER LINES (synthetic transaction values)")
    for row in conn.execute(sql.SQL('''SELECT order_item_id, order_id, quantity, unit_price,
            line_amount, order_status FROM {}.fact_order_items ORDER BY order_item_id LIMIT 5''').format(sql.Identifier(schemas.analytics))):
        print(" | ".join(str(x) for x in row))
    print("\nFIRST FIVE DAILY TOTALS (not recognized revenue)")
    for row in conn.execute(sql.SQL('''SELECT order_day, order_count, order_item_count,
            gross_order_value_usd_all_statuses, delivered_order_value_usd
            FROM {}.mart_daily_sales ORDER BY order_day LIMIT 5''').format(sql.Identifier(schemas.analytics))):
        print(" | ".join(str(x) for x in row))


def run_pipeline(data_dir: Path, reports: Path, batch_id: str, output_id: str | None = None,
                 verify_only: bool = False, show_preview: bool = False, rebuild_models: bool = False,
                 schemas: Schemas = Schemas(), log_root: Path | None = None) -> dict[str, Any]:
    from psycopg import sql
    start = time.perf_counter()
    logs = log_root or PROJECT_ROOT / "logs"
    if rebuild_models and (verify_only or show_preview):
        raise PipelineError("--rebuild-models cannot be combined with verification/preview.")
    LOG.info("[1/4] Verifying immutable Phase 2 files and explicit Parquet schemas")
    directory, manifest, source_hash = read_processed(data_dir, batch_id, output_id)
    for table in TABLES:
        arrow_dataset(directory, table)
    with connect() as conn:
        lock_key = int.from_bytes(hashlib.sha256(schemas.meta.encode()).digest()[:8], "big", signed=True)
        acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,)).fetchone()[0]
        if not acquired:
            raise PipelineError("Another Phase 3 process is using this warehouse; retry after it finishes.")
        try:
            LOG.info("[2/4] Loading or verifying PostgreSQL landing data")
            load_action, state = load_snapshot(conn, directory, manifest, source_hash, schemas, verify_only or show_preview)
            model_hash = fingerprint("models")
            complete = state["status"] == "complete" and state["model_fingerprint"] == model_hash
            if (verify_only or show_preview) and not complete:
                raise PipelineError("Models are incomplete or code changed; run the ordinary build first (use --rebuild-models for changed models).")
            if state["status"] == "complete" and not complete and not rebuild_models:
                raise PipelineError("dbt model files changed. Review changes, then use --rebuild-models to rebuild derived tables only.")
            command = "test" if complete and not rebuild_models else "build"
            if command == "build":
                conn.execute(sql.SQL("UPDATE {}.snapshot SET status='loaded' WHERE slot=1").format(sql.Identifier(schemas.meta)))
            LOG.info("[3/4] %s dbt models and complete data-test suite", "Building" if command == "build" else "Verifying")
            dbt_result = run_dbt(command, schemas, reports, logs)
            LOG.info("[4/4] Reconciling dimension/fact counts and transaction amounts")
            with conn.transaction():
                metrics = analytics_summary(conn, schemas, manifest)
                validate_landing(conn, schemas, manifest, state["row_digests"])
                if command == "build":
                    conn.execute(sql.SQL('''UPDATE {}.snapshot SET status='complete', model_fingerprint=%s,
                            completed_at=now() WHERE slot=1''').format(sql.Identifier(schemas.meta)), (model_hash,))
            if show_preview:
                preview(conn, schemas)
            action = ("verified" if verify_only else "inspected" if show_preview else "rebuilt_models" if rebuild_models
                      else "created" if load_action == "created" else "already_complete" if complete else "resumed")
            return {"status": "success", "phase": 3, "action": action, "batch_id": batch_id,
                    "output_id": manifest["output_id"], "source_mode": manifest["source_mode"],
                    "source_manifest_sha256": source_hash, "landing_row_counts": manifest["quality"]["row_counts"],
                    "analytics": metrics, "dbt": dbt_result, "quality_status": "passed", "completed_at": utc_now(),
                    "postgres_version": conn.execute("SHOW server_version").fetchone()[0],
                    "elapsed_seconds": round(time.perf_counter()-start, 3), "data_provenance": manifest.get("data_provenance", {}),
                    "scope": "Single immutable snapshot in local PostgreSQL plus dbt models/tests. Not incremental, CDC, SCD2, cloud, or Airflow."}
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
