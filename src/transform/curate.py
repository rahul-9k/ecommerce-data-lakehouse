"""Typed, validated raw-to-Parquet transformations for the Phase 1 contracts.

Keep source data immutable. Remove only EXACT normalized duplicate records;
conflicting records for one business key are errors, not arbitrary winners.
No pandas, Python UDFs, internet access, or full-table collect() are needed.
"""
from __future__ import annotations

import csv
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.common import METADATA_COLUMNS, PipelineError
from src.contracts import CSV_CONTRACTS, ORDER_STATUSES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

TABLES = ("products", "customers", "orders", "order_items")
KEYS = {"products": "product_id", "customers": "customer_id", "orders": "order_id", "order_items": "order_item_id"}
PARTITION_COLUMNS = ("order_year", "order_month")


def start_spark() -> SparkSession:
    """Two local threads, UTC timestamps, four shuffle partitions; not a cluster."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("ecommerce-phase2")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    if spark.version != "4.0.4":
        raise PipelineError(f"Expected Spark 4.0.4, found {spark.version}. Use the provided Docker image.")
    return spark


def check_header(path: Path, table: str) -> None:
    expected = [*CSV_CONTRACTS[table], *METADATA_COLUMNS]
    with path.open("r", encoding="utf-8", newline="") as handle:
        actual = next(csv.reader(handle, strict=True), None)
    if actual != expected:
        raise PipelineError(f"Unexpected {table} CSV header: expected {expected}; got {actual}.")


def read_csv(spark: SparkSession, raw_dir: Path, table: str) -> DataFrame:
    from pyspark.sql.types import StringType, StructField, StructType
    path = raw_dir / table / f"{table}.csv"
    check_header(path, table)
    schema = StructType([StructField(c, StringType(), True) for c in (*CSV_CONTRACTS[table], *METADATA_COLUMNS)])
    # Python's CSV writer doubles embedded quotes. multiLine allows quoted newlines.
    return (spark.read.schema(schema).option("header", True).option("enforceSchema", False)
            .option("mode", "FAILFAST").option("encoding", "UTF-8").option("multiLine", True)
            .option("quote", '"').option("escape", '"').csv(str(path)))


def metadata() -> list:
    from pyspark.sql import functions as F
    return [F.col("_batch_id"), F.to_timestamp("_ingested_at").alias("_ingested_at"), F.col("_source")]


def read_and_type(spark: SparkSession, raw_dir: Path) -> dict[str, DataFrame]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType
    product_schema = StructType([
        StructField("id", LongType()), StructField("title", StringType()),
        StructField("category", StringType()), StructField("price", DecimalType(18, 2)),
        *[StructField(c, StringType()) for c in METADATA_COLUMNS],
    ])
    products = (spark.read.schema(product_schema).option("mode", "FAILFAST")
                .json(str(raw_dir / "products" / "products.jsonl")))
    products = products.select(
        F.col("id").alias("product_id"), F.trim("title").alias("product_name"),
        F.lower(F.trim("category")).alias("category"), F.col("price").alias("catalog_price"), *metadata(),
    )
    customers = read_csv(spark, raw_dir, "customers").select(
        F.trim("customer_id").alias("customer_id"), F.trim("customer_name").alias("customer_name"),
        F.trim("city").alias("city"), F.upper(F.trim("country")).alias("country"),
        F.to_timestamp("created_at").alias("created_at"), F.to_timestamp("updated_at").alias("updated_at"), *metadata(),
    )
    orders = read_csv(spark, raw_dir, "orders").select(
        F.trim("order_id").alias("order_id"), F.trim("customer_id").alias("customer_id"),
        F.to_timestamp("order_date").alias("order_date"), F.lower(F.trim("status")).alias("status"),
        F.upper(F.trim("currency")).alias("currency"), F.col("order_total").cast("decimal(28,2)").alias("order_total"),
        F.to_timestamp("updated_at").alias("updated_at"), *metadata(),
    ).withColumn("order_year", F.year("order_date")).withColumn("order_month", F.month("order_date"))
    items = read_csv(spark, raw_dir, "order_items").select(
        F.trim("order_item_id").alias("order_item_id"), F.trim("order_id").alias("order_id"),
        F.col("line_number").cast("int").alias("line_number"), F.col("product_id").cast("bigint").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"), F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.to_timestamp("updated_at").alias("updated_at"), *metadata(),
    ).withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(28,2)"))
    return {"products": products, "customers": customers, "orders": orders, "order_items": items}


def require_no_rows(frame: DataFrame, message: str) -> None:
    # Existence check, not a full-table collect; include no personal record contents.
    if frame.limit(1).count():
        raise PipelineError(message)


def require_unique(frame: DataFrame, columns: list[str], table: str) -> None:
    from pyspark.sql import functions as F
    duplicates = frame.groupBy(*columns).count().filter(F.col("count") > 1)
    require_no_rows(duplicates, f"{table}: conflicting/duplicate key {columns}.")


def exact_deduplicate(frame: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    before = frame.count()
    result = frame.dropDuplicates()
    after = result.count()
    return result, {"input_rows": before, "output_rows": after, "exact_duplicates_removed": before - after}


def validate_tables(tables: dict[str, DataFrame], batch_id: str) -> dict[str, Any]:
    """Validate both pre-write tables and the Parquet readback; fail, don't silently drop."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    counts: dict[str, int] = {}
    for table, frame in tables.items():
        counts[table] = frame.count()
        if not counts[table]:
            raise PipelineError(f"{table}: no rows.")
        missing = [F.col(c).isNull() for c in frame.columns]
        missing += [F.length(F.trim(F.col(f.name))) == 0 for f in frame.schema.fields if isinstance(f.dataType, StringType)]
        require_no_rows(frame.filter(reduce(lambda a, b: a | b, missing)), f"{table}: null/empty field or failed cast.")
        require_no_rows(frame.filter(F.col("_batch_id") != batch_id), f"{table}: batch provenance mismatch.")
        require_unique(frame, [KEYS[table]], table)
    p, c, o, i = (tables[t] for t in TABLES)
    require_no_rows(p.filter((F.col("product_id") <= 0) | (F.col("catalog_price") < 0)), "products: invalid ID/price.")
    require_no_rows(c.filter(F.col("updated_at") < F.col("created_at")), "customers: invalid timestamps.")
    require_no_rows(o.filter(~F.col("status").isin(*ORDER_STATUSES) | (F.col("currency") != "USD") |
                            (F.col("order_total") < 0) | (F.col("updated_at") < F.col("order_date"))),
                    "orders: invalid status, currency, amount or timestamps.")
    require_no_rows(i.filter((F.col("quantity") <= 0) | (F.col("line_number") <= 0) | (F.col("product_id") <= 0) |
                            (F.col("unit_price") < 0) |
                            (F.col("line_amount") != (F.col("quantity") * F.col("unit_price")).cast("decimal(28,2)"))),
                    "order_items: invalid quantity, line number, price or line amount.")
    require_unique(i, ["order_id", "line_number"], "order_items")
    require_no_rows(o.join(c.select("customer_id"), "customer_id", "left_anti"), "orders: unknown customer.")
    require_no_rows(i.join(o.select("order_id"), "order_id", "left_anti"), "order_items: unknown order.")
    require_no_rows(i.join(p.select("product_id"), "product_id", "left_anti"), "order_items: unknown product.")
    customer_times = c.select("customer_id", F.col("created_at").alias("customer_created_at"))
    require_no_rows(o.join(customer_times, "customer_id").filter(F.col("order_date") < F.col("customer_created_at")),
                    "orders: order predates customer creation.")
    order_times = o.select("order_id", F.col("order_date").alias("parent_order_date"))
    require_no_rows(i.join(order_times, "order_id").filter(F.col("updated_at") < F.col("parent_order_date")),
                    "order_items: update predates order.")
    totals = i.groupBy("order_id").agg(F.sum("line_amount").alias("item_total"))
    reconciled = o.join(totals, "order_id", "left")
    require_no_rows(reconciled.filter(F.col("item_total").isNull() | (F.col("order_total") != F.col("item_total"))),
                    "orders: missing items or order total != sum(line amounts).")
    for name in ("orders", "order_items"):
        frame = tables[name]
        if "order_date" in frame.columns:
            require_no_rows(frame.filter((F.col("order_year") != F.year("order_date")) |
                                         (F.col("order_month") != F.month("order_date"))),
                            f"{name}: partition values disagree with order date.")
    # Enriched line context must still agree with the order; row counts alone cannot prove this.
    if "customer_id" in i.columns:
        parents = o.select("order_id", F.col("customer_id").alias("parent_customer"),
                           F.col("status").alias("parent_status"), F.col("currency").alias("parent_currency"),
                           F.col("order_date").alias("parent_date"))
        joined = i.join(parents, "order_id")
        require_no_rows(joined.filter((F.col("customer_id") != F.col("parent_customer")) |
                                     (F.col("order_status") != F.col("parent_status")) |
                                     (F.col("currency") != F.col("parent_currency")) |
                                     (F.col("order_date") != F.col("parent_date"))),
                        "order_items: enriched context disagrees with orders.")
    values = o.agg(F.sum("order_total").alias("gross"),
                   F.sum(F.when(F.col("status") == "delivered", F.col("order_total")).otherwise(F.lit(0))).alias("delivered")).first()
    return {
        "status": "passed", "row_counts": counts,
        "gross_order_value_usd_all_statuses": format(values["gross"], ".2f"),
        "delivered_order_value_usd": format(values["delivered"], ".2f"),
        "checks": ["typed nonempty fields", "unique primary and line keys", "foreign keys", "valid timestamps",
                   "positive quantities and exact decimal arithmetic", "order/item total reconciliation",
                   "batch provenance", "partition dates and enriched order context"],
        "note": "Synthetic order values, not recognized revenue; cancelled orders retained with their status.",
    }


def enrich_items(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    from pyspark.sql import functions as F
    context = tables["orders"].select("order_id", "customer_id", "order_date",
                                        F.col("status").alias("order_status"), "currency", *PARTITION_COLUMNS)
    enriched = dict(tables)
    enriched["order_items"] = tables["order_items"].join(context, "order_id", "inner")
    return enriched


def write_parquet(tables: dict[str, DataFrame], destination: Path) -> None:
    for table in TABLES:
        frame = tables[table]
        target = str(destination / table)
        if table in ("orders", "order_items"):
            # A teaching-scale monthly layout. Do not partition on order/customer IDs.
            (frame.repartition(4, *PARTITION_COLUMNS).write.mode("errorifexists")
             .partitionBy(*PARTITION_COLUMNS).parquet(target))
        else:
            frame.coalesce(1).write.mode("errorifexists").parquet(target)


def read_parquet(spark: SparkSession, directory: Path) -> dict[str, DataFrame]:
    return {table: spark.read.parquet(str(directory / table)) for table in TABLES}


def schema_types(frame: DataFrame) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in frame.schema.fields}
