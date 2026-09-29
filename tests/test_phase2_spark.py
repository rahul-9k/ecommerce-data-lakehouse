"""Real Spark tests: run with run_phase2.cmd --self-test.

Without pyspark they are explicitly skipped, not treated as validated Spark code.
"""
from __future__ import annotations
import importlib.util
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from phase2_fixture import write_fixture
from src.common import PipelineError, inventory
from src.run_phase2 import run_pipeline
from src.transform.curate import (enrich_items, exact_deduplicate, read_and_type, read_parquet,
                                  require_unique, schema_types, start_spark, validate_tables)


@unittest.skipUnless(importlib.util.find_spec("pyspark") is not None, "PySpark not installed; use the Docker self-test")
class Phase2SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import functions as F
        cls.F = F
        cls.temp = tempfile.TemporaryDirectory()
        cls.data = Path(cls.temp.name) / "data"
        cls.raw = write_fixture(cls.data)
        cls.spark = start_spark()
        cls.tables = read_and_type(cls.spark, cls.raw)
        for frame in cls.tables.values():
            frame.cache()

    @classmethod
    def tearDownClass(cls):
        for frame in cls.tables.values():
            frame.unpersist()
        cls.spark.stop()
        cls.temp.cleanup()

    def test_explicit_types_and_normalization(self):
        F = self.F
        product = self.tables["products"].filter(F.col("product_id") == 1).first()
        self.assertEqual(product["product_name"], "Keyboard")
        self.assertEqual(product["category"], "electronics")
        self.assertEqual(schema_types(self.tables["order_items"])["unit_price"], "decimal(18,2)")
        self.assertEqual(schema_types(self.tables["orders"])["order_date"], "timestamp")

    def test_line_amount_uses_transaction_price(self):
        item = self.tables["order_items"].filter(self.F.col("order_item_id") == "I3").first()
        self.assertEqual(item["line_amount"], Decimal("3.75"))
        # Items are not repriced by joining today's product catalogue.
        self.assertEqual(item["unit_price"], Decimal("1.25"))

    def test_exact_duplicate_rows_removed(self):
        clean, stats = exact_deduplicate(self.tables["products"].unionByName(self.tables["products"]))
        self.assertEqual(stats, {"input_rows": 6, "output_rows": 3, "exact_duplicates_removed": 3})
        require_unique(clean, ["product_id"], "products")

    def test_conflicting_duplicate_key_rejected(self):
        changed = self.tables["products"].withColumn("product_name", self.F.lit("Changed name"))
        combined, _ = exact_deduplicate(self.tables["products"].unionByName(changed))
        with self.assertRaises(PipelineError):
            require_unique(combined, ["product_id"], "products")

    def test_quality_and_cancelled_order_retained(self):
        quality = validate_tables(self.tables, "fixture_001")
        self.assertEqual(quality["gross_order_value_usd_all_statuses"], "31.25")
        self.assertEqual(quality["delivered_order_value_usd"], "25.00")
        self.assertEqual(quality["row_counts"]["orders"], 2)

    def test_broken_product_reference_rejected(self):
        broken = dict(self.tables)
        broken["order_items"] = broken["order_items"].withColumn("product_id", self.F.lit(999).cast("bigint"))
        with self.assertRaisesRegex(PipelineError, "unknown product"):
            validate_tables(broken, "fixture_001")

    def test_wrong_order_total_rejected(self):
        broken = dict(self.tables)
        broken["orders"] = broken["orders"].withColumn("order_total", self.F.col("order_total") + 1)
        with self.assertRaisesRegex(PipelineError, "order total"):
            validate_tables(broken, "fixture_001")

    def test_order_context_join_keeps_line_grain(self):
        items = enrich_items(self.tables)["order_items"]
        self.assertEqual(items.count(), 4)
        self.assertNotIn("order_total", items.columns)
        self.assertEqual(items.select("order_year", "order_month").distinct().count(), 2)

    def test_full_build_readback_replay_verify_and_raw_unchanged(self):
        before = inventory(self.raw)
        reports = Path(self.temp.name) / "reports"
        first = run_pipeline(self.data, reports, "fixture_001", spark_session=self.spark)
        self.assertEqual(first["action"], "created")
        output = self.data / "processed" / "fixture_001"
        committed = inventory(output)
        second = run_pipeline(self.data, reports, "fixture_001", spark_session=self.spark)
        self.assertEqual(second["action"], "already_complete")
        self.assertEqual(inventory(output), committed)
        third = run_pipeline(self.data, reports, "fixture_001", verify_only=True, spark_session=self.spark)
        self.assertEqual(third["action"], "verified")
        self.assertEqual(inventory(self.raw), before)
        self.assertEqual(third["row_counts"], {"products": 3, "customers": 2, "orders": 2, "order_items": 4})
        self.assertTrue(list(output.glob("order_items/order_year=2026/order_month=1/*.parquet")))
        self.assertTrue(list(output.glob("order_items/order_year=2026/order_month=2/*.parquet")))


if __name__ == "__main__":
    unittest.main()
