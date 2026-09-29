"""Offline tests. No network, AWS, database, Spark or Docker is used."""
from __future__ import annotations

import copy
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from src.common import PROJECT_ROOT, PipelineError, read_json, sha256_file, validate_batch_id, verify_snapshot
from src.contracts import CUSTOMER_COLUMNS
from src.extract.extract_csv import ingest_csv
from src.extract.extract_products_api import extract_products, request_page, validate_product
from src.extract.generate_csv import generate_csv_sources
from src.quality import parse_money, parse_timestamp, validate_raw
from src.run_phase1 import FIXTURE, run_pipeline

STAMP = "2026-09-29T09:00:00Z"


def response_for(payload, status=200):
    response = requests.Response()
    response.status_code = status
    response.url = "https://dummyjson.com/products"
    response._content = json.dumps(payload).encode("utf-8")
    response._content_consumed = True
    response.encoding = "utf-8"
    response.headers["Retry-After"] = "0"
    return response


class TemporaryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.products = read_json(FIXTURE)["products"]


class ApiTests(TemporaryTest):
    def test_offline_extraction_keeps_payload_and_metadata(self):
        products, info = extract_products(self.root / "products", "test_001", STAMP, offline_fixture=FIXTURE)
        self.assertEqual(len(products), 12)
        self.assertEqual(info["source"], "local_test_fixture")
        page = self.root / "products" / "pages" / "page_0000.json"
        self.assertEqual(page.read_bytes(), FIXTURE.read_bytes())
        first = json.loads((self.root / "products" / "products.jsonl").read_text().splitlines()[0])
        self.assertEqual(first["_batch_id"], "test_001")
        self.assertEqual(first["_ingested_at"], STAMP)

    def test_pagination_reads_all_pages(self):
        session = Mock()
        session.get.side_effect = [
            response_for({"products": self.products[:2], "total": 3, "skip": 0}),
            response_for({"products": self.products[2:3], "total": 3, "skip": 2}),
        ]
        products, info = extract_products(self.root / "products", "test_001", STAMP, page_size=2, session=session)
        self.assertEqual(len(products), 3)
        self.assertEqual(info["pages"], 2)
        self.assertEqual(session.get.call_args_list[1].kwargs["params"], {"skip": 2, "limit": 2})
        self.assertEqual(session.get.call_args.kwargs["timeout"], (5, 30))

    def test_duplicate_api_ids_rejected(self):
        session = Mock()
        session.get.return_value = response_for({"products": [self.products[0]] * 2, "total": 2, "skip": 0})
        with self.assertRaisesRegex(PipelineError, "Duplicate"):
            extract_products(self.root / "products", "test", STAMP, session=session)

    def test_early_empty_page_rejected(self):
        session = Mock()
        session.get.return_value = response_for({"products": [], "total": 4, "skip": 0})
        with self.assertRaisesRegex(PipelineError, "empty page"):
            extract_products(self.root / "products", "test", STAMP, session=session)

    def test_changed_total_rejected(self):
        session = Mock()
        session.get.side_effect = [
            response_for({"products": self.products[:1], "total": 2, "skip": 0}),
            response_for({"products": self.products[1:2], "total": 3, "skip": 1}),
        ]
        with self.assertRaisesRegex(PipelineError, "Catalogue size changed"):
            extract_products(self.root / "products", "test", STAMP, session=session)

    def test_wrong_offset_rejected(self):
        session = Mock()
        session.get.return_value = response_for({"products": self.products, "total": 12, "skip": 2})
        with self.assertRaisesRegex(PipelineError, "offset"):
            extract_products(self.root / "products", "test", STAMP, session=session)

    def test_invalid_json_rejected(self):
        response = response_for({})
        response._content = b"not JSON"
        session = Mock()
        session.get.return_value = response
        with self.assertRaisesRegex(PipelineError, "invalid JSON"):
            extract_products(self.root / "products", "test", STAMP, session=session)

    def test_retry_temporary_http_error(self):
        session = Mock()
        session.get.side_effect = [response_for({}, 503), response_for({"ok": True})]
        with patch("src.extract.extract_products_api.time.sleep") as sleep:
            self.assertEqual(request_page(session, 0, 100).status_code, 200)
            self.assertEqual(sleep.call_count, 1)

    def test_http_404_not_retried(self):
        session = Mock()
        session.get.return_value = response_for({}, 404)
        with self.assertRaisesRegex(PipelineError, "HTTP 404"):
            request_page(session, 0, 100)
        self.assertEqual(session.get.call_count, 1)

    def test_timeout_retried_then_fails(self):
        session = Mock()
        session.get.side_effect = requests.Timeout("timeout")
        with patch("src.extract.extract_products_api.time.sleep"):
            with self.assertRaisesRegex(PipelineError, "after retries"):
                request_page(session, 0, 100)
        self.assertEqual(session.get.call_count, 4)

    def test_ssl_failure_does_not_disable_verification(self):
        session = Mock()
        session.get.side_effect = requests.exceptions.SSLError("bad certificate")
        with self.assertRaisesRegex(PipelineError, "Do not disable"):
            request_page(session, 0, 100)
        self.assertEqual(session.get.call_count, 1)
        self.assertNotIn("verify", session.get.call_args.kwargs)

    def test_invalid_price_and_id_rejected(self):
        for changes in ({"price": "NaN"}, {"price": -1}, {"id": True}, {"id": 0}, {"title": ""}):
            with self.subTest(changes=changes):
                with self.assertRaises(PipelineError):
                    validate_product({**self.products[0], **changes})


class GenerationTests(TemporaryTest):
    def test_same_seed_and_catalogue_produce_same_source_bytes(self):
        first, second = self.root / "first", self.root / "second"
        counts = generate_csv_sources(first, self.products, 20, 100, 42)
        other = generate_csv_sources(second, list(reversed(self.products)), 20, 100, 42)
        self.assertEqual(counts, other)
        for table in ("customers", "orders", "order_items"):
            self.assertEqual(sha256_file(first / f"{table}.csv"), sha256_file(second / f"{table}.csv"))

    def test_different_seed_changes_data(self):
        generate_csv_sources(self.root / "first", self.products, 20, 100, 42)
        generate_csv_sources(self.root / "second", self.products, 20, 100, 43)
        self.assertNotEqual(sha256_file(self.root / "first" / "orders.csv"), sha256_file(self.root / "second" / "orders.csv"))

    def test_bad_counts_rejected(self):
        for customers, orders in ((0, 10), (10, 0), (-1, 10)):
            with self.subTest(customers=customers, orders=orders):
                with self.assertRaises(PipelineError):
                    generate_csv_sources(self.root / "bad", self.products, customers, orders)

    def test_csv_ingestion_preserves_source_values_and_adds_metadata(self):
        source = self.root / "customers.csv"
        row = dict(zip(CUSTOMER_COLUMNS, ["C1", 'Customer, "Example"', "Austin", "US", STAMP, STAMP]))
        with source.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=CUSTOMER_COLUMNS)
            writer.writeheader()
            writer.writerow(row)
        target = self.root / "raw" / "customers.csv"
        ingest_csv(source, target, "customers", "batch", STAMP)
        with target.open(newline="", encoding="utf-8") as handle:
            actual = next(csv.DictReader(handle))
        self.assertEqual({key: actual[key] for key in CUSTOMER_COLUMNS}, row)
        self.assertEqual(actual["_source"], "synthetic_csv_generator_v1")

    def test_missing_header_rejected(self):
        source = self.root / "customers.csv"
        source.write_text("customer_id,city\nC1,Austin\n")
        with self.assertRaisesRegex(PipelineError, "expected columns"):
            ingest_csv(source, self.root / "raw.csv", "customers", "batch", STAMP)

    def test_extra_csv_value_rejected(self):
        source = self.root / "customers.csv"
        source.write_text(",".join(CUSTOMER_COLUMNS) + "\nC1,Name,Austin,US," + STAMP + "," + STAMP + ",unexpected\n")
        with self.assertRaisesRegex(PipelineError, "wrong number of fields"):
            ingest_csv(source, self.root / "raw.csv", "customers", "batch", STAMP)

    def test_money_rejects_negative_float_nan_and_excess_precision(self):
        for value in ("-1.00", "NaN", "1.001", "1", "1.0"):
            with self.subTest(value=value):
                with self.assertRaises(PipelineError):
                    parse_money(value, "test")

    def test_timestamp_requires_timezone(self):
        with self.assertRaises(PipelineError):
            parse_timestamp("2026-01-01T00:00:00", "test")


class PipelineTests(TemporaryTest):
    def setUp(self):
        super().setUp()
        self.data = self.root / "data"
        self.reports = self.root / "reports"
        self.batch = "test_001"

    def run_small(self, **changes):
        arguments = dict(data_dir=self.data, reports_dir=self.reports, batch_id=self.batch,
                         customers=20, orders=100, seed=42, offline=True)
        arguments.update(changes)
        return run_pipeline(**arguments)

    def mutate_csv(self, table, mutation):
        path = self.data / "raw" / self.batch / table / f"{table}.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            rows = list(reader)
        mutation(rows)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_end_to_end_offline_counts_and_provenance(self):
        summary = self.run_small()
        self.assertEqual(summary["action"], "created")
        self.assertEqual(summary["row_counts"]["customers"], 20)
        self.assertEqual(summary["row_counts"]["orders"], 100)
        self.assertTrue(100 <= summary["row_counts"]["order_items"] <= 400)
        self.assertEqual(summary["source_mode"], "offline_fixture")
        self.assertEqual(verify_snapshot(self.data / "raw" / self.batch)["quality"]["status"], "passed")
        self.assertTrue((self.data / "source" / self.batch / "orders.csv").is_file())

    def test_same_batch_is_verified_noop(self):
        self.run_small()
        manifest = self.data / "raw" / self.batch / "manifest.json"
        before = manifest.read_bytes()
        with patch("src.run_phase1.extract_products") as extract:
            result = self.run_small()
        extract.assert_not_called()
        self.assertEqual(result["action"], "already_complete")
        self.assertEqual(manifest.read_bytes(), before)

    def test_same_batch_different_parameters_rejected(self):
        self.run_small()
        with self.assertRaisesRegex(PipelineError, "different parameters"):
            self.run_small(orders=101)

    def test_tampered_file_rejected(self):
        self.run_small()
        path = self.data / "raw" / self.batch / "orders" / "orders.csv"
        with path.open("a") as handle:
            handle.write("corrupted\n")
        with self.assertRaisesRegex(PipelineError, "changed after publication"):
            self.run_small()

    def test_unexpected_extra_file_rejected(self):
        self.run_small()
        (self.data / "raw" / self.batch / "unexpected.txt").write_text("extra")
        with self.assertRaisesRegex(PipelineError, "inventory"):
            verify_snapshot(self.data / "raw" / self.batch)

    def test_foreign_key_failure(self):
        self.run_small()
        self.mutate_csv("orders", lambda rows: rows[0].update(customer_id="missing"))
        with self.assertRaisesRegex(PipelineError, "unknown customer_id"):
            validate_raw(self.data / "raw" / self.batch, self.batch)

    def test_product_foreign_key_failure(self):
        self.run_small()
        self.mutate_csv("order_items", lambda rows: rows[0].update(product_id="9999999"))
        with self.assertRaisesRegex(PipelineError, "unknown product_id"):
            validate_raw(self.data / "raw" / self.batch, self.batch)

    def test_duplicate_order_item_rejected(self):
        self.run_small()
        self.mutate_csv("order_items", lambda rows: rows.append(copy.deepcopy(rows[0])))
        with self.assertRaisesRegex(PipelineError, "Duplicate order_item_id"):
            validate_raw(self.data / "raw" / self.batch, self.batch)

    def test_duplicate_order_line_grain_rejected(self):
        self.run_small()
        def add_row(rows):
            extra = copy.deepcopy(rows[0])
            extra["order_item_id"] = "OI_OTHER"
            rows.append(extra)
        self.mutate_csv("order_items", add_row)
        with self.assertRaisesRegex(PipelineError, "line_number"):
            validate_raw(self.data / "raw" / self.batch, self.batch)

    def test_order_total_mismatch_rejected(self):
        self.run_small()
        self.mutate_csv("orders", lambda rows: rows[0].update(order_total="9999999.00"))
        with self.assertRaisesRegex(PipelineError, "total mismatch"):
            validate_raw(self.data / "raw" / self.batch, self.batch)

    def test_failure_does_not_publish_snapshot_or_replace_latest_report(self):
        self.run_small()
        old_report = (self.reports / "phase1_latest.json").read_bytes()
        with patch("src.run_phase1.validate_raw", side_effect=PipelineError("forced validation failure")):
            with self.assertRaises(PipelineError):
                self.run_small(batch_id="failed_001")
        self.assertFalse((self.data / "raw" / "failed_001").exists())
        self.assertFalse((self.data / "source" / "failed_001").exists())
        self.assertEqual((self.reports / "phase1_latest.json").read_bytes(), old_report)
        self.assertFalse((self.data / ".locks" / "failed_001").exists())

    def test_locked_batch_fails_without_overwriting_lock(self):
        lock = self.data / ".locks" / self.batch
        lock.mkdir(parents=True)
        with self.assertRaisesRegex(PipelineError, "locked"):
            self.run_small()
        self.assertTrue(lock.exists())

    def test_path_traversal_and_windows_device_names_rejected(self):
        for name in ("../escape", "a/b", "a\\b", "..", "NUL", "CON", "COM1", "bad name"):
            with self.subTest(name=name):
                with self.assertRaises(PipelineError):
                    validate_batch_id(name)

    def test_verify_only_cli(self):
        self.run_small()
        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "src" / "run_phase1.py"), "--verify-only",
             "--batch-id", self.batch, "--data-dir", str(self.data), "--reports-dir", str(self.reports)],
            cwd=self.root, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("verified", result.stdout)

    def test_cli_offline_from_different_working_directory(self):
        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "src" / "run_phase1.py"), "--offline",
             "--batch-id", "cli_001", "--customers", "5", "--orders", "10",
             "--data-dir", str(self.data), "--reports-dir", str(self.reports)],
            cwd=self.root, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PHASE 1 COMPLETE", result.stdout)


if __name__ == "__main__":
    unittest.main()
