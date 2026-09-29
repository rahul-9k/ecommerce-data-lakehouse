from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.cloud.s3_publish import build_inventory, safe_prefix


class Phase6PureTests(unittest.TestCase):
    def make_file(self, root: Path, rel: str, data: bytes = b"x"):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def test_prefix_validation(self):
        self.assertEqual(safe_prefix("a/b/"), "a/b")
        for bad in ("", "../x", "a/../b", r"a\\b"):
            with self.assertRaises(Exception):
                safe_prefix(bad)

    def test_inventory_maps_layers_and_excludes_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_file(root, "data/raw/initial_001/manifest.json", b"raw")
            self.make_file(root, "data/processed/initial_001/orders/part.parquet", b"pq")
            change = {"status": "success", "phase": 4}
            self.make_file(root, "data/changes/delta_001/manifest.json", json.dumps(change).encode())
            self.make_file(root, "data/changes/delta_001/events.jsonl", b"{}\n")
            self.make_file(root, "reports/phase5_latest.json", b"{}")
            self.make_file(root, ".env.phase3", b"SECRET=x")
            items = build_inventory(root, "project/v1")
            keys = {i.key for i in items}
            self.assertIn("project/v1/raw/initial_001/manifest.json", keys)
            self.assertIn("project/v1/curated/initial_001/orders/part.parquet", keys)
            self.assertIn("project/v1/change_feed/delta_001/events.jsonl", keys)
            self.assertIn("project/v1/metadata/reports/phase5_latest.json", keys)
            self.assertFalse(any(".env" in k for k in keys))

    def test_incomplete_change_directory_is_not_uploaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_file(root, "data/raw/initial_001/manifest.json")
            self.make_file(root, "data/processed/initial_001/x.parquet")
            self.make_file(root, "data/changes/.building_bad/events.jsonl")
            items = build_inventory(root, "project/v1")
            self.assertFalse(any("change_feed" in i.key for i in items))


if __name__ == "__main__":
    unittest.main()
