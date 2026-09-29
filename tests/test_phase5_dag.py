from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


class Phase5DagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path("/opt/airflow/dags/ecommerce_incremental_pipeline.py")
        spec = importlib.util.spec_from_file_location("phase5_dag", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        cls.dag = module.dag

    def test_dag_identity_and_safety(self):
        self.assertEqual(self.dag.dag_id, "ecommerce_incremental_pipeline")
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_expected_tasks(self):
        self.assertEqual(
            set(self.dag.task_ids),
            {"preflight", "generate_delta", "apply_delta", "verify_delta", "publish_phase5_report"},
        )

    def test_dependency_chain(self):
        expected = {
            "preflight": {"generate_delta"},
            "generate_delta": {"apply_delta"},
            "apply_delta": {"verify_delta"},
            "verify_delta": {"publish_phase5_report"},
            "publish_phase5_report": set(),
        }
        for task_id, downstream in expected.items():
            self.assertEqual(self.dag.get_task(task_id).downstream_task_ids, downstream)


if __name__ == "__main__":
    unittest.main()
