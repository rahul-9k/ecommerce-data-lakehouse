"""Daily incremental e-commerce warehouse orchestration.

The DAG deliberately orchestrates the already-tested Phase 4 CLI instead of
reimplementing its transactional loading logic inside Airflow tasks.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

DAG_ID = "ecommerce_incremental_pipeline"
BATCH_ID_JINJA = "{{ dag_run.conf.get('batch_id') if dag_run and dag_run.conf.get('batch_id') else 'airflow_' ~ logical_date.strftime('%Y%m%dT%H%M%S') }}"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    description="Generate, apply, verify and publish a small idempotent warehouse delta.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["portfolio", "incremental", "postgres", "dbt"],
) as dag:
    preflight = BashOperator(
        task_id="preflight",
        bash_command="/opt/airflow/project-venv/bin/python /workspace/src/orchestration/phase5.py --preflight",
    )

    generate_delta = BashOperator(
        task_id="generate_delta",
        bash_command=(
            "set -euo pipefail; export PATH=/opt/airflow/project-venv/bin:$PATH; "
            f"BATCH_ID=\"{BATCH_ID_JINJA}\"; "
            "/opt/airflow/project-venv/bin/python /workspace/src/run_phase4.py --generate-demo --batch-id \"$BATCH_ID\" "
            "--new-orders 10 --status-updates 2 --quantity-corrections 1"
        ),
    )

    apply_delta = BashOperator(
        task_id="apply_delta",
        bash_command=(
            "set -euo pipefail; export PATH=/opt/airflow/project-venv/bin:$PATH; "
            f"BATCH_ID=\"{BATCH_ID_JINJA}\"; "
            "/opt/airflow/project-venv/bin/python /workspace/src/run_phase4.py --batch-id \"$BATCH_ID\""
        ),
    )

    verify_delta = BashOperator(
        task_id="verify_delta",
        bash_command=(
            "set -euo pipefail; export PATH=/opt/airflow/project-venv/bin:$PATH; "
            f"BATCH_ID=\"{BATCH_ID_JINJA}\"; "
            "/opt/airflow/project-venv/bin/python /workspace/src/run_phase4.py --verify-only --batch-id \"$BATCH_ID\""
        ),
    )

    publish_phase5_report = BashOperator(
        task_id="publish_phase5_report",
        bash_command=(
            "set -euo pipefail; export PATH=/opt/airflow/project-venv/bin:$PATH; "
            f"BATCH_ID=\"{BATCH_ID_JINJA}\"; "
            "/opt/airflow/project-venv/bin/python /workspace/src/orchestration/phase5.py --finalize --batch-id \"$BATCH_ID\""
        ),
    )

    preflight >> generate_delta >> apply_delta >> verify_delta >> publish_phase5_report
