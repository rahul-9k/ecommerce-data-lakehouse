FROM apache/airflow:3.3.2-python3.12

# Keep Airflow itself on its pinned runtime, adding only the Standard provider used by BashOperator.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-standard==1.19.0

# Project dependencies live in a separate immutable venv so dbt/pyarrow cannot perturb Airflow's dependencies.
COPY docker/phase5/requirements.txt /tmp/requirements-phase5.txt
RUN python -m venv /opt/airflow/project-venv \
    && /opt/airflow/project-venv/bin/pip install --no-cache-dir -r /tmp/requirements-phase5.txt \
    && /opt/airflow/project-venv/bin/python -m pip check
