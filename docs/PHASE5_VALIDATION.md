# Phase 5 validation

Author-side checks performed before packaging:

- Python syntax compilation for all new Python files.
- Dependency-free unit tests for Phase 5 report/preflight logic.
- Static YAML parsing of the Compose file.
- Static inspection of the DAG dependency chain.

The Airflow image, PostgreSQL metadata database and real DAG execution cannot be run in the artifact-authoring environment. `run_phase5.cmd --self-test` therefore includes DAG-import tests that must pass on the user's Docker environment before the real trigger.
