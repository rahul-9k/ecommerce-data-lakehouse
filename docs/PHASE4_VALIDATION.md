# Phase 4 validation status

## Executed in the author's runtime

100 dependency-free tests passed, with zero failures/errors/skips:

- Phase 1: 35 tests.
- Phase 2 pure helpers: 13 tests.
- Phase 3 pure helpers: 20 tests.
- Phase 4 pure contracts/files: 32 tests.

Python compilation and Python 3.12-grammar parsing passed for all new source/test
files. YAML parsing passed. Package contents were checked not to overwrite any
existing file from the supplied Phase 1/2/3 packages. Windows wrapper uses CRLF.

The new tests cover fixed event grain, tied timestamps, exact decimal round trips,
required fields, source metadata, UTC handling, bad types/amounts, key/context
checks, sequence gaps/duplicates, stale versions, missing lines, unchanged
transaction prices, file integrity, safe paths and disjoint schemas.

## Not executed in the author's runtime

Docker Engine, PostgreSQL server, PyArrow, Psycopg and dbt were unavailable.
The new code has NOT been proven by an author-run PostgreSQL/dbt integration run.
Windows CMD and the actual user's initial_001 database were not executed here.
Do not mistake source compilation or pure unit tests for that validation.

## Included local integration suite: 14 tests

Run run_phase4.cmd --self-test. This enables actual PostgreSQL/dbt tests in the
existing Phase 3 container. It must report 46 total tests, OK, and zero skips.

The integration suite checks baseline copy/preservation, inserts/updates and
amount totals, replay, full storage rollback under injected failure, foreign-key
failure, changed-feed rejection, stale parent/sequence/version rejection,
cooperative writer locking, untracked changes, journal/checkpoint agreement, and
recovery after the landing transaction commits but the model stage fails.

The final recovery test injects a Python exception BEFORE a model invocation,
then performs real dbt builds/tests during recovery. A injected model failure is
not presented as successful dbt execution. Temporary baselines themselves are
also built/tested through Phase 3 before being used by these tests.

Only generated p4test_* schemas and temporary test directories are removed by
test cleanup. Your landing, analytics, pipeline_meta, landing_inc, analytics_inc
and pipeline_inc schemas are not the test targets.

A successful self-test still does not validate your real initial_001/delta_001.
Run bootstrap, generation, apply, verify and replay as documented, and inspect
reports/phase4_latest.json for that evidence.
