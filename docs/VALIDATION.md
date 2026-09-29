# Build-time validation

Validation date: 2026-09-29.

## Environment actually used

- OS: Linux, not Windows.
- Python: 3.13.5.
- Requests: 2.32.5; this satisfies the supplied `requests>=2.32.5,<3` range.
- User setup instructions target Windows CMD with Python 3.12. That exact
  Windows/Python combination has not been executed in this build environment.

## Checks completed

- Python syntax compilation passed for all source and test modules.
- 35 offline unit/integration tests passed.
- A complete offline pipeline run succeeded with 12 fixture products, 10,000
  customers, 100,000 orders and 249,490 order items.
- Schema, key, foreign-key, timestamp and order-total checks passed on that run.
- An independent `--verify-only` run passed, including file hashes and data rules.
- Repeating the 100,000-order run under the same batch ID returned
  `already_complete` without rewriting the snapshot.
- Recorded on this one Linux run: 7.561 seconds wall time, approximately
  208.07 MiB peak child-process memory and 66.79 MiB source/raw files.
  These are local measurements, not performance promises for a Windows laptop
  or a real API. The full synthetic data is not included in the ZIP.

## Live validation not completed

Outbound DNS/network access failed from the code-execution runtime, so the real
DummyJSON endpoint could not be fetched here. API behavior was checked against
its public documentation and pagination/retry/error paths were tested with
controlled simulated HTTP responses. The user must run the normal command
without `--offline` to validate live API access on their laptop.

The package never silently substitutes fixture data for a failed API request.
No AWS, Spark, database, Airflow or Docker integration is implemented or claimed
as tested in this phase.
