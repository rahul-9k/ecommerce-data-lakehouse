# E-Commerce Data Pipeline - Phase 1

A local, testable extraction and ingestion pipeline built as the first stage of a
larger data-engineering portfolio project.

## Implemented now

- Paginated REST extraction of the DummyJSON product catalogue.
- Retention of original API response payloads plus a metadata-enriched JSONL file.
- Reproducible synthetic customers, orders and order-item CSV source files.
- CSV ingestion with source, batch ID and UTC ingestion-time metadata.
- Schema, primary-key, foreign-key, timestamp and order-total validation.
- Per-batch manifests with row counts, provenance and SHA-256 file checksums.
- Safe completed-batch replay: the same batch ID and parameters produce a verified no-op.
- Explicit offline test mode, automated tests and readable local logs.

## Not implemented yet

PySpark, Parquet output, PostgreSQL, dimensional warehouse models, dbt, S3,
Airflow, Docker and source-level incremental loads are **future phases**. This
repository's current folder name is a project name, not proof of a functioning
transactional lakehouse. There is no Delta Lake or Iceberg table format here.

## Data provenance

DummyJSON is a public test-data API. Its product records are placeholders, not a
live retailer's catalogue. Customers and transactions are generated locally and
are entirely synthetic. The generator interprets API price numbers as synthetic
USD amounts for a consistent exercise; the API response does not establish that
currency. No real customer data, revenue or employer data is used.

Offline mode uses 12 handcrafted fixture products, not downloaded API data.
Reports always distinguish `live_api` from `offline_fixture`.

## Windows CMD setup

Copy this package's contents into your existing `ecommerce-data-lakehouse`
folder. Merge the `src` and `tests` folders. Replace the empty starter README,
requirements file and .gitignore. Do not create a nested second project folder.
The package contains no `.git` directory, credentials or generated datasets.

The commands below use Python 3.12. The Phase 1 code supports Python 3.10+.
If the Windows launcher cannot find Python 3.12, use `py --list` to inspect the
available interpreters before creating the virtual environment.

```cmd
cd /d "C:\Users\rahul\Desktop\ecommerce-data-lakehouse"
py -3.12 --version
py -3.12 -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Use this project's new `.venv`, not an environment belonging to another project.
Only Requests is a third-party dependency in Phase 1. CSV, JSON, logging,
datetime, decimal, hashing and unittest are Python standard-library modules.

## 1. Run the offline test suite

```cmd
python -m unittest discover -s tests -v
```

Expected: **35 tests**, ending with `OK`. Tests do not call the real API. They
check simulated HTTP responses, generator determinism, CSV ingestion, integrity
rules, repeat-run safety and error handling. Retry warnings inside tests are
expected: they come from intentionally simulated failures.

## 2. Run the first real-API batch

```cmd
python src\run_phase1.py --batch-id initial_001 --customers 1000 --orders 10000
```

The coordinator fetches the complete product catalogue, generates source CSVs,
ingests the CSVs, validates the result and publishes the finished raw snapshot.
The product count depends on the API at run time. Each synthetic order has
between one and four distinct product lines, so order-item count is not fixed.

Success ends with `PHASE 1 COMPLETE`, `Source: live_api` and `Quality: PASSED`.
The API needs an internet connection, but not an API key for this endpoint.
There are no AWS resources and no cloud billing in this phase.

## 3. Inspect and independently verify the result

Open `reports/phase1_latest.json`, then the batch's `manifest.json` and
`quality_report.json` in VS Code. Compare a source CSV with its raw counterpart:
source fields are retained and three ingestion metadata columns are appended.

```cmd
python src\run_phase1.py --verify-only --batch-id initial_001
```

Verification checks raw-file hashes and reruns data-quality checks. It does not
call the API or regenerate anything. Do not edit committed raw files in place;
verification is intended to detect such changes.

## 4. Demonstrate safe replay

Repeat the exact original command:

```cmd
python src\run_phase1.py --batch-id initial_001 --customers 1000 --orders 10000
```

Expected `Action: already_complete`. The snapshot is checked but not rewritten;
there are no API requests. A changed count, seed or mode with the same ID is
rejected. Use a new batch ID for changed input parameters or a fresh API snapshot.

**This is local batch replay safety, not incremental extraction or exactly-once
stream processing.** A new batch ID still creates a full snapshot. A future
warehouse loader must merge/deduplicate repeated business keys across snapshots.

## Output structure

```text
data/
  source/
    initial_001/
      customers.csv
      orders.csv
      order_items.csv
  raw/
    initial_001/
      products/
        pages/page_0000.json
        ...
        products.jsonl
      customers/customers.csv
      orders/orders.csv
      order_items/order_items.csv
      quality_report.json
      manifest.json
reports/
  phase1_latest.json
logs/
  phase1_<timestamp>_<unique-id>.log
```

`data/source` represents the simulator's source system. `data/raw` is the
pipeline's published landing layer. Source files are retained for inspection;
the raw snapshot's inventory hashes its raw files and saved API payloads.
`data/processed` remains unused until the transformation phase.

## Optional: larger batch, after the small run succeeds

```cmd
python src\run_phase1.py --batch-id volume_001 --customers 10000 --orders 100000
```

Do not start with millions of rows. Correctness and reproducibility come before
size. The generator and CSV readers stream records, but the validator retains
key indexes and per-order totals in RAM; memory usage grows with dataset size.

## Explicit offline troubleshooting

When the public API is unreachable, an explicitly labelled offline run can
still exercise the local pipeline:

```cmd
python src\run_phase1.py --offline --batch-id offline_001 --customers 1000 --orders 10000
```

This is **not** evidence that live REST extraction succeeded. It uses only the
fixture under `tests/fixtures`. Never silently replace API failure with fixture
success. Preserve the error log and run the real-API command separately later.

## Git

The supplied .gitignore excludes `.venv`, generated data, logs, reports and
common secret files. Keep the small test fixture in Git.

```cmd
git status
git add .
git commit -m "Implement Phase 1 extraction and raw ingestion"
```

Before committing, inspect the staged files. Git user identity must already be
configured; a commit does not publish anything to GitHub.

## Learn and inspect the implementation

Start with `docs/PHASE1_GUIDE.md` and `docs/DATA_CONTRACTS.md`.
Read `src/run_phase1.py` to see the complete dependency order, then inspect each
function it calls. Build-time validation evidence is in `docs/VALIDATION.md`.

## Troubleshooting and limitations

- Run commands from the project root, with this project's environment activated.
- `ModuleNotFoundError: requests` means install the requirements in that environment.
- HTTP failures produce a nonzero exit code; preserve the console message/log.
  The latest report remains the previous successful run after a failed attempt.
- HTTPS verification remains enabled. Do not work around certificate failures
  by changing the code to `verify=False`.
- A hard-killed process can leave a lock or staging folder. First confirm the
  old process has stopped. Prefer a new batch ID rather than deleting unknown
  files. Handled exceptions clean up their own staging files.
- The final raw directory is published only after validation. The source-copy
  rename and raw rename are not a cross-filesystem/database transaction.
- Manifest checksums detect ordinary file changes, not malicious edits to both
  the data and manifest; manifests are not cryptographically signed.
- The API uses offset pagination. Count/offset/duplicate checks detect several
  consistency problems but cannot guarantee an upstream transactional snapshot.
- Known-good generated data is the first baseline. Deliberate bad-data fixtures,
  quarantine and corrective transformations belong to the next phase; this
  phase fails fast instead of silently deleting or repairing bad records.

## Official references

- DummyJSON product API: https://dummyjson.com/docs/products
- Requests quickstart and timeouts: https://docs.python-requests.org/en/latest/user/quickstart/
- Python virtual environments: https://docs.python.org/3.12/library/venv.html
- Python CSV handling: https://docs.python.org/3.12/library/csv.html
