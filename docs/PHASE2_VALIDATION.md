# Validation record and limits

## Actually executed in the package-building environment

- 13 Phase 2 pure-Python contract/filesystem tests: passed.
- 35 original Phase 1 regression tests: passed.
- Python syntax compilation for new source/tests: passed.
- YAML parsing and static checks of the runtime mounts and image: passed.

## Not executed there

- Docker Desktop on Windows, WSL setup, or CMD wrapper execution.
- Docker image pull or Spark session creation.
- The nine included Spark integration tests.
- A real raw-to-Parquet run or performance measurement on the user's batch.

The build environment had neither a usable Docker engine nor PySpark and could
not obtain PySpark over its network. Do not interpret successful pure tests or
skipped integration tests as evidence that the Spark pipeline has run.

## Tests to execute on the user's machine

```cmd
run_phase2.cmd --self-test
```

This requires all 22 tests (13 pure + 9 Spark) to pass with no skips. It runs only
handcrafted temporary fixtures. The Spark tests cover explicit types, string
normalization, exact-row deduplication, conflicting keys, money calculations,
foreign-key rejection, order-total rejection, line-grain preservation, partitioned
Parquet readback, verification/replay, and raw-file immutability.

Then run the actual initial_001 batch and its --verify-only command. Only those
results validate the user's actual data and local environment.

## Troubleshooting

- `docker` not recognized: install Docker Desktop, or reopen VS Code after install.
- Docker client exists but `docker info` fails: start Docker Desktop and wait for
  the Linux engine; check its displayed WSL/virtualization error.
- Docker image pull fails: check connection/proxy and keep the full error. Do not
  replace the image with an unreviewed third-party binary or disable TLS checks.
- `ModuleNotFoundError: src...`: files are nested one folder too deep, or Phase 1
  files were not preserved. `src/run_phase1.py`, `src/common.py`, and the new
  `src/run_phase2.py` must be together under the same root.
- No raw snapshot: use the completed Phase 1 batch ID, not a newly invented ID.
- Raw checksum mismatch: investigate the changed file. Do not rewrite the manifest
  just to bypass integrity checking.
- Self-test fails: stop before the real batch and inspect the first failure. Keep
  the terminal output and reports/phase2_selftest.json when present.
- JVM exited / memory errors: close heavy applications and check Docker's available
  resources. Do not respond by generating a larger dataset.
- A lock remains after a hard stop: confirm no corresponding container is running
  before touching `data/processed/.locks`. Normal failures clean up automatically.
- Existing output is incompatible: preserve it and select a new --output-id for
  the same raw batch; never regenerate the source as a workaround.

## Known scope limits

This stage fails on bad records rather than maintaining a quarantine table. It
handles only the four Phase 1 source contracts and uses complete snapshots. It
is a single-machine Spark learning project, not an audited production platform.
