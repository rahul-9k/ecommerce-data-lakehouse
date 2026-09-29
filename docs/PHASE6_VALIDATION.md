# Phase 6 validation

Author-side checks performed before packaging:

- Python syntax compilation for the S3 publisher/configuration scripts.
- Dependency-free tests for prefix validation, layer-to-key mapping and secret exclusion.
- No AWS API calls were made from the artifact-authoring environment.

The user's real Phase 6 validation is `run_phase6.cmd --verify-only`, which uses `HeadObject` to compare the selected local files with S3 object size and the stored SHA-256 metadata.
