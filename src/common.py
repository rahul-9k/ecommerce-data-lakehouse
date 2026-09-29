"""Small filesystem, JSON, hashing, and timestamp utilities."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADATA_COLUMNS = ("_batch_id", "_ingested_at", "_source")


class PipelineError(Exception):
    """An actionable data-contract or pipeline error."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]


def validate_batch_id(value: str) -> str:
    # A batch ID becomes a directory name, so do not allow separators or traversal.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value):
        raise PipelineError("Batch ID must be 1-80 letters, digits, underscores or hyphens.")
    # Windows reserved device names cannot be used as directories.
    if re.fullmatch(r"(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])", value):
        raise PipelineError("Batch ID is a reserved Windows device name.")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    """Publish a JSON file using a temporary sibling, not a partial final file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def inventory(directory: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path.relative_to(directory).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]


def verify_snapshot(directory: Path) -> dict[str, Any]:
    """Verify committed raw-file bytes. This is integrity checking, not security signing."""
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        raise PipelineError(f"No completed snapshot found at {directory}.")
    manifest = read_json(manifest_path)
    if manifest.get("status") != "success" or manifest.get("format_version") != 1:
        raise PipelineError("Snapshot manifest is not a supported successful run.")
    expected_paths: set[str] = set()
    for entry in manifest.get("files", []):
        candidate = (directory / entry["path"]).resolve()
        if not candidate.is_relative_to(directory.resolve()):
            raise PipelineError("Invalid path outside the snapshot in manifest.")
        if entry["path"] in expected_paths:
            raise PipelineError("Duplicate path in snapshot manifest.")
        expected_paths.add(entry["path"])
        if not candidate.is_file():
            raise PipelineError(f"Snapshot file missing: {entry['path']}")
        if candidate.stat().st_size != entry["bytes"] or sha256_file(candidate) != entry["sha256"]:
            raise PipelineError(f"Snapshot changed after publication: {entry['path']}")
    actual_paths = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if not expected_paths or actual_paths != expected_paths:
        raise PipelineError("Snapshot inventory does not match its manifest.")
    return manifest
