"""Publish explicit project data layers to a private, versioned Amazon S3 bucket."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import re
import sys
from typing import Iterable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common import PROJECT_ROOT, PipelineError, sha256_file, utc_now, write_json


@dataclass(frozen=True)
class UploadItem:
    local_path: Path
    key: str
    size: int
    sha256: str


def safe_prefix(value: str) -> str:
    value = value.strip("/")
    if not value or ".." in value.split("/") or "\\" in value:
        raise PipelineError("S3 prefix must be a non-empty slash-separated path without traversal.")
    return value


def _add_tree(items: list[UploadItem], directory: Path, key_prefix: str) -> None:
    if not directory.is_dir():
        raise PipelineError(f"Required source directory is missing: {directory}")
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            rel = path.relative_to(directory).as_posix()
            items.append(UploadItem(path, f"{key_prefix}/{rel}", path.stat().st_size, sha256_file(path)))


def build_inventory(root: Path, prefix: str, baseline_id: str = "initial_001") -> list[UploadItem]:
    prefix = safe_prefix(prefix)
    items: list[UploadItem] = []
    _add_tree(items, root / "data" / "raw" / baseline_id, f"{prefix}/raw/{baseline_id}")
    _add_tree(items, root / "data" / "processed" / baseline_id, f"{prefix}/curated/{baseline_id}")

    changes_root = root / "data" / "changes"
    if changes_root.is_dir():
        for directory in sorted(p for p in changes_root.iterdir() if p.is_dir() and not p.name.startswith(".")):
            manifest = directory / "manifest.json"
            if not manifest.is_file():
                continue
            try:
                meta = json.loads(manifest.read_text(encoding="utf-8"))
            except Exception:
                continue
            if meta.get("status") == "success" and meta.get("phase") == 4:
                _add_tree(items, directory, f"{prefix}/change_feed/{directory.name}")

    report_names = [
        "phase1_latest.json", "phase2_latest.json", "phase2_selftest.json",
        "phase3_latest.json", "phase3_selftest.json", "phase4_latest.json",
        "phase4_selftest.json", "phase5_latest.json", "phase5_preflight.json",
    ]
    for name in report_names:
        path = root / "reports" / name
        if path.is_file():
            items.append(UploadItem(path, f"{prefix}/metadata/reports/{name}", path.stat().st_size, sha256_file(path)))

    seen = set()
    for item in items:
        if item.key in seen:
            raise PipelineError(f"Duplicate S3 key planned: {item.key}")
        seen.add(item.key)
    if not items:
        raise PipelineError("Nothing selected for S3 publication.")
    return items


def _client(profile: str | None, region: str):
    import boto3
    session = boto3.Session(profile_name=profile or None, region_name=region)
    return session, session.client("s3")


def ensure_bucket(client, bucket: str, region: str) -> dict:
    from botocore.exceptions import ClientError
    created = False
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchBucket", "NotFound"} or status == 404:
            kwargs = {"Bucket": bucket}
            if region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            client.create_bucket(**kwargs)
            created = True
        else:
            raise PipelineError(f"Cannot access S3 bucket {bucket!r}; it may exist in another account or permissions are missing.") from exc

    client.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    client.put_bucket_ownership_controls(
        Bucket=bucket,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
    )
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    client.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
        },
    )
    client.put_bucket_tagging(
        Bucket=bucket,
        Tagging={"TagSet": [
            {"Key": "Project", "Value": "ecommerce-data-lakehouse"},
            {"Key": "Purpose", "Value": "portfolio-data-engineering"},
        ]},
    )
    return {"created": created, "public_access_block": True, "versioning": "Enabled", "default_encryption": "AES256", "object_ownership": "BucketOwnerEnforced"}


def _head_matches(client, bucket: str, item: UploadItem) -> bool:
    from botocore.exceptions import ClientError
    try:
        head = client.head_object(Bucket=bucket, Key=item.key)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404 or str(exc.response.get("Error", {}).get("Code", "")) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return head.get("ContentLength") == item.size and head.get("Metadata", {}).get("sha256") == item.sha256


def publish(client, bucket: str, items: Iterable[UploadItem], dry_run: bool = False) -> dict:
    uploaded = skipped = total_bytes = 0
    details = []
    for item in items:
        total_bytes += item.size
        if _head_matches(client, bucket, item):
            skipped += 1
            details.append({"key": item.key, "bytes": item.size, "sha256": item.sha256, "action": "unchanged"})
            continue
        if dry_run:
            details.append({"key": item.key, "bytes": item.size, "sha256": item.sha256, "action": "would_upload"})
            continue
        content_type = mimetypes.guess_type(item.local_path.name)[0] or "application/octet-stream"
        client.upload_file(
            str(item.local_path), bucket, item.key,
            ExtraArgs={
                "ContentType": content_type,
                "Metadata": {"sha256": item.sha256},
                "ServerSideEncryption": "AES256",
                "ChecksumAlgorithm": "SHA256",
            },
        )
        if not _head_matches(client, bucket, item):
            raise PipelineError(f"S3 verification failed after upload: s3://{bucket}/{item.key}")
        uploaded += 1
        details.append({"key": item.key, "bytes": item.size, "sha256": item.sha256, "action": "uploaded"})
    return {"uploaded": uploaded, "unchanged": skipped, "bytes_selected": total_bytes, "objects": details}


def verify_remote(client, bucket: str, items: Iterable[UploadItem]) -> dict:
    missing = []
    for item in items:
        if not _head_matches(client, bucket, item):
            missing.append(item.key)
    if missing:
        raise PipelineError(f"S3 verification found {len(missing)} missing/changed object(s); first: {missing[0]}")
    return {"verified_objects": len(list(items)) if not isinstance(items, list) else len(items)}


def write_deployment_manifest(root: Path, bucket: str, region: str, prefix: str, security: dict, result: dict, account: str) -> Path:
    path = root / "reports" / "phase6_latest.json"
    value = {
        "status": "success",
        "phase": 6,
        "provider": "Amazon S3",
        "account_id": account,
        "region": region,
        "bucket": bucket,
        "prefix": prefix,
        "security": security,
        "object_count": len(result["objects"]),
        "uploaded": result["uploaded"],
        "unchanged": result["unchanged"],
        "bytes_selected": result["bytes_selected"],
        "completed_at": utc_now(),
        "source_scope": "Explicit raw snapshot, curated Parquet snapshot, completed Phase 4 change feeds, and selected JSON reports. No secrets, database volume, Airflow metadata DB, or logs are uploaded.",
        "objects": result["objects"],
    }
    write_json(path, value)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--prefix", default="ecommerce-data-lakehouse/v1")
    parser.add_argument("--baseline-id", default="initial_001")
    parser.add_argument("--init-bucket", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if sum(bool(x) for x in (args.init_bucket, args.publish, args.verify_only)) != 1:
        parser.error("Choose exactly one of --init-bucket, --publish, --verify-only")
    try:
        prefix = safe_prefix(args.prefix)
        session, client = _client(args.profile, args.region)
        identity = session.client("sts").get_caller_identity()
        account = str(identity["Account"])
        if args.init_bucket:
            security = ensure_bucket(client, args.bucket, args.region)
            result = {"status": "success", "phase": 6, "action": "bucket_ready", "bucket": args.bucket,
                      "region": args.region, "account_id": account, "security": security, "completed_at": utc_now()}
            write_json(PROJECT_ROOT / "reports" / "phase6_bucket.json", result)
            print("PHASE 6 BUCKET READY")
            print("Bucket:", args.bucket)
            print("Security:", security)
            return 0

        items = build_inventory(PROJECT_ROOT, prefix, args.baseline_id)
        if args.verify_only:
            verify_remote(client, args.bucket, items)
            print("PHASE 6 VERIFIED")
            print("Objects:", len(items))
            return 0
        security = ensure_bucket(client, args.bucket, args.region)
        result = publish(client, args.bucket, items, dry_run=args.dry_run)
        if args.dry_run:
            print("PHASE 6 DRY RUN")
            print("Objects selected:", len(items))
            print("Bytes selected:", result["bytes_selected"])
            return 0
        verify_remote(client, args.bucket, items)
        manifest_path = write_deployment_manifest(PROJECT_ROOT, args.bucket, args.region, prefix, security, result, account)
        # Publish the deployment manifest itself under a stable metadata key after it is written.
        manifest_item = UploadItem(manifest_path, f"{prefix}/metadata/phase6_latest.json", manifest_path.stat().st_size, sha256_file(manifest_path))
        publish(client, args.bucket, [manifest_item], dry_run=False)
        print("PHASE 6 COMPLETE")
        print("Bucket:", args.bucket)
        print("Objects selected:", len(items))
        print("Uploaded:", result["uploaded"], "Unchanged:", result["unchanged"])
        print("Report: reports/phase6_latest.json")
        return 0
    except Exception as exc:
        print(f"PHASE 6 FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
