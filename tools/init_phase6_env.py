"""Create non-secret S3 deployment configuration after AWS credentials work."""
from __future__ import annotations

import argparse
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--bucket", help="Default: ecommerce-data-lakehouse-<aws-account-id>-<region>")
    parser.add_argument("--prefix", default="ecommerce-data-lakehouse/v1")
    args = parser.parse_args()

    import boto3
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    identity = session.client("sts").get_caller_identity()
    account = str(identity["Account"])
    bucket = args.bucket or f"ecommerce-data-lakehouse-{account}-{args.region}"
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        parser.error("Bucket name must be a valid lowercase general-purpose S3 bucket name.")
    path = ROOT / ".env.phase6"
    if path.exists():
        print(".env.phase6 already exists; unchanged. Delete it yourself only if you intentionally want different non-secret settings.")
        return 0
    path.write_text(
        "# Non-secret deployment configuration. AWS credentials stay in the normal AWS credential chain.\n"
        f"PHASE6_AWS_PROFILE={args.profile}\n"
        f"PHASE6_AWS_REGION={args.region}\n"
        f"PHASE6_S3_BUCKET={bucket}\n"
        f"PHASE6_S3_PREFIX={args.prefix.strip('/')}\n",
        encoding="utf-8",
    )
    print("Created .env.phase6")
    print("Bucket:", bucket)
    print("Region:", args.region)
    print("Profile:", args.profile)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
