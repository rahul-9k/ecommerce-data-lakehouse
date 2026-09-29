"""Create local-only Airflow secrets once. Existing values are never replaced."""
from __future__ import annotations

import base64
import os
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / ".env.phase5"

if PATH.exists():
    print(".env.phase5 already exists; unchanged.")
else:
    values = {
        "PHASE5_AIRFLOW_DB_PASSWORD": secrets.token_hex(24),
        "PHASE5_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii"),
        "PHASE5_JWT_SECRET": secrets.token_hex(48),
        "PHASE5_API_SECRET": secrets.token_hex(48),
    }
    fd = os.open(PATH, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("# LOCAL ONLY. Do not commit or share this file.\n")
        for key, value in values.items():
            handle.write(f"{key}={value}\n")
    print("Created .env.phase5 with randomly generated Airflow secrets. No secret values printed.")

for name in ("reports", "logs", "data/changes"):
    (ROOT / name).mkdir(parents=True, exist_ok=True)
