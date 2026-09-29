"""Generate local credentials once. Existing credentials are never replaced."""
from pathlib import Path
import os
import secrets

root = Path(__file__).resolve().parents[1]
path = root / ".env.phase3"
if path.exists():
    print(".env.phase3 already exists; unchanged. Keep it with your existing database volume.")
else:
    # Exclusive creation also prevents concurrent setup from overwriting credentials.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        f.write("# LOCAL ONLY. Do not commit or share this file.\n")
        f.write("PHASE3_ADMIN_PASSWORD=" + secrets.token_urlsafe(32) + "\n")
        f.write("PHASE3_APP_PASSWORD=" + secrets.token_urlsafe(32) + "\n")
    print("Created .env.phase3 with randomly generated passwords. No credentials printed.")
for name in ("reports", "logs"):
    (root / name).mkdir(exist_ok=True)
