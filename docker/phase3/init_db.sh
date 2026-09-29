#!/bin/bash
set -eo pipefail
# Executed only when the named PostgreSQL data volume is initialized.
# SQL literal quoting is handled by psql, not shell concatenation.
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 --set=app_password="$PHASE3_APP_PASSWORD" <<'SQL'
CREATE ROLE de_user LOGIN PASSWORD :'app_password'
  NOSUPERUSER NOCREATEDB NOCREATEROLE;
GRANT CONNECT, TEMPORARY, CREATE ON DATABASE ecommerce TO de_user;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
