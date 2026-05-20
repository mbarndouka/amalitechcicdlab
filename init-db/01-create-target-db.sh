#!/bin/bash
set -e

# Create target DB for processed data, separate from Airflow metadata DB.
# Only runs on first postgres init (empty data dir).

if [ -z "${TARGET_DB}" ] || [ "${TARGET_DB}" = "${POSTGRES_DB}" ]; then
  echo "TARGET_DB unset or same as POSTGRES_DB; skipping target DB creation."
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    CREATE DATABASE ${TARGET_DB};
    GRANT ALL PRIVILEGES ON DATABASE ${TARGET_DB} TO ${POSTGRES_USER};
EOSQL

echo "Created target database: ${TARGET_DB}"
