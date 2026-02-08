#!/bin/bash

DB_HOST="localhost"
DB_PORT="54320"
DB_NAME="destdb"
DB_USER="pgsqluser"
DB_PASS="pgsqlpass"

export PGPASSWORD="$DB_PASS"

echo "=== PostgreSQL Row Counts ==="

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
SELECT 
    (SELECT COUNT(*) FROM users) as users,
    (SELECT COUNT(*) FROM sites) as sites,
    (SELECT COUNT(*) FROM jobs) as jobs;
EOF
