#!/bin/bash

DB_HOST="localhost"
DB_PORT="33060"
DB_NAME="sourcedb"
DB_USER="mariadbuser"
DB_PASS="mariadbpass"

echo "=== MariaDB Row Counts ==="

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" << 'EOF'
SELECT 
    (SELECT COUNT(*) FROM users) as users,
    (SELECT COUNT(*) FROM sites) as sites,
    (SELECT COUNT(*) FROM jobs) as jobs;
EOF
