#!/bin/bash

DB_HOST="localhost"
DB_PORT="33060"
DB_NAME="sourcedb"
DB_USER="mariadbuser"
DB_PASS="mariadbpass"

mysql_exec() {
    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "$1"
}

echo "Creating tables..."

# Drop and create tables
mysql_exec "DROP TABLE IF EXISTS jobs;"
mysql_exec "DROP TABLE IF EXISTS sites;"
mysql_exec "DROP TABLE IF EXISTS users;"

mysql_exec "
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username TEXT,
    email TEXT,
    password TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);"

mysql_exec "
CREATE TABLE sites (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
    user_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);"

mysql_exec "
CREATE TABLE jobs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    start_date DATE,
    site_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (site_id) REFERENCES sites(id)
);"

echo "✓ Database refreshed successfully!"
