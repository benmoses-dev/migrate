#!/bin/bash

DB_HOST="localhost"
DB_PORT="54320"
DB_NAME="destdb"
DB_USER="pgsqluser"
DB_PASS="pgsqlpass"

export PGPASSWORD="$DB_PASS"

echo "Creating tables in PostgreSQL..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS sites;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    username TEXT,
    email TEXT,
    password TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sites (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT,
    user_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE jobs (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    start_date DATE,
    site_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
EOF

echo "✓ PostgreSQL tables created successfully!"
