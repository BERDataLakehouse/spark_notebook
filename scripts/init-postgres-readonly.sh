#!/bin/bash
set -e

# Create readonly user for datalake-mcp-server
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create readonly user if it doesn't exist
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'readonly_user') THEN
            CREATE USER readonly_user WITH PASSWORD 'readonly_password';
        END IF;
    END
    \$\$;

    -- Grant connect privilege
    GRANT CONNECT ON DATABASE hive TO readonly_user;

    -- Grant usage on schema
    GRANT USAGE ON SCHEMA public TO readonly_user;

    -- Grant select on all existing tables
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

    -- Grant select on future tables
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;

    -- Grant select on sequences (needed for some queries)
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO readonly_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO readonly_user;
EOSQL

echo "Readonly user 'readonly_user' created successfully"

