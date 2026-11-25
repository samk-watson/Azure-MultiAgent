#!/bin/bash
set -euo pipefail

psql \
  --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=checkpoint_password="$ORCHESTRATOR_CHECKPOINT_RUNTIME_PASSWORD" \
  --set=migrator_password="$ORCHESTRATOR_CHECKPOINT_MIGRATOR_PASSWORD" \
  --set=retail_probe_password="$ORCHESTRATOR_RETAIL_PROBE_PASSWORD" <<'SQL'
-- Fresh-install roles only. The privileged provisioning CLI
-- (supply-chain-orchestrator-provision) completes ownership, migrations, grants
-- and verification, and is also the upgrade path for existing volumes (this
-- script never reruns on an existing data directory).
CREATE ROLE orchestrator_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE ROLE orchestrator_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS
    NOINHERIT PASSWORD :'migrator_password';
GRANT orchestrator_owner TO orchestrator_migrator WITH INHERIT FALSE, SET TRUE;
CREATE ROLE orchestrator_checkpoint LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS
    NOINHERIT PASSWORD :'checkpoint_password';
CREATE DATABASE orchestrator_checkpoint OWNER orchestrator_owner;

CREATE ROLE orchestrator_retail_test NOLOGIN;
CREATE DATABASE orchestrator_retail_test OWNER orchestrator_retail_test;
CREATE ROLE orchestrator_retail_probe LOGIN PASSWORD :'retail_probe_password';
GRANT CONNECT ON DATABASE orchestrator_retail_test TO orchestrator_retail_probe;
\connect orchestrator_retail_test
SET ROLE orchestrator_retail_test;
CREATE SCHEMA retail AUTHORIZATION orchestrator_retail_test;
CREATE TABLE retail.products (id integer PRIMARY KEY);
CREATE TABLE retail.locations (id integer PRIMARY KEY);
CREATE TABLE retail.inventory_positions (id integer PRIMARY KEY);
CREATE TABLE retail.inventory_policies (id integer PRIMARY KEY);
CREATE TABLE retail.inbound_inventory (id integer PRIMARY KEY);
CREATE TABLE retail.location_transfer_rules (id integer PRIMARY KEY);
RESET ROLE;
SQL
