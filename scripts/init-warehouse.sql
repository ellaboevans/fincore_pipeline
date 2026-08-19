-- ============================================================
-- Warehouse bootstrap: schemas only.
--
-- This file is mounted into the Postgres entrypoint
-- (docker-entrypoint-initdb.d) and runs once, on a fresh data
-- volume, before any other init script.
--
-- Table definitions deliberately do NOT live here. They live in
-- scripts/create-warehouse-tables.sql, which is the single source
-- of truth and is mounted as the next init script. Defining a
-- table in both files silently diverges: every CREATE TABLE uses
-- IF NOT EXISTS, so whichever file runs first wins and the other
-- becomes a no-op.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;
