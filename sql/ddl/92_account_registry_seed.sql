-- Version 92 (2026-06-20; SEED NEUTRALIZED 2026-06-24) — core.account_registry schema.
--
-- History: the table was created ad-hoc before DDL tracking (158,772 MailIn+MilkBox rows). This file
-- formalized the schema and, originally, one-time-seeded the full fleet from
-- seed_data/final_data_registry.csv (2.65M rows).
--
-- WHY THE SEED IS REMOVED (2026-06-24, box owner): `seed_data/final_data_registry.csv` is GONE — not
-- in the repo, not on the box, not in any archive — and the seed never landed (account_registry has
-- only the 158,772 ad-hoc rows, never 2.65M). The original `read_csv_auto('…final_data_registry.csv')`
-- with a `WHERE glob(...) > 0` guard does NOT work: DuckDB binds/opens the read_csv in the FROM clause
-- BEFORE the WHERE is evaluated, so a missing file throws `IO Error: No files found …` and the whole
-- statement fails. That failure killed `setup_db` here every nightly since 2026-06-20, blocking every
-- DDL that sorts after it from applying via the nightly. The table is maintained by
-- scripts/load_account_registry.py (DDL 79) + downstream pipelines, so the one-time CSV seed is dead
-- weight. If the 2.65M seed is ever needed again, restore the CSV and add it as a NEW versioned DDL
-- (a working "seed if present" needs conditional logic plain SQL DDL can't express in one statement).
--
-- This DDL is now purely the idempotent table definition — always succeeds, applies cleanly.
--
-- @gate: add

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.account_registry (
    email            VARCHAR NOT NULL,
    domain           VARCHAR,
    first_name       VARCHAR,
    last_name        VARCHAR,
    rg_tag           VARCHAR,
    rg_range         VARCHAR,
    email_tag        VARCHAR,
    vendor           VARCHAR,
    batch_tag        VARCHAR,
    workspace_label  VARCHAR,
    inbox_type       VARCHAR,
    status           VARCHAR,
    gender           VARCHAR,
    panel            VARCHAR,
    offer            VARCHAR,
    cohort           VARCHAR,
    source_tab       VARCHAR,
    _staged_at       TIMESTAMPTZ,
    PRIMARY KEY (email)
);
