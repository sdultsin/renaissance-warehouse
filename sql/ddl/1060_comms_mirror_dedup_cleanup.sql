-- @gate: data-backfill (one-time in-place dedup; no column add/rename/drop; row grain becomes 1 row per source id)
-- Depends on 16, 47, 56 (the raw_comms_* tables this dedups)
--
-- One-time dedup of the raw_comms_* mirror tables (warehouse-flags#12, 2026-07-01).
--
-- The comms mirror (entities/comms_mirror.py) deleted only the CURRENT run's
-- _run_id before inserting a full snapshot, so every nightly run APPENDED a
-- complete copy of each source table: 12-34 stacked snapshots per table
-- (e.g. raw_comms_phone_enrichment 405,608 rows / 33,641 distinct ids;
-- raw_comms_call_opportunity 1,094,822 rows / 39,949 distinct ids). Naive
-- aggregates read ~3-30x inflated (Prospeo credits since 06-26: naive 2,980 vs
-- true 930) and joins fanned out. The mirror is now REPLACE-style (one snapshot
-- per table); this migration collapses the existing rows to ONE row per source
-- id, keeping the freshest copy (max _loaded_at).
--
-- Semantics per table: keep row_number() = 1 partitioned by the source PK,
-- ordered by _loaded_at DESC. Verified pre-apply that the latest run contains
-- 100% of all distinct ids ever seen (no upstream deletes), so this equals
-- keeping the latest full snapshot. Two tables have no `id` column:
--   * raw_comms_enrichment_vendor_pricing -> PK is `provider`
--   * raw_comms_suppression               -> no PK; dedup on all data columns
--
-- Idempotent: re-running keeps the same one-row-per-id set. Runs inside the
-- apply transaction (core/db.py apply_ddl_file wraps BEGIN/COMMIT — no explicit
-- transaction statements here). raw_comms_app_link_check is empty but included
-- for completeness.

-- raw_comms_brand (id VARCHAR)
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_brand
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_brand;
INSERT INTO raw_comms_brand SELECT * FROM _dedup_comms;

-- raw_comms_conversation
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_conversation
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_conversation;
INSERT INTO raw_comms_conversation SELECT * FROM _dedup_comms;

-- raw_comms_message
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_message
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_message;
INSERT INTO raw_comms_message SELECT * FROM _dedup_comms;

-- raw_comms_suppression (NO id column -> dedup on all data columns)
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_suppression
QUALIFY row_number() OVER (
    PARTITION BY prospect_number, reason, suppressed_at, expires_at,
                 source_conversation_id, triggering_message_id, notes
    ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_suppression;
INSERT INTO raw_comms_suppression SELECT * FROM _dedup_comms;

-- raw_comms_escalation
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_escalation
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_escalation;
INSERT INTO raw_comms_escalation SELECT * FROM _dedup_comms;

-- raw_comms_call_opportunity
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_call_opportunity
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_call_opportunity;
INSERT INTO raw_comms_call_opportunity SELECT * FROM _dedup_comms;

-- raw_comms_phone_enrichment
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_phone_enrichment
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_phone_enrichment;
INSERT INTO raw_comms_phone_enrichment SELECT * FROM _dedup_comms;

-- raw_comms_instantly_message
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_instantly_message
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_instantly_message;
INSERT INTO raw_comms_instantly_message SELECT * FROM _dedup_comms;

-- raw_comms_enrichment_vendor_pricing (PK is provider — 4 providers, 28 stacked runs)
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_enrichment_vendor_pricing
QUALIFY row_number() OVER (PARTITION BY provider ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_enrichment_vendor_pricing;
INSERT INTO raw_comms_enrichment_vendor_pricing SELECT * FROM _dedup_comms;

-- raw_comms_ai_decision_log
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_ai_decision_log
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_ai_decision_log;
INSERT INTO raw_comms_ai_decision_log SELECT * FROM _dedup_comms;

-- raw_comms_close_sync
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_close_sync
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_close_sync;
INSERT INTO raw_comms_close_sync SELECT * FROM _dedup_comms;

-- raw_comms_gbc_application
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_gbc_application
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_gbc_application;
INSERT INTO raw_comms_gbc_application SELECT * FROM _dedup_comms;

-- raw_comms_app_link_check (empty today; included for completeness)
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_app_link_check
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_app_link_check;
INSERT INTO raw_comms_app_link_check SELECT * FROM _dedup_comms;

-- raw_comms_lead_application (id is uuid-as-VARCHAR)
CREATE OR REPLACE TEMP TABLE _dedup_comms AS
SELECT * FROM raw_comms_lead_application
QUALIFY row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1;
DELETE FROM raw_comms_lead_application;
INSERT INTO raw_comms_lead_application SELECT * FROM _dedup_comms;

-- _dedup_comms is a TEMP table — it dies with the applying connection; no DROP needed.
