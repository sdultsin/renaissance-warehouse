-- Sendivo outbound-blast mirror (R6 gap-close, 2026-06-09 real-time-enrichment build).
--
-- The Sendivo OUTBOUND SMS bodies (the cold blast = the "original message", plus
-- the full outbound cadence) live ONLY in comms.webhook_receipt as
-- webhook_type='sendivo_outbound_status' rows, surfaced by the cleaned view
-- comms.v_sendivo_outbound_message (phone10 / message / sent_at / status_group,
-- ~1.55M rows). The v1 comms mirror (entities/comms_mirror.py) intentionally
-- EXCLUDES webhook_receipt as raw noise (6.37M rows) — but that left the
-- outbound blast body NON-warehouse-native, so full-thread assembly + the
-- Sendivo "original" message (S1 root-cause) could not be queried in the
-- warehouse. This table closes that R6 gap by mirroring the CLEANED view only
-- (not the 6.37M raw webhook log).
--
-- INCREMENTAL by sent_at: entities/sendivo_outbound_mirror.py pulls only rows
-- with sent_at > max(sent_at) already in the warehouse (with a small overlap),
-- so the nightly run does NOT re-scan 6.37M webhook rows each time. Append-only;
-- each pull tagged with _run_id. De-dup on (phone10, sent_at, message) is left
-- to query-time views (raw stays append-only, matching the raw_* convention).
--
-- ADDITIVE ONLY: new table; touches nothing in 16_comms_mirror.sql /
-- 47_comms_mirror_gaps.sql.
--
-- Type conventions (Postgres view -> DuckDB), identical to the other comms raw
-- tables:
--   text                     -> VARCHAR
--   timestamp with time zone -> TIMESTAMPTZ

CREATE TABLE IF NOT EXISTS raw_comms_sendivo_outbound (
    phone10         VARCHAR,        -- last-10 digits of the recipient number
    message         VARCHAR,        -- merged outbound SMS body (the blast / cadence text)
    sent_at         TIMESTAMPTZ,    -- when Sendivo sent it
    status_group    VARCHAR,        -- Sendivo delivery status bucket
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- Point-lookup support for per-phone thread assembly + the Sendivo-original
-- resolver doing warehouse-side reconciliation.
CREATE INDEX IF NOT EXISTS idx_raw_comms_sendivo_outbound_phone10
    ON raw_comms_sendivo_outbound(phone10);
CREATE INDEX IF NOT EXISTS idx_raw_comms_sendivo_outbound_sent_at
    ON raw_comms_sendivo_outbound(sent_at);

-- Convenience de-duped view: one row per (phone10, sent_at, message), latest
-- load wins. This is the queryable SMS-AIM "outbound thread" surface.
CREATE OR REPLACE VIEW v_comms_sendivo_outbound AS
SELECT DISTINCT ON (phone10, sent_at, message)
    phone10, message, sent_at, status_group, _loaded_at, _run_id
FROM raw_comms_sendivo_outbound
ORDER BY phone10, sent_at, message, _loaded_at DESC;
