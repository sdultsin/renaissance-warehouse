-- @gate: add
-- Intent: mirror the NEW comms-hub opp-state ledger comms.instantly_lead_state_event
--         (comms migs 042/043, 2026-07-11/12) into the nightly comms mirror as
--         raw_comms_instantly_lead_state_event. The ledger is Renaissance's ONLY
--         faithful history of Instantly lead interest-state changes — Instantly
--         keeps no event history (the API exposes only current status + last
--         change ts; every relabel overwrites the previous state), so a 30-min
--         worker sweep appends every observed (workspace, lead_email,
--         status_changed_at, observed_status) transition. Derived daily-opp /
--         ever-opp views build on this mirror later, warehouse-side.
-- Depends on: 47
--
-- ADDITIVE ONLY: this file ADDS one raw_comms_* table. Loaded by
-- entities/comms_mirror.py (same postgres_scanner full-refresh REPLACE pattern
-- as 16/47/56/1092):
--   * the raw_* table holds exactly ONE full snapshot (REPLACE-style,
--     warehouse-flags#12) — DELETE all + INSERT fresh, atomic, idempotent.
--     (The SOURCE table is append-only, so the snapshot only ever grows.)
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * no jsonb/enum/array columns → no VARCHAR casts needed.
--
-- Column names/types match the live source (comms mig 042; verified against the
-- applied table 2026-07-12: bigint identity id, text workspace/lead_email/
-- lead_id/campaign_id/observed_status_label, integer observed_status/
-- sweep_window_days, timestamptz status_changed_at/first_seen_at).
--
-- observed_status semantics (open set — do NOT hard-enum): 1 Interested,
-- 2 Meeting Booked, 3 Meeting Completed, 4 Won, 0 OOO, -1 NI, -2 Wrong Person,
-- -3 Lost, -4 No Show, other negatives = workspace custom labels, NULL = reset
-- to plain "Lead". observed_status_label captures the label TEXT at observation
-- time (Instantly labels are deletable while leads keep orphan values).

CREATE TABLE IF NOT EXISTS raw_comms_instantly_lead_state_event (
    id                     BIGINT,
    workspace              VARCHAR,      -- workspace slug, e.g. 'funding-2-ido'
    lead_email             VARCHAR,      -- lower(trim(email))
    lead_id                VARCHAR,      -- Instantly lead uuid (per-campaign entity)
    campaign_id            VARCHAR,      -- Instantly campaign uuid (nullable)
    observed_status        INTEGER,      -- lt_interest_status; NULL = reset to "Lead"
    observed_status_label  VARCHAR,      -- label text at observation time
    status_changed_at      TIMESTAMPTZ,  -- Instantly timestamp_last_interest_change (relabel moment)
    first_seen_at          TIMESTAMPTZ,  -- when OUR sweep first observed the transition
    sweep_window_days      INTEGER,      -- occurrence-days window of the observing sweep
    _loaded_at             TIMESTAMPTZ NOT NULL,
    _run_id                VARCHAR
);
