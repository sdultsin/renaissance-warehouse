-- @gate: add
-- Depends on 1110
-- ============================================================================
-- 1126_raw_lead_dialer_attrs.sql — slim per-lead caller attributes, pre-materialized
-- from the LEAD MIRROR for the labeled-lead universe (dialer feed input, R14/R24).
--
-- WHY A TABLE: the lead mirror (phone master + names/company/state) is a separate
-- DuckDB on the droplet volume — a warehouse VIEW cannot cross-file-join it. Same
-- pattern as core.lead_attrs (scripts/prebuild_lead_attrs.py): a small pre-nightly
-- droplet job (/root/mof/tracker/prebuild_dialer_attrs.py, cron 04:30Z) ATTACHes the
-- mirror serving copy READ-ONLY, resolves attrs for every distinct labeled lead_email
-- (escrow parquets ∪ warehouse ledger — ~100k rows, trivial), and rebuilds this table
-- under the writer flock. The 05:30Z nightly promote publishes it with the same
-- night's label events. ⚠ REHOME: the builder + mirror path die with the droplet
-- (~Jul 25-27) — carried on the workstream rehome list (VISION §4).
--
-- COLUMN NOTES:
--   company_clean  = mirror.company_name_clean_cache (the R34b upload-pass cleaner
--                    output; LLM-cleaned, conversation-friendly) with a light
--                    deterministic fallback (suffix-strip) marked in
--                    company_clean_source ('cache' | 'fallback_regex' | NULL).
--   phone          = COALESCE(mirror phone, mirror enriched_phone) — mirror is the
--                    phone master (lead-mirror phone-truth rule).
--   timezone       = IANA tz from a simple state→tz map (dominant tz per state;
--                    tz_source records the basis). Honest NULL when state unknown.
--
-- Reversible: DROP TABLE (rebuilt from mirror + ledger by the next 04:30Z run).
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.raw_lead_dialer_attrs (
    lead_email            VARCHAR PRIMARY KEY,     -- lowercased
    first_name            VARCHAR,
    last_name             VARCHAR,
    company_raw           VARCHAR,
    company_clean         VARCHAR,
    company_clean_source  VARCHAR,                 -- 'cache' | 'fallback_regex' | NULL
    phone                 VARCHAR,                 -- mirror phone master (raw as stored)
    city                  VARCHAR,
    state                 VARCHAR,
    timezone              VARCHAR,                 -- IANA, from state map
    tz_source             VARCHAR,                 -- 'state_map' | NULL
    mirror_matched        BOOLEAN NOT NULL DEFAULT FALSE,
    _loaded_at            TIMESTAMPTZ DEFAULT now(),
    _run_id               VARCHAR
);
