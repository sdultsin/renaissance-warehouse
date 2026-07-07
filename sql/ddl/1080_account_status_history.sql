-- @gate: add
-- Depends on 00
-- core.account_status_history — append-only CHANGE-LOG of every inbox's status + disconnect reason over time.
-- Appends one row for an inbox ONLY when its (status_label, warmup_status_label, error_string) changes vs its
-- prior observation (or first-ever sighting). NEVER trimmed -> complete lifecycle history forever at a tiny
-- fraction of a daily snapshot's size. Complements core.account_census (full daily status snapshot, kept
-- forever from 2026-06-21, but with NO error_string) by adding the disconnect REASON history + a compact,
-- directly-queryable transition log. Built 2026-07-06 after the MilkBox-IMAP investigation, where the
-- 15-day-blind census + reason-less poll parquets left us unable to see WHY the June batch errored.
--
-- GRAIN = (email, workspace_uuid): the SAME email under two workspaces is tracked as two independent
--   lifecycles (change-detection + prev_* lookups partition on email+workspace_uuid), matching the census.
-- error_string IS part of the change key: a disconnect reason appearing / changing / clearing is itself a
--   logged event (that is the "why" history we built this for). One-time caveat: at the backfill->forward
--   boundary an erroring inbox logs one extra row when its reason flips NULL (backfill has no reason) -> the
--   live reason, even if status is unchanged. Read `source` to distinguish 'parquet_backfill' vs 'census'.
-- No PRIMARY KEY on purpose: plain append avoids the DuckDB ART "duplicate key" abort; the feeding entity is
--   naturally idempotent (a re-run with an unchanged census inserts nothing). Backfilled to 2026-06-17 from
--   the hourly poll parquets (status only; error_string NULL for the historical portion — genuinely absent).
CREATE TABLE IF NOT EXISTS core.account_status_history (
    email                VARCHAR NOT NULL,
    workspace_uuid       VARCHAR,
    workspace_slug       VARCHAR,
    status_label         VARCHAR,   -- active | connection_error | sending_error | paused | soft_bounce
    warmup_status_label  VARCHAR,   -- active(=warming) | paused | banned
    error_string         VARCHAR,   -- disconnect reason (e.g. "IMAP access is disabled ..."); NULL in backfill
    daily_limit          DOUBLE,
    provider_code        INTEGER,
    observed_date        DATE NOT NULL,
    observed_at          TIMESTAMPTZ,
    prev_status_label    VARCHAR,   -- state it changed FROM (NULL = first observation of this inbox)
    prev_warmup_label    VARCHAR,
    prev_error_string    VARCHAR,
    is_first_seen        BOOLEAN,   -- TRUE = baseline row (first time we ever recorded this inbox)
    source               VARCHAR,   -- 'census' (forward) | 'parquet_backfill' (historical)
    _loaded_at           TIMESTAMPTZ,
    _run_id              VARCHAR
);
