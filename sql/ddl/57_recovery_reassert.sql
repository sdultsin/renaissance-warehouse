-- Version 57 (2026-06-11) — Re-assert tables lost in the 06-08/09 corruption recovery.
--
-- The recovery restored a DB whose schema_version was already at latest, so
-- setup_db's version tracking never re-ran DDLs 19/31 and three tables stayed
-- missing for days:
--   * core.sending_account_tag    -> every nightly compaction import ABORTED on the
--                                    surviving v_tag_coverage_gaps view reference
--   * core.sending_account_daily  -> derived.sending_dq FAILED nightly (daily
--                                    actuals raw loads worked, canonical rebuild died)
--   * (core.sending_account itself survived but with a corrupted ART/PK index from
--     the Jun-9 OOM kill — every DELETE FATALed and killed the rest of canonical +
--     ALL of derived on Jun-10/11. Rebuilt manually 2026-06-11; included here so a
--     fresh DB gets the _snapshot_date column DDL 19 lacks.)
--
-- All idempotent CREATE IF NOT EXISTS — a no-op on the repaired live DB; insurance
-- + provenance for any future restore. core.domain_registry (Track I, created
-- ad-hoc, never in versioned DDL) is STILL missing and needs its own rebuild.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.sending_account_tag (
    email          VARCHAR NOT NULL,
    workspace_slug VARCHAR,
    tag_id         VARCHAR,
    tag_label      VARCHAR NOT NULL,
    first_seen_at  TIMESTAMPTZ,
    last_seen_at   TIMESTAMPTZ,
    PRIMARY KEY (email, tag_label)
);

CREATE TABLE IF NOT EXISTS core.sending_account_daily (
    date             DATE NOT NULL,
    account_id       VARCHAR NOT NULL,
    workspace_slug   VARCHAR NOT NULL,
    esp              VARCHAR,
    daily_limit      INTEGER,
    expected_sends   INTEGER,
    actual_sends     INTEGER,
    delta            INTEGER,
    fulfillment      DOUBLE,
    active_campaign_count INTEGER,
    PRIMARY KEY (date, account_id)
);

-- Identical to DDL 19 plus the _snapshot_date column entities/sending_dq.py (D4)
-- otherwise bolts on at runtime.
CREATE TABLE IF NOT EXISTS core.sending_account (
    account_id          VARCHAR PRIMARY KEY,
    email               VARCHAR NOT NULL,
    domain              VARCHAR NOT NULL,
    workspace_slug      VARCHAR NOT NULL,
    workspace_id        VARCHAR,
    esp                 VARCHAR,
    infra_provider      VARCHAR,
    lifecycle_state     VARCHAR NOT NULL,
    rotation_state      VARCHAR,
    created_at          TIMESTAMPTZ,
    warmup_started_at   TIMESTAMPTZ,
    warmup_completed_at TIMESTAMPTZ,
    rampup_started_at   TIMESTAMPTZ,
    rampup_completed_at TIMESTAMPTZ,
    paused_at           TIMESTAMPTZ,
    retired_at          TIMESTAMPTZ,
    status              VARCHAR,
    warmup_phase        VARCHAR,
    warmup_score        DOUBLE,
    daily_limit         INTEGER,
    daily_limit_used    INTEGER,
    cost_per_day_usd_estimated  DOUBLE,
    vendor_billing_cycle        VARCHAR,
    is_active           BOOLEAN NOT NULL,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    _snapshot_date      DATE
);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_domain    ON core.sending_account (domain);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_workspace ON core.sending_account (workspace_slug);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_esp       ON core.sending_account (esp);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_lifecycle ON core.sending_account (lifecycle_state);
