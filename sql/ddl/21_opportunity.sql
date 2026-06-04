-- Phase 3: core.opportunity canonical entity (spec 10).
-- Applied at schema version 21 by scripts/setup_db.py / orchestrator DDL applier.
--
-- One row per LEAD-LEVEL opportunity record. Source = raw_comms_call_opportunity
-- (the warm-call/AIM opportunity table), source-aware via its `source` column
-- ('sendivo' SMS opps + a few 'instantly' email opps routed to calling).
--
-- DEFINITION NOTE (Sam, 2026-05-31): "opportunities" is NOT Instantly lead-status
-- `lead_interested`. We track opportunities, not interested-status. An earlier build
-- sourced the Instantly side from lead_events lead_interested — that was the wrong
-- signal and was removed (see entities/opportunity.py + GAPS B8).
--
-- Instantly EMAIL opportunities (the dominant dashboard KPI) exist only as the
-- aggregate raw_pipeline_campaign_daily_metrics.opportunities (per campaign×day) —
-- there is no populated lead-level Instantly opportunity table in the mirror. So
-- core.opportunity is the lead-level warm-call/AIM surface; the Instantly opp KPI is
-- the aggregate metric (query campaign_daily_metrics).
--
-- Close CRM excluded (no data yet). Cross-source dedup (same prospect via email AND
-- SMS) is v1.5; is_duplicate_of carries only the same-source duplicate pointer.
--
-- Canonical (core schema), full rebuild each run from the raw snapshots.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.opportunity (
    opportunity_id    VARCHAR PRIMARY KEY,   -- '{source}:{source_event_id}'
    source            VARCHAR NOT NULL,      -- instantly | sendivo
    source_event_id   VARCHAR NOT NULL,
    lead_email        VARCHAR,
    campaign_id       VARCHAR,               -- FK to core.campaign (Instantly only; NULL for Sendivo SMS)
    workspace_id      VARCHAR,               -- Instantly workspace id/slug where attributable
    opened_at         TIMESTAMPTZ,           -- when interest was registered
    state             VARCHAR,               -- normalized: 'interested' (Instantly) | Sendivo status string
    state_updated_at  TIMESTAMPTZ,
    is_duplicate_of   VARCHAR,               -- same-source (Sendivo duplicate_of); cross-source dedup is v1.5
    cost_per_opp_usd_estimated  DOUBLE,      -- spec 13 cost projection; NULL until v3
    raw               VARCHAR,               -- JSON of the original event
    _resolved_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_core_opportunity_source   ON core.opportunity (source);
CREATE INDEX IF NOT EXISTS ix_core_opportunity_email    ON core.opportunity (lead_email);
CREATE INDEX IF NOT EXISTS ix_core_opportunity_campaign ON core.opportunity (campaign_id);
CREATE INDEX IF NOT EXISTS ix_core_opportunity_opened   ON core.opportunity (opened_at);
