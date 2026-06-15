-- 73_deals_funded.sql  [2026-06-15]  GAP-1: the deals-funded terminal funnel stage.
-- Applied via apply_ddl_file(conn, <this file>, version=73) in a post-cutover idle writer
-- window (outside 03:30-05:45 UTC) under the warehouse write lock. Idempotent
-- (CREATE TABLE/VIEW IF NOT EXISTS / CREATE OR REPLACE) — re-applying is a no-op, and
-- apply_ddl_file additionally version-gates on core.schema_version (=73 after this file).
-- Live schema_version was 72 (72_sending_account_vendor) immediately before this file.
-- !! CONFIRM 73 IS STILL THE NEXT-FREE NUMBER AT APPLY TIME — a parallel "email-type"
--    workstream took 72; verify nothing claimed 73 between authoring (Jun-15) and apply.
-- Migration-agnostic standard SQL (must port off single-file DuckDB unchanged).
--
-- ============================================================================
-- WHY (Sam, "Gap 1"): the strategic bottom-line KPI is
--     deals_funded × commission ÷ all-in cost.
-- We have NOT started tracking funded deals yet — but the KPI is load-bearing, so we
-- build the DB + portal structure NOW so real data slots in cleanly later. A funded deal
-- is the downstream conversion of a booked meeting:
--     emails -> replies -> opportunities -> meetings booked -> *DEALS FUNDED* -> revenue.
-- core.deal_funded is the terminal funnel fact and the exact source that will eventually
-- populate the typed-but-EMPTY `meeting_result` slot already reserved on
-- derived.v_funnel / derived.v_funnel_detail (DDL 65).
--
-- ============================================================================
-- SHIPS EMPTY — IN ANTICIPATION. The 100%-or-WIPE rule
-- (memory feedback_partial_data_100pct_or_wipe_20260614) governs here: this structure
-- ships with ZERO rows. No placeholder / fake / partial rows are inserted, ever. The
-- portal deals-funded section is HELD/HIDDEN (scripts/portal_data.py) and must stay held
-- until real ~100%-complete funded-deal data exists — never show a partial or
-- zero-as-if-real number. The KPI view returns 0 rows cleanly while empty (EMPTY-safe).
--
-- ============================================================================
-- LINKAGE — mirrors core.meeting so a funded deal attributes the SAME way a meeting does
-- (DDL 20 / 64 / 70): channel-aware, lead-email-joinable, advisor/IM/partner-normalized.
-- Type conventions match the rest of the warehouse:
--     text                     -> VARCHAR
--     timestamp with time zone -> TIMESTAMPTZ
--     numeric / money / rate   -> DOUBLE
-- Idempotent upsert semantics will live in entities/deal_funded.py (insert net-new
-- deal_id, update last-seen fields) once a source is wired — same shape as entities/
-- meeting.py. The table is canonical (core schema), NOT append-only.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;

-- ============================================================================
-- core.deal_funded — one row per FUNDED deal (the terminal conversion).
-- Empty until a funded-deal source (almost certainly a Funding-Form column / new sheet
-- tab) is ingested. NO seed, NO backfill in this DDL.
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.deal_funded (
  -- Identity ----------------------------------------------------------------
  deal_id            VARCHAR PRIMARY KEY,    -- stable surrogate; once a source is wired,
                                             -- derived deterministically (e.g.
                                             -- md5(source || ':' || source_event_id) or
                                             -- '{sheet_id}:{row_index}') so re-ingest is idempotent.
  source             VARCHAR NOT NULL,       -- 'funding_form' (expected) | 'sheet' | 'close' | 'manual'
  source_event_id    VARCHAR,               -- original row/record id from the source (provenance)

  -- Funnel linkage (attribute a funded deal the SAME way a meeting attributes) -------
  meeting_id         VARCHAR,               -- -> core.meeting.meeting_id (the booked meeting it converted from)
  lead_key           VARCHAR,               -- -> core.lead.lead_key (md5(coalesce(lower(email),phone_e164)))
  lead_email         VARCHAR,               -- lower-cased; lead-grain joins (mirrors core.meeting.lead_email)
  campaign_id        VARCHAR,               -- -> core.campaign.campaign_id (the source campaign, if attributed)
  campaign_name_raw  VARCHAR,               -- literal campaign name as it appeared in the source
  workspace_id       VARCHAR,               -- Instantly workspace the lead/campaign belongs to
  channel            VARCHAR,               -- booking/funnel channel (Email/SMS/WhatsApp/Call/LinkedIn) — channel-aware KPI

  -- People / partner (normalized exactly like core.meeting: DDL 70) ------------------
  cm                 VARCHAR,               -- campaign manager (samuel/sam/ido/leo/eyver/...); resolve like core.meeting.cm
  advisor            VARCHAR,               -- RAW advisor string, "<PARTNER_PREFIX>: <Full Name>" (sheet idx5)
  advisor_name       VARCHAR,               -- name portion only (after ": ")
  advisor_partner    VARCHAR,               -- partner the prefix maps to (BTC->Big Think Capital, GQ->GoQualifi, ...)
  inbox_manager      VARCHAR,               -- Funding-Form Inbox Manager, full-name normalized (sheet idx15)
  partner_key        VARCHAR,               -- -> core.funding_partner.partner_key (normalized slug; commission source)
  partner_label      VARCHAR,               -- normalized display label (PARTNER_NORM); NULL/'(unattributed)' if unmatched

  -- Money — the KPI numerator inputs ------------------------------------------------
  funded_date        DATE,                  -- date the deal funded (the rollup window key)
  amount_funded      DOUBLE,                -- principal/amount funded to the merchant (deal size)
  commission_rate    DOUBLE,               -- our commission as a fraction (e.g. 0.12). NULL until known.
  commission_amount  DOUBLE,               -- our commission in USD. PREFERRED if the source gives it directly;
                                            -- otherwise derive amount_funded * commission_rate at load (NOT in views).
  currency           VARCHAR DEFAULT 'USD', -- ISO currency of amount/commission (assume USD until proven otherwise)
  deal_status        VARCHAR,               -- 'funded' (canonical here) | 'clawback' | 'reversed' — for net-of-clawback later

  -- Free-form provenance ------------------------------------------------------------
  notes              VARCHAR,
  raw_text           VARCHAR,               -- raw source row text (audit trail; mirrors core.meeting.raw_text)

  -- Ingest bookkeeping (idempotency + which run created/last-touched the row) ---------
  row_hash           VARCHAR,               -- hash of the source-meaningful fields; skip write when unchanged
  _first_run_id      VARCHAR,
  _last_run_id       VARCHAR,
  _first_seen_at     TIMESTAMPTZ,
  _last_seen_at      TIMESTAMPTZ,
  _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Common access paths: per-window / per-CM / per-partner / per-workspace rollups + linkage.
CREATE INDEX IF NOT EXISTS ix_deal_funded_funded_date ON core.deal_funded (funded_date);
CREATE INDEX IF NOT EXISTS ix_deal_funded_cm          ON core.deal_funded (cm);
CREATE INDEX IF NOT EXISTS ix_deal_funded_partner     ON core.deal_funded (partner_key);
CREATE INDEX IF NOT EXISTS ix_deal_funded_workspace   ON core.deal_funded (workspace_id);
CREATE INDEX IF NOT EXISTS ix_deal_funded_meeting     ON core.deal_funded (meeting_id);
CREATE INDEX IF NOT EXISTS ix_deal_funded_lead_email  ON core.deal_funded (lead_email);
CREATE INDEX IF NOT EXISTS ix_deal_funded_campaign    ON core.deal_funded (campaign_id);

-- ============================================================================
-- core.v_deal_funded_resolved — single grain the rollups read. Resolves the commission
-- consistently (prefer an explicit commission_amount; else amount_funded * commission_rate)
-- and excludes non-funded statuses (clawback/reversed) so every rollup counts FUNDED only.
-- EMPTY-safe: returns 0 rows over an empty table, never errors.
-- ============================================================================
CREATE OR REPLACE VIEW core.v_deal_funded_resolved AS
SELECT
  d.deal_id,
  d.funded_date,
  date_trunc('month', d.funded_date)        AS funded_month,
  d.cm,
  COALESCE(NULLIF(d.partner_label,''), '(unattributed)') AS partner_label,
  d.partner_key,
  d.workspace_id,
  d.channel,
  d.advisor_name,
  d.inbox_manager,
  d.campaign_id,
  d.amount_funded,
  -- Canonical commission resolution. Explicit amount wins; else rate*amount; else NULL
  -- (NEVER fabricated — a deal with neither is surfaced with NULL commission, not 0).
  COALESCE(
    d.commission_amount,
    d.amount_funded * d.commission_rate
  )                                          AS commission_resolved,
  d.commission_rate,
  d.deal_status
FROM core.deal_funded d
WHERE COALESCE(d.deal_status, 'funded') = 'funded';

-- ============================================================================
-- Rollups — deals + $ funded + commission, per dimension and per time window.
-- All four are EMPTY-safe (GROUP BY over zero rows = zero rows; no division here).
-- ============================================================================

-- Per CM (campaign manager).
CREATE OR REPLACE VIEW core.v_deals_funded_by_cm AS
SELECT
  COALESCE(NULLIF(cm,''), '(no cm)')        AS cm,
  COUNT(*)                                   AS deals_funded,
  SUM(amount_funded)                         AS amount_funded,
  SUM(commission_resolved)                   AS commission_total
FROM core.v_deal_funded_resolved
GROUP BY 1;

-- Per funding partner (normalized label; commission terms live on core.funding_partner).
CREATE OR REPLACE VIEW core.v_deals_funded_by_partner AS
SELECT
  partner_label,
  partner_key,
  COUNT(*)                                   AS deals_funded,
  SUM(amount_funded)                         AS amount_funded,
  SUM(commission_resolved)                   AS commission_total
FROM core.v_deal_funded_resolved
GROUP BY 1, 2;

-- Per workspace.
CREATE OR REPLACE VIEW core.v_deals_funded_by_workspace AS
SELECT
  COALESCE(NULLIF(workspace_id,''), '(unknown)') AS workspace_id,
  COUNT(*)                                   AS deals_funded,
  SUM(amount_funded)                         AS amount_funded,
  SUM(commission_resolved)                   AS commission_total
FROM core.v_deal_funded_resolved
GROUP BY 1;

-- Per time window (monthly grain — daily/weekly = GROUP BY date_trunc over this/base view).
CREATE OR REPLACE VIEW core.v_deals_funded_by_month AS
SELECT
  funded_month,
  COUNT(*)                                   AS deals_funded,
  SUM(amount_funded)                         AS amount_funded,
  SUM(commission_resolved)                   AS commission_total
FROM core.v_deal_funded_resolved
GROUP BY 1;

-- ============================================================================
-- core.v_kpi_bottom_line — THE strategic bottom-line KPI wiring:
--     deals_funded × commission ÷ all-in cost
-- (feedback_bottom_line_kpi_only: "Strategic KPI = deals-funded × commission ÷ all-in cost").
-- Grain: monthly. Daily/weekly/quarterly = re-aggregate from the base rollups, NEVER by
-- averaging the ratio rows (period-ratio rule, same as v_kpi_email).
--
-- NUMERATOR  = SUM(commission_resolved) over funded deals in the month  (this is the
--              "deals_funded × commission" product expressed at the deal grain — each deal
--              already carries its own commission, so summing commission across the funded
--              deals IS deals × commission; deals_funded is carried alongside for display).
-- DENOMINATOR = all-in cost for the month, from core.cost_ledger (the authoritative cost
--              fact, DDL 13). Cost rows are amortizable (period_start/period_end +
--              amortize_method) and attributed (attribution_dim/id). The TRUE all-in monthly
--              cost requires the amortization expansion (a month overlaps multi-month
--              subscriptions / annual spreads) which is NOT yet built as a warehouse fact.
--
--   *** COST DENOMINATOR IS A STUBBED JOIN POINT — TODO ***
--   Until a monthly all-in-cost FACT/view exists (proposed: derived.v_all_in_cost_monthly,
--   the correct amortized + globally-allocated roll-up of core.cost_ledger), this view does
--   NOT fabricate a cost number. It LEFT JOINs the join point and leaves all_in_cost +
--   roi_per_dollar / net_margin NULL when no cost fact is present. A NULL denominator yields
--   a NULL ratio (never a divide-by-zero, never a fake 0). Wire the cost view, then this KPI
--   lights up with NO change to this file (the LEFT JOIN target just starts returning rows).
--
-- EMPTY-safe: with deal_funded empty, the deals CTE returns 0 rows -> the view returns 0 rows.
-- ============================================================================
CREATE OR REPLACE VIEW core.v_kpi_bottom_line AS
WITH deals AS (
  SELECT
    funded_month,
    COUNT(*)                  AS deals_funded,
    SUM(amount_funded)        AS amount_funded,
    SUM(commission_resolved)  AS commission_total   -- = Σ(deals × commission)
  FROM core.v_deal_funded_resolved
  GROUP BY 1
),
-- ── COST JOIN POINT (STUB) ───────────────────────────────────────────────────────
-- Replace this CTE body with a SELECT from the real monthly all-in-cost view once it
-- exists, e.g.:  SELECT cost_month AS funded_month, all_in_cost FROM derived.v_all_in_cost_monthly
-- It is deliberately empty today (WHERE FALSE) so the KPI ships with NO fabricated cost.
cost AS (
  SELECT
    CAST(NULL AS TIMESTAMP) AS funded_month,
    CAST(NULL AS DOUBLE)    AS all_in_cost
  WHERE FALSE
)
SELECT
  d.funded_month,
  d.deals_funded,
  d.amount_funded,
  d.commission_total,                              -- KPI numerator (deals × commission)
  c.all_in_cost,                                   -- KPI denominator (NULL until cost fact wired)
  -- Bottom-line ratios. NULL (not 0, not error) until the cost denominator is wired.
  CASE WHEN c.all_in_cost IS NOT NULL AND c.all_in_cost <> 0
       THEN d.commission_total / c.all_in_cost END AS roi_commission_per_dollar,  -- commission ÷ all-in cost
  CASE WHEN c.all_in_cost IS NOT NULL
       THEN d.commission_total - c.all_in_cost END AS net_contribution            -- commission − all-in cost
FROM deals d
LEFT JOIN cost c ON c.funded_month = d.funded_month;
