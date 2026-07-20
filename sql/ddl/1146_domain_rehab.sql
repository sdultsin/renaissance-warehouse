-- @gate: add
-- Depends on 1142
-- ============================================================================
-- 1146_domain_rehab.sql — Domain-rehab automation: scorer view + state / audit /
--   cancel-ledger tables. SHADOW-mode DDL (task #20 / Scope 3).
--
-- SPEC: deliverables/2026-07-19-domain-rehab-automation/{SPEC.md,
--   HANDOFF-TO-MAIN-CHAT.md, ARM-PREP-RULINGS.md}. The orchestrator
--   (domain_rehab_orchestrator.py) is dry-run gated on REHAB_LIVE=1; these objects
--   are its warehouse home. Applying this DDL actions NOTHING — it only lets the
--   shadow orchestrator score domains and persist its would-do ledger/audit/queue.
--
-- WHY: an autonomous per-DOMAIN deliverability state machine
--   (active -> rehab -> active | cancel) with two entry triggers: Spamhaus DBL
--   listing and human-reply-rate below the domain's per-infrastructure baseline.
--   Thresholds are Sam-CONFIRMED (ARM-PREP-RULINGS #1), held FIXED (anchored to
--   each infra's own p10/p25), NOT rolling percentiles.
--
-- INFRA DERIVATION (the key adaptation to the current warehouse): the SPEC's draft
--   assumed core.v_domain_reply_daily carried an `infra` column and columns
--   reply_date/unique_replies — it does NOT. The live view exposes
--   (date, domain, sent, human_replies, auto_replies, human_reply_rate_pct, ...).
--   Infra is instead derived here from the DOMINANT provider_group at the send grain
--   in main.raw_instantly_account_daily: imap -> otd, google -> reseller,
--   outlook -> milkbox (verified 2026-07-19: imap 31,438 dom / google 5,261 /
--   outlook 984; matches the account-tag families 1:1, zero multi-family ambiguity).
--
-- THRESHOLDS + eligibility (SPEC §3, Sam-confirmed):
--   OTD:      min_sends 1000 · rehab-line 0.30% · retire-band 0.18%
--   Reseller: min_sends 1000 · rehab-line 0.55% · retire-band 0.38%
--   Milkbox:  EXCLUDED (scored+logged, never actioned)
--   Human RR = 100 * SUM(human_replies) / SUM(sent) over a trailing 30d window.
--   NOTE on `state`: it is the RR-SEVERITY band. The actuator treats BOTH 'rehab'
--   and 'retire' bands as "below the rehab line". A domain is only CANCELLED after
--   TWO failed rehab tries (or an immediate Spamhaus listing / straight-to-cancel
--   retire band). 'retire' here != auto-cancel of the account.
--
-- DATA-DEPTH CAVEAT (shadow): core.v_domain_reply_daily is warehouse-native and only
--   has history from 2026-07-10 (the sync rebuild). Until the trailing window fills
--   (~30d), few/no domains clear the 1,000-send eligibility gate, so RR bands read
--   'unscored' and ramp in over the next ~1-2 weeks. Spamhaus immediate-cancels do
--   NOT depend on the window and fire from day one. The view is correct as-is; this
--   is a data-accumulation property, exactly why we shadow before arming.
--
-- LOAD: read-only views + orchestrator-written tables. The orchestrator writes
--   local JSON/JSONL in shadow; at go-live it mirrors into these tables.
--
-- Reversible: remove the new objects created below — the view
--   core.v_domain_rr_state, the tables core.domain_cancel_ledger,
--   core.domain_rehab_event, core.domain_rehab_state, and the sequence
--   core.domain_rehab_event_seq. All are new/additive; no existing object, column,
--   or row is altered, so the down-migration is a clean removal with zero data loss.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- 1. SCORER — core.v_domain_rr_state
--    Per-domain infra + trailing-30d human RR + severity band + eligibility.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_domain_rr_state AS
WITH dom_prov AS (   -- total sends per (domain, provider_group) over the window
  SELECT domain, provider_group, SUM(sent) AS sent_sum
  FROM main.raw_instantly_account_daily
  WHERE domain IS NOT NULL
  GROUP BY domain, provider_group
),
dom_infra AS (   -- authoritative infra = provider_group with the MOST SENDS per domain
  SELECT                            -- (send-weighted, not day-count-weighted; honors the send grain)
    domain,
    CASE arg_max(provider_group, sent_sum)
      WHEN 'imap'    THEN 'otd'
      WHEN 'google'  THEN 'reseller'
      WHEN 'outlook' THEN 'milkbox'
      ELSE 'unknown'
    END AS infra
  FROM dom_prov
  GROUP BY domain
),
win AS (              -- trailing 30d per-domain reply aggregate (window = calibration horizon)
  SELECT
    domain,
    SUM(sent)          AS sent_30d,
    SUM(human_replies) AS human_30d,
    SUM(auto_replies)  AS auto_30d,
    SUM(human_replies) * 100.0 / NULLIF(SUM(sent), 0) AS human_rr_pct
  FROM core.v_domain_reply_daily
  WHERE date >= CURRENT_DATE - 30
  GROUP BY domain
)
SELECT
  w.domain,
  COALESCE(di.infra, 'unknown')                         AS infra,
  w.sent_30d,
  w.human_30d,
  ROUND(w.human_rr_pct, 4)                              AS human_rr,
  CASE
    WHEN di.infra = 'milkbox'                                     THEN 'excluded'
    WHEN di.infra IS NULL OR di.infra NOT IN ('otd', 'reseller') THEN 'unknown_infra'
    WHEN w.sent_30d < 1000                                        THEN 'unscored'         -- eligibility gate
    WHEN di.infra = 'otd'      AND w.human_rr_pct < 0.18          THEN 'retire'           -- severity band
    WHEN di.infra = 'otd'      AND w.human_rr_pct < 0.30          THEN 'rehab'
    WHEN di.infra = 'otd'                                         THEN 'good'
    WHEN di.infra = 'reseller' AND w.human_rr_pct < 0.38          THEN 'retire'
    WHEN di.infra = 'reseller' AND w.human_rr_pct < 0.55          THEN 'rehab'
    WHEN di.infra = 'reseller'                                    THEN 'good'
    ELSE 'unknown_infra'                                                                  -- defensive terminal
  END                                                   AS state,
  CURRENT_DATE                                          AS scored_on
FROM win w
LEFT JOIN dom_infra di USING (domain);

-- ---------------------------------------------------------------------------
-- 2. STATE LEDGER — core.domain_rehab_state (source of truth for the machine)
--    lifecycle: active | rehab_rest (14d cold off) | rehab_relaunch (14d cold on
--    at low volume, generating fresh reply data to judge on) | cancel_queued.
--    rehab_tries 1-2 (each = rest+relaunch); resets to 0 on recovery to active.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.domain_rehab_state (
  domain          VARCHAR PRIMARY KEY,
  infra           VARCHAR,                                -- 'otd' | 'reseller'
  lifecycle       VARCHAR NOT NULL DEFAULT 'active',
  rehab_tries     INTEGER NOT NULL DEFAULT 0,
  entered_at      TIMESTAMPTZ,                            -- when the current phase started
  flagged_at      TIMESTAMPTZ,                            -- when first flagged (episode start)
  trigger         VARCHAR,                                -- rr_below_rehab | rr_really_bad | spamhaus_dbl
  last_human_rr   DOUBLE,
  next_action_at  DATE,                                   -- next send (rest) / round-2 judge (relaunch)
  reason          VARCHAR,                                -- set when cancel_queued
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 3. AUDIT TRAIL — core.domain_rehab_event (one row per orchestrator action;
--    "no automation without tracking"). dry_run TRUE for every shadow row.
-- ---------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS core.domain_rehab_event_seq START 1;

CREATE TABLE IF NOT EXISTS core.domain_rehab_event (
  event_id        BIGINT DEFAULT nextval('core.domain_rehab_event_seq'),  -- monotonic id
  ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
  domain          VARCHAR NOT NULL,
  infra           VARCHAR,
  workspaces      VARCHAR[],                              -- account workspaces touched
  action          VARCHAR NOT NULL,                       -- ENTER_REST|RELAUNCH|REACTIVATE|CANCEL
  reason          VARCHAR,                                -- rr_below_rehab|rr_really_bad|rest_done|recovered|still_below_retry|failed_2_rehab_tries|spamhaus_dbl
  human_rr        DOUBLE,
  rehab_tries     INTEGER,
  lifecycle       VARCHAR,                                -- resulting lifecycle
  next_action_at  DATE,
  dry_run         BOOLEAN NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- 4. CANCEL LEDGER — core.domain_cancel_ledger  (ARM-PREP-RULINGS #2, MVP req)
--    The human-facing VENDOR-cancellation queue. The AUTOMATED half (registrar
--    auto-renew-off + de-Warmy) is done by the orchestrator (gated on REHAB_LIVE);
--    this ledger is the HUMAN half — every account that needs a vendor cancel, with
--    the infra->assignee routing (OTD->Max; Reseller/Milkbox->Darcy), the reply-grace
--    deadline (earliest domain expiry), and billing anchors where known.
--    Cancel is TERMINAL (Sam 07-19): no billing-expiry hold, no reuse.
--    priority: 'immediate' (spamhaus | rr_really_bad) | 'normal' (failed 2 rehab tries)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.domain_cancel_ledger (
  domain            VARCHAR PRIMARY KEY,
  infra             VARCHAR,                              -- otd | reseller | milkbox
  workspaces        VARCHAR[],                            -- workspace slug(s) the inboxes sit in
  account_emails    VARCHAR[],                            -- the inbox(es) on this domain to cancel
  reason            VARCHAR NOT NULL,                     -- spamhaus_dbl | rr_really_bad | rehab_exhausted
  priority          VARCHAR NOT NULL,                     -- immediate | normal
  rr_at_flag        DOUBLE,                               -- human RR when flagged
  registrar         VARCHAR,                              -- core.domain_registry.registrar
  registrar_account VARCHAR,                              -- core.domain_registry.registrar_account
  earliest_expiry   DATE,                                 -- reply-grace deadline (domain_registry.expires_at)
  purchased_at      DATE,                                 -- billing anchor where known
  assignee          VARCHAR,                              -- 'Max' (otd) | 'Darcy' (reseller|milkbox)
  queued_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  status            VARCHAR NOT NULL DEFAULT 'queued',    -- queued | auto_renew_off | de_warmy_done | cancelled
  auto_renew_off_at TIMESTAMPTZ,                          -- when automated registrar auto-renew-off ran
  de_warmy_done     BOOLEAN DEFAULT FALSE,                -- accounts removed from Warmy pool
  reply_catch_set   BOOLEAN DEFAULT FALSE,                -- catch-all / Cloudflare reply routing in place (SPEC §12)
  note              VARCHAR,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
