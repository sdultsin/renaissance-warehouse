-- @gate: add
-- Depends on: none (additive table + index)
-- core.campaign_infra — persistent campaign→sending-infra registry. Version 1071. [2026-07-03]
--
-- WHY THIS EXISTS (campaign-truth build, TKT-1 + TKT-2 unified — DESIGN §1):
--   Sending infra per campaign was figured out impromptu every time. The live surfaces
--   are lossy: core.account_campaign is truncate-and-reload (135 campaigns in today's
--   census vs 809 that sent in June — 83% already lost their live settings surface),
--   core.campaign_sending_tag is frozen at 2026-06-15, and campaign NAMES are unstable
--   (bounce guard prefixes 'BOUNCED '). campaign_id is the only stable key.
--
-- FIX: one durable row per campaign_id ever seen, UPSERT-ONCE / NEVER TRUNCATED,
--   populated nightly by entities/campaign_infra.py ('derived' phase). Two sides:
--
--   SENDING ARM (immutable-ish): infra_vendor / infra_esp / matched_tag / mixed_infra /
--     mixed_detail / n_accounts / derivation_source / derivation_note. Set once; a later
--     run may overwrite ONLY when its source is STRICTLY higher on the precedence ladder
--     or the current value is 'unknown' — every upgrade is appended to derivation_note.
--     Precedence (high→low):
--       manual(7) > dim_tag(6) > census_majority(5) > manifest_pump_day(4)
--       > frozen_tag_table(3) > rg_partner(2) > name_heuristic(1) > unknown(0)
--     Tag→(vendor,esp) map (Sam-specified; anything else = vendor from the raw tag,
--     esp 'unknown', surfaced — never guessed):
--       'Outreach Today%'/'OTD' → (OTD, OTD) · 'Reseller%' → (Reseller, google)
--       · 'Milkbox%' → (MilkBox, outlook) · 'Cheap Inboxes%' → (CheapInboxes, unknown)
--       · 'Google Panel%' → (GooglePanel, unknown) · 'MS Panel%' → (MSPanel, unknown)
--     ≥2 infra families on one campaign → first by that order + mixed_infra=true
--     (one-infra-per-campaign is Sam's invariant; never average or split).
--
--   RECIPIENT SIDE (recomputed nightly — measured stats, ALLOWED to change; they
--     self-improve as the unknown-ESP DNS backfill lands): send-weighted mix of
--     pg contact_frequency_campaign_daily vs core.recipient_domain.
--     Label rule: sends_total>=100 AND share>=0.8 → dominant ESP; sends_total>=100
--     AND the KNOWN buckets genuinely disagree (dominant/known-total < 0.8) →
--     'mixed'; else 'unknown' — when unknown-domain coverage is what prevents a
--     >=0.8 share the label is 'unknown' (coverage gap), never a definite 'mixed'.
--     unknown-domain sends stay in the unknown bucket and the dominant-share
--     denominator (100%-or-wipe: never redistributed).
--     A labeled recipient_esp is never downgraded to 'unknown' by a window with no sends.
--
-- GRAIN: one row per campaign_id (PK). Writes are UPDATE...FROM staged + INSERT
--   anti-join (NO ON CONFLICT DO UPDATE — the ART-index INTERNAL duplicate-key abort
--   class documented in scripts/backfill_account_tags_full.py).
--
-- Verified read-only on serving snapshot warehouse_20260703_043558_874.duckdb:
--   * universe (dim ∪ census ∪ sent>0 since 2026-05-01) = 1,877 campaigns
--     (dim 272 · census 135 · sent-since-May 1,842); 1,836 have a
--     raw_pipeline_campaigns identity row, all 1,836 slug-resolve via core.workspace.
--   * dim_tag classification: OTD 168 · Reseller 19 · MilkBox 7 · raw-tag others 48
--     (Google 43, I-Google/MailIn/Outlook/Instantly - Pre-warms/ai-sdr-… 1 each).
--   * census_majority (account_campaign ⋈ account_tags): OTD 112 · Reseller 20, 8 mixed.
--   * dim_tag vs census_majority both-resolved 57, disagree 4 (kept dim, flagged).
--   * frozen core.campaign_sending_tag: OTD 102 · Reseller 15 · raw Outlook 83 /
--     Google 28 / MailIn 16 / Outlook PP 16 / Gmail 3 / … (RG#### batch tags excluded).

CREATE TABLE IF NOT EXISTS core.campaign_infra (
  campaign_id             VARCHAR PRIMARY KEY,
  -- identity
  workspace_slug          VARCHAR,          -- canonical slug (core.workspace); NULL if unresolvable (raw id kept in derivation_note)
  first_seen_name         VARCHAR,          -- immutable after insert (names are unstable)
  last_seen_name          VARCHAR,
  campaign_status         INTEGER,
  -- sending arm (upgrade only by strictly-higher precedence, or from 'unknown')
  infra_vendor            VARCHAR NOT NULL DEFAULT 'unknown',
  infra_esp               VARCHAR NOT NULL DEFAULT 'unknown',
  matched_tag             VARCHAR,          -- the raw tag / family label that decided the vendor
  mixed_infra             BOOLEAN NOT NULL DEFAULT false,
  mixed_detail            VARCHAR,          -- 'families:a+b' and/or 'disagrees:census=<fam>'
  n_accounts              INTEGER,          -- distinct census accounts backing a census_majority derivation
  derivation_source       VARCHAR NOT NULL DEFAULT 'unknown',
  derivation_note         VARCHAR,          -- append-only audit trail (upgrades, raw workspace id fallback)
  -- recipient side (measured, recomputed — allowed to change)
  recipient_esp           VARCHAR NOT NULL DEFAULT 'unknown',  -- microsoft|google|other|mixed|unknown
  recipient_esp_share     DOUBLE,           -- dominant-bucket share of sends_total (unknown stays in denominator)
  recipient_sends_total   BIGINT,
  recip_sends_google      BIGINT,
  recip_sends_microsoft   BIGINT,
  recip_sends_other       BIGINT,
  recip_sends_unknown     BIGINT,
  recipient_esp_source    VARCHAR,          -- 'send_mix' | 'manifest_pump_day'
  recipient_esp_computed_at TIMESTAMPTZ,
  -- lifecycle
  first_seen_at           TIMESTAMPTZ DEFAULT now(),
  last_seen_live_at       TIMESTAMPTZ,
  _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id                 VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_campaign_infra_workspace_slug
  ON core.campaign_infra (workspace_slug);
