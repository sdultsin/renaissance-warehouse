-- Version 60 (2026-06-12) — infra batch dimension + account→batch bridge +
-- domain CSV enrichment + batch-lifecycle derived views.
--
-- Connects Darcy's two manual infra exports into the warehouse so we can answer,
-- for any account / domain / batch / workspace / TLD: which batch it belongs to,
-- that batch's partner/workspace/offer/provider, and when warmup / cold-email
-- started — joined to the warehouse's real per-account daily send actuals.
--
-- Sources (point-in-time snapshots; sheet last-QA'd Apr 29 2026):
--   1. "Batches - Renaissance" Google Sheet  → core.infra_batch / infra_batch_root
--   2. "FINAL DATA - ALL.csv" (account-level) → core.sending_account_batch /
--                                               core.domain_infra_csv
-- Population is NOT in this DDL (structure only, version-gated). It is loaded from
-- compact parquet extracts by scripts/build_infra_batch.sql, run when a fresh
-- export drops (these are manual snapshots, not a nightly feed). The derived views
-- are live (query current sending_account_daily) so they stay fresh regardless.
--
-- Batch-join crux: the Sheet (B36.1-.5, B54-R, "1st batch-R") and the CSV (B36,
-- B54, "1st_batch") disagree on labels. batch_root() normalizes both to a join key
-- with 100% account coverage (0 unmatched). The -R reconnect rows are the SAME
-- inboxes migrated to the Funding 1-6 workspaces; the rollup takes MIN(date) across
-- a root family = the TRUE biological warmup/cold start (the -R row shows an
-- artificially-later Instantly date — see the Sheet's own "Explanation row"), and
-- the current workspace from the warehouse per-account, not the stale Sheet string.
--
-- Domain-purchase reality: this export's "Domain Purchase Date" and "Domain
-- Registrar" columns are EMPTY (0% / all-NULL); only accounts_per_domain and a
-- clean expiration-date subset survive. core.domain_registry remains the canonical
-- source for purchase/registrar — no clobber.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;

-- --------------------------------------------------------------------------
-- core.infra_batch — one row per Sheet batch label (the full ~165-row detail)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.infra_batch (
    batch_label         VARCHAR PRIMARY KEY,    -- e.g. 'B54-R', 'B36.1', '1st batch-R', 'Emran Batch 10'
    batch_root          VARCHAR NOT NULL,       -- canonical join key (B54 / B36 / 1STBATCH / EMRANBATCH10)
    is_reconnect        BOOLEAN DEFAULT FALSE,  -- '-R' rebuild row (same inboxes, later Instantly warmup date)
    partner             VARCHAR,                -- Outreach Today / MailIn / Inboxing / Cheap Inboxes / Panel / ...
    workspace_raw       VARCHAR,                -- Sheet workspace string (stale; current truth = sending_account)
    n_domains_sheet     INTEGER,                -- Sheet '# of Domains' (reference/QA only, NOT authoritative count)
    sip_raw             VARCHAR,                -- raw SIP cell (some are text "End of May / Beginning of June 2026")
    sip_date            DATE,                   -- parsed SIP date (NULL where non-date text)
    warmup_raw          VARCHAR,
    warmup_start_date   DATE,                   -- Batch-Info "Warmup Start Date" (col F) — chosen truth column
    cold_raw            VARCHAR,
    cold_start_date     DATE,                   -- explicit "Cold Email Start Date" when present
    warmup_start_qa     DATE,                   -- QA-block second warmup col (can disagree w/ warmup_start_date)
    billing_date        DATE,
    offer               VARCHAR,                -- Funding / Section 125 / ERC
    provider            VARCHAR,                -- Google / Outlook
    batch_url           VARCHAR,
    qa_num_accounts     VARCHAR,
    qa_started          VARCHAR,
    qa_settings_correct VARCHAR,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_infra_batch_root ON core.infra_batch (batch_root);

-- --------------------------------------------------------------------------
-- core.infra_batch_root — one row per canonical batch_root (the JOIN TARGET for
-- the lifecycle views). Rolls up decimal sub-batches (B36.1-.5) and -R reconnects
-- to true-start dates + current canonical metadata.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.infra_batch_root (
    batch_root          VARCHAR PRIMARY KEY,
    partner             VARCHAR,                -- from canonical row (prefer -R / latest)
    workspace_raw       VARCHAR,                -- canonical Sheet workspace string (reference)
    offer               VARCHAR,
    provider            VARCHAR,
    sip_date            DATE,                   -- MIN across family (earliest true SIP)
    warmup_start_date   DATE,                   -- MIN across family (true biological warmup start)
    cold_start_date     DATE,                   -- MIN of explicit cold dates across family (NULL if none)
    cold_start_resolved DATE,                   -- explicit when present, else warmup + median gap
    cold_start_source   VARCHAR,                -- 'explicit' | 'derived_median' | 'unknown'
    n_sheet_rows        INTEGER,                -- # Sheet labels under this root
    has_reconnect       BOOLEAN DEFAULT FALSE,
    n_domains_sheet_sum INTEGER,                -- reference sum (NOT authoritative; -R may double-count)
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- --------------------------------------------------------------------------
-- core.sending_account_batch — account→batch bridge (one row per email; full
-- historical fleet, ~2.55M, superset of live core.sending_account ~1.36M).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sending_account_batch (
    account_email       VARCHAR PRIMARY KEY,    -- lower(trim(email)); join to lower(sending_account.account_id)
    domain              VARCHAR,
    raw_batch           VARCHAR,                -- CSV batch label as-is
    batch_root          VARCHAR,                -- normalized join key
    raw_workspace       VARCHAR,                -- CSV workspace string (stale)
    provider_tag        VARCHAR,                -- Outreach Today / MailIn / Inboxing / Panel
    offer               VARCHAR,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_sab_root   ON core.sending_account_batch (batch_root);
CREATE INDEX IF NOT EXISTS ix_sab_domain ON core.sending_account_batch (domain);

-- --------------------------------------------------------------------------
-- core.domain_infra_csv — per-domain CSV enrichment (staging for cross-validation
-- against core.domain_registry; does NOT clobber it). purchase_date / registrar
-- are absent from the export, so only accounts_per_domain + expiration survive.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.domain_infra_csv (
    domain              VARCHAR PRIMARY KEY,
    tld                 VARCHAR,
    accounts_per_domain INTEGER,                -- cleaned ('99', '99 Accounts'); ~10k malformed rows dropped
    expiration_date     DATE,                   -- clean date subset of "Domain Expiration Date"
    n_accounts_in_csv   INTEGER,                -- # CSV account rows on this domain
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- ==========================================================================
-- Derived views
-- ==========================================================================

-- Per-account lifecycle: account → batch dates → live warehouse state. Lightweight
-- (no daily-sends scan); use v_batch_lifecycle_summary for send rollups.
CREATE OR REPLACE VIEW derived.v_batch_lifecycle AS
SELECT
    b.account_email,
    b.batch_root,
    (ib.batch_root IS NOT NULL)        AS batch_known,
    sa.account_id                       AS warehouse_account_id,
    (sa.account_id IS NOT NULL)         AS in_warehouse,
    sa.workspace_slug                   AS current_workspace_slug,   -- warehouse truth (current)
    b.raw_workspace,
    b.domain,
    COALESCE(dom.tld, dr.tld)           AS tld,
    b.provider_tag,
    b.offer,
    ib.partner,
    ib.provider,
    ib.sip_date,
    ib.warmup_start_date,
    ib.cold_start_resolved              AS cold_start_date,
    ib.cold_start_source,
    sa.lifecycle_state,
    sa.status                           AS account_status,
    sa.is_active,
    sa.warmup_score,
    sa.last_seen_at,
    dr.purchased_at                     AS domain_purchased_at,      -- canonical (registry)
    dom.expiration_date                 AS domain_expiration_csv,
    dom.accounts_per_domain
FROM core.sending_account_batch b
LEFT JOIN core.infra_batch_root  ib  USING (batch_root)
LEFT JOIN core.sending_account   sa  ON lower(sa.account_id) = b.account_email
LEFT JOIN core.domain_infra_csv  dom ON dom.domain = b.domain
LEFT JOIN core.domain_registry   dr  ON dr.domain  = b.domain;

-- Per batch_root × current workspace rollup with REAL send actuals from the
-- warehouse (account/domain counts come from the join, never the Sheet numbers).
CREATE OR REPLACE VIEW derived.v_batch_lifecycle_summary AS
WITH acct_sends AS (  -- per-account FIRST, so sends never fan out across the grain
    SELECT account_id,
           sum(actual_sends)                                         AS total_actual_sends,
           sum(actual_sends) FILTER (WHERE date >= current_date - 7) AS sends_last7,
           max(date) FILTER (WHERE actual_sends > 0)                 AS last_send_date
    FROM core.sending_account_daily GROUP BY 1
),
acct AS (
    SELECT b.batch_root, sa.workspace_slug, b.account_email, b.domain,
           sa.is_active, sa.lifecycle_state, sa.account_id,
           s.total_actual_sends, s.sends_last7, s.last_send_date
    FROM core.sending_account_batch b
    LEFT JOIN core.sending_account sa ON lower(sa.account_id) = b.account_email
    LEFT JOIN acct_sends            s  ON lower(s.account_id)  = b.account_email
)
SELECT
    a.batch_root,
    a.workspace_slug                                       AS current_workspace_slug,
    ib.partner, ib.offer, ib.provider,
    ib.sip_date, ib.warmup_start_date,
    ib.cold_start_resolved AS cold_start_date, ib.cold_start_source,
    count(*)                                               AS n_accounts_in_batch,
    count(*) FILTER (WHERE a.account_id IS NOT NULL)       AS n_in_warehouse,
    count(*) FILTER (WHERE a.is_active)                    AS n_active,
    count(*) FILTER (WHERE a.lifecycle_state = 'warming')  AS n_warming,
    count(DISTINCT a.domain)                               AS n_domains,
    sum(a.total_actual_sends)                              AS total_actual_sends,
    sum(a.sends_last7)                                     AS sends_last7,
    max(a.last_send_date)                                  AS last_send_date
FROM acct a
LEFT JOIN core.infra_batch_root ib USING (batch_root)
GROUP BY ALL;
