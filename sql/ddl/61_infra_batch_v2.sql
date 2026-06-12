-- Version 61 (2026-06-12) — infra-batch layer v2: corrected batch semantics.
--
-- Supersedes the DDL-60 structure after Sam's corrections to what the labels MEAN:
--
--   * `-R` suffix = REPLACEMENT SET. A batch that got disconnected was replaced by
--     a NEW set of inboxes on NEW domains. Verified in the data: every base/-R pair
--     has ZERO account overlap and ZERO domain overlap. DDL 60 wrongly collapsed
--     base+-R into one "root" with MIN-date rollup — that merged two different
--     generations of infrastructure. -R batches are now first-class separate batches.
--
--   * Decimal suffix (B36.1-.5) = ONE batch SPLIT ACROSS N WORKSPACES (the decimal
--     counts workspaces). The account CSV uses the base label (B36) for all of it,
--     so account→sub-workspace attribution is not available (and per Sam, not
--     required). These DO aggregate to a family-level row.
--
--   * Bridge grain = (email, batch_label) MEMBERSHIP, not one-row-per-email:
--     45,723 emails appear under multiple batches (an address from a disconnected
--     batch re-provisioned into a new one, e.g. B54→B104). `is_current_batch`
--     marks the most-recent generation per email.
--
-- Three layers, by design (structure > completeness; every row says how it joined):
--   core.infra_batch          — one row per Sheet label (the raw truth, 165 rows)
--   core.infra_batch_key      — one row per CSV-observable batch key (the JOIN
--                               TARGET): exact label, or split-family aggregate,
--                               with join_type flagging how it resolved
--   core.sending_account_batch — (email, batch_key) membership facts
--
-- Population: scripts/build_infra_batch.sql (re-run when a fresh export drops;
-- manual snapshots, not a nightly feed). Views are live.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;

-- v1 objects being restructured (DDL 60) + own objects, dropped so the file is
-- cleanly re-runnable (re-pipe after an amend; population is a separate script).
DROP VIEW  IF EXISTS derived.v_batch_lifecycle;
DROP VIEW  IF EXISTS derived.v_batch_lifecycle_summary;
DROP TABLE IF EXISTS core.infra_batch_root;
DROP TABLE IF EXISTS core.infra_batch;
DROP TABLE IF EXISTS core.infra_batch_key;
DROP TABLE IF EXISTS core.sending_account_batch;

-- --------------------------------------------------------------------------
-- core.infra_batch — one row per Sheet batch label (raw truth)
-- --------------------------------------------------------------------------
CREATE TABLE core.infra_batch (
    batch_label         VARCHAR PRIMARY KEY,    -- 'B54-R', 'B36.1', '1st batch-R', 'Emran Batch 10'
    batch_family        VARCHAR NOT NULL,       -- navigation grouping ONLY (B54-R→B54, B36.2→B36); no date semantics
    is_replacement      BOOLEAN DEFAULT FALSE,  -- '-R': NEW inboxes/domains replacing a disconnected batch
    partner             VARCHAR,                -- Outreach Today / MailIn / Inboxing / Cheap Inboxes / Panel / ...
    workspace_raw       VARCHAR,                -- Sheet workspace string (point-in-time; live truth = sending_account)
    n_domains_sheet     INTEGER,                -- Sheet '# of Domains' (reference/QA only)
    sip_raw             VARCHAR,                -- raw cell (some are text: "End of May / Beginning of June 2026")
    sip_date            DATE,
    warmup_raw          VARCHAR,
    warmup_start_date   DATE,                   -- Batch-Info col F (chosen truth column)
    cold_raw            VARCHAR,
    cold_start_date     DATE,                   -- explicit only
    warmup_start_qa     DATE,                   -- QA-block second warmup col (can disagree; re-setup artifact)
    billing_raw         VARCHAR,                -- raw cell: ordinal day-of-month ("15th"), NOT a date
    billing_day_of_month INTEGER,               -- recurring monthly billing day (1-31)
    offer               VARCHAR,                -- Funding / Section 125 / ERC
    provider            VARCHAR,                -- Google / Outlook
    batch_url           VARCHAR,
    qa_num_accounts     VARCHAR,
    qa_started          VARCHAR,
    qa_settings_correct VARCHAR,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ix_infra_batch_family ON core.infra_batch (batch_family);

-- --------------------------------------------------------------------------
-- core.infra_batch_key — one row per CSV-observable batch key (JOIN TARGET).
-- join_type says HOW the key resolved to Sheet truth:
--   'exact'        — key == a Sheet label; dates from that one row
--   'split_family' — key is the base of a decimal split (B36 → B36.1-.5, one
--                    batch across N workspaces); dates = MIN over the sub-labels,
--                    n_sheet_labels > 1, same generation only (-R excluded)
--   'no_sheet_row' — no valid Sheet row for THIS generation (e.g. CSV '1st_batch':
--                    the Sheet only has '1st batch-R', a replacement = DIFFERENT
--                    inboxes); all dates NULL by design, membership still usable
-- Also contains Sheet labels never seen in the CSV (join_type 'exact',
-- sheet-only) so batch-level questions cover the full Sheet.
-- --------------------------------------------------------------------------
CREATE TABLE core.infra_batch_key (
    batch_key           VARCHAR PRIMARY KEY,
    join_type           VARCHAR NOT NULL,       -- exact | split_family | no_sheet_row
    sheet_labels        VARCHAR,                -- which Sheet label(s) feed this key, comma-joined
    n_sheet_labels      INTEGER,
    batch_family        VARCHAR,
    is_replacement      BOOLEAN,
    in_csv              BOOLEAN,                -- key observed in the account export
    partner             VARCHAR,
    offer               VARCHAR,
    provider            VARCHAR,
    workspace_raw       VARCHAR,
    sip_date            DATE,
    warmup_start_date   DATE,
    cold_start_date     DATE,                   -- explicit only
    cold_start_resolved DATE,                   -- explicit, else warmup + observed median gap
    cold_start_source   VARCHAR,                -- explicit | derived_median | unknown
    billing_day_of_month INTEGER,               -- recurring monthly billing day (1-31)
    n_domains_sheet     INTEGER,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ix_ibk_family ON core.infra_batch_key (batch_family);

-- --------------------------------------------------------------------------
-- core.sending_account_batch — (email, batch_key) membership facts.
-- An email under multiple batches = real history (replacement generations).
-- batch_key NULL = the export row had no batch label.
-- --------------------------------------------------------------------------
CREATE TABLE core.sending_account_batch (
    account_email       VARCHAR NOT NULL,       -- lower(trim); join lower(sending_account.account_id)
    batch_key           VARCHAR,                -- FK → infra_batch_key.batch_key; NULL = unlabeled in export
    batch_family        VARCHAR,
    is_current_batch    BOOLEAN,                -- latest generation for this email (by batch date, then number)
    domain              VARCHAR,
    raw_workspace       VARCHAR,                -- export workspace string (point-in-time)
    provider_tag        VARCHAR,                -- partner vendor (Outreach Today / MailIn / ...)
    email_tag           VARCHAR,                -- ESP per export: Google / Outlook / SMTP
    offer               VARCHAR,
    first_name          VARCHAR,
    last_name           VARCHAR,
    status_csv          VARCHAR,                -- sparse (Active / Warm Up only); live status = warehouse
    n_source_rows       INTEGER,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX ux_sab_email_key ON core.sending_account_batch (account_email, batch_key);
CREATE INDEX ix_sab_key    ON core.sending_account_batch (batch_key);
CREATE INDEX ix_sab_domain ON core.sending_account_batch (domain);

-- core.domain_infra_csv unchanged from DDL 60 (kept as-is).

-- ==========================================================================
-- Derived views
-- ==========================================================================

-- Per-membership lifecycle: (email, batch) → batch dates → live warehouse state.
CREATE OR REPLACE VIEW derived.v_batch_lifecycle AS
SELECT
    b.account_email,
    b.batch_key,
    b.batch_family,
    b.is_current_batch,
    k.join_type                         AS batch_join_type,
    k.is_replacement,
    (sa.account_id IS NOT NULL)         AS in_warehouse,
    sa.workspace_slug                   AS current_workspace_slug,   -- live truth
    b.raw_workspace,
    b.domain,
    COALESCE(dom.tld, dr.tld)           AS tld,
    b.provider_tag,
    b.email_tag,
    b.offer,
    b.first_name,
    b.last_name,
    k.partner,
    k.provider,
    k.sip_date,
    k.warmup_start_date,
    k.cold_start_resolved               AS cold_start_date,
    k.cold_start_source,
    k.billing_day_of_month,
    sa.lifecycle_state,
    sa.status                           AS account_status,
    sa.is_active,
    sa.warmup_score,
    sa.last_seen_at,
    dr.purchased_at                     AS domain_purchased_at,      -- canonical (registry)
    dom.expiration_date                 AS domain_expiration_csv,
    dom.accounts_per_domain
FROM core.sending_account_batch b
LEFT JOIN core.infra_batch_key   k   ON k.batch_key = b.batch_key
LEFT JOIN core.sending_account   sa  ON lower(sa.account_id) = b.account_email
LEFT JOIN core.domain_infra_csv  dom ON dom.domain = b.domain
LEFT JOIN core.domain_registry   dr  ON dr.domain  = b.domain;

-- Per batch_key × current workspace rollup with REAL send actuals. Sends are
-- attributed only to each email's CURRENT batch so a replaced+reused address never
-- double-counts volume across generations.
CREATE OR REPLACE VIEW derived.v_batch_lifecycle_summary AS
WITH acct_sends AS (
    SELECT account_id,
           sum(actual_sends)                                         AS total_actual_sends,
           sum(actual_sends) FILTER (WHERE date >= current_date - 7) AS sends_last7,
           max(date) FILTER (WHERE actual_sends > 0)                 AS last_send_date
    FROM core.sending_account_daily GROUP BY 1
),
m AS (
    SELECT b.batch_key, b.batch_family, b.is_current_batch,
           sa.workspace_slug, b.account_email, b.domain,
           sa.is_active, sa.lifecycle_state, sa.account_id,
           CASE WHEN b.is_current_batch THEN s.total_actual_sends END AS total_actual_sends,
           CASE WHEN b.is_current_batch THEN s.sends_last7        END AS sends_last7,
           CASE WHEN b.is_current_batch THEN s.last_send_date     END AS last_send_date
    FROM core.sending_account_batch b
    LEFT JOIN core.sending_account sa ON lower(sa.account_id) = b.account_email
    LEFT JOIN acct_sends            s  ON lower(s.account_id)  = b.account_email
)
SELECT
    m.batch_key,
    m.batch_family,
    m.workspace_slug                                       AS current_workspace_slug,
    k.join_type                                            AS batch_join_type,
    k.is_replacement,
    k.partner, k.offer, k.provider,
    k.sip_date, k.warmup_start_date,
    k.cold_start_resolved AS cold_start_date, k.cold_start_source,
    k.billing_day_of_month,
    count(*)                                               AS n_memberships,
    count(*) FILTER (WHERE m.is_current_batch)             AS n_current_accounts,
    count(*) FILTER (WHERE m.account_id IS NOT NULL)       AS n_in_warehouse,
    count(*) FILTER (WHERE m.is_active)                    AS n_active,
    count(*) FILTER (WHERE m.lifecycle_state = 'warming')  AS n_warming,
    count(DISTINCT m.domain)                               AS n_domains,
    sum(m.total_actual_sends)                              AS total_actual_sends,
    sum(m.sends_last7)                                     AS sends_last7,
    max(m.last_send_date)                                  AS last_send_date
FROM m
LEFT JOIN core.infra_batch_key k ON k.batch_key = m.batch_key
GROUP BY ALL;
