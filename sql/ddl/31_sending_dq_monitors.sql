-- Phase 4 (SPEC D): Sending Data-Quality monitors.
-- Applied at schema version 31.
--
-- Five views/tables:
--   D1. v_sending_account_anomalies       — daily_limit deviations from per-ESP norm
--   D2. v_tag_coverage_gaps               — active accounts missing ESP vendor tag (depends on SPEC A tag-fold)
--   D3. raw_account_truth_daily_actuals   — mirror of account-truth daily CSV (configured vs actual)
--       + core.sending_account_daily      — canonical per-account-per-day actuals
--   D4. (freshness signal added to core.sending_account — new column _snapshot_date)
--   D5. v_brand_stem_clusters             — token-sorted SLD permutation detector
--
-- Sequencing: D1/D3/D4/D5 are independent of SPEC A/B.
--             D2 depends on core.sending_account_tag (SPEC A).

CREATE SCHEMA IF NOT EXISTS core;

-- ============================================================================
-- D1: daily_limit anomaly view
-- Flags active accounts whose daily_limit deviates from the per-ESP norm.
-- Surfaces cohort hints: same created_at day, same workspace, same domain stem.
-- ============================================================================
CREATE OR REPLACE VIEW v_sending_account_anomalies AS
WITH esp_norms AS (
    SELECT esp,
           MODE(daily_limit) AS norm_limit,
           MEDIAN(daily_limit) AS median_limit
    FROM core.sending_account
    WHERE is_active AND daily_limit IS NOT NULL AND esp IS NOT NULL
    GROUP BY esp
)
SELECT
    sa.account_id,
    sa.email,
    sa.domain,
    sa.esp,
    sa.infra_provider,
    sa.workspace_slug,
    w.name AS workspace_name,
    sa.daily_limit,
    n.norm_limit AS esp_norm,
    sa.daily_limit - n.norm_limit AS deviation,
    ROUND(ABS(sa.daily_limit - n.norm_limit) * 100.0 / NULLIF(n.norm_limit, 0), 1) AS deviation_pct,
    sa.lifecycle_state,
    sa.created_at,
    -- Cohort hints
    CAST(sa.created_at AS DATE) AS created_date,
    split_part(sa.domain, '.', 1) AS domain_stem
FROM core.sending_account sa
JOIN esp_norms n ON n.esp = sa.esp
LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
WHERE sa.is_active
  AND sa.daily_limit IS NOT NULL
  AND sa.daily_limit <> n.norm_limit;

-- ============================================================================
-- D2: tag coverage gaps (LIVE — uses core.sending_account_tag from SPEC A)
-- Surfaces active accounts whose ESP has no matching vendor tag.
-- ============================================================================
CREATE OR REPLACE VIEW v_tag_coverage_gaps AS
WITH vendor_tags AS (
    SELECT tag_label FROM (VALUES
        ('Google'), ('Reseller'), ('Reseller PP'),
        ('MailIn'), ('Mailin'), ('CheapInboxes'), ('Inboxing'),
        ('Outreach Today')
    ) AS t(tag_label)
),
account_vendor_tagged AS (
    SELECT DISTINCT t.email
    FROM core.sending_account_tag t
    WHERE t.tag_label IN (SELECT tag_label FROM vendor_tags)
)
SELECT
    sa.workspace_slug,
    w.name AS workspace_name,
    sa.esp,
    COUNT(*) AS total_accounts,
    COUNT(vt.email) AS tagged,
    COUNT(*) - COUNT(vt.email) AS untagged,
    ROUND(100.0 * COUNT(vt.email) / COUNT(*), 1) AS pct_covered
FROM core.sending_account sa
LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
LEFT JOIN account_vendor_tagged vt ON vt.email = sa.email
WHERE sa.is_active AND sa.esp IS NOT NULL
GROUP BY sa.workspace_slug, w.name, sa.esp
HAVING (COUNT(*) - COUNT(vt.email)) > 0
ORDER BY (COUNT(*) - COUNT(vt.email)) DESC;

-- ============================================================================
-- D3: Mirror account_truth daily actuals (configured vs actual sends per account)
-- Raw table + canonical rollup.
-- ============================================================================
CREATE TABLE IF NOT EXISTS raw_account_truth_daily_actuals (
    date             DATE NOT NULL,
    workspace_slug   VARCHAR NOT NULL,
    workspace_name   VARCHAR,
    email            VARCHAR NOT NULL,
    domain           VARCHAR,
    infra_type       VARCHAR,
    provider_code    INTEGER,
    account_status   INTEGER,
    account_status_label VARCHAR,
    daily_limit      INTEGER,
    expected_sends   INTEGER,
    actual_sends     INTEGER,
    delta            INTEGER,       -- expected - actual
    fulfillment      DOUBLE,        -- actual / expected (0-1+)
    active_campaign_count INTEGER,
    canonical_tag    VARCHAR,
    undersend_reason VARCHAR,
    warning_flags    VARCHAR,
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
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

-- ============================================================================
-- D4: Freshness signal — add _snapshot_date to core.sending_account
-- (ALTER TABLE is idempotent via TRY; DuckDB doesn't have IF NOT EXISTS for columns)
-- ============================================================================
-- Column addition handled in entity code (ALTER TABLE ADD COLUMN pattern).

CREATE OR REPLACE VIEW v_sending_account_freshness AS
SELECT
    MAX(_snapshot_date) AS latest_snapshot,
    current_date - MAX(_snapshot_date) AS days_stale,
    CASE
        WHEN current_date - MAX(_snapshot_date) > 2 THEN 'STALE'
        WHEN current_date - MAX(_snapshot_date) > 1 THEN 'WARNING'
        ELSE 'FRESH'
    END AS freshness_status,
    COUNT(*) AS total_accounts,
    COUNT(*) FILTER (WHERE is_active) AS active_accounts
FROM core.sending_account;

-- ============================================================================
-- D5: Stem/token-set brand-cluster detector
-- Sorts the characters of each SLD to create a canonical cluster key.
-- Domains whose sorted-character multiset matches are stem permutations.
-- ============================================================================
CREATE OR REPLACE VIEW v_brand_stem_clusters AS
WITH domain_chars AS (
    SELECT
        domain,
        brand_prefix,
        esp,
        -- Sort all characters of brand_prefix alphabetically to create cluster key
        (SELECT string_agg(ch, '' ORDER BY ch)
         FROM (SELECT unnest(string_split(brand_prefix, '')) AS ch)) AS char_sorted_key,
        LENGTH(brand_prefix) AS prefix_len
    FROM core.domain
    WHERE brand_prefix IS NOT NULL
      AND LENGTH(brand_prefix) >= 6
),
clusters AS (
    SELECT
        char_sorted_key,
        COUNT(*) AS cluster_size,
        LIST(domain ORDER BY domain) AS domains,
        LIST(DISTINCT esp) AS esps,
        MIN(brand_prefix) AS example_prefix,
        MIN(prefix_len) AS prefix_len
    FROM domain_chars
    WHERE char_sorted_key IS NOT NULL
    GROUP BY char_sorted_key
    HAVING COUNT(*) >= 3
)
SELECT
    char_sorted_key AS cluster_key,
    cluster_size,
    prefix_len,
    example_prefix,
    esps,
    domains
FROM clusters
ORDER BY cluster_size DESC;
