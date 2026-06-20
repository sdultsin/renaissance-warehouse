-- Version 72 (2026-06-15) — account-level VENDOR CATEGORY (portal "Email Type").
--
-- The portal's Accounts ▸ "Email Types" tab buckets every sending account by its
-- INFRA-VENDOR category — the 8 portal values: Reseller, MailIn, Cheap Inboxes,
-- Outreach Today, Inboxing, Panel, Microsoft Panel, Maildoso. This is the VENDOR
-- axis, NOT the consumer/business email_type (that lives on public.leads) and NOT
-- the ESP (esp = google/outlook/otd on core.sending_account, a different axis).
--
-- WHY this isn't already a column: vendor category is a TAG-keyed concept in
-- Instantly (decision 2026-05-20: provider_code only gives MX provider — Google /
-- Microsoft / Zoho — it can NOT tell OTD from Reseller, or MailIn from Outlook;
-- only the account's Instantly infra TAG can). core.sending_account_tag is empty
-- (live tags were never ingested), and the public Instantly API does not return an
-- account's tags inline. So vendor category must be assembled from two sources:
--
--   PRIMARY (live, authoritative): scripts/build_sending_account_vendor.py pulls
--     every infra tag's membership per workspace via GET /custom-tags +
--     GET /accounts?tag_ids=… (the proven blocklist-surveillance mechanism;
--     User-Agent curl/8.4.0 to dodge CF 1010), normalizes each tag label to one of
--     the 8 categories, and writes core.sending_account_vendor (one row per email).
--   FALLBACK (snapshot): core.sending_account_batch.provider_tag (the infra-batch
--     CSV export) for any email the live pull didn't tag.
--
-- The resolved per-account category + its source + confidence is exposed by
-- core.v_sending_account_vendor, joined back to core.sending_account.
--
-- Population is NOT in this DDL (structure only, version-gated) — run
-- scripts/build_sending_account_vendor.py under flock in an idle writer window.
-- Migration-agnostic standard SQL (must port off single-file DuckDB unchanged).

CREATE SCHEMA IF NOT EXISTS core;

-- --------------------------------------------------------------------------
-- core.sending_account_vendor — one row per email, the live-tag vendor truth.
-- Populated by build_sending_account_vendor.py (full re-pull, DELETE+INSERT).
-- vendor_category ∈ the 8 portal values (or 'Unmapped' when an account carries no
-- recognizable vendor tag AND no batch provider_tag — surfaced, never silently
-- bucketed).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sending_account_vendor (
    account_email    VARCHAR PRIMARY KEY,   -- lower(trim(email)); join lower(sending_account.account_id)
    workspace_slug   VARCHAR,               -- warehouse slug the live pull mapped the key to
    vendor_category  VARCHAR NOT NULL,      -- Reseller | MailIn | Cheap Inboxes | Outreach Today | Inboxing | Panel | Microsoft Panel | Maildoso | Unmapped
    vendor_source    VARCHAR NOT NULL,      -- 'instantly_tag' (live, primary) | 'batch_csv' (fallback) | 'esp_weak' (ESP home tag only) | 'none'
    matched_tag      VARCHAR,               -- the winning tag label (provenance)
    n_vendor_tags    INTEGER,               -- # distinct vendor tags this email carried (>1 = conflict, resolved by precedence)
    esp              VARCHAR,               -- account's ESP at pull time (google/outlook/otd) — cross-check
    _loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id          VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sav_workspace ON core.sending_account_vendor (workspace_slug);
CREATE INDEX IF NOT EXISTS ix_sav_category  ON core.sending_account_vendor (vendor_category);

-- --------------------------------------------------------------------------
-- core.v_sending_account_vendor — canonical per-account vendor category for
-- every LIVE account in core.sending_account, with a graceful fallback chain:
--   1. live Instantly tag (core.sending_account_vendor, source='instantly_tag')
--   2. batch CSV provider_tag (core.sending_account_batch, current batch preferred)
--   3. ESP-only weak signal (source='esp_weak') — last resort, flagged low-confidence
--   4. 'Unmapped' — no vendor signal at all (the honest gap)
-- One row per live account; resolved_source + confidence make the gap auditable.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_account_vendor AS
WITH live AS (
    SELECT account_id, lower(account_id) AS k, workspace_slug, esp, is_active, status
    FROM core.sending_account
),
-- batch-bridge fallback: best provider_tag per email (prefer current generation)
batch_pt AS (
    SELECT account_email AS k,
           arg_max(provider_tag, (is_current_batch, provider_tag IS NOT NULL)) AS provider_tag
    FROM core.sending_account_batch
    GROUP BY 1
)
SELECT
    l.account_id,
    l.workspace_slug,
    l.esp,
    l.is_active,
    l.status,
    COALESCE(
        CASE WHEN sav.vendor_category <> 'Unmapped' THEN sav.vendor_category END,  -- 1. live tag
        bp.provider_tag,                                                            -- 2. batch CSV
        CASE                                                                        -- 3. ESP-weak
            WHEN sav.vendor_source = 'esp_weak' THEN sav.vendor_category END,
        'Unmapped'                                                                  -- 4. honest gap
    )                                       AS vendor_category,
    CASE
        WHEN sav.vendor_category IS NOT NULL AND sav.vendor_category <> 'Unmapped'
             AND sav.vendor_source = 'instantly_tag'                  THEN 'instantly_tag'
        WHEN bp.provider_tag IS NOT NULL                              THEN 'batch_csv'
        WHEN sav.vendor_source = 'esp_weak'                          THEN 'esp_weak'
        ELSE 'none'
    END                                     AS resolved_source,
    CASE
        WHEN sav.vendor_source = 'instantly_tag'
             AND sav.vendor_category <> 'Unmapped'                    THEN 'high'
        WHEN bp.provider_tag IS NOT NULL                              THEN 'medium'
        WHEN sav.vendor_source = 'esp_weak'                          THEN 'low'
        ELSE 'none'
    END                                     AS confidence,
    sav.matched_tag,
    sav.n_vendor_tags
FROM live l
LEFT JOIN core.sending_account_vendor sav ON sav.account_email = l.k
LEFT JOIN batch_pt                     bp  ON bp.k             = l.k;
