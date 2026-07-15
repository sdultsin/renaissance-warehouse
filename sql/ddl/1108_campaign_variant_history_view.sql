-- @gate: add
-- Depends on 1101
-- Depends on 1104
-- ============================================================================
-- 1108_campaign_variant_history_view.sql — core.v_campaign_variant_history: ONE
-- step/variant-grain analytics surface with append/freeze semantics.
--
-- WHY (charter §5: total sent by step and variant, incl. DELETED campaigns): variant
-- analytics for deleted campaigns exist nowhere live — the 2026-07-15 escrow capture
-- (main.raw_instantly_campaign_steps_history, DDL 1101: 19,059 rows / 1,680 campaigns,
-- incl. campaigns hard-deleted from Instantly) is their only record and is FROZEN.
-- Live campaigns keep refreshing via the EXISTING nightly pipeline mirror
-- (main.raw_pipeline_campaign_data step/variant rows) — no new sync is introduced.
--
-- KEY NORMALIZATION (measured 2026-07-15): the two sides use different vocabularies —
-- escrow: 0-based numeric step + 0-based numeric variant ('0','1',…);
-- pipeline: 1-based numeric step + letter variant ('A','B',…).
-- step_norm (1-based INT) + variant_norm (letter) unify them deterministically.
--
-- SEMANTICS HONESTY (do not blur):
--   * escrow rows = LIFETIME totals AS OF fetched_at (2026-07-15), with the full
--     unique/auto reply split but NO opportunities.
--   * pipeline rows = current lifetime state per nightly, emails_sent/replies/
--     opportunities only (no auto split; `replies` semantics = pipeline's, not
--     verified identical to Instantly unique_replies).
--   Both sides are exposed (row_source) with an is_preferred flag:
--     still_live_in_instantly (core.v_campaign_dim_unified) -> pipeline_live preferred
--     (fresher); else escrow preferred (captured AFTER deletion — the pipeline freeze
--     can predate final sends, metrics-cut Defect D2). The other side falls back when
--     the preferred side has no row.
--   Variant COPY lives in core.variant_copy (campaign_id, step, content_hash) — a
--   different key space; join deliberately not attempted here.
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_campaign_variant_history AS
WITH escrow AS (
  SELECT campaign_id,
         CAST(step AS INTEGER) + 1                    AS step_norm,
         CASE WHEN CAST(variant AS INTEGER) BETWEEN 0 AND 25
              THEN chr(65 + CAST(variant AS INTEGER))
              ELSE 'V' || variant END                  AS variant_norm,
         workspace_slug,
         sent, replies, unique_replies, replies_automatic, unique_replies_automatic,
         opened, unique_opened, clicks, unique_clicks,
         CAST(NULL AS BIGINT)                          AS opportunities,
         fetched_at                                    AS as_of,
         'escrow_frozen_20260715'                      AS row_source
  FROM main.raw_instantly_campaign_steps_history
),
pipeline AS (
  SELECT campaign_id,
         TRY_CAST(step AS INTEGER)                     AS step_norm,
         variant                                       AS variant_norm,
         CAST(NULL AS VARCHAR)                         AS workspace_slug,
         CAST(emails_sent AS BIGINT)                   AS sent,
         CAST(replies AS BIGINT)                       AS replies,
         CAST(NULL AS BIGINT) AS unique_replies, CAST(NULL AS BIGINT) AS replies_automatic,
         CAST(NULL AS BIGINT) AS unique_replies_automatic,
         CAST(NULL AS BIGINT) AS opened, CAST(NULL AS BIGINT) AS unique_opened,
         CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS unique_clicks,
         CAST(opportunities AS BIGINT)                 AS opportunities,
         CAST(_loaded_at AS DATE)                      AS as_of,
         'pipeline_live'                               AS row_source
  FROM main.raw_pipeline_campaign_data
  WHERE step <> '__ALL__' AND variant <> '__ALL__'
    AND TRY_CAST(step AS INTEGER) IS NOT NULL
),
unioned AS (
  SELECT * FROM escrow
  UNION ALL
  SELECT * FROM pipeline
)
SELECT u.campaign_id,
       d.campaign_name,
       COALESCE(u.workspace_slug, d.workspace_slug) AS workspace_slug,
       u.step_norm, u.variant_norm,
       u.sent, u.replies, u.unique_replies, u.replies_automatic, u.unique_replies_automatic,
       u.opened, u.unique_opened, u.clicks, u.unique_clicks, u.opportunities,
       u.as_of, u.row_source,
       COALESCE(d.still_live_in_instantly, FALSE) AS still_live_in_instantly,
       CASE WHEN COALESCE(d.still_live_in_instantly, FALSE)
            THEN (u.row_source = 'pipeline_live'
                  OR NOT EXISTS (SELECT 1 FROM pipeline p
                                 WHERE p.campaign_id = u.campaign_id
                                   AND p.step_norm = u.step_norm
                                   AND p.variant_norm = u.variant_norm))
            ELSE (u.row_source = 'escrow_frozen_20260715'
                  OR NOT EXISTS (SELECT 1 FROM escrow e
                                 WHERE e.campaign_id = u.campaign_id
                                   AND e.step_norm = u.step_norm
                                   AND e.variant_norm = u.variant_norm))
       END AS is_preferred
FROM unioned u
LEFT JOIN core.v_campaign_dim_unified d ON d.campaign_id = u.campaign_id;
