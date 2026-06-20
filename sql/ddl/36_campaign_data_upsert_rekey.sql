-- Spec 15 amendment (2026-06-06): re-key raw_pipeline_campaign_data from the
-- insert_hash key  md5(campaign_id|step|variant|content_hash)  to the upsert key
-- md5(campaign_id|step|variant), and drop the now-vestigial content_hash column.
--
-- WHY: campaign_data carries the daily metric rollup (emails_sent / opportunities /
-- v_disabled / total_leads / leads_* …) alongside copy. Under insert_hash the _key
-- only moved when copy text changed, so ON CONFLICT DO NOTHING discarded every new
-- daily metric snapshot for an unchanged-copy campaign. The warehouse table (and the
-- variant='__ALL__' rollup row that flows through it) went stale. Switching the entity
-- to UPSERT on (campaign_id|step|variant) lands metric updates in place. Copy-version
-- HISTORY is unaffected — it lives in raw_pipeline_variant_copy (still insert_hash).
--
-- DDL 04 (fresh installs) already builds the new shape (no content_hash, upsert key),
-- so this file only fixes a LIVE table that DDL 04 won't re-touch (version 4 already
-- recorded in core.schema_version). Idempotent: if the table is already collapsed to
-- one row per (campaign_id|step|variant) with no content_hash column, re-running is a
-- harmless no-op rebuild.
--
-- Non-destructive: the pre-migration table is preserved as
-- raw_pipeline_campaign_data__prehash_legacy for rollback. Drop it after one clean
-- nightly + verification.
--
-- Build-new-then-swap (avoids unique-index collisions while UPDATEing _key, and
-- sidesteps the DuckDB ART index-delete bug on large in-place DELETEs).

-- 1. Collapse to one row per natural key, latest _loaded_at wins, with the new _key.
CREATE TABLE IF NOT EXISTS raw_pipeline_campaign_data__rekey_new AS
SELECT
  md5(
    coalesce(CAST(campaign_id AS VARCHAR), '') || '|' ||
    coalesce(CAST(step        AS VARCHAR), '') || '|' ||
    coalesce(CAST(variant     AS VARCHAR), '')
  )                          AS _key,
  campaign_id, campaign_name, workspace_id, workspace_name, cm_name,
  segment, product, infra_type, status, date_launched, daily_limit,
  lead_source, tags, excluded_from_analysis, exclusion_reason, step,
  variant, emails_sent, replies, opportunities, analytics_sequence_started,
  leads_closed, e_op, reply_rate, close_rate, campaign_score, subject,
  body, subject_preview, body_preview, signature, v_disabled, synced_at,
  meetings_booked, rg_batch_tags, pair_tag, sender_tags, other_tags,
  total_leads, leads_completed, leads_bounced, leads_unsubscribed,
  lead_sequence_started, _loaded_at, _run_id
FROM (
  SELECT
    c.*,
    row_number() OVER (
      PARTITION BY
        coalesce(CAST(campaign_id AS VARCHAR), '') || '|' ||
        coalesce(CAST(step        AS VARCHAR), '') || '|' ||
        coalesce(CAST(variant     AS VARCHAR), '')
      ORDER BY _loaded_at DESC
    ) AS __rn
  FROM raw_pipeline_campaign_data c
)
WHERE __rn = 1;

-- 2. Preserve the original table for rollback (drop after one clean nightly).
DROP TABLE IF EXISTS raw_pipeline_campaign_data__prehash_legacy;
-- [2026-06-06 fix] DuckDB blocks RENAME while indexes exist; drop them first.
DROP INDEX IF EXISTS uxk_raw_pipeline_campaign_data;
DROP INDEX IF EXISTS ixc_raw_pipeline_campaign_data;
ALTER TABLE raw_pipeline_campaign_data RENAME TO raw_pipeline_campaign_data__prehash_legacy;

-- 3. Promote the re-keyed table.
ALTER TABLE raw_pipeline_campaign_data__rekey_new RENAME TO raw_pipeline_campaign_data;

-- 4. Rebuild indexes (entity's ON CONFLICT (_key) target + campaign_id helper).
--    The legacy unique index (uxk_/ux_ on the old table) went away with the rename.
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_campaign_data_key
  ON raw_pipeline_campaign_data (_key);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_campaign_data_campaign
  ON raw_pipeline_campaign_data (campaign_id);
