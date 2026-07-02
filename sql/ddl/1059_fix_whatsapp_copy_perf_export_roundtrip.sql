-- 1059: v_whatsapp_copy_performance — make the view definition EXPORT/IMPORT round-trip safe
-- ---------------------------------------------------------------------------------------------
-- WHY (2026-07-01): DDL 1028 wrote the FROM clause as
--       FROM first_out f
--       CROSS JOIN cov
--       LEFT JOIN v_whatsapp_conversation_performance c USING (conversation_id)
-- which parses as ((first_out CROSS JOIN cov) LEFT JOIN c) — fine. But DuckDB's catalog
-- serializer emits the CROSS JOIN as a COMMA join:
--       FROM first_out AS f , cov LEFT JOIN v_whatsapp_conversation_performance AS c USING (conversation_id)
-- and a comma has LOWER precedence than JOIN, so the re-parse binds the LEFT JOIN to `cov`
-- alone -> "Binder Error: Column conversation_id does not exist on left side of join!".
-- The LIVE view works; only its EXPORTED form is unreconstructable. That single statement
-- aborted the warehouse compaction (EXPORT->IMPORT rebuild) on 2026-07-01 — the last blocker
-- of the 165GB bloat remediation — and would equally break any EXPORT-based restore.
--
-- FIX: identical semantics, join order that survives the round-trip: `cov` is a single-row CTE,
-- so CROSS JOINing it AFTER the LEFT JOIN changes nothing about the result; serialized as a
-- trailing comma join it re-parses as ((first_out LEFT JOIN c) , cov) — binds cleanly.
-- Everything else is byte-identical to DDL 1028 (see its header for the copy-attribution spec).
-- Verified: fixed definition round-trips through EXPORT DATABASE / re-import cleanly.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_whatsapp_copy_performance AS
WITH opener AS (
  SELECT conversation_id, body, template_id, template_name, campaign_id, campaign_name,
         row_number() OVER (PARTITION BY conversation_id ORDER BY created_at, id) AS rn
  FROM raw_iskra_messages
  WHERE direction = 'outbound' AND body IS NOT NULL
),
first_out AS (
  SELECT
    conversation_id, body, template_id, template_name, campaign_id, campaign_name,
    -- normalize rendered opener -> template skeleton (strip per-lead name + company, collapse ws).
    regexp_replace(
      regexp_replace(
        regexp_replace(trim(body), '^(Hi|Hello|Hey)[^,.!?\n]{0,45},', '\1 {name},'),
      'regarding [^,.!?\n]{1,70}', 'regarding {company}'),
    '\s+', ' ', 'g') AS copy_body
  FROM opener
  WHERE rn = 1
),
cov AS (  -- single scalar: fraction of openers carrying a structured template_id (0.0 today).
  SELECT count(*) FILTER (WHERE template_id IS NOT NULL) * 1.0 / NULLIF(count(*), 0) AS tmpl_cov
  FROM first_out
)
SELECT
  -- coverage-gated key: template_id only once it's near-complete, else the normalized body skeleton.
  CASE WHEN cov.tmpl_cov >= 0.95 THEN COALESCE(f.template_id, f.copy_body)
       ELSE f.copy_body END                          AS copy_key,
  (cov.tmpl_cov >= 0.95)                              AS keyed_on_template_id,
  round(cov.tmpl_cov, 4)                              AS template_id_coverage, -- global all-time fraction
  max(f.template_id)                                  AS template_id,
  max(f.template_name)                                AS template_name,
  max(f.campaign_id)                                  AS campaign_id,
  max(f.campaign_name)                                AS campaign_name,
  count(*)                                            AS conversations,      -- opener sends (denominator)
  count(*) FILTER (WHERE c.replied)                   AS replied,
  count(*) FILTER (WHERE c.is_positive_reply)         AS positive_replies,
  count(*) FILTER (WHERE c.meeting_booked)            AS meetings_booked,
  round(count(*) FILTER (WHERE c.replied)           * 1.0 / NULLIF(count(*), 0), 4) AS reply_rate,
  round(count(*) FILTER (WHERE c.is_positive_reply) * 1.0 / NULLIF(count(*), 0), 4) AS positive_rate,
  round(count(*) FILTER (WHERE c.meeting_booked)    * 1.0 / NULLIF(count(*), 0), 4) AS booked_rate,
  count(DISTINCT f.body)                              AS n_rendered_variants, -- raw bodies under this key
  min(f.copy_body)                                    AS copy_sample          -- representative skeleton
FROM first_out f
LEFT JOIN v_whatsapp_conversation_performance c USING (conversation_id)
CROSS JOIN cov
GROUP BY CASE WHEN cov.tmpl_cov >= 0.95 THEN COALESCE(f.template_id, f.copy_body)
              ELSE f.copy_body END,
         cov.tmpl_cov;
