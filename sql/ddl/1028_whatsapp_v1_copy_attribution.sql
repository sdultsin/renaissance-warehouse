-- @gate: add
-- Depends on 82
-- 1028_whatsapp_v1_copy_attribution.sql  [2026-06-27]  ISKRA v1 integration: capture the new
-- outbound identity fields + ship per-copy-variant WhatsApp attribution.
-- Applied at schema version 1028 by scripts/setup_db.py / the warehouse DDL applier.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS (no-op if present) + CREATE OR REPLACE VIEW. Standard SQL.
-- NON-destructive — adds 4 nullable columns to raw_iskra_messages and creates ONE new view; no DROP,
-- no rename, no type change, no data mutation, no change to any existing view's column contract.
--
-- CONTEXT (handoff 2026-06-27 + ARSENY-WHATSAPP-API-VERIFY.md). Arseny shipped the ISKRA v1 API.
-- Two unlocks land here:
--   (1) Outbound identity fields. /v1/messages/whatsapp now returns campaign_id, campaign_name,
--       template_id, template_name on every outbound row. They EXIST in the contract but Iskra
--       writes them 100% NULL today (verified: 0/3,618 outbound non-null). `body` (final rendered
--       copy) is 100% populated and is therefore the copy-attribution key TODAY. We add the four
--       columns now (entities/iskra.py captures them) so they auto-fill the moment Arseny populates
--       them — at which point copy_key below silently upgrades from body-text to template_id.
--   (2) Complete meeting set. /v1/meetings now cursor-paginates (old 500-newest cap lifted); the
--       ingest (entities/iskra.py run_meetings) now pulls the COMPLETE ~6,065-row set, fixing the
--       W1e under-count (was ~2.2k rows / ~54 booked; true ~6.1k / ~181 booked). v_whatsapp_copy_
--       performance and v_whatsapp_performance (DDL 1023) reflect the complete set automatically
--       once that ingest has run on the box (nightly, or an out-of-band run).

-- ---------------------------------------------------------------------------------------------
-- (1) New v1 outbound identity columns on raw_iskra_messages (NULL until Iskra populates them).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE raw_iskra_messages ADD COLUMN IF NOT EXISTS campaign_id   VARCHAR;
ALTER TABLE raw_iskra_messages ADD COLUMN IF NOT EXISTS campaign_name VARCHAR;
ALTER TABLE raw_iskra_messages ADD COLUMN IF NOT EXISTS template_id   VARCHAR;
ALTER TABLE raw_iskra_messages ADD COLUMN IF NOT EXISTS template_name VARCHAR;

-- ---------------------------------------------------------------------------------------------
-- (2) v_whatsapp_copy_performance — per-copy-variant WhatsApp funnel: which WhatsApp copy converts.
--
-- GRAIN = the OPENER (first outbound message per conversation) — the cold-outreach copy that drives
-- whether a lead replies/books, the WhatsApp analogue of email step-1/subject attribution. One row
-- per conversation feeds the rollup; follow-ups (reactive copy) are deliberately NOT credited with a
-- conversation's outcome (that would misattribute the reply, which precedes the follow-up).
--
-- COPY KEY = a COVERAGE-GATED choice between the structured template_id and the normalized body:
--   * `copy_body` = the rendered opener body NORMALIZED to a template skeleton — per-lead
--     personalization stripped (salutation first-name -> {name}; "regarding <Company>" ->
--     "regarding {company}"; whitespace collapsed). This is REQUIRED: the raw rendered body is ~66%
--     unique (45,632 distinct over 69,319 outbound), so grouping by raw `body` would yield
--     near-one-row-per-lead noise. Normalization collapses it to the actual copy variants (the
--     high-volume openers carry thousands of conversations each). It is a heuristic on two known
--     personalization tokens — it does NOT strip the rotating sender persona ("This is
--     James/Thomas/Hannah/...") or the offer line ("$400k no PG" vs "$250k based on revenue"), which
--     are deliberate copy dimensions and remain visible/distinct.
--   * COVERAGE GATE (resolves the cutover-fragmentation hazard the moderator flagged): template_id
--     populates FORWARD-ONLY (new outbound rows) once Arseny wires it; a naive COALESCE(template_id,
--     copy_body) would fragment ONE real variant into two key-groups during the partial-population
--     window (new openers keyed on template_id, old/backfilled openers on copy_body) — splitting the
--     `conversations` denominator and mis-attributing copy performance. So the key switches to
--     template_id ONLY when template_id coverage over ALL openers is essentially complete
--     (>= TMPL_COVERAGE_FLOOR = 0.95); below that floor EVERY row keys on copy_body, so the grain is
--     stable and never fragments. This is a clean, automatic cutover with no mixed-key window:
--     0% today -> pure body grouping; ~complete later -> pure template_id grouping. template_id /
--     template_name / campaign_* are always exposed as attributes (max()) regardless of the key.
--   * The floor is a GLOBAL all-time fraction (column `template_id_coverage`, exposed for
--     transparency). This is a DELIBERATE choice: template_id populates FORWARD-ONLY, so the
--     body->template_id cutover fires only AFTER template_id is backfilled across historical openers
--     (an Arseny dependency). Scoping the floor to a recent window while still keying the whole table
--     would re-introduce the exact fragmentation across the time boundary (old NULL-template openers
--     on copy_body, new ones on template_id). So we accept "stays body-keyed until ~full coverage";
--     body-skeleton grouping is a fully valid attribution (Arseny's own guidance was "group by body"),
--     NOT a degraded mode. `keyed_on_template_id` + `template_id_coverage` make the regime visible.
--
-- GROUP BY is the copy_key expression ONLY (never the raw template_id), so the denominator is always
-- either pure-body (below floor) or pure-template (at/above floor) — the fragmentation cannot recur.
--
-- DEPENDENCY: the view body references v_whatsapp_conversation_performance, which is created by
-- DDL 82 (this file's `Depends on 82`). The applier runs DDLs in numeric order (82 << 1028) so the
-- target view exists at CREATE time; confirmed present in the live warehouse. (The outcome-flag
-- semantics also align with the W1f fix in DDL 1023, which reuses the same conversation-tag truth.)
--
-- Outcome flags reuse v_whatsapp_conversation_performance (replied / is_positive_reply=reply_sentiment
-- 'positive' / meeting_booked=meeting_status 'booked') so this view inherits the W1f positive-intent
-- definition and the complete meeting set once run_meetings has refreshed the table on the box.
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
CROSS JOIN cov
LEFT JOIN v_whatsapp_conversation_performance c USING (conversation_id)
GROUP BY CASE WHEN cov.tmpl_cov >= 0.95 THEN COALESCE(f.template_id, f.copy_body)
              ELSE f.copy_body END,
         cov.tmpl_cov;
