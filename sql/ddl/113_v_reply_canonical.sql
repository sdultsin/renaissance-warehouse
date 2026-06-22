-- 113_v_reply_canonical.sql
-- Ask #4 of WAREHOUSE-REPLY-HISTORY-ISSUE.md (chat: reply-history-fix, 2026-06-21).
-- Full writeup: deliverables/2026-06-21-warehouse-reply-history/FINDINGS-reply-history-fix.md.
--
-- PURPOSE: ONE canonical, attribution-enriched reply *CONTENT / THREAD* surface, with the maximum
--   reply history depth currently in the warehouse, funding-attributable via is_funding + cm,
--   robust to the dual workspace_id encoding (UUID vs slug).
--
-- ⛔ NOT A COUNT SURFACE. Reply COUNT truth = Instantly NATIVE
--   (raw_pipeline_campaign_daily_metrics.unique_replies / unique_replies_automatic, v_kpi_email).
--   Never COUNT(*) this view for human/auto/total/positive replies — it unions per-reply content
--   rows from two sources and is for content/threads/attribution depth, a different need.
--
-- DUAL-ENCODING FIX (critical): core.reply.workspace_id is stored TWO ways —
--   source='instantly' rows use the Instantly UUID (e.g. f02d3d50…),
--   source='pipeline'  rows use the warehouse SLUG (e.g. renaissance-2) AND, for some rows, the UUID.
--   The two encodings hold DISJOINT physical replies (verified: 0 content-key overlap for Eyver;
--   distinct_reply_id == row count), so unioning both is correct and does NOT double-count.
--   A UUID-only filter undercounts funding ~3x (66,891 → 207,019). We resolve EITHER encoding to a
--   canonical workspace via wmap (core.workspace_alias.instantly_uuid ∪ .warehouse_slug).
--
-- COMPOSITION:
--   Base = core.reply, which ALREADY unions source='pipeline' (raw_pipeline_reply_data) +
--     source='instantly' (raw_instantly_email). thread_id/ue_type/from_address_email/message_id
--     grafted from raw_instantly_email via reply_id = email_id (172,514/172,514 = 100% match for
--     instantly-source rows; NULL for pipeline-source rows — never captured with a thread_id).
--
-- DEPTH / KNOWN GAPS (verified on snapshot warehouse_20260621_214649_095.duckdb):
--   Funding reply content present: Mar 9,055 · Apr 38,240 · May 60,426 · Jun 99,292 (total 207,019).
--   * LEFT-CENSORED at 2026-03-26 — that is an ETL INGESTION CUTOFF, not a natural start. Funding
--     workspaces sent 1.0–1.9M/mo in Jan–Mar with ~zero in-table replies before Mar 26; those
--     replies exist in Instantly and are MISSING from the warehouse (recoverable via re-pull if a
--     KPI/attribution need requires them — out of scope for this view).
--   * ROW CAP: workspace_id='renaissance-2' (Funding 5 / Eyver, slug encoding) = exactly 40,000 rows
--     floored at 2026-04-16 (its UUID encoding f02d3d50 floors at 2026-03-26). The 40,000 is a
--     suspected ingestion cap that left-censored that stream — Eyver pipeline depth is partially lost.
--
-- CAVEATS for consumers (variant-matrix-bi / AIM eval):
--   * is_auto_reply = the BROKEN heuristic (~3.5% vs ~63% native). Reference only; use native for splits.
--   * thread_id only for source='instantly' (Jun+); Mar–May pipeline replies have NULL thread_id.
--     raw_instantly_email holds the REPLY side of threads only (ue_type=2), not outbound steps.
--   * workspace identity is emitted as TWO explicit columns (no ambiguous bare `workspace_id`):
--       workspace_id_raw       = value as stored (UUID for instantly source, slug for pipeline).
--       workspace_id_canonical = resolved Instantly UUID via wmap (NULL if the encoding is not in
--                                core.workspace_alias). Join/filter on workspace_id_canonical.
--     workspace_unresolved = (workspace_id_canonical IS NULL) flags any encoding not yet in the
--     alias, so a future unmapped funding stream surfaces instead of silently dropping from is_funding.

-- @gate: add
-- Depends on 109
CREATE OR REPLACE VIEW derived.v_reply_canonical AS
WITH wmap AS (   -- resolve EITHER workspace_id encoding (UUID or slug) → canonical UUID + name + cm
  SELECT DISTINCT ON (wid) wid, canonical_uuid, wname, cm
  FROM (
    SELECT instantly_uuid AS wid, instantly_uuid AS canonical_uuid,
           canonical_current_name AS wname, cm, status
      FROM core.workspace_alias WHERE instantly_uuid IS NOT NULL
    UNION ALL
    SELECT warehouse_slug AS wid, instantly_uuid AS canonical_uuid,
           canonical_current_name AS wname, cm, status
      FROM core.workspace_alias WHERE warehouse_slug IS NOT NULL
  )
  ORDER BY wid, (status = 'active') DESC, wname   -- dedup wid (e.g. the-gatekeepers): prefer active
)
SELECT
  r.reply_id,
  r.source,                                    -- 'pipeline' (depth, no thread_id) | 'instantly' (thread_id)
  r.workspace_id               AS workspace_id_raw,        -- as stored (UUID or slug)
  m.canonical_uuid             AS workspace_id_canonical,  -- resolved Instantly UUID (NULL if unmapped)
  (m.canonical_uuid IS NULL)   AS workspace_unresolved,    -- TRUE = workspace_id_raw not in workspace_alias
  COALESCE(m.wname, w.name)    AS workspace_name,
  m.cm                         AS cm,
  (m.canonical_uuid IN (
     'cdae94c6-5a88-4614-92e2-09e28a073a2e',   -- Funding 1 (Samuel) / slug renaissance-4
     '88de6a7c-55db-4594-8851-ed7d56342a45',   -- Funding 2 (Ido)    / slug renaissance-5
     'd5ebf2bd-d7c8-4feb-8310-e57e6140e12a',   -- Funding 3 (Leo)    / slug prospects-power
     '6ab744f5-be81-4c5b-8333-c0c119a19b80',   -- Funding 4 (Sam)    / slug koi-and-destroy
     'f02d3d50-0e9f-4687-981d-6134e789baa4'     -- Funding 5 (Eyver)  / slug renaissance-2
   ))                          AS is_funding,
  r.campaign_id,
  c.name                       AS campaign_name,
  c.offer                      AS campaign_offer,
  r.lead_email,
  r.eaccount,
  r.step,
  r.variant,
  r.subject,
  r.reply_text,
  r.reply_timestamp,
  rie.thread_id,                               -- only source='instantly' (Jun+); NULL otherwise
  rie.ue_type,                                 -- constant 2 = reply-received (Instantly email_type)
  rie.from_address_email,
  rie.message_id,
  r.is_auto_reply                              -- BROKEN heuristic; reference only (truth: native)
FROM core.reply r
LEFT JOIN main.raw_instantly_email rie
       ON rie.email_id = r.reply_id AND r.source = 'instantly'
LEFT JOIN wmap m
       ON m.wid = r.workspace_id
LEFT JOIN core.workspace w
       ON w.workspace_id = r.workspace_id
LEFT JOIN core.campaign c
       ON c.campaign_id = r.campaign_id;
