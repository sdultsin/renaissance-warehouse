-- 1141_v_reply_canonical_ws_fallback.sql
-- Ticket: handoffs/2026-07-18-reply-workspace-attribution-gap-TICKET.md
-- (reply-grain workspace attribution gap: 6% of replies had workspace_id resolvable-but-NULL).
--
-- WHAT THIS CHANGES vs 113_v_reply_canonical.sql:
--   v_reply_canonical resolved workspace ONLY from the reply's stored (payload) workspace_id via wmap.
--   When core.reply.workspace_id is NULL (48,329 pipeline-source rows, all carrying a campaign_id),
--   workspace_id_canonical came out NULL and the reply dropped from is_funding / per-workspace splits.
--   We now ADD a campaign->workspace FALLBACK: when the payload encoding does not resolve, resolve the
--   workspace from the canonical campaign dimension core.v_campaign_dim_unified (which retains
--   deleted-in-Instantly campaigns via the pipeline dim + the 1104 patch) -> workspace_slug -> wmap.
--   Coverage of the previously-NULL rows: 47,777 / 48,359 (98.8%); residual = campaigns present in NO
--   dim. This is ADD-ONLY: every column keeps its name/semantics; newly-resolved rows flip
--   workspace_unresolved TRUE->FALSE and gain a canonical workspace. workspace_id_raw is untouched.
--
-- @gate: add
-- Depends on 1104
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
  -- campaign->workspace FALLBACK (ticket 2026-07-18): payload encoding first, else the reply's campaign.
  COALESCE(m.canonical_uuid, mc.canonical_uuid)
                               AS workspace_id_canonical,  -- resolved Instantly UUID (NULL if unmapped)
  (COALESCE(m.canonical_uuid, mc.canonical_uuid) IS NULL)
                               AS workspace_unresolved,    -- TRUE = neither payload nor campaign resolved
  -- STRICTLY add-only: the campaign fallback (mc) supplies name/cm ONLY when the payload workspace
  -- (m) did not resolve — so a row already resolved via payload is byte-identical to the old view and
  -- cm can never cross to a different workspace than workspace_id_canonical.
  COALESCE(m.wname, w.name, CASE WHEN m.canonical_uuid IS NULL THEN mc.wname END)  AS workspace_name,
  COALESCE(m.cm,            CASE WHEN m.canonical_uuid IS NULL THEN mc.cm    END)  AS cm,
  (COALESCE(m.canonical_uuid, mc.canonical_uuid) IN (
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
       ON c.campaign_id = r.campaign_id
-- campaign->workspace fallback resolvers (LEFT, never gate a row).
-- cdu is pre-deduped to EXACTLY ONE row per campaign_id so this join can NEVER fan out /
-- duplicate a reply row, regardless of v_campaign_dim_unified's grain (verified unique
-- 2026-07-18: 3,349 rows = 3,349 distinct campaign_id; the GROUP BY makes it fan-out-proof
-- even if that ever changes). wmap is already DISTINCT ON (wid).
LEFT JOIN (
  SELECT campaign_id, any_value(workspace_slug) AS workspace_slug
  FROM core.v_campaign_dim_unified
  WHERE campaign_id IS NOT NULL
  GROUP BY campaign_id
) cdu
       ON cdu.campaign_id = r.campaign_id
LEFT JOIN wmap mc
       ON mc.wid = cdu.workspace_slug;
