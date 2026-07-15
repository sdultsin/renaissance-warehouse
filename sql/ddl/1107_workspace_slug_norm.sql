-- @gate: add
-- Depends on 1037
-- Depends on 1081
-- Depends on 1102
-- ============================================================================
-- 1107_workspace_slug_norm.sql — normalized workspace identity on the two reply/message
-- surfaces whose workspace column is a MIXED encoding.
--
-- WHY (113_v_reply_canonical header + pass-2 recovery findings): core.reply.workspace_id
-- stores TWO encodings (Instantly UUID for source='instantly' rows, warehouse slug —
-- and sometimes UUID — for source='pipeline' rows). A UUID-only filter undercounts
-- funding replies ~3x. Every consumer today must re-derive the fix by hand.
-- raw_instantly_email_message.workspace_id is already slug-encoded (R2, DDL 1037) but
-- gets the same column for uniformity (norm is ~identity there).
--
-- WHAT: ADDITIVE nullable column `workspace_slug_norm` on both tables = the canonical
-- warehouse slug resolved through core.v_workspace_slug_norm (DDL 1102, all 4 identifier
-- spaces). Populated incrementally each nightly by entities/mof_bi_history.py
-- (post-insert enrichment — the entities/meeting.py offer-inheritance precedent; the hot
-- loaders' INSERT column lists are NOT touched, and raw workspace_id values are NEVER
-- rewritten in place). NULL = not yet enriched OR encoding unknown to the alias map.
--
-- core.email_message view: reproduced VERBATIM from DDL 1081 (same 24 columns in the
-- same order) with workspace_slug_norm APPENDED (positional consumers of 1..24 unaffected).
--
-- COVERAGE HONESTY: the-eagles has ZERO rows in raw_instantly_email_message (verified
-- 2026-07-15: 9 workspaces present, the-eagles absent — its thread history was never
-- synced before the workspace was dropped). Nothing is fabricated for it; eagles reply
-- CONTENT exists only via the pipeline rows in core.reply.
--
-- Reversible: the column is additive+nullable (ignore it to roll back semantics);
-- view rollback = re-run DDL 1081's view body.
-- ============================================================================

ALTER TABLE core.reply ADD COLUMN IF NOT EXISTS workspace_slug_norm VARCHAR;
ALTER TABLE main.raw_instantly_email_message ADD COLUMN IF NOT EXISTS workspace_slug_norm VARCHAR;

-- core.email_message — DDL 1081 body verbatim + workspace_slug_norm appended.
CREATE OR REPLACE VIEW core.email_message AS
SELECT
    message_id,
    rfc_message_id,
    thread_id,
    thread_key,
    lead_anchor_key,
    workspace_id,
    organization_id,
    campaign_id,
    lead_email,
    direction,
    ue_type,
    step_path,
    subject,
    body_text,
    body_html,
    from_email,
    to_emails,
    eaccount,
    message_at,
    source,
    _loaded_at,
    _run_id,
    -- Instantly AI-agent id of the authoring AIM agent; NULL = human-authored (or a
    -- direction where authorship doesn't apply). Extracted from the raw item because
    -- the curated column set predates the flag's discovery (2026-07-05).
    json_extract_string(api_response_raw, '$.ai_agent_id')             AS ai_agent_id,
    (json_extract_string(api_response_raw, '$.ai_agent_id') IS NOT NULL) AS is_aim,
    -- canonical warehouse slug via core.workspace_alias_unified (DDL 1102/1107);
    -- NULL = not yet enriched by the nightly, or encoding unknown to the alias map.
    workspace_slug_norm
FROM raw_instantly_email_message;
