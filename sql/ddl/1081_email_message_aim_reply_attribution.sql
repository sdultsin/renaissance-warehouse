-- @gate: alter-type   (CREATE OR REPLACE VIEW core.email_message — two ADDITIVE columns APPENDED
--                      at the END; existing 22-column contract byte-identical, no rename/drop/
--                      retype) + one NEW view core.reply_attribution. No table touched.
-- Depends on 1037
-- Depends on 84
-- Depends on 3
-- ============================================================================
-- 1081_email_message_aim_reply_attribution.sql — human/AIM authorship split +
-- the per-reply attribution surface (Sam's spec, 2026-07-05 reply-form-bi lane).
-- ----------------------------------------------------------------------------
-- WHY:
--   1. AIM (Instantly AI agent) replies ship through the SAME inboxes as human IM
--      replies with no curated flag — DDL 1070's standing caveat ("no per-message
--      human/AI split in the source") is STALE: the /emails item carries
--      `ai_agent_id` (non-null on AI-authored messages; [[reference_instantly_ai_
--      authored_flag_ai_agent_id_20260705]]). It lives in api_response_raw on
--      raw_instantly_email_message but the curated view dropped it. Verified on the
--      live warehouse 2026-07-06 (read-only): non-null on 7,448 / 45,036 ue_type=3
--      rows (16.5%); stray non-nulls elsewhere are negligible (1 on ue_type=1,
--      6 on ue_type=2).
--   2. There is NO one-row-per-inbound-reply surface carrying WHICH send (step) the
--      prospect answered + whether/how we answered. v_reply_canonical is the
--      content/depth union (and ⛔ not a count surface); core.email_thread is
--      thread-grain. core.reply_attribution below is reply-grain: ALL ue_type=2
--      rows (incl. unanswered/negative), each attributed to the last prior cold
--      send in its conversation, with the answering message's human/AIM flag.
--
-- VERIFIED (read-only probe vs the writer DB, 2026-07-06 ~04:35Z, this exact SQL):
--   * view rows == ue_type=2 rows == 168,793 (row-preserving, no fan-out).
--   * replied_to_step last-30d: step1=58,114 step2=22,725 (matches the reply-form-bi
--     reference distribution).
--   * answered split: 28,910 human + 7,608 AIM answered; 132,275 unanswered kept.
--   * campaign name joins for 129,876 rows (core.campaign is best-effort — DDL 1070
--     documents the unmatched remainder; LEFT JOIN, never filters).
--
-- ADDITIVE + IDEMPOTENT. Two CREATE OR REPLACE VIEWs + alias rows. No ALTER/DROP of
-- any table. Reversible: re-run 1037's view body + DROP VIEW core.reply_attribution.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ── 1. core.email_message — SAME 22 columns as DDL 1037 IN THE SAME ORDER, plus
--        ai_agent_id + is_aim APPENDED (positional consumers of 1..22 unaffected).
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
    (json_extract_string(api_response_raw, '$.ai_agent_id') IS NOT NULL) AS is_aim
FROM raw_instantly_email_message;

-- ── 2. core.reply_attribution — ONE ROW PER INBOUND PROSPECT REPLY (ue_type=2, ALL
--        of them — unanswered/negative included), attributed to the send it answered.
--   Grain key = reply_message_id (PK of the raw atom -> unique).
--   Conversation scope = (workspace_id, lead_email, thread_key) — the R1 grain of
--   DDL 1037 (thread_key = campaign_id, or 'unattributed:'||lead_anchor_key).
--   replied_to_step        = COUNT of prior cold sends (ue_type=1, message_at <= reply
--                            ts) in the conversation — raw send count by spec (a resend
--                            of the same step_path counts; cf. core.email_thread's
--                            n_seq_sends which de-dups — different metric, both wanted).
--                            0 = a reply with no captured prior send (manual/unattributed).
--   replied_to_message_id  = the LAST such send (ties broken by message_id desc) + its subject.
--   answered               = an our-side reply (ue_type=3) exists AFTER this reply in the
--                            same conversation; our_reply_* describe the EARLIEST one,
--                            incl. its human/AIM authorship (is_aim from #1 above).
CREATE OR REPLACE VIEW core.reply_attribution AS
WITH replies AS (
  SELECT message_id, message_at, lead_email, workspace_id, campaign_id, thread_key
  FROM core.email_message
  WHERE ue_type = 2
),
sends AS (
  SELECT workspace_id, lead_email, thread_key, message_id, message_at, subject
  FROM core.email_message
  WHERE ue_type = 1
),
ours AS (
  SELECT workspace_id, lead_email, thread_key, message_id, message_at, ai_agent_id, is_aim
  FROM core.email_message
  WHERE ue_type = 3
),
-- prior cold sends per reply: count them all + rank to pick the last one. ONE join pass
-- feeds both aggregates (count via window, last-send via send_rank=1).
joined AS (
  SELECT r.message_id AS reply_message_id,
         s.message_id AS send_message_id,
         s.subject    AS send_subject,
         row_number() OVER (PARTITION BY r.message_id
                            ORDER BY s.message_at DESC, s.message_id DESC) AS send_rank,
         count(*)     OVER (PARTITION BY r.message_id) AS n_prior_sends
  FROM replies r
  JOIN sends s
    ON s.workspace_id = r.workspace_id
   AND s.lead_email   = r.lead_email
   AND s.thread_key   = r.thread_key
   AND s.message_at  <= r.message_at
),
prior AS (
  SELECT reply_message_id, n_prior_sends, send_message_id, send_subject
  FROM joined
  WHERE send_rank = 1
),
-- earliest our-side reply strictly AFTER the prospect reply (ans_rank=1); its is_aim is
-- the human/AIM attribution of how the desk answered THIS reply.
answers AS (
  SELECT r.message_id  AS reply_message_id,
         o.message_id  AS our_reply_message_id,
         o.message_at  AS our_reply_at,
         o.ai_agent_id AS our_reply_ai_agent_id,
         o.is_aim      AS our_reply_is_aim,
         row_number() OVER (PARTITION BY r.message_id
                            ORDER BY o.message_at ASC, o.message_id ASC) AS ans_rank
  FROM replies r
  JOIN ours o
    ON o.workspace_id = r.workspace_id
   AND o.lead_email   = r.lead_email
   AND o.thread_key   = r.thread_key
   AND o.message_at   > r.message_at
)
SELECT
  r.message_id                     AS reply_message_id,
  r.message_at                     AS reply_at,
  r.lead_email,
  r.workspace_id,
  r.campaign_id,
  c.name                           AS campaign_name,   -- LEFT JOIN core.campaign (best-effort dim)
  r.thread_key,
  coalesce(p.n_prior_sends, 0)     AS replied_to_step,
  p.send_message_id                AS replied_to_message_id,
  p.send_subject                   AS replied_to_subject,
  (a.reply_message_id IS NOT NULL) AS answered,
  a.our_reply_message_id,
  a.our_reply_at,
  a.our_reply_ai_agent_id,
  a.our_reply_is_aim
FROM replies r
LEFT JOIN prior p         ON p.reply_message_id = r.message_id
LEFT JOIN core.campaign c ON c.campaign_id = r.campaign_id
LEFT JOIN answers a       ON a.reply_message_id = r.message_id AND a.ans_rank = 1;

-- ── 3. Column aliases — steer later editors to the canonicals this DDL introduces. ──
INSERT INTO core.column_aliases (alias, canonical_name, scope, reason, added_by) VALUES
  ('is_ai',            'is_aim',          'core.email_message',     'AIM authorship flag = ai_agent_id non-null','reply-attribution'),
  ('is_ai_reply',      'is_aim',          'core.email_message',     'AIM authorship flag','reply-attribution'),
  ('agent_id',         'ai_agent_id',     'core.email_message',     'Instantly AI agent id from api_response_raw','reply-attribution'),
  ('reply_ts',         'reply_at',        'core.reply_attribution', 'the inbound reply message_at','reply-attribution'),
  ('reply_timestamp',  'reply_at',        'core.reply_attribution', 'the inbound reply message_at','reply-attribution'),
  ('step',             'replied_to_step', 'core.reply_attribution', 'count of prior cold sends at reply time (raw, not step_path-deduped)','reply-attribution'),
  ('is_answered',      'answered',        'core.reply_attribution', 'our-side ue_type=3 reply exists after the reply','reply-attribution')
ON CONFLICT (alias, scope) DO NOTHING;
