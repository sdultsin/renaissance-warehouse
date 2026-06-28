-- @gate: add
-- Depends on 36
-- Depends on 84
-- ============================================================================
-- 1037_email_message.sql — full both-direction Instantly email-thread atom.
-- ----------------------------------------------------------------------------
-- DEPENDENCIES (apply ordering — the moderator/apply path must run these first):
--   * 36  — the inbound-reply atom (raw_instantly_email / email_type=received): the
--           replied-lead discovery + per-workspace watermark source this entity reads.
--   * 84  — sql/ddl/84_schema_gate.sql CREATEs core.column_aliases (PK (alias, scope));
--           the §3d alias-seed INSERT below writes into it with ON CONFLICT (alias, scope).
--           Without 84 applied first the §3d INSERT references a non-existent table and
--           the migration fails (alias-seed dependency, FINALIZED-SPEC §3 header rule).
-- ----------------------------------------------------------------------------
-- WHY (FINALIZED-SPEC 2026-06-28, deliverables/2026-06-28-email-thread-sync/):
--   For EVERY replied lead, sync that lead's ENTIRE thread — our cold sends
--   (ue_type=1), the prospect's replies (ue_type=2), and our/IM replies
--   (ue_type=3) — the ACTUAL rendered emails (spintax already collapsed), so a
--   "thread" is a trivial group-by. The pull primitive is GET /api/v2/emails?lead=
--   which returns a lead's complete multi-campaign, multi-direction history.
--
-- SUPERSEDES (R4 — consolidation, NOT a semantic dupe):
--   main.raw_comms_instantly_message (DDL 16, ~61,939 rows) already holds
--   both-direction Instantly messages with a near-identical shape
--   (body_text/body_html/direction/subject/from_email/to_emails). This new atom
--   SUPERSEDES it in concept and REUSES its column vocabulary deliberately. It is
--   NOT dropped here — declared superseded-in-concept; a follow-up may drop
--   raw_comms_instantly_message once G4 parity proves full coverage (OPEN-1).
--   This is a consolidation of one concept under a canonical, not a fork.
--
-- DESIGN DECISIONS folded in (FINALIZED-SPEC §0 conflict resolutions):
--   R1  conversation grain = (workspace_id, lead_email, thread_key) where
--       thread_key = campaign_id (the FULL id; the thread_id 2-char prefix is
--       campaign_id[:2]). The per-lead suffix is stored as lead_anchor_key for QA
--       ONLY — it is CONSTANT across a lead's campaigns and must NEVER be the
--       conversation key (it would merge every campaign).
--   R1a NULL campaign_id (manual IM reply) -> thread_key =
--       'unattributed:'||lead_anchor_key so it attaches to the lead's anchor.
--   R2  workspace_id stores the canonical core.workspace SLUG (joinable), NEVER
--       the Instantly organization UUID. Dedup-by-organization_id happens in the
--       entity; organization_id is kept here only as provenance/audit.
--   R3  the atom is a RAW table (raw_instantly_email_message, main schema, keeps
--       api_response_raw) + a thin curated VIEW core.email_message that consumers
--       read. Curated name, raw drill-through stays out of the view.
--   R5  step_path VARCHAR (composite send path '0_0_2') — NOT the INTEGER `step`;
--       stored as a raw string, never _to_int()'d (which corrupts '0_0_2'->NULL).
--   R6  PRIMARY KEY column is `message_id` for readability but is POPULATED FROM
--       item['id'] (the per-email UUID, matching raw_instantly_email.email_id) —
--       NOT the RFC822 message_id header. The header is stored separately as
--       nullable rfc_message_id for cross-system join only. UPSERT key = item['id'].
--   R9  direction derived from ue_type ALONE ('inbound' if ue_type=2 else
--       'outbound') — never a from/to-vs-lead heuristic.
--
-- ADDITIVE + IDEMPOTENT. Creates one raw table + two views + 6 alias rows. No
-- ALTER/DROP/RENAME of any pre-existing object. Fully reversible (DROP the table +
-- two views, DELETE the 6 alias rows).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ── 3a. raw_instantly_email_message — the atom (raw API pull, main/bare schema) ──
CREATE TABLE IF NOT EXISTS raw_instantly_email_message (
    -- PK populated from item['id'] (per-email UUID), NOT the RFC822 header. Upsert key.
    -- LOAD-BEARING INVARIANT (do not change the population rule): message_id == the Instantly
    -- /emails item['id'] (equal to raw_instantly_email.email_id), and lead_email is ALWAYS
    -- lowercased+trimmed. Downstream consumers join on these exact contracts (R6).
    message_id        VARCHAR PRIMARY KEY,
    -- RFC822 message_id header (<CAM…> / <ins-u-1-…>); nullable; cross-system join only.
    rfc_message_id    VARCHAR,
    thread_id         VARCHAR,          -- raw Instantly thread_id ('34-iZEy_…')
    -- conversation key (R1): campaign_id; COALESCE(campaign_id,'unattributed:'||lead_anchor_key).
    thread_key        VARCHAR,
    -- thread_id SUFFIX (per-lead constant) — QA only ("same human across campaigns").
    lead_anchor_key   VARCHAR,
    -- canonical core.workspace SLUG ('renaissance-4'); NEVER organization_id/UUID (R2).
    workspace_id      VARCHAR NOT NULL,
    -- the Instantly org UUID the row was pulled under (provenance / dedup audit).
    organization_id   VARCHAR,
    campaign_id       VARCHAR,          -- nullable for manual IM replies
    lead_email        VARCHAR NOT NULL, -- lowercased + trimmed conversation lead
    -- 'inbound' (ue 2) / 'outbound' (ue 1,3) — from ue_type ALONE (R9).
    direction         VARCHAR NOT NULL,
    ue_type           INTEGER,          -- 1 seq send · 2 prospect reply · 3 IM/our reply
    -- composite send path '0_0_2' (R5); RAW string, never _to_int()'d; NULL on replies.
    step_path         VARCHAR,
    subject           VARCHAR,          -- actual sent/received subject
    body_text         VARCHAR,          -- rendered clean text (html+quote stripped per §7)
    body_html         VARCHAR,          -- raw html (nullable; fidelity / recovery)
    from_email        VARCHAR,          -- matches raw_comms_instantly_message.from_email
    to_emails         VARCHAR,          -- matches raw_comms_instantly_message.to_emails
    eaccount          VARCHAR,          -- our sending / receiving mailbox
    message_at        TIMESTAMPTZ,      -- = item['timestamp_email']; canonical message-grain order key
    source            VARCHAR DEFAULT 'instantly',  -- 'instantly' (real) | 'template' (§7 approx)
    api_response_raw  VARCHAR,          -- JSON drill-through (raw-table only; not in the view)
    _loaded_at        TIMESTAMPTZ NOT NULL,
    _run_id           VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_email_message_lead
    ON raw_instantly_email_message (lead_email);
CREATE INDEX IF NOT EXISTS idx_email_message_ws_lead
    ON raw_instantly_email_message (workspace_id, lead_email);
CREATE INDEX IF NOT EXISTS idx_email_message_thread_key
    ON raw_instantly_email_message (thread_key);
CREATE INDEX IF NOT EXISTS idx_email_message_at
    ON raw_instantly_email_message (message_at);

-- ── 3b. core.email_message — curated VIEW (stable name consumers read) ──────────
-- Selects the business columns from the raw atom; everything EXCEPT api_response_raw.
-- organization_id is exposed for audit (R2 provenance). _loaded_at/_run_id kept so
-- consumers can reason about freshness (and so G2's hash explicitly EXCLUDES them).
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
    _run_id
FROM raw_instantly_email_message;

-- ── 3c. core.email_thread — the rollup (a VIEW, never a table) ──────────────────
-- Grain = (workspace_id, lead_email, thread_key) where thread_key = COALESCE(campaign_id,…).
-- One row = one thread. n_seq_sends de-dups by (thread_key, step_path) keeping the LATEST
-- message_at so a resend with a new id does not inflate the cold-send count.
-- Materialize only on a measured perf need, via a separate numbered DDL.
CREATE OR REPLACE VIEW core.email_thread AS
WITH msg AS (
    SELECT
        workspace_id,
        lead_email,
        thread_key,
        thread_id,
        campaign_id,
        message_id,
        direction,
        ue_type,
        step_path,
        subject,
        body_text,
        message_at
    FROM core.email_message
),
-- De-dup the cold-send (ue_type=1) rows by (thread_key, step_path) WITHIN A LEAD'S THREAD:
-- a resend with a new id but the SAME step_path must count once. The dedup window MUST be
-- partitioned by the FULL conversation grain (workspace_id, lead_email, thread_key, step_path)
-- — NOT just (thread_key, step_path). thread_key = campaign_id is SHARED across every lead in a
-- campaign, so partitioning by (thread_key, step_path) alone keeps exactly ONE arbitrary lead's
-- row per (campaign, step) and drops the rest; the GROUP BY then emits no row for the dropped
-- leads and the LEFT JOIN coalesces their n_seq_sends to 0 (verified-wrong on DuckDB 1.5.3: two
-- leads in one campaign each sent steps 0_0_0 & 0_0_1 -> one reads 2, the other reads 0). Adding
-- workspace_id+lead_email scopes the dedup to a single lead's thread so each lead reports its own
-- distinct-step count (verified-correct on DuckDB 1.5.3: both leads -> 2).
seq_dedup AS (
    SELECT workspace_id, lead_email, thread_key, count(*) AS n_seq_sends
    FROM (
        SELECT
            workspace_id, lead_email, thread_key, step_path,
            row_number() OVER (
                PARTITION BY workspace_id, lead_email, thread_key, step_path
                ORDER BY message_at DESC NULLS LAST
            ) AS rn
        FROM msg
        WHERE ue_type = 1
    ) s
    WHERE rn = 1
    GROUP BY workspace_id, lead_email, thread_key
),
-- latest interest status per (workspace_id, lead_email) from the reply-tag lineage.
-- Pre-deduped once with a window function and LEFT JOINed (NOT a per-output-group correlated
-- subquery) so the rollup stays a single scan-friendly aggregation rather than O(threads*scan).
--
-- JOIN-KEY NAMESPACE VERIFIED (resolves the moderator's silent-NULL concern empirically, not
-- by assertion): raw_pipeline_conversation_messages.workspace_id is SLUG-keyed, NOT a UUID —
-- live `SELECT DISTINCT workspace_id` returns 'renaissance-1','warm-leads','the-gatekeepers',
-- 'section-125-1',… (probed 2026-06-28). This atom stores the same canonical core.workspace
-- slug in workspace_id (R2), so the equality predicate is in ONE namespace and matches real
-- rows (does NOT silently NULL).
--
-- SUPERSEDE COUPLING (OPEN-1 guard): this is the ONLY runtime dependency on the superseded
-- raw_pipeline_conversation_messages. If a future drop migration removes/stops populating it,
-- RE-HOME interest_status here (e.g. carry it onto the atom at ingest, or source it from a
-- non-superseded reply-intent table) — do not let it silently go all-NULL.
interest AS (
    SELECT workspace_id, lead_email, interest_status
    FROM (
        SELECT
            cm.workspace_id,
            lower(trim(cm.lead_email)) AS lead_email,
            cm.interest_status,
            row_number() OVER (
                PARTITION BY cm.workspace_id, lower(trim(cm.lead_email))
                ORDER BY cm.message_timestamp DESC NULLS LAST
            ) AS rn
        FROM raw_pipeline_conversation_messages cm
        WHERE cm.interest_status IS NOT NULL
    ) z
    WHERE rn = 1
)
SELECT
    m.workspace_id,
    m.lead_email,
    m.thread_key,
    count(*)                                                         AS n_messages,
    count(*) FILTER (WHERE m.direction = 'inbound')                  AS n_inbound,
    count(*) FILTER (WHERE m.direction = 'outbound')                 AS n_outbound,
    -- sd.n_seq_sends is already 1:1 per (workspace_id,lead_email,thread_key); surface it with
    -- any_value so it does NOT appear as a grouping dimension (clearer intent than GROUP BY it).
    coalesce(any_value(sd.n_seq_sends), 0)                           AS n_seq_sends,
    bool_or(m.ue_type = 3)                                           AS answered,
    min(m.message_at) FILTER (WHERE m.ue_type = 1)                   AS first_send_at,
    min(m.message_at) FILTER (WHERE m.direction = 'inbound')         AS first_reply_at,
    max(m.message_at)                                                AS last_message_at,
    any_value(i.interest_status)                                     AS lead_interest_status,
    array_agg(DISTINCT m.thread_id)                                  AS thread_ids,
    -- ordered transcript: 'IN/OUT [ts] body' ascending by message_at.
    string_agg(
        (CASE WHEN m.direction = 'inbound' THEN 'IN ' ELSE 'OUT ' END)
        || '[' || coalesce(m.message_at::VARCHAR, '') || '] '
        || coalesce(m.body_text, ''),
        E'\n---\n' ORDER BY m.message_at ASC NULLS LAST
    )                                                                AS thread_text
FROM msg m
LEFT JOIN seq_dedup sd
       ON sd.workspace_id = m.workspace_id
      AND sd.lead_email   = m.lead_email
      AND sd.thread_key   = m.thread_key
LEFT JOIN interest i
       ON i.workspace_id = m.workspace_id
      AND i.lead_email   = m.lead_email
GROUP BY m.workspace_id, m.lead_email, m.thread_key;

-- ── 3d. Column aliases (declared here so the gate records them as deliberate) ────
-- Each row tells the dupe gate the canonical for a concept this atom introduces, so a
-- later editor reaching for a synonym is steered to the canonical (no semantic fork).
INSERT INTO core.column_aliases (alias, canonical_name, scope, reason, added_by) VALUES
  ('reply_timestamp','message_at','core.email_message','message-grain time (both directions)','email-thread-sync'),
  ('date_sent',      'message_at','core.email_message','message-grain time','email-thread-sync'),
  ('sent_at',        'message_at','core.email_message','message-grain time','email-thread-sync'),
  ('timestamp_email','message_at','core.email_message','source field is timestamp_email','email-thread-sync'),
  ('step',           'step_path', 'core.email_message','composite send path, not the INTEGER step','email-thread-sync'),
  ('to_email',       'to_emails', 'core.email_message','recipients may be multiple','email-thread-sync')
ON CONFLICT (alias, scope) DO NOTHING;
