-- @gate: add
-- Depends on 1037
--
-- Email-thread CONSOLIDATION (handoff 2026-06-28 §B): make core.email_thread the ONE canonical
-- reply-thread source, and cut its last dependency on the superseded frozen table.
--
-- Three changes vs DDL 1037's core.email_thread view:
--
--   1. REPLY-THREADS ONLY (HAVING n_inbound >= 1). The ?lead= pull returns a replier's COMPLETE
--      multi-campaign history, and thread_key = campaign_id, so a replier who only received cold
--      sends in some OTHER campaign produced an inbound-less thread row (n_inbound=0). Those are not
--      reply threads (the whole concept is "for every person who REPLIES, their thread") and they
--      made QA G5 bad_inbound>0 unsatisfiable. The message-grain atom core.email_message still holds
--      EVERY message (the full superset); this rollup is now the reply-conversation surface only.
--
--   2. lead_interest_status RE-HOMED onto the atom (off the superseded raw_pipeline_conversation_
--      messages). It is now sourced from the Instantly i_status carried in
--      raw_instantly_email_message.api_response_raw — the SAME integer code namespace as the frozen
--      interest_status (-1/0/1/4/-2999x — verified identical 2026-06-29), latest message per
--      (workspace_id, lead_email). The gap-fill writes {"i_status": <frozen interest_status>} into the
--      api_response_raw of its pipeline-sourced rows, so pre-retention repliers keep their interest
--      too. This removes the ONLY runtime read of the frozen table — a future drop of it can no longer
--      silently NULL lead_interest_status (resolves the 1037 OPEN-1 / interest_status_guard WARN).
--
--   3. SUPERSEDE NOTE extended: raw_pipeline_conversation_messages AND raw_instantly_email are no
--      longer the place to read reply threads — core.email_thread is the one door. Both stay LIVE
--      (raw_instantly_email is still the replied-lead DISCOVERY source for the nightly delta; the
--      frozen table is the historical archive folded in via the gap-fill) but neither should be
--      SELECTed for reply-thread content. (1037 already declared the consolidation vs
--      raw_comms_instantly_message; this extends it to the two pipeline sources.)
--
-- Non-destructive: a CREATE OR REPLACE of a VIEW only. No table/column/data change.

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
-- De-dup the cold-send (ue_type=1) rows by (thread_key, step_path) WITHIN A LEAD'S THREAD (unchanged
-- from 1037): a resend with a new id but the SAME step_path counts once; window partitioned by the
-- FULL conversation grain so each lead reports its own distinct-step count.
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
-- lead_interest_status RE-HOMED onto the atom (change #2): latest Instantly i_status per (ws, lead)
-- from raw_instantly_email_message.api_response_raw. Same integer codes as the old frozen source.
-- TRY_CAST is NULL-safe; rows whose raw JSON lacks i_status are filtered out before the row_number so
-- the "latest" is the latest row that actually carries an interest code.
interest AS (
    SELECT workspace_id, lead_email, interest_status
    FROM (
        SELECT
            workspace_id,
            lower(trim(lead_email)) AS lead_email,
            TRY_CAST(json_extract_string(api_response_raw, '$.i_status') AS INTEGER) AS interest_status,
            row_number() OVER (
                PARTITION BY workspace_id, lower(trim(lead_email))
                ORDER BY message_at DESC NULLS LAST
            ) AS rn
        FROM raw_instantly_email_message
        WHERE api_response_raw IS NOT NULL
          AND json_extract_string(api_response_raw, '$.i_status') IS NOT NULL
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
    coalesce(any_value(sd.n_seq_sends), 0)                           AS n_seq_sends,
    bool_or(m.ue_type = 3)                                           AS answered,
    min(m.message_at) FILTER (WHERE m.ue_type = 1)                   AS first_send_at,
    min(m.message_at) FILTER (WHERE m.direction = 'inbound')         AS first_reply_at,
    max(m.message_at)                                                AS last_message_at,
    any_value(i.interest_status)                                     AS lead_interest_status,
    array_agg(DISTINCT m.thread_id)                                  AS thread_ids,
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
GROUP BY m.workspace_id, m.lead_email, m.thread_key
-- reply-threads only (change #1 / G5): drop inbound-less threads (a replier's cold-send-only campaigns)
HAVING count(*) FILTER (WHERE m.direction = 'inbound') >= 1;
