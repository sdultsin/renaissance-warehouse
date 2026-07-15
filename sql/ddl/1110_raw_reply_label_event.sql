-- @gate: add
-- ============================================================================
-- 1110_raw_reply_label_event.sql — the reply-label EVENT STREAM (append-only,
-- CRM-style; charter §4 label-history principle).
--
-- WHY: the 4-label labeler (opportunity/engagement/confused/not_interested, v0.2.0)
-- labels lead-grain threads (workspace_slug × lead_email — core.email_thread.thread_key
-- is campaign-collapsed and deliberately NOT used). A lead's label CHANGES over time;
-- events are NEVER updated or deleted — cohort counts (ever-was-opportunity) never
-- decrement, current-state is a derived view (DDL 1111). Re-labeling under a new prompt
-- version APPENDS under a new labeler_version; uniqueness grain =
-- (message_ref_table, message_ref_id, labeler_version).
--
-- DATA / LOAD PATH: droplet escrow /root/mof/labeling/backfill/escrow/events.parquet
-- (+ .jsonl; keys 1:1 with these columns), regenerated at PAUSED/DONE and by the daily
-- increment cron. Loaded incrementally (anti-join) each nightly by
-- entities/reply_label_event.py — never by DDL (gate rejects filesystem paths; the
-- DDL-92 read-from-file-in-DDL failure class). Escrow at prep time: 960 events, all
-- labeler_version='0.2.0', 0 key dupes (NI 791 · opp 73 · auto 40 · engagement 36 ·
-- confused 18 · bot 2) — the backfill keeps appending; counts grow monotonically.
-- ⚠ REHOME: /root/mof dies with the droplet (~2026-07-25). The escrow path is
-- env-overridable in the entity (REPLY_LABEL_ESCROW_PARQUET); the migration must carry
-- the escrow (or repoint the labeler's output) — flagged to the migration lane.
--
-- Gate classes 'auto'/'bot' ARE collected (recipient-attributed, charter §6) but are
-- excluded from every label-stat view; 'labeler_error' rows are runner failures kept
-- for audit.
--
-- Reversible: DROP TABLE (escrow JSONL+parquet retain everything; views 1111-1114 stack
-- on top and drop independently).
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.raw_reply_label_event (
    event_id          VARCHAR PRIMARY KEY,        -- uuid4 (escrow-generated)
    workspace_slug    VARCHAR NOT NULL,           -- normalized warehouse slug (deleted ws included)
    lead_email        VARCHAR NOT NULL,           -- lowercased
    campaign_id       VARCHAR,                    -- campaign of the anchoring message
    message_ref_table VARCHAR NOT NULL,           -- 'core.email_message' | 'core.reply'
    message_ref_id    VARCHAR NOT NULL,           -- message_id / reply_id in that table
    message_ts        TIMESTAMP WITH TIME ZONE,   -- anchoring inbound message timestamp (escrow is UTC)
    label             VARCHAR NOT NULL,           -- opportunity|engagement|confused|not_interested
                                                  --   | 'auto' | 'bot' (gate classes) | 'labeler_error'
    opt_out           BOOLEAN NOT NULL DEFAULT FALSE,  -- orthogonal; feeds comms.suppression LATER (report-only now)
    confidence        INTEGER,                    -- 0-100 (calibrated bands, prompt v0.2 rule 9)
    refute_fired      BOOLEAN NOT NULL DEFAULT FALSE,
    refute_agree      BOOLEAN,                    -- NULL when refute not fired
    refute_alt_label  VARCHAR,
    evidence          VARCHAR,                    -- verbatim deciding quote (<=160 chars)
    rationale         VARCHAR,                    -- one-line why (<=120 chars)
    deterministic_gate VARCHAR,                   -- 'auto_regex'|'bare_optout'|'bot_heuristic'|NULL (LLM-labeled)
    flag_human        BOOLEAN NOT NULL DEFAULT FALSE,
    n_inbound         INTEGER,                    -- lead inbound count in thread at labeling time
    trick_class       VARCHAR,                    -- removal-offer class(es), comma-joined; NULL if none
    labeler_version   VARCHAR NOT NULL,           -- e.g. '0.2.0' (prompt+runner version)
    prompt_hash       VARCHAR,                    -- sha256[:12] over the 3 LLM prompt files
    model             VARCHAR,                    -- 'claude-sonnet-5' | 'deterministic'
    snapshot_id       VARCHAR,                    -- warehouse snapshot the thread was read from (data honesty)
    labeled_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    _loaded_at        TIMESTAMPTZ DEFAULT now(),  -- warehouse-side provenance (not in escrow)
    _run_id           VARCHAR,                    -- nightly run that loaded the row
    UNIQUE (message_ref_table, message_ref_id, labeler_version)
);
