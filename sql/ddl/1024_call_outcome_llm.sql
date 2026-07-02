-- @gate: add
-- Depends on 42
-- W1i (RevOps funnel deep-dive, 2026-06-26) — warm-caller appt-set refinement layer.
--
-- THE PROBLEM this closes: core.call_outcome.outcome_class is disposition-only and
-- classify_outcome() (entities/close_calls.py) emits ONLY voicemail/no_answer/answered —
-- it NEVER emits 'answered_appt_set'. So core.conversion_event's warm-caller feeder
-- (entities/conversion_event.py, WHERE outcome_class='answered_appt_set') always matched 0
-- rows: every warm-caller appointment was invisible to the warehouse's own conversion fact,
-- and core.warm_caller.appt_set_calls stayed NULL. The 109 real call bookings lived ONLY in
-- Grace's manual Funding Form (core.meeting channel='Call'), capped at ~77% attribution.
--
-- THE FIX (two layers, both additive — this file owns the schema for both):
--   • Fix-1a (deterministic, $0): the rep note 'booked via call' is an explicit booking
--     marker the setter types by hand → recover it with a note-regex. High precision; bounded
--     by note coverage (~24% of calls), so it is a FLOOR (~77/109 of sheet bookings).
--   • Fix-1b (LLM): scripts/classify_call_outcomes.py runs the qwen transcript pass (mirrors
--     the Sendivo opportunity classifier) and writes core.call_outcome_llm — refining the
--     'answered' calls into answered_appt_set / answered_not_interested / objection(+type),
--     plus a rate_quoted coaching flag. Picks up the bookings the note never recorded.
--
-- core.v_call_outcome_final coalesces them: note-booking > LLM > disposition. conversion_event
-- reads the view; warm_caller.appt_set_calls is filled from it. Pure additive — no ALTER/DROP/
-- rename of any existing object. core.call_outcome stays disposition-only (its contract intact).

CREATE SCHEMA IF NOT EXISTS core;

-- ── LLM refinement table (one row per classified call; persists across the close-phase
--    DELETE+INSERT rebuild of core.call_outcome). Column names mirror core.reply_intent. ──
CREATE TABLE IF NOT EXISTS core.call_outcome_llm (
    call_id            VARCHAR PRIMARY KEY,    -- joins core.call / core.call_outcome / core.call_transcript
    outcome_class      VARCHAR,                -- answered_appt_set | answered_not_interested | objection | answered_other
    objection_type     VARCHAR,                -- price|terms|trust|timing|already_have|no_need|not_dm|other | NULL
    rate_quoted        BOOLEAN,                -- did the rep quote a rate/price on the call? (pre-booking coaching signal)
    sentiment          VARCHAR,                -- positive | neutral | negative
    summary            VARCHAR,                -- one-line: what happened on the call
    confidence         DOUBLE,                 -- 0.0-1.0
    classifier_model   VARCHAR,
    classifier_version INTEGER,
    classified_at      TIMESTAMPTZ
);

-- ── Final per-call outcome. Precedence: deterministic note booking marker (rep ground truth)
--    > LLM refinement (only over a connected 'answered' base) > disposition base. This is what
--    conversion_event + warm_caller read so call appointments finally surface. ──
CREATE OR REPLACE VIEW core.v_call_outcome_final AS
SELECT
    o.call_id,
    CASE
      WHEN lower(COALESCE(o.note, '')) LIKE '%booked via call%'        THEN 'answered_appt_set'
      WHEN o.outcome_class = 'answered' AND l.outcome_class IS NOT NULL THEN l.outcome_class
      ELSE o.outcome_class
    END                                                                AS outcome_class,
    o.outcome_class                                                    AS disposition_class,
    l.outcome_class                                                    AS llm_class,
    (lower(COALESCE(o.note, '')) LIKE '%booked via call%')             AS booked_via_note,
    CASE
      WHEN lower(COALESCE(o.note, '')) LIKE '%booked via call%'        THEN 'note_regex'
      WHEN o.outcome_class = 'answered' AND l.outcome_class IS NOT NULL THEN 'llm'
      ELSE 'disposition'
    END                                                                AS outcome_source,
    l.objection_type,
    l.rate_quoted,
    l.sentiment,
    l.summary,
    l.confidence,
    l.classifier_model,
    l.classifier_version,
    o.note,
    o.resolved_at
FROM core.call_outcome o
LEFT JOIN core.call_outcome_llm l ON l.call_id = o.call_id;
