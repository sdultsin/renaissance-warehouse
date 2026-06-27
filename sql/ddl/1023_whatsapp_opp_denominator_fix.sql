-- @gate: add
-- Depends on 82
-- 1023_whatsapp_opp_denominator_fix.sql  [2026-06-27]  W1f RevOps deep-dive: fix the WhatsApp opportunity denominator.
-- Applied at schema version 1023 by scripts/setup_db.py / the warehouse DDL applier.
-- Idempotent (CREATE OR REPLACE VIEW) — re-applying is a no-op. Standard SQL. NON-destructive
-- (re-points two SELECT expressions in one view; no DROP, no data change, no column add/remove/rename;
-- the view's column list, order, names, and types are byte-identical to DDL 82).
--
-- WHY (W1f audit, 2026-06-26, verified against literal Iskra message text):
-- v_whatsapp_performance.opportunities was sourced straight from raw_iskra_stats.opportunities,
-- which is Iskra's OWN "opportunities" field ≈ ANY INBOUND (~86-92% of sends) — NOT a Renaissance
-- positive-intent opportunity (DDL 82 even flags this in the column comment). The result was a
-- denominator ~420x too high: 56,230 reported vs ~134 real positive-intent conversations at the
-- latest window (2026-06-26). It was even LARGER than the reply count (5,990) — impossible for a
-- real opportunity count, which must be a subset of replies. Every downstream WhatsApp positive-rate /
-- opp->booked / opp->funded built on it was wrong by 2+ orders of magnitude.
--
-- THE FIX: re-point `opportunities` to the per-conversation positive-intent tag the WhatsApp->Close
-- push already gates on — raw_iskra_meetings.reply_sentiment = 'positive' — windowed to the same
-- [window_from, window_to] the stats row covers (date-inclusive, so same-day tags are not lost).
-- This is the conversation-grain truth surfaced in v_whatsapp_conversation_performance.is_positive_reply.
--
-- meetings_booked IS ALSO re-pointed (raw_iskra_meetings.meeting_status = 'booked') for the SAME reason
-- and to keep the view INTERNALLY CONSISTENT: had only `opportunities` been fixed, the view would have
-- shipped opportunities(134) < meetings_booked(187, the old stats artifact) — a NEW impossible shape
-- (a booked meeting IS an opportunity). Sourcing both from the same conversation-tag table guarantees
-- opportunities >= meetings_booked >= deals_won at every window (e.g. 134 >= 40 >= 0).
-- NOTE for W1e (WhatsApp pipe lane): the old raw_iskra_stats.meetings_booked (187) and the
-- conversation-tag count (40) disagree — raw_iskra_meetings may under-capture bookings that the Iskra
-- stats summary counts. W1e should confirm which is authoritative; this DDL chooses the conversation-tag
-- source because it is the same demonstrably-correct surface as the opportunity fix and is internally
-- consistent. deals_won is left from raw_iskra_stats (currently 0; 0 <= meetings_booked, consistent).
--
-- Reversible: revert by re-applying DDL 82's definition (or a follow-up CREATE OR REPLACE VIEW).

CREATE OR REPLACE VIEW v_whatsapp_performance AS
WITH latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY channel, window_from, window_to ORDER BY captured_at DESC) AS rn
  FROM raw_iskra_stats
)
SELECT
  l.channel, l.window_from, l.window_to,
  l.messages_sent, l.messages_delivered, l.delivery_rate,
  l.replies, l.reply_rate,
  -- W1f fix: Renaissance positive-intent opportunity (NOT Iskra's any-inbound stats.opportunities).
  (SELECT COUNT(*) FROM raw_iskra_meetings m
     WHERE m.reply_sentiment = 'positive'
       AND CAST(m.tagged_at AS DATE) BETWEEN l.window_from AND l.window_to) AS opportunities,
  -- W1f fix: booked from the same conversation-tag truth (keeps opportunities >= meetings_booked).
  (SELECT COUNT(*) FROM raw_iskra_meetings m
     WHERE m.meeting_status = 'booked'
       AND CAST(m.tagged_at AS DATE) BETWEEN l.window_from AND l.window_to) AS meetings_booked,
  l.deals_won, l.captured_at
FROM latest l
WHERE l.rn = 1;
