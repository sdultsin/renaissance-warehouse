-- core.inbox_date_history — APPEND-ONLY record of every change to an inbox's creation date and
-- warm-up start. [2026-07-21, David]
--
-- WHY THIS EXISTS. Suppliers replace a broken mailbox by DELETING and RE-ADDING it, which resets both
-- timestamps in Instantly. Verified over the last 30 days: 47,714 warm-up-start changes and 60,388
-- creation-date changes, and every warm-up-start change came with a creation-date change — i.e. every
-- one is a re-add, not an edit. Instantly is CORRECT about the mailbox that exists now; what is lost is
-- what the date used to be. 35,985 live inboxes already carry a warm-up start LATER than their own
-- first cold send, which is only explicable by a re-add.
--
-- SOURCE. core.account_census is the only thing that can recover the old values: a daily per-inbox
-- snapshot carrying both timestamps, from 2026-06-21, never pruned. Rows here are produced by comparing
-- consecutive census days per email (LAG over census_date).
--   * core.sending_account_state_event CANNOT do this — 0 of those 35,985 inboxes have an event
--     predating their current creation date. Do not use it for history.
--
-- APPEND-ONLY, on purpose. Never UPDATE, never DELETE. The whole point is that the old date stays
-- knowable forever; a row that can be rewritten is not history. Idempotency is ENFORCED BY THE ENGINE:
-- the unique index below makes (email, field, detected_on) unrepeatable, so a re-run — or two runs
-- racing — cannot produce a duplicate 'history' row. The loader's anti-join is a cheap pre-filter on
-- top of that, not the guarantee. (Moderator caught the earlier version claiming a uniqueness that
-- nothing actually enforced; this is that claim made true.)
--
-- HONEST LIMIT. Every row IN THIS TABLE is exact: it is a value observed on one census day next to the
-- value observed the day before. What does not exist is history BEFORE the census start (2026-06-21).
-- For inboxes recreated before that there is no row here at all, and the only surviving evidence is the
-- first cold send — a LOWER BOUND on the original warm-up start, not the true value. That inference
-- belongs to whatever consumer chooses to make it and must never be written into this table, which is
-- why there is no provenance column: everything here is observed, nothing here is inferred.
-- FOR CONSUMERS: a row is an OBSERVED TRANSITION, not a guaranteed re-add. A correction, a revert or
-- an out-of-order census day all emit a row too. Every row is individually exact; reconstructing
-- "this inbox was re-added N times" is the consumer's inference to make, and any filtering for that
-- belongs on the consuming side, never in this append-only table.
CREATE TABLE IF NOT EXISTS core.inbox_date_history (
    email          VARCHAR,      -- the inbox the change happened to
    workspace_slug VARCHAR,      -- workspace as of the day the change was detected
    field          VARCHAR,      -- 'created' | 'warmup_start'
    old_value      TIMESTAMP,    -- value on the previous census day
    new_value      TIMESTAMP,    -- value on detected_on
    detected_on    DATE,         -- census day the new value first appeared
    _loaded_at     TIMESTAMP,
    _run_id        VARCHAR
);

-- Idempotency, enforced rather than asserted: one row per (inbox, field, day it changed).
CREATE UNIQUE INDEX IF NOT EXISTS inbox_date_history_uq
    ON core.inbox_date_history (email, field, detected_on);
