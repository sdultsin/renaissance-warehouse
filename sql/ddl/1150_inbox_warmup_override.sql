-- core.inbox_warmup_override — the per-inbox warm-up start David sets by hand, when Instantly's own
-- date is wrong because the supplier deleted and re-added the mailbox. [2026-07-21, David]
--
-- THE DECISION THIS IMPLEMENTS. Instantly's warm-up start stays the DEFAULT and keeps auto-updating;
-- this table only ever holds the exceptions. An override is PER INBOX, never per batch — a batch is a
-- purchase, not a fact about a mailbox, and the mailboxes inside one are recreated individually. It is
-- applied in BULK to a selected set (one call sets 10,000), but what gets stored is still one row per
-- inbox. Where an override exists it WINS everywhere: reports, the Hub, and the Activation Pipeline's
-- flip gate. Empty by default — no override means nothing changes for that inbox.
--
-- STORAGE PATTERN — deliberately identical to core.batch_registry, not a new mechanism:
--   * the LIVE editable copy is a JSON file on the Hub's persistent volume,
--   * entities/inbox_warmup_override.py full-replaces this table from the Hub's token-gated export,
--   * so the Hub is the single writer and the warehouse is a nightly mirror of it.
-- Claude Code edits go through that same token-gated Hub endpoint. There is exactly one write path.
-- Reference: sql/ddl/1123_batch_registry.sql, entities/batch_registry.py.
--
-- WHY NOT DERIVE IT. core.inbox_date_history (DDL 1149) records what the date USED to be, which is
-- evidence, not a decision. Choosing which of several prior values is the true original start is a
-- human judgement — this table is where that judgement is recorded, with who made it and why.
--
-- warmup_start_override is VARCHAR (ISO date) to match the batch_registry pattern of storing
-- operator-entered values verbatim; core.v_inbox_overview does the cast when it coalesces.
-- EMAIL IS THE JOIN KEY, and it must match on both sides or an override silently fails to apply —
-- a correctness miss, not a crash, which is the worst kind. The loader stores lower(trim(email)).
-- Verified 2026-07-21: all 397,902 rows of core.v_inbox_overview are already canonically lowercased
-- (0 exceptions), so the two forms agree today. DDL 1151 must still join on lower(trim(email)) so
-- this stays true if an upstream source ever stops canonicalising.
CREATE TABLE IF NOT EXISTS core.inbox_warmup_override (
    email                  VARCHAR,   -- the inbox this applies to (one row per inbox)
    warmup_start_override  VARCHAR,   -- ISO date the operator asserts warm-up really began
    reason                 VARCHAR,   -- why — required, so a bare date is never unexplained
    set_by                 VARCHAR,   -- who set it (Hub sign-in identity)
    set_at                 VARCHAR,   -- when it was set, ET
    _loaded_at             TIMESTAMP,
    _run_id                VARCHAR
);

-- One override per inbox: the whole point is a single authoritative answer per mailbox, and a full
-- replace from the Hub must not be able to leave two rows disagreeing about the same email.
CREATE UNIQUE INDEX IF NOT EXISTS inbox_warmup_override_uq
    ON core.inbox_warmup_override (email);
