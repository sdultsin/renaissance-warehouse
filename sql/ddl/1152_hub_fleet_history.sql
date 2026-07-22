-- core.hub_fleet_history — durable mirror of the Inbox Hub's provider / provstate / lifecycle
-- history. [2026-07-22 history brief §3b, David's decision — priority 1 of the migration list]
--
-- WHY THIS CANNOT BE RECOMPUTED. These metrics join the daily census to core.v_inbox_overview,
-- which is a CURRENT-state view: recomputing a past day labels every inbox with TODAY's provider,
-- and inboxes deleted since have vanished from the view entirely — so they silently drop out of
-- the recomputed numbers. The rows the Hub stored on the day ARE the truth, and their accuracy
-- advantage grows every day. (metric='state' is deliberately NOT mirrored: it is re-derivable
-- from core.account_census + core.account_first_cold_send, both permanent.)
--
-- STORAGE PATTERN — same Hub-export mechanism as core.batch_registry / core.inbox_warmup_override
-- (the Hub exposes a token-gated export; entities/hub_fleet_history.py pulls it nightly), with ONE
-- deliberate difference: this loader UPSERTS BY (metric, d) and NEVER deletes days absent from the
-- export. Durability is the whole point — if the Hub's Railway volume is ever lost, the Hub
-- restarts empty and a full-replace mirror would wipe the surviving copy at the next nightly.
-- Keeping absent days is what makes this table the restore source instead of a casualty.
CREATE TABLE IF NOT EXISTS core.hub_fleet_history (
    metric      VARCHAR,      -- 'provider' | 'provstate' | 'lifecycle'
    d           VARCHAR,      -- ISO date the Hub measured (its nightly photograph)
    ws          VARCHAR,      -- workspace slug ('' for fleet-wide rows)
    k           VARCHAR,      -- series key: provider name / provider|state / lifecycle stage
    inbox_n     BIGINT,
    dom_n       BIGINT,
    _loaded_at  TIMESTAMPTZ   -- when this row last arrived from the Hub
);
