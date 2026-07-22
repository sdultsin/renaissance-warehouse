-- core.hub_domain_flag — durable mirror of the Inbox Hub's website/special domain marks.
-- [2026-07-22 history brief §3b — priority 3] Typed by a human (David / Darcy), not re-derivable:
-- these marks are what keeps our own websites out of the reuse pool and cancellation lists.
-- Same Hub-export mechanism as core.batch_registry; FULL-REPLACE (a mark an editor REMOVES must
-- disappear here too) with an empty-export guard in the loader: an export of 0 rows while this
-- table holds >0 is treated as a Hub-side failure and skipped LOUDLY, never mirrored as a wipe.
CREATE TABLE IF NOT EXISTS core.hub_domain_flag (
    domain      VARCHAR,
    kind        VARCHAR,      -- 'website' | 'special'
    _loaded_at  TIMESTAMPTZ
);
