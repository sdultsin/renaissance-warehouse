-- core.hub_problem_ack(+_log,_member) — durable mirror of the Inbox Hub's "I have handled this"
-- acknowledgements. [2026-07-22 history brief §3b — priority 2] Typed by a human, not
-- re-derivable. Same Hub-export pattern as core.batch_registry (token-gated export, nightly
-- full-replace with the empty-payload guard in the loader — 0 rows vs non-empty is refused
-- loudly, never mirrored as a wipe).
CREATE TABLE IF NOT EXISTS core.hub_problem_ack (
    gkey VARCHAR, problem VARCHAR, workspace VARCHAR, acked BOOLEAN, note VARCHAR,
    acked_by VARCHAR, acked_at VARCHAR, first_flagged VARCHAR, last_seen VARCHAR,
    last_count BIGINT, _loaded_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS core.hub_problem_ack_log (
    ts VARCHAR, gkey VARCHAR, problem VARCHAR, workspace VARCHAR, cnt BIGINT,
    action VARCHAR, note VARCHAR, who VARCHAR, _loaded_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS core.hub_problem_ack_member (
    gkey VARCHAR, email VARCHAR, snap_at VARCHAR, _loaded_at TIMESTAMPTZ
);
