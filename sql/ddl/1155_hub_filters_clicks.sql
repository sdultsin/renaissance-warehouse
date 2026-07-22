-- core.hub_saved_filter + core.hub_click_log — the last two Railway-only Hub datasets.
-- [2026-07-22 history brief §3b — priorities 4 and 5]
-- Saved filters: full-replace mirror of filters.json (payload kept verbatim as JSON text).
-- Click log: APPEND-ONLY mirror — the loader pulls ?since=<max ts we hold>, so the warehouse copy
-- only ever grows and a Hub volume loss cannot shrink it.
CREATE TABLE IF NOT EXISTS core.hub_saved_filter (
    name VARCHAR, payload VARCHAR, _loaded_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS core.hub_click_log (
    ts VARCHAR, email VARCHAR, tab VARCHAR, label VARCHAR, kind VARCHAR,
    fx DOUBLE, fy DOUBLE, vw BIGINT, vh BIGINT, _loaded_at TIMESTAMPTZ
);
