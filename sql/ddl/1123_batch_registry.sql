-- core.batch_registry — the per-batch roster David edits in the Data Hub (Fleet > Batches), mirrored to
-- the warehouse nightly so it is queryable alongside the rest of the fleet data. The LIVE editable copy is
-- a JSON file on the Hub's persistent volume; entities/batch_registry.py full-replaces this table from the
-- Hub's token-gated /api/batches_export. Retires the old "Batches" Google Sheet.
-- [2026-07-17, David: "I write this in the Hub, and we have all the information in the warehouse ... we
--  don't need the sheet anymore at all"]. All-VARCHAR by design: operator-entered free text + mixed types.
CREATE TABLE IF NOT EXISTS core.batch_registry (
    batch_key      VARCHAR,
    provider       VARCHAR,
    workspace      VARCHAR,
    n_domains      VARCHAR,
    n_inboxes      VARCHAR,
    sip_date       VARCHAR,
    warmup_start   VARCHAR,
    cold_start     VARCHAR,
    billing_date   VARCHAR,
    offer          VARCHAR,
    email_provider VARCHAR,
    batch_url      VARCHAR,
    notes          VARCHAR,
    updated_at     VARCHAR,
    updated_by     VARCHAR,
    _loaded_at     TIMESTAMP,
    _run_id        VARCHAR
);
