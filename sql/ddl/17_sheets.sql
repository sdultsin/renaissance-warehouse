-- Google Sheets snapshot mirror (raw layer).
-- Append-only snapshots of trustworthy operational sheets: the Domain Tech
-- Sheet (domain/inbox inventory) and the Master Blacklist sheet.
--
-- REFERENCE DATA ONLY. These are messy human-maintained sheets. On any conflict
-- with Instantly, INSTANTLY WINS. Nothing downstream should treat raw_sheets_*
-- as canonical.
--
-- STORAGE-SHAPE DECISION (deliberate):
--   We do NOT split each tab into typed columns. These sheets are ragged
--   (variable column count per row, merged cells, dashboard formulas, sparse
--   grids). Instead each sheet ROW is stored as a single JSON array string in
--   row_json (e.g. '["rena-grow01.co","Dynadot","active",...]'). This is the
--   simplest robust option:
--     * tolerant of ragged rows / added columns / reordered columns over time
--     * never coerces types (everything is text as it appears in the cell)
--     * downstream can json_extract / unnest as needed, keyed off the header row
--   The header row is stored like any other row (it is just row_index 0 of each
--   tab), so consumers can recover column names without a separate schema.
--
-- One table per (sheet, tab). Naming: raw_sheets_<sheet>_<tab> with the tab
-- slugified (lowercased, non-alnum -> underscore). Schema stays the default
-- (raw_* live alongside the other raw mirrors, not under core.).

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- Domain Tech Sheet (spreadsheet 1bGj5bPyyGHg6eY6nRrkfXzTed44L0qHWhf8-4-gLlqM)
-- ---------------------------------------------------------------------------

-- 'MAIN' — dashboard / summary tab (sparse, merged cells, formula-driven).
CREATE TABLE IF NOT EXISTS raw_sheets_domain_tech_main (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,   -- 0-based row position within the tab
    row_json    VARCHAR,                -- JSON array of the row's cell values (all text)
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- 'Domains' — flat per-domain inventory (Domain/Registrar/Status/Workspace/...).
CREATE TABLE IF NOT EXISTS raw_sheets_domain_tech_domains (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- 'Domains(Table)' — the registrar-account x .co/.com used/not-used matrix.
-- THE most valuable tab: one row per (account, tld) with domainNN columns + used/available.
CREATE TABLE IF NOT EXISTS raw_sheets_domain_tech_domains_table (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- 'ADMIN - Renaissance' — requested in the spec but NOT PRESENT in the live tab
-- list as of 2026-05-30 (actual tabs: MAIN, Domains, Domains(Table), Old Domains,
-- Sheet15, Domains(GBC), Domains(WL), DMARC, Mailboxes Per Day, Reminders,
-- Inboxes(SBC), Inboxes(GBC), Inboxes(WL), Domains(BSC), Domains Reseller,
-- Settings, Daily Email Capacity, Examples). Table created for forward-compat;
-- the phase skips any tab that does not exist at run time, so this stays empty
-- until/unless the tab appears.
CREATE TABLE IF NOT EXISTS raw_sheets_domain_tech_admin_renaissance (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- ---------------------------------------------------------------------------
-- Blacklist sheet (spreadsheet 1fKqwQkEy4vRDYIrj7bq13aUZdTxBhjvKVRXbU1bXf6o,
-- title "Renaissance - Domains"). The spec's "Master Blacklist" tab does NOT
-- exist; the real tabs are Summary / All Domains / All Blocklisted Domains /
-- Unflagged Blocklisted Domains / All Outlook Domains (MailIn). We snapshot the
-- two operationally useful ones.
-- ---------------------------------------------------------------------------

-- 'All Domains' — full per-domain inventory: Domain, Infra, Provider Type,
-- Workspaces, Tags, Blocklisted (Yes/No), Blocklists, Cancel Status/Partner,
-- Outreachify Status, Flagged Retire, Sent, Replies, Reply Rate %, CSV columns.
CREATE TABLE IF NOT EXISTS raw_sheets_blacklist_all_domains (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- 'All Blocklisted Domains' — one row per blocklisted domain: Domain, Primary
-- Workspace, All Workspaces, Accounts, Blocklists, # Lists, Infra, Provider
-- Type, Tags, Status, Partner. This is the actual blacklist tracking tab.
CREATE TABLE IF NOT EXISTS raw_sheets_blacklist_blocklisted (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);
