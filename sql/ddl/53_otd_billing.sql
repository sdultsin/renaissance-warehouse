-- OTD billing integration (per specs/2026-06-09-otd-billing-integration.md).
-- Source: OTD account statement Google Sheet 1lRbIk1TvQyBkU9W-aTmI9iKIvY4IcnfWqOPxz4pVTm4
-- (tabs: "Account Summary", "Charges by Batch").
--
-- THREE layers:
--   1. raw_sheets_otd_*          — faithful row_json mirror (loaded by entities/sheets_mirror.py,
--                                  same shape/pattern as the other raw_sheets_* tables).
--   2. core.otd_*                — typed tables parsed from the raw mirror by entities/otd_billing.py.
--   3. core.cost_ledger          — entities/otd_billing.py rewrites the OTD rows from core.otd_rate_tier
--                                  (no new table here; cost_ledger ships in 13_cost.sql).
--
-- Batch IDs (B53 … B101, "new 50k") are OTD's OWN identifiers, preserved as-sent. They are
-- intentionally NOT linked to any Renaissance sending account / tag — a batch maps only to a
-- mailbox count. That dead-end is expected (per Sam 2026-06-09).

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- RAW LAYER — row_json snapshots of the two tabs (sheets_mirror consumes these)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_sheets_otd_account_summary (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_sheets_otd_charges_by_batch (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,
    row_json    VARCHAR,
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- ---------------------------------------------------------------------------
-- TYPED CORE LAYER
-- ---------------------------------------------------------------------------

-- Volume-tiered per-mailbox pricing over time. Drives the cost_ledger OTD rows.
CREATE TABLE IF NOT EXISTS core.otd_rate_tier (
    period_label   VARCHAR NOT NULL,   -- e.g. 'Oct – Jan 2026', 'From Apr 15, 2026'
    period_start   DATE,               -- parsed start of the tier (NULL if unparseable)
    mailboxes      INTEGER,            -- billed mailboxes at this tier
    rate_usd       DOUBLE,             -- $/mailbox/month
    monthly_usd    DOUBLE,             -- tier monthly total as stated
    is_promo       BOOLEAN DEFAULT FALSE,
    notes          VARCHAR,
    _loaded_at     TIMESTAMPTZ NOT NULL,
    _run_id        VARCHAR
);

-- One row per OTD batch. batch_id is OTD's label (dead-ends at mailbox count on our side).
CREATE TABLE IF NOT EXISTS core.otd_batch (
    batch_id            VARCHAR NOT NULL,   -- 'B53', 'B46 → B88', 'new 50k', '1st batch', ...
    mailboxes           INTEGER,
    billing_from        DATE,               -- "Billing From" date (NULL for 'Pre-pay' / unparseable)
    billing_from_label  VARCHAR,            -- raw value ('Pre-pay', 'Oct 23, 2025', ...)
    setup_deposit_usd   DOUBLE,             -- setup deposit (B97–B101); 0/NULL otherwise
    lifetime_total_usd  DOUBLE,             -- batch "Total" column
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

-- Long form of the per-batch monthly charge grid (one row per batch × billing period column).
CREATE TABLE IF NOT EXISTS core.otd_charge (
    batch_id      VARCHAR NOT NULL,
    period_label  VARCHAR NOT NULL,   -- grid column header: 'Oct','Nov',...,'From Apr 15','15 May','15 June','Setup Dep.'
    amount_usd    DOUBLE,
    _loaded_at    TIMESTAMPTZ NOT NULL,
    _run_id       VARCHAR
);

-- Payments received (treasury / AP reconciliation — kept separate from unit-cost).
CREATE TABLE IF NOT EXISTS core.otd_payment (
    seq          INTEGER,
    paid_on      DATE,
    paid_on_label VARCHAR,
    description  VARCHAR,
    invoice      VARCHAR,
    method       VARCHAR,
    amount_usd   DOUBLE,
    _loaded_at   TIMESTAMPTZ NOT NULL,
    _run_id      VARCHAR
);

-- Referral / promo credits applied.
CREATE TABLE IF NOT EXISTS core.otd_credit (
    seq           INTEGER,
    credited_on   DATE,
    credited_on_label VARCHAR,
    description   VARCHAR,
    period        VARCHAR,
    amount_usd    DOUBLE,
    _loaded_at    TIMESTAMPTZ NOT NULL,
    _run_id       VARCHAR
);
