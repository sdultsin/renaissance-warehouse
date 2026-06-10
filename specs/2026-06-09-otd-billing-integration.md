# Spec — OTD Billing Integration into the Warehouse [2026-06-09]

**Owner:** Sam · **Status:** approved, executing · **Repo:** `sdultsin/renaissance-warehouse`

## Motivation

OTD (provider_code 1) is our largest sending-infra vendor. The warehouse `core.cost_ledger`
carried OTD at a single flat **$1.38/inbox/mo** `reference_rate` row whose own note said
*"NEEDS ACTUAL CURRENT INVOICE"* (it was a target-scale projection: $548k/mo ÷ 396,800 inboxes).
Memory's economics note said $1.09. The **actual OTD account statement** (Google Sheet
`1lRbIk1TvQyBkU9W-aTmI9iKIvY4IcnfWqOPxz4pVTm4`) shows the real billed rate at current scale is
**$0.76/inbox/mo** — a volume-tiered curve from $1.50 (2k mailboxes) down to $0.76 (187,600
mailboxes), with a June promo of $0.67 on the newest 50k batch.

So our modeled OTD cost was **~45–80% too high**. Every OTD unit-economic downstream of
`cost_ledger` (cost-per-send, cost-per-reply, cost-per-meeting) was overstating cost. This spec
corrects the rate and integrates the full statement (both tabs) into the warehouse so the data is
auditable, queryable, and refreshable as the fleet scales.

## Source data (two tabs)

**Account Summary** — pricing tier history, lifetime charges, payments received ($238,447.77),
referral credits ($1,940), setup deposit (B97–B101, $15,656), current balance due.

**Charges by Batch** — per-batch (B53, B58, … B101, "new 50k") monthly charge grid: each batch =
N mailboxes with a "Billing From" date and month-by-month pro-rata charges; lifetime total
$397,313.77 across ~30 batches.

Batch IDs are **OTD's own identifiers**, deliberately preserved as-sent. They are NOT mapped to
anything on our side (no Instantly tag / account-group link) — a batch links only to its mailbox
count. That dead-end is expected and acceptable (per Sam).

## Design — 3 layers

### 1. Raw capture (faithful, as-communicated)
Mirror both tabs verbatim via the existing **sheets_mirror** mechanism (row-as-`row_json`):
- `raw_sheets_otd_account_summary`
- `raw_sheets_otd_charges_by_batch`

Added to `sources/sheets.OTD_SHEET_ID` + `SHEET_TABS`. Producer (`scripts/stage_otd_billing.py`,
run on the Mac which has the google-sheets token) pulls the tabs, encodes the row_json CSVs, and
scps them to the droplet staging dir. The droplet `sheets` phase consumes them. This logs exactly
what OTD sent, and is the monthly-refresh entry point.

### 2. Typed core tables (query-ready)
`entities/otd_billing.py` parses the raw row_json (anchored on section-header text markers, so it
survives row additions) into typed tables (DDL `sql/ddl/53_otd_billing.sql`):
- `core.otd_rate_tier`  — (period_label, period_start, mailboxes, rate_usd, monthly_usd)
- `core.otd_batch`      — (batch_id, mailboxes, billing_from, setup_deposit_usd, lifetime_total_usd)
- `core.otd_charge`     — long form of the monthly grid: (batch_id, period_label, amount_usd)
- `core.otd_payment`    — (seq, paid_on, description, invoice, method, amount_usd)
- `core.otd_credit`     — (seq, credited_on, description, period, amount_usd)

### 3. cost_ledger correction (the headline fix)
The same entity rewrites the OTD rows in `core.cost_ledger` **from** `core.otd_rate_tier`:
delete the stale flat `reference:otd:inbox_monthly` row, insert **one row per pricing tier**
(`source='otd_statement'`, `sku='inbox_monthly'`, `cost_unit='inbox'`, `attribution_dim='channel'`,
`attribution_id='otd'`, `period_start`/`period_end` per tier, `total_usd`=rate). Result: date-aware
OTD cost — a send in Feb is costed at $0.85, today at $0.76. Promo tier carries a note.

## Refresh model
- **Structure/parse (nightly):** `otd_billing` phase runs after `sheets` in `PHASE_ORDER` — re-parses
  whatever raw snapshot is staged and re-syncs the typed tables + cost_ledger. Idempotent.
- **Source refresh (monthly, manual):** run `scripts/stage_otd_billing.py` on the Mac after OTD sends
  the new statement → re-stages CSVs → next nightly picks them up. A stale snapshot never breaks the
  build (missing CSV = skip, last-known-good preserved — existing sheets_mirror behavior).

## Out of scope
- Linking batches to our sending accounts (no mapping exists; confirmed dead-end).
- Modeling OTD's internal backend/server topology.

## Verification (DoD)
1. `core.otd_rate_tier` has 7 tiers; current (latest period_start ≤ today) rate = **0.76**.
2. `core.cost_ledger` OTD rows = 7 tiered rows, source `otd_statement`; no flat $1.38 row remains.
3. `core.otd_batch` lifetime totals sum to **$397,313.77** (± rounding); ~30 batches.
4. `core.otd_payment` sums to **$238,447.77**; `core.otd_credit` to **$1,940.00**.
5. Phase is idempotent (re-run yields identical row counts).
