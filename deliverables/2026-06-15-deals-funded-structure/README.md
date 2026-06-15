# Deals-Funded Structure — Gap 1 [2026-06-15]

Builds the warehouse + portal **structure** for the strategic bottom-line KPI **in
anticipation** of data we do not yet collect. We have NOT started tracking funded deals;
the structure ships **EMPTY** and the portal tile is **HELD/HIDDEN** until real
~100%-complete data exists (100%-or-WIPE rule, `feedback_partial_data_100pct_or_wipe_20260614`).

> **Strategic KPI (load-bearing):** `deals_funded × commission ÷ all-in cost`
> (`feedback_bottom_line_kpi_only`). The funded deal is the terminal funnel stage:
> emails → replies → opportunities → meetings booked → **deals funded** → revenue.

**Status:** DESIGN ONLY. DDL written + validated against a throwaway scratch DuckDB; **NOT
applied to the live warehouse** (another agent holds the single writer). Apply in a clean
post-cutover idle writer window under the write lock. Files committed to the
renaissance-warehouse repo locally (not pushed).

---

## 1. DDL — `sql/ddl/73_deals_funded.sql`

Number **73** = next free (live max is `72_sending_account_vendor`). A parallel "email-type"
workstream took 72; the DDL header flags **confirm 73 is still free at apply time**. Applied
via `apply_ddl_file(conn, <file>, version=73)`; `core.schema_version` → 73. Idempotent
(`CREATE … IF NOT EXISTS` / `CREATE OR REPLACE`), version-gated → re-apply is a no-op.

### Table: `core.deal_funded` (one row per FUNDED deal — ships empty)

Mirrors `core.meeting` conventions so a funded deal attributes the SAME way a meeting does
(channel-aware, lead-email-joinable, advisor/IM/partner-normalized — DDL 20/64/70).

| Group | Columns |
|---|---|
| Identity | `deal_id` (PK, stable surrogate), `source` ('funding_form' expected), `source_event_id` |
| Funnel linkage | `meeting_id`→core.meeting, `lead_key`→core.lead, `lead_email`, `campaign_id`→core.campaign, `campaign_name_raw`, `workspace_id`, `channel` |
| People / partner | `cm`, `advisor`, `advisor_name`, `advisor_partner`, `inbox_manager`, `partner_key`→core.funding_partner, `partner_label` (normalized) |
| Money (KPI numerator) | `funded_date`, `amount_funded`, `commission_rate`, `commission_amount`, `currency`, `deal_status` ('funded'/'clawback'/'reversed') |
| Provenance | `notes`, `raw_text` |
| Ingest bookkeeping | `row_hash` (idempotency), `_first_run_id`, `_last_run_id`, `_first_seen_at`, `_last_seen_at`, `_loaded_at` |

7 indexes: funded_date, cm, partner_key, workspace_id, meeting_id, lead_email, campaign_id.

### Views (all EMPTY-safe — 0 rows cleanly over an empty table, never error)

| View | Purpose |
|---|---|
| `core.v_deal_funded_resolved` | Base grain. Resolves commission (explicit `commission_amount` wins, else `amount_funded × commission_rate`, else NULL — never fabricated), adds `funded_month`, and **excludes non-funded** statuses (clawback/reversed). |
| `core.v_deals_funded_by_cm` | deals + $ funded + commission **per CM**. |
| `core.v_deals_funded_by_partner` | per **funding partner** (normalized label + partner_key). |
| `core.v_deals_funded_by_workspace` | per **workspace**. |
| `core.v_deals_funded_by_month` | per **time window** (monthly; daily/weekly = re-group the base view). |
| `core.v_kpi_bottom_line` | **THE bottom-line KPI:** deals × commission ÷ all-in cost. |

---

## 2. KPI wiring — `core.v_kpi_bottom_line` (monthly grain)

- **Numerator** = `SUM(commission_resolved)` over funded deals in the month. Each deal
  already carries its own commission, so summing commission across funded deals **IS**
  `deals × commission`. `deals_funded` and `amount_funded` ride alongside for display.
- **Denominator** = monthly **all-in cost** — **STUBBED / TODO**. Sourced from
  `core.cost_ledger` (DDL 13, the authoritative cost fact). Cost rows are amortizable
  (`period_start`/`period_end`/`amortize_method`) and attributed (`attribution_dim`/`_id`);
  the true monthly all-in cost needs an **amortization-expansion + global-allocation
  roll-up** that is **not yet a warehouse fact**.
  - The view contains a `cost` CTE that is **deliberately empty** (`WHERE FALSE`) and
    `LEFT JOIN`ed in. While empty: `all_in_cost`, `roi_commission_per_dollar`, and
    `net_contribution` are **NULL** (a NULL denominator → NULL ratio; **never** a
    divide-by-zero, **never** a fake 0). **No cost number is fabricated.**
  - **To light it up (no change to this file):** create the monthly cost view (proposed
    `derived.v_all_in_cost_monthly` over `core.cost_ledger`) and repoint the `cost` CTE
    body to `SELECT cost_month AS funded_month, all_in_cost FROM derived.v_all_in_cost_monthly`.
    The LEFT JOIN then starts returning rows and the ratios populate automatically.
- **Commission schedule already has a home:** `core.funding_partner` (DDL 25) carries
  `commercial_model` / `rev_share_pct` / `ppa_flag` per partner (loaded from the gitignored
  `seed_data/funding_partner.csv`). When the funded-deal source does not give commission
  directly, the loader can resolve `commission_rate` from `funding_partner.rev_share_pct`
  via `partner_key` — so the commission side is largely pre-wired.

**Validated (scratch DuckDB, not live):** 3 funded deals + 1 clawback → resolved view = 3
rows (clawback excluded); commission resolution (rate×amount **and** explicit) both correct;
per-CM/per-partner rollups aggregate correctly; KPI numerator = 36,000; cost stub → NULL
ratios, no error. Empty-table → every view returns 0 rows. DDL re-apply = no-op.

---

## 3. Portal scaffold — HELD/HIDDEN (`scripts/portal_data.py`)

Added one new key `data["deals_funded"]` (function `_deals_funded_held`). **No live tile was
changed** — purely additive, table-guarded so it cannot error before DDL 73 is applied.

It emits `held: True` + `held_reason` + EMPTY payloads (`by_cm`/`by_partner`/`by_workspace`/
`by_month` = `[]`, `kpi_bottom_line` = `null`). The portal render gate keys on
`deals_funded.held` (same hold convention as the partial all-time advisor/IM cuts): **held →
draw nothing.** It stays `held=True` until **BOTH**: (a) `core.deal_funded` has real data,
**and** (b) the all-in-cost denominator is wired (`v_kpi_bottom_line.all_in_cost` non-NULL).
Validated across all three states (no DDL / empty / data-but-no-cost) → all `held=True`.

---

## 4. Ingestion plan (how funded-deal rows will arrive)

**Most likely path:** the **Funding Form** Google Sheet (`raw_sheets_funding_form_data`,
tab `Data`) is already the source of truth for `core.meeting` ≥ Jun-1 (DDL 64) — advisor =
`row_json[5]`, inbox_manager = `row_json[15]`. The natural source for funded deals is a
**funded/amount column on that same form** (or a new tab/sheet), so a funded deal links to
its meeting and lead by **email** exactly like a meeting. Likely flow, once a source exists:

1. Register the source tab/columns in `entities/sheets_mirror.py` (`SHEET_TABS`) — same as
   the funding-form mirror — OR add a dedicated raw mirror if it is a new sheet.
2. New `entities/deal_funded.py` (modeled on `entities/meeting.py`): project sheet rows →
   `core.deal_funded`, derive `deal_id` deterministically, normalize partner via the same
   `PARTNER_NORM` map, resolve advisor/advisor_name/advisor_partner/inbox_manager identically
   to `meeting.py`, link `meeting_id` by (lead_email + nearest prior booked meeting), resolve
   `commission_rate` from `core.funding_partner` when the sheet does not give commission.
   Idempotent upsert by `row_hash`.
3. This also populates the typed-but-EMPTY `meeting_result` slot already reserved on
   `derived.v_funnel` / `derived.v_funnel_detail` (DDL 65) — wire it from `deal_funded`
   (funded/declined) **only** once data is real; never infer it from `partner_disposition`.

**Still unknown:** whether funded deals live on the existing form, a new tab, or a partner
system (Close/partner portal); the exact column for amount + commission; how "declined"/
"clawback" is recorded.

---

## 5. EMPTY-until-real status (explicit)

- `core.deal_funded` ships with **0 rows**. No placeholder/fake/partial rows, ever.
- Portal `deals_funded` tile is **HELD/HIDDEN** (`held=True`) and shows nothing.
- KPI cost denominator is a clearly-marked **stub**; ratios are NULL until a cost fact exists.
- Flip the hold to visible **only** when funded-deal data is real & ~100% complete **and**
  the all-in-cost denominator is wired.

---

## 6. Inputs still needed from Sam (to begin populating)

1. **Where funded-deal data will come from** — a column/tab on the existing Funding Form, a
   new sheet, or a partner system (Close / partner portal)? This decides the ingest path.
2. **The exact fields** that source carries: amount funded, commission (amount or rate),
   funded date, deal/decline status, and the lead/email or meeting key to link on.
3. **Commission schedule per partner** — confirm/seed `rev_share_pct` (+ `commercial_model`,
   `ppa_flag`) in `seed_data/funding_partner.csv` for every partner, so commission resolves
   when the source gives only an amount funded.
4. **All-in-cost source for the KPI denominator** — confirm `core.cost_ledger` is the
   intended cost fact and sign off on building `derived.v_all_in_cost_monthly` (amortized +
   globally-allocated monthly roll-up). Without this the bottom-line ratio stays NULL.
5. **Definition of "funded"** — does the KPI count gross funded, or net of clawbacks/
   reversals? (`deal_status` supports both; the base view counts `funded` only today.)

---

## Apply checklist (deferred — clean writer window only)

- [ ] Confirm **73** is still the next-free DDL number (parallel email-type took 72).
- [ ] Apply `sql/ddl/73_deals_funded.sql` via `apply_ddl_file(conn, …, version=73)` under the
      write lock, outside 03:30–05:45 UTC. Verify `schema_version` = 73 and all views return
      0 rows cleanly.
- [ ] Deploy the `portal_data.py` change with the next nightly portal feed — confirm
      `deals_funded.held == True` in the emitted `window.PORTAL_DATA`.
- [ ] Portal-side: add the render gate `if (PORTAL_DATA.deals_funded?.held) renderNothing()`
      (do **not** draw the tile while held).
