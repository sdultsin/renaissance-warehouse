"""OTD billing — parse the raw_sheets_otd_* mirror into typed core tables and
rewrite the OTD rows in core.cost_ledger from the actual rate tiers.

Source: OTD account statement (Google Sheet 1lRbIk1TvQyBkU9W-aTmI9iKIvY4IcnfWqOPxz4pVTm4),
tabs "Account Summary" + "Charges by Batch", mirrored to raw_sheets_otd_* by
entities/sheets_mirror.py (the existing CSV-staged sheets pattern).

Runs in the 'otd_billing' phase (after 'sheets' in core.config.PHASE_ORDER). Reads the
LATEST staged snapshot (max _loaded_at) so it is robust to a 'sheets' run that skipped
(missing CSV → last-known-good rows retained).

Parsing is anchored on section-header TEXT markers (PRICING / PAYMENTS RECEIVED /
CREDITS APPLIED / CHARGES BY BATCH) rather than fixed row indices, so it survives row
additions when OTD updates the statement. Anything it cannot parse is skipped, never fatal —
the raw mirror remains the faithful record.

The headline correction: cost_ledger previously carried a single flat OTD reference_rate
of $1.38/inbox/mo ("NEEDS ACTUAL CURRENT INVOICE"). The actual billed rate at current scale
is $0.76/inbox/mo (volume-tiered from $1.50). This entity replaces that flat row with one
cost_ledger row per pricing tier, source='otd_statement'.
"""
from __future__ import annotations

import json
import re
from datetime import date

from core.registry import RunContext
from core.sync_run import PhaseResult

SHEET_URL = "https://docs.google.com/spreadsheets/d/1lRbIk1TvQyBkU9W-aTmI9iKIvY4IcnfWqOPxz4pVTm4"
FAR_FUTURE = date(2027, 12, 31)

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ------------------------------------------------------------------ parse helpers
def _money(s) -> float | None:
    """'$3,000.00' -> 3000.0 ; '—'/''/None -> None ; '$0.00' -> 0.0"""
    if s is None:
        return None
    t = str(s).strip().replace("$", "").replace(",", "")
    if t in ("", "—", "-", "–"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _int(s) -> int | None:
    if s is None:
        return None
    t = str(s).strip().replace(",", "")
    if t in ("", "—", "-", "–"):
        return None
    try:
        return int(float(t))
    except ValueError:
        return None


def _date_from_label(s) -> date | None:
    """Tolerant date parse for the statement's period/date labels.

    Handles: 'Oct 23, 2025' / 'From Apr 15, 2026' / 'Feb 2026' / 'Oct – Jan 2026'.
    Returns None for 'Pre-pay' and anything unparseable.
    """
    if not s:
        return None
    t = str(s).strip()
    low = t.lower()

    # 'Mon DD, YYYY' (optionally prefixed 'From ')
    m = re.search(r"([a-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", low)
    if m and m.group(1)[:3] in _MONTHS:
        return date(int(m.group(3)), _MONTHS[m.group(1)[:3]], int(m.group(2)))

    # range 'Mon – Mon YYYY' (e.g. 'Oct – Jan 2026') → first month; year-1 if it wraps
    m = re.search(r"([a-z]{3,9})\s*[–-]\s*([a-z]{3,9})\s+(\d{4})", low)
    if m and m.group(1)[:3] in _MONTHS and m.group(2)[:3] in _MONTHS:
        m1, m2, yr = _MONTHS[m.group(1)[:3]], _MONTHS[m.group(2)[:3]], int(m.group(3))
        return date(yr - 1 if m1 > m2 else yr, m1, 1)

    # 'Mon YYYY' → first of month
    m = re.search(r"([a-z]{3,9})\s+(\d{4})", low)
    if m and m.group(1)[:3] in _MONTHS:
        return date(int(m.group(2)), _MONTHS[m.group(1)[:3]], 1)

    return None


def _cells(row_json) -> list[str]:
    try:
        v = json.loads(row_json) if row_json else []
        return [("" if c is None else str(c)) for c in v]
    except (json.JSONDecodeError, TypeError):
        return []


def _get(cells: list[str], i: int) -> str:
    return cells[i] if 0 <= i < len(cells) else ""


def _latest_rows(db, table: str) -> list[list[str]]:
    """Return parsed cell-lists for the most recently staged snapshot, ordered by row_index."""
    run = db.execute(
        f"SELECT _run_id FROM {table} ORDER BY _loaded_at DESC LIMIT 1"
    ).fetchone()
    if not run:
        return []
    rid = run[0]
    rows = db.execute(
        f"SELECT row_index, row_json FROM {table} "
        f"WHERE _run_id IS NOT DISTINCT FROM ? ORDER BY row_index",
        [rid],
    ).fetchall()
    return [_cells(rj) for (_idx, rj) in rows]


def _has(cells: list[str], marker: str) -> bool:
    m = marker.lower()
    return any(m == c.strip().lower() for c in cells)


# ------------------------------------------------------------------ section parsers
def _parse_rate_tiers(summary_rows: list[list[str]]) -> list[dict]:
    """PRICING section of Account Summary → rate tiers.
    Layout: col2=Period, col4=Mailboxes, col5=Rate, col6=Monthly. Header row has col2=='Period'.
    Tier rows have empty col1; the section ends when col1 becomes non-empty (next block)."""
    tiers: list[dict] = []
    in_section = False
    seen_header = False
    for cells in summary_rows:
        if _has(cells, "PRICING"):
            in_section, seen_header = True, False
            continue
        if not in_section:
            continue
        if _get(cells, 2).strip().lower() == "period":
            seen_header = True
            continue
        if not seen_header:
            continue
        # end of PRICING block: a row that starts a new labeled section in col1
        if _get(cells, 1).strip():
            break
        period = _get(cells, 2).strip()
        rate = _money(_get(cells, 5))
        if not period or rate is None:
            continue
        tiers.append({
            "period_label": period,
            "period_start": _date_from_label(period),
            "mailboxes": _int(_get(cells, 4)),
            "rate_usd": rate,
            "monthly_usd": _money(_get(cells, 6)),
            "is_promo": "promo" in period.lower(),
        })
    return tiers


def _parse_payments(summary_rows: list[list[str]]) -> list[dict]:
    out: list[dict] = []
    in_section = False
    for cells in summary_rows:
        if _has(cells, "PAYMENTS RECEIVED"):
            in_section = True
            continue
        if not in_section:
            continue
        c2 = _get(cells, 2).strip().lower()
        if c2 == "total payments" or _has(cells, "CREDITS APPLIED"):
            break
        seq = _int(_get(cells, 1))
        if seq is None:
            continue
        out.append({
            "seq": seq,
            "paid_on_label": _get(cells, 2).strip(),
            "paid_on": _date_from_label(_get(cells, 2)),
            "description": _get(cells, 3).strip(),
            "invoice": _get(cells, 4).strip(),
            "method": _get(cells, 5).strip(),
            "amount_usd": _money(_get(cells, 6)),
        })
    return out


def _parse_credits(summary_rows: list[list[str]]) -> list[dict]:
    out: list[dict] = []
    in_section = False
    for cells in summary_rows:
        if _has(cells, "CREDITS APPLIED"):
            in_section = True
            continue
        if not in_section:
            continue
        c2 = _get(cells, 2).strip().lower()
        if c2 == "total credits" or _has(cells, "BALANCE"):
            break
        seq = _int(_get(cells, 1))
        if seq is None:
            continue
        out.append({
            "seq": seq,
            "credited_on_label": _get(cells, 2).strip(),
            "credited_on": _date_from_label(_get(cells, 2)),
            "description": _get(cells, 3).strip(),
            "period": _get(cells, 5).strip(),
            "amount_usd": _money(_get(cells, 6)),
        })
    return out


def _parse_batches(batch_rows: list[list[str]]):
    """CHARGES BY BATCH grid → (batches, charges).
    Header row has col1=='Batch'; capture period columns from it. Data rows until col1=='Total'."""
    headers: list[str] = []
    hdr_idx = -1
    for cells in batch_rows:
        if _get(cells, 1).strip().lower() == "batch":
            headers = [c.strip() for c in cells]
            hdr_idx = batch_rows.index(cells)
            break
    if hdr_idx < 0:
        return [], []

    # Columns that are NOT charge amounts.
    non_charge = {"", "batch", "mboxes", "billing from", "days", "total"}
    # index of the special columns
    def col(name):
        for i, h in enumerate(headers):
            if h.strip().lower() == name:
                return i
        return -1
    i_total = col("total")
    i_setup = col("setup dep.")

    batches: list[dict] = []
    charges: list[dict] = []
    for cells in batch_rows[hdr_idx + 1:]:
        bid = _get(cells, 1).strip()
        if not bid:
            continue
        if bid.lower() == "total":
            break
        billing_label = _get(cells, 3).strip()
        batches.append({
            "batch_id": bid,
            "mailboxes": _int(_get(cells, 2)),
            "billing_from": _date_from_label(billing_label),
            "billing_from_label": billing_label,
            "setup_deposit_usd": _money(_get(cells, i_setup)) if i_setup >= 0 else None,
            "lifetime_total_usd": _money(_get(cells, i_total)) if i_total >= 0 else None,
        })
        for i, h in enumerate(headers):
            if h.strip().lower() in non_charge:
                continue
            amt = _money(_get(cells, i))
            if amt is None:
                continue
            charges.append({"batch_id": bid, "period_label": h.strip(), "amount_usd": amt})
    return batches, charges


# ------------------------------------------------------------------ cost_ledger rewrite
def _rewrite_cost_ledger(db, tiers: list[dict], run_id: str) -> int:
    """Replace OTD rows in core.cost_ledger with one row per pricing tier.

    Whole-fleet tiers (label has no 'new 50k') chain period_end = next whole-fleet start - 1 day,
    last open to FAR_FUTURE. Incremental 50k tiers: promo → end of its month; base → open."""
    db.execute("DELETE FROM core.cost_ledger WHERE vendor = 'otd' AND sku = 'inbox_monthly'")

    def is_incremental(t):
        return "50k" in t["period_label"].lower()

    whole = sorted(
        [t for t in tiers if not is_incremental(t) and t["period_start"]],
        key=lambda t: t["period_start"],
    )
    n = 0
    for t in tiers:
        ps = t["period_start"]
        if ps is None:
            continue
        if is_incremental(t):
            if t["is_promo"]:
                # end of the promo month
                if ps.month == 12:
                    pe = date(ps.year, 12, 31)
                else:
                    pe = date(ps.year, ps.month + 1, 1).toordinal() - 1
                    pe = date.fromordinal(pe)
                note = "Incremental new-50k batch, promo month 1"
            else:
                pe = FAR_FUTURE
                note = "Incremental new-50k batch, base rate"
        else:
            later = [w for w in whole if w["period_start"] > ps]
            pe = date.fromordinal(later[0]["period_start"].toordinal() - 1) if later else FAR_FUTURE
            note = "Whole-fleet billed rate (volume tier)"
        if t["is_promo"]:
            note += " — PROMO rate"
        cost_id = f"otd_statement:inbox_monthly:{ps.isoformat()}"
        if is_incremental(t):
            cost_id += ":new50k" + ("_promo" if t["is_promo"] else "_base")
        db.execute(
            """
            INSERT INTO core.cost_ledger
              (cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start, period_end,
               amortize_method, attribution_dim, attribution_id, source, source_ref, notes,
               _loaded_at, _run_id)
            VALUES (?, 'otd', 'inbox_monthly', 'inbox', ?, ?, ?, ?, 'monthly', 'channel', 'otd',
                    'otd_statement', ?, ?, now(), ?)
            ON CONFLICT (cost_id) DO UPDATE SET
              unit_count = excluded.unit_count, total_usd = excluded.total_usd,
              period_start = excluded.period_start, period_end = excluded.period_end,
              notes = excluded.notes, _loaded_at = excluded._loaded_at, _run_id = excluded._run_id
            """,
            [cost_id, t["mailboxes"], t["rate_usd"], ps, pe, SHEET_URL,
             f"{t['period_label']} — {note}", run_id],
        )
        n += 1
    return n


# ------------------------------------------------------------------ phase entry
def _reload(db, table: str, rows: list[dict], cols: list[str], run_id: str) -> int:
    """Idempotent full reload of a typed core.otd_* table for this run."""
    db.execute(f"DELETE FROM core.{table}")
    if not rows:
        return 0
    collist = ", ".join(cols) + ", _loaded_at, _run_id"
    valph = ", ".join(["?"] * len(cols)) + ", now(), ?"
    for r in rows:
        db.execute(
            f"INSERT INTO core.{table} ({collist}) VALUES ({valph})",
            [r.get(c) for c in cols] + [run_id],
        )
    return len(rows)


def run_otd_billing(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    summary = _latest_rows(db, "raw_sheets_otd_account_summary")
    batch = _latest_rows(db, "raw_sheets_otd_charges_by_batch")
    if not summary and not batch:
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no raw_sheets_otd_* snapshot"})

    tiers = _parse_rate_tiers(summary)
    payments = _parse_payments(summary)
    credits = _parse_credits(summary)
    batches, charges = _parse_batches(batch)

    n_tier = _reload(db, "otd_rate_tier", tiers,
                     ["period_label", "period_start", "mailboxes", "rate_usd", "monthly_usd",
                      "is_promo", "notes"], ctx.run_id)
    n_batch = _reload(db, "otd_batch", batches,
                      ["batch_id", "mailboxes", "billing_from", "billing_from_label",
                       "setup_deposit_usd", "lifetime_total_usd"], ctx.run_id)
    n_charge = _reload(db, "otd_charge", charges,
                       ["batch_id", "period_label", "amount_usd"], ctx.run_id)
    n_pay = _reload(db, "otd_payment", payments,
                    ["seq", "paid_on", "paid_on_label", "description", "invoice", "method",
                     "amount_usd"], ctx.run_id)
    n_cred = _reload(db, "otd_credit", credits,
                     ["seq", "credited_on", "credited_on_label", "description", "period",
                      "amount_usd"], ctx.run_id)
    n_ledger = _rewrite_cost_ledger(db, tiers, ctx.run_id)

    total = n_tier + n_batch + n_charge + n_pay + n_cred
    return PhaseResult(
        rows_in=len(summary) + len(batch),
        rows_out=total,
        notes={
            "rate_tiers": n_tier, "batches": n_batch, "charges": n_charge,
            "payments": n_pay, "credits": n_cred, "cost_ledger_otd_rows": n_ledger,
        },
    )


def register(registry) -> None:
    registry.add_phase("otd_billing", "otd_billing", run_otd_billing)
