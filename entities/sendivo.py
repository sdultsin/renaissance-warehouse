"""Sendivo SMS send-side mirror + cost (spec 14).

Two registrations:
  - run_sendivo_mirror   (phase 'sendivo')    : delivery-metrics (per-day) + campaigns + brands + billing → raw_sendivo_*
  - run_sendivo_cost     (phase 'canonical')  : raw_sendivo_billing → core.cost_ledger (source='invoice_api', the first actuals)

Defaults (Sam, 2026-05-31): agency-aggregate delivery-metrics + per-sub-account billing,
SKIP /sms/logs (use delivery_rate), single SMS cost line per fee-type. Backfill depth via
SENDIVO_BACKFILL_DAYS (default 7 nightly; set high for a one-time history pull).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.sendivo import SendivoClient

logger = logging.getLogger("entities.sendivo")

BACKFILL_DAYS = int(os.environ.get("SENDIVO_BACKFILL_DAYS", "7"))


def _as_str(v):
    """Coerce a maybe-scalar/maybe-collection API field to a VARCHAR-safe value.
    Lists/dicts (e.g. tags, an array vetting_score) -> JSON; scalars -> str; None -> None."""
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    return str(v)


def register(registry: Registry) -> None:
    registry.add_phase("sendivo", "mirror", run_sendivo_mirror)
    registry.add_phase("canonical", "sendivo_cost", run_sendivo_cost)


def _month_windows(today: date, months_back: int = 1) -> list[tuple[str, str]]:
    """Current month + N prior months, as (first_day, last_day) ISO strings."""
    wins = []
    y, m = today.year, today.month
    for _ in range(months_back + 1):
        first = date(y, m, 1)
        last = (date(y + (m // 12), (m % 12) + 1, 1) - timedelta(days=1))
        wins.append((first.isoformat(), min(last, today).isoformat()))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return wins


def run_sendivo_mirror(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id
    api_key = ctx.credentials.require("SENDIVO_API_KEY")
    today = datetime.now(timezone.utc).date()

    rows_total = 0
    per: dict[str, int] = {}
    with SendivoClient(api_key) as cli:
        # --- delivery-metrics: per-day backfill (one row/day) ---
        conn.execute("DELETE FROM raw_sendivo_delivery_metrics WHERE _run_id = ?", [run_id])
        dm_rows = []
        for i in range(BACKFILL_DAYS, 0, -1):
            d = (today - timedelta(days=i)).isoformat()
            try:
                m = cli.delivery_metrics(d, d)
            except Exception as exc:  # noqa: BLE001 — skip days outside retention / transient
                logger.warning("delivery_metrics %s skipped: %s", d, exc)
                continue
            if not m:
                continue
            dm_rows.append(("agency", d, m.get("sms_sent"), m.get("segments_sent"),
                            m.get("inbound_sms_received"), m.get("delivery_rate"),
                            m.get("opt_out_rate"), m.get("response_rate"), run_id))
            time.sleep(0.15)
        if dm_rows:
            conn.executemany(
                "INSERT INTO raw_sendivo_delivery_metrics (scope, metric_date, sms_sent, segments_sent, "
                "inbound_sms_received, delivery_rate, opt_out_rate, response_rate, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, now(), ?)", dm_rows)
        per["delivery_metrics_days"] = len(dm_rows)
        rows_total += len(dm_rows)

        # --- brands (snapshot) — fetched BEFORE campaigns so we can recover a campaign's
        #     sub_account_id from its brand (the /campaigns payload dropped top-level
        #     sub_account_id around 2026-06; without this the column silently goes all-NULL). ---
        conn.execute("DELETE FROM raw_sendivo_brands WHERE _run_id = ?", [run_id])
        brands = cli.brands()
        brand_sub = {b.get("id"): b.get("sub_account_id") for b in brands if b.get("id")}
        if brands:
            conn.executemany(
                "INSERT INTO raw_sendivo_brands (brand_id, name, legal_company_name, verification_status, "
                "registration_state, campaigns_count, sub_account_id, created_at, dba_name, country, "
                "vertical_type, website, vetting_score, brand_identity_status, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                [(b.get("id"), b.get("name"), b.get("legal_company_name"), b.get("verification_status"),
                  b.get("registration_state"), b.get("campaigns_count"), b.get("sub_account_id"),
                  b.get("created_at"), b.get("dba_name"), b.get("country"), b.get("vertical_type"),
                  b.get("website"), _as_str(b.get("vetting_score")), b.get("brand_identity_status"),
                  run_id) for b in brands])
        per["brands"] = len(brands)
        rows_total += len(brands)

        # --- campaigns (snapshot) ---
        conn.execute("DELETE FROM raw_sendivo_campaigns WHERE _run_id = ?", [run_id])
        camps = cli.campaigns()
        if camps:
            conn.executemany(
                "INSERT INTO raw_sendivo_campaigns (campaign_id, name, status, brand_id, brand_name, "
                "phone_numbers, sub_account_id, created_at, description, tcr_status, campaign_type, "
                "use_case, is_default, registered_on, expiration_date, auto_renew, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                [(c.get("id"), c.get("name"), c.get("status"),
                  (c.get("brand") or {}).get("id"), (c.get("brand") or {}).get("name"),
                  json.dumps(c.get("phone_numbers")),
                  c.get("sub_account_id") or brand_sub.get((c.get("brand") or {}).get("id")),
                  c.get("created_at"), c.get("description"), c.get("tcr_status"), c.get("campaign_type"),
                  json.dumps(c.get("use_case")), c.get("is_default"), _as_str(c.get("registered_on")),
                  _as_str(c.get("expiration_date")), c.get("auto_renew"), run_id) for c in camps])
        per["campaigns"] = len(camps)
        rows_total += len(camps)

        # --- phone numbers (snapshot) — sending-asset inventory (audit G2). ---
        conn.execute("DELETE FROM raw_sendivo_phone_numbers WHERE _run_id = ?", [run_id])
        try:
            numbers = cli.phone_numbers()
        except Exception as exc:  # noqa: BLE001 — new endpoint; never let it fail the whole mirror
            logger.warning("phone_numbers skipped: %s", exc)
            numbers = []
        if numbers:
            conn.executemany(
                "INSERT INTO raw_sendivo_phone_numbers (phone_number_id, phone_number, friendly_name, "
                "number_status, messaging_status, phone_number_type, is_default, campaign_id, campaign_name, "
                "campaign_status, brand_id, brand_name, sub_account_id, tags, purchase_date, renewal_date, "
                "created_at, updated_at, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                [(n.get("id"), n.get("phone_number"), n.get("friendly_name"),
                  n.get("number_status"), n.get("messaging_status"), n.get("phone_number_type"),
                  n.get("is_default"), (n.get("campaign") or {}).get("id"),
                  (n.get("campaign") or {}).get("name"), (n.get("campaign") or {}).get("status"),
                  (n.get("brand") or {}).get("id"), (n.get("brand") or {}).get("name"),
                  n.get("sub_account_id"), _as_str(n.get("tags")), n.get("purchase_date"),
                  n.get("renewal_date"), n.get("created_at"), n.get("updated_at"), run_id) for n in numbers])
        per["phone_numbers"] = len(numbers)
        rows_total += len(numbers)

        # --- billing: current + prior month, per sub-account ---
        conn.execute("DELETE FROM raw_sendivo_billing WHERE _run_id = ?", [run_id])
        bill_rows = []
        for start, end in _month_windows(today, months_back=1):
            try:
                report = cli.billing_report(start, end)
            except Exception as exc:  # noqa: BLE001
                logger.warning("billing %s..%s skipped: %s", start, end, exc)
                continue
            for r in report:
                def fee(k):  # noqa: E306
                    f = r.get(k) or {}
                    return f.get("quantity"), f.get("total_spend")
                sms_q, sms_u = fee("sms_fees")
                car_q, car_u = fee("carrier_fees")
                bill_rows.append((
                    r.get("sub_account_id"), r.get("locationID"), start, end, r.get("total_spend"),
                    sms_q, sms_u, car_q, car_u,
                    (r.get("campaign_setup_fees") or {}).get("total_spend"),
                    (r.get("campaign_renewal_fees") or {}).get("total_spend"),
                    (r.get("brand_fees") or {}).get("total_spend"),
                    (r.get("phone_number_setup_fees") or {}).get("total_spend"),
                    (r.get("phone_number_renewal_fees") or {}).get("total_spend"),
                    json.dumps(r), run_id))
            time.sleep(0.15)
        if bill_rows:
            conn.executemany(
                "INSERT INTO raw_sendivo_billing (sub_account_id, location_id, period_start, period_end, "
                "total_spend, sms_fee_qty, sms_fee_usd, carrier_fee_qty, carrier_fee_usd, campaign_setup_usd, "
                "campaign_renewal_usd, brand_fee_usd, phone_setup_usd, phone_renewal_usd, raw_json, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)", bill_rows)
        per["billing_rows"] = len(bill_rows)
        rows_total += len(bill_rows)

    logger.info("sendivo mirror: %s", per)
    return PhaseResult(rows_in=rows_total, rows_out=rows_total, notes=per)


# Itemized fee → cost_ledger mapping. (sku, cost_unit, usd_col, qty_col)
_FEES = [
    ("sms",             "send",    "sms_fee_usd",        "sms_fee_qty"),
    ("carrier",         "send",    "carrier_fee_usd",    "carrier_fee_qty"),
    ("campaign_setup",  "service", "campaign_setup_usd", None),
    ("campaign_renewal","service", "campaign_renewal_usd", None),
    ("brand",           "service", "brand_fee_usd",      None),
    ("phone_setup",     "service", "phone_setup_usd",    None),
    ("phone_renewal",   "service", "phone_renewal_usd",  None),
]


def run_sendivo_cost(ctx: RunContext) -> PhaseResult:
    """raw_sendivo_billing (latest run) → core.cost_ledger actuals (source='invoice_api')."""
    conn = ctx.db
    if conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='raw_sendivo_billing'").fetchone()[0] == 0:
        return PhaseResult(notes={"skipped": "no billing table"})
    latest = "(SELECT _run_id FROM raw_sendivo_billing ORDER BY _loaded_at DESC LIMIT 1)"
    rows = conn.execute(f"SELECT * FROM raw_sendivo_billing WHERE _run_id = {latest}").fetchall()
    cols = [d[0] for d in conn.description]
    recs = [dict(zip(cols, r)) for r in rows]

    conn.execute("DELETE FROM core.cost_ledger WHERE vendor = 'sendivo'")
    ledger = []
    for r in recs:
        for sku, unit, usd_col, qty_col in _FEES:
            usd = r.get(usd_col)
            if not usd:
                continue
            cost_id = f"sendivo:{sku}:{r['period_start']}:{r['sub_account_id']}"
            ledger.append((
                cost_id, "sendivo", sku, unit,
                r.get(qty_col) if qty_col else None, float(usd),
                r["period_start"], r["period_end"], "monthly",
                "channel", "sms", "invoice_api", "sendivo /billing/report",
                f"sub_account {r['sub_account_id']}"))
    if ledger:
        conn.executemany(
            "INSERT INTO core.cost_ledger (cost_id, vendor, sku, cost_unit, unit_count, total_usd, "
            "period_start, period_end, amortize_method, attribution_dim, attribution_id, source, "
            "source_ref, notes, _loaded_at, _run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), NULL)", ledger)
    total = sum(l[5] for l in ledger)
    logger.info("sendivo cost: %d ledger rows, $%.2f total", len(ledger), total)
    return PhaseResult(rows_in=len(recs), rows_out=len(ledger),
                       notes={"ledger_rows": len(ledger), "total_usd": round(total, 2)})
