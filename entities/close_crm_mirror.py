"""Close CRM → warehouse daily mirror (the mirror-coverage-audit §4 custody gap).

Pulls the full Close warm-funnel record into raw_close_* nightly:
  leads (snapshot upsert) · contacts (from lead payloads) · status-change history,
  email + SMS activity (incremental by date_created watermark) · 3 tiny dims.

Runs in the `close` phase AFTER close_calls (alphabetical registration order).
Idempotent: leads/activities UPSERT on id; contacts + dims are DELETE+INSERT
rebuilds. Raw layer only — no core.* tables are touched here (core.lead_disposition
is a follow-on once this has baked).

Read-only against Close (GETs only). See sql/ddl/1097_close_crm_mirror.sql for the
full design rationale, incl. why email body_html is stripped and why notes/
opportunities are skipped (0 rows, unused).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.close import CloseClient, CloseError

logger = logging.getLogger("entities.close_crm_mirror")

_DDL = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "1097_close_crm_mirror.sql"

# Activity feeds are ~newest-first but not contractually ordered; re-pull this much
# behind the stored watermark each night. Upserts make the overlap a cheap no-op.
_WATERMARK_OVERLAP = timedelta(days=2)

# The named custom fields resolved onto raw_close_lead columns (by field NAME —
# the id→name map comes from raw_close_custom_field each run, so a field rename
# degrades to NULL columns rather than breaking; custom_json always has everything).
_CF_COLUMNS = {
    "Campaign": "cf_campaign",
    "Source": "cf_source",
    "Source Workspace": "cf_source_workspace",
    "Application Status": "cf_application_status",
}


def _parse_dt(v):
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
    except (TypeError, ValueError):
        return None


def _upsert_sql(table: str, cols: list[str]) -> str:
    placeholders = ", ".join("?" for _ in cols)
    update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "id")
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_set}"
    )


# ── row builders (pure; unit-testable) ────────────────────────────────────────

def lead_row(lead: dict, cf_name_by_id: dict[str, str], now, run_id) -> list:
    """Flatten a Close lead payload. Custom fields arrive as flattened
    `custom.cf_<id>` keys (stable) plus a friendly `custom` dict; we resolve the
    4 named columns via the id→name dim map with the friendly dict as fallback."""
    named = {col: None for col in _CF_COLUMNS.values()}
    custom_flat = {}
    friendly = lead.get("custom") if isinstance(lead.get("custom"), dict) else {}
    for k, v in lead.items():
        if k.startswith("custom.cf_"):
            cf_id = k[len("custom.cf_"):]
            custom_flat[cf_id] = v
            name = cf_name_by_id.get(cf_id)
            if name in _CF_COLUMNS:
                named[_CF_COLUMNS[name]] = v if not isinstance(v, (list, dict)) else json.dumps(v)
    for name, col in _CF_COLUMNS.items():
        if named[col] is None and friendly.get(name) is not None:
            v = friendly[name]
            named[col] = v if not isinstance(v, (list, dict)) else json.dumps(v)
    return [
        lead.get("id"),
        lead.get("display_name"),
        lead.get("name"),
        lead.get("status_id"),
        lead.get("status_label"),
        lead.get("description"),
        lead.get("url"),
        named["cf_campaign"],
        named["cf_source"],
        named["cf_source_workspace"],
        named["cf_application_status"],
        json.dumps(custom_flat) if custom_flat else (json.dumps(friendly) if friendly else None),
        len(lead.get("contacts") or []),
        lead.get("created_by"),
        lead.get("updated_by"),
        _parse_dt(lead.get("date_created")),
        _parse_dt(lead.get("date_updated")),
        lead.get("organization_id"),
        json.dumps(lead),
        now,  # _last_seen_at
        now,
        run_id,
    ]


def contact_rows(lead: dict, now, run_id) -> list[list]:
    out = []
    for ct in lead.get("contacts") or []:
        emails = [e.get("email") for e in (ct.get("emails") or []) if e.get("email")]
        phones = [p.get("phone") for p in (ct.get("phones") or []) if p.get("phone")]
        out.append([
            ct.get("id"),
            lead.get("id"),
            ct.get("name"),
            ct.get("title"),
            emails[0].strip().lower() if emails else None,
            phones[0] if phones else None,
            json.dumps(emails) if emails else None,
            json.dumps(phones) if phones else None,
            _parse_dt(ct.get("date_created")),
            _parse_dt(ct.get("date_updated")),
            now,
            run_id,
        ])
    return out


def status_change_row(act: dict, now, run_id) -> list:
    return [
        act.get("id"), act.get("lead_id"),
        act.get("old_status_id"), act.get("old_status_label"),
        act.get("new_status_id"), act.get("new_status_label"),
        act.get("user_id"), act.get("user_name"),
        _parse_dt(act.get("date_created")), act.get("organization_id"),
        json.dumps(act), now, run_id,
    ]


def email_row(act: dict, now, run_id) -> list:
    # Strip the heavy html bodies from the stored raw JSON (see DDL header).
    slim = {k: v for k, v in act.items()
            if k not in ("body_html", "body_preview", "body_html_quoted", "body_text_quoted")}
    return [
        act.get("id"), act.get("lead_id"), act.get("contact_id"), act.get("user_id"),
        act.get("direction"), act.get("status"), act.get("subject"), act.get("sender"),
        json.dumps(act.get("to")) if act.get("to") else None,
        json.dumps(act.get("cc")) if act.get("cc") else None,
        act.get("body_text"),
        act.get("template_id"), act.get("thread_id"),
        _parse_dt(act.get("date_created")), _parse_dt(act.get("date_updated")),
        act.get("organization_id"), json.dumps(slim), now, run_id,
    ]


def sms_row(act: dict, now, run_id) -> list:
    return [
        act.get("id"), act.get("lead_id"), act.get("contact_id"), act.get("user_id"),
        act.get("direction"), act.get("status"), act.get("text"),
        act.get("local_phone"), act.get("remote_phone"),
        act.get("error_message"), act.get("cost"),
        _parse_dt(act.get("date_created")), _parse_dt(act.get("date_updated")),
        act.get("organization_id"), json.dumps(act), now, run_id,
    ]


_LEAD_COLS = ["id", "display_name", "name", "status_id", "status_label", "description",
              "url", "cf_campaign", "cf_source", "cf_source_workspace",
              "cf_application_status", "custom_json", "contacts_count", "created_by",
              "updated_by", "date_created", "date_updated", "organization_id",
              "api_response_raw", "_last_seen_at", "_loaded_at", "_run_id"]
_CONTACT_COLS = ["id", "lead_id", "name", "title", "primary_email", "primary_phone",
                 "emails_json", "phones_json", "date_created", "date_updated",
                 "_loaded_at", "_run_id"]
_SC_COLS = ["id", "lead_id", "old_status_id", "old_status_label", "new_status_id",
            "new_status_label", "user_id", "user_name", "date_created",
            "organization_id", "api_response_raw", "_loaded_at", "_run_id"]
_EMAIL_COLS = ["id", "lead_id", "contact_id", "user_id", "direction", "status",
               "subject", "sender", "to_json", "cc_json", "body_text", "template_id",
               "thread_id", "date_created", "date_updated", "organization_id",
               "api_response_raw", "_loaded_at", "_run_id"]
_SMS_COLS = ["id", "lead_id", "contact_id", "user_id", "direction", "status", "text",
             "local_phone", "remote_phone", "error_message", "cost", "date_created",
             "date_updated", "organization_id", "api_response_raw", "_loaded_at", "_run_id"]


def _watermark(conn, table: str):
    row = conn.execute(f"SELECT max(date_created) FROM {table}").fetchone()
    wm = row[0] if row and row[0] is not None else None
    if wm is not None:
        if wm.tzinfo is None:
            wm = wm.replace(tzinfo=timezone.utc)
        wm = wm - _WATERMARK_OVERLAP
    return wm


# ── phase entrypoint ──────────────────────────────────────────────────────────

def register(registry: Registry) -> None:
    registry.add_phase("close", "close_crm_mirror", run)


def run(ctx: RunContext) -> PhaseResult:
    api_key = ctx.credentials.optional("CLOSE_API_KEY")
    if not api_key:
        logger.warning("No CLOSE_API_KEY — skipping Close CRM mirror")
        return PhaseResult(notes={"reason": "no_key"})

    conn = ctx.db
    conn.execute(_DDL.read_text())
    now = datetime.now(timezone.utc)
    counts: dict[str, int] = {}
    failures: list[dict] = []

    try:
        with CloseClient(api_key) as client:
            # 1. Dims first (the lead flattener needs the custom-field id→name map).
            statuses = client.get_dim("/status/lead/")
            conn.execute("DELETE FROM raw_close_lead_status")
            if statuses:
                conn.executemany(
                    "INSERT INTO raw_close_lead_status (id, label, type, _loaded_at) VALUES (?,?,?,?)",
                    [[s.get("id"), s.get("label"), s.get("type"), now] for s in statuses],
                )
            cfs = client.get_dim("/custom_field/lead/")
            conn.execute("DELETE FROM raw_close_custom_field")
            if cfs:
                conn.executemany(
                    "INSERT INTO raw_close_custom_field (id, name, type, _loaded_at) VALUES (?,?,?,?)",
                    [[c.get("id"), c.get("name"), c.get("type"), now] for c in cfs],
                )
            views = client.get_dim("/saved_search/")
            conn.execute("DELETE FROM raw_close_smart_view")
            if views:
                conn.executemany(
                    "INSERT INTO raw_close_smart_view (id, name, type, _loaded_at) VALUES (?,?,?,?)",
                    [[v.get("id"), v.get("name"), v.get("type"), now] for v in views],
                )
            counts["lead_statuses"] = len(statuses)
            counts["custom_fields"] = len(cfs)
            counts["smart_views"] = len(views)
            cf_name_by_id = {c["id"]: c.get("name") for c in cfs if c.get("id")}

            # 2. Leads — full snapshot upsert + contacts rebuild from the payloads.
            lead_sql = _upsert_sql("raw_close_lead", _LEAD_COLS)
            all_contact_rows: list[list] = []
            n_leads = 0
            for lead in client.iter_leads():
                if not lead.get("id"):
                    continue
                conn.execute(lead_sql, lead_row(lead, cf_name_by_id, now, ctx.run_id))
                all_contact_rows.extend(contact_rows(lead, now, ctx.run_id))
                n_leads += 1
            counts["leads"] = n_leads
            # Contacts: full rebuild (payload-derived; a partial lead pull must not
            # wipe contacts, so only rebuild when the snapshot looks complete).
            if n_leads > 0:
                conn.execute("DELETE FROM raw_close_contact")
                if all_contact_rows:
                    # Dedupe by contact id (a contact can only belong to one lead, but be safe).
                    seen: set[str] = set()
                    uniq = []
                    for r in all_contact_rows:
                        if r[0] and r[0] not in seen:
                            seen.add(r[0])
                            uniq.append(r)
                    conn.executemany(
                        f"INSERT INTO raw_close_contact ({', '.join(_CONTACT_COLS)}) "
                        f"VALUES ({', '.join('?' for _ in _CONTACT_COLS)})",
                        uniq,
                    )
                    counts["contacts"] = len(uniq)

            # 3. Incremental activity feeds (watermark on date_created, 2-day overlap).
            for table, url_type, cols, builder, key in (
                ("raw_close_status_change", "status_change/lead", _SC_COLS, status_change_row, "status_changes"),
                ("raw_close_email", "email", _EMAIL_COLS, email_row, "emails"),
                ("raw_close_sms", "sms", _SMS_COLS, sms_row, "sms"),
            ):
                wm = _watermark(conn, table)
                sql = _upsert_sql(table, cols)
                n = 0
                for act in client.iter_activities(url_type, since=wm):
                    if not act.get("id"):
                        continue
                    conn.execute(sql, builder(act, now, ctx.run_id))
                    n += 1
                counts[key] = n
                logger.info("%s: %d new/updated (watermark %s)", table, n, wm.isoformat() if wm else "full backfill")

    except CloseError as exc:
        logger.error("Close API error: %s", exc)
        failures.append({"error": str(exc)[:300]})

    rows_in = sum(counts.values())
    notes = {**counts, "failures": failures}
    logger.info("Close CRM mirror: %s", notes)
    return PhaseResult(rows_in=rows_in, rows_out=rows_in, notes=notes)
