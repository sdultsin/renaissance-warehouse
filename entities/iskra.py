"""Iskra WhatsApp → warehouse ingest (phase 'iskra'). The WhatsApp analogue of entities/sendivo*.

Six independently-registered ingests under phase 'iskra' (each isolated — the orchestrator catches
a per-ingest exception and runs the rest, so one flaky surface never sinks the others):
  - messages       : incremental by created_at; PK id; UPSERT. The integrity anchor.
  - conversations  : incremental by created_at (the feed's sort key — /conversations ignores
                     server `since`, so we early-stop client-side on created_at); PK id; UPSERT.
                     NOTE: mutable conversation fields (last_message_at, unread_count) only refresh
                     for conversations within the overlap window; the AUTHORITATIVE activity truth
                     is the message grain (run_messages is complete), and v_whatsapp_conversation_
                     performance derives last_message_at from messages — conv_last_message_at is a
                     best-effort snapshot. A periodic full walk would fully refresh old rows.
  - meetings       : COMPLETE full pull every run (v1 /meetings now cursor-paginates; ~6k rows / 7
                     reqs); PK conversation_id; UPSERT (sentiment + meeting tag). The W1e under-count
                     fix — the old 500-newest-only cap stalled the table at ~2.2k / ~54 booked.
  - deals          : incremental by updated_at; PK id; UPSERT.
  - numbers        : full snapshot/run, APPEND (asset-health time series) + aggregate snapshot.
  - stats          : the agency funnel for a window, APPEND (reconciliation source-of-truth row).

INTEGRITY (the Sendivo lesson — audit E1 silent-drop / E2 no attempted-vs-committed / E3 silent
truncation). For every paginated pull we (a) HARD-FAIL if the cursor walk hit the page cap without
the API signalling has_more=false (truncation = data loss, never silent), and (b) cross-check the
rows we committed for a window against Iskra's own stats/summary funnel and record the gap +
`reconciled` flag in PhaseResult.notes. A non-reconciling window logs LOUD (warning) so the nightly
log + watchdog see it; benign window-edge timing skew is tolerated within a small threshold.

Facts mirror the Iskra API exactly (warehouse-query-prompt contract). raw_iskra_stats.opportunities
is Iskra's OWN field (≈ any inbound, ~86% of sends) — it is stored verbatim and is NOT Renaissance's
opp/positive-reply definition. The positive-intent signal is reply_sentiment/meeting_status on
raw_iskra_meetings (surfaced by v_whatsapp_conversation_performance), which is what gates the
Phase-2 WhatsApp-opp → Close push.

Tables/views: sql/ddl/82_whatsapp_iskra.sql.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.iskra import IskraClient

logger = logging.getLogger("entities.iskra")

CHANNEL = "whatsapp"


def register(registry: Registry) -> None:
    registry.add_phase("iskra", "messages", run_messages)
    registry.add_phase("iskra", "conversations", run_conversations)
    registry.add_phase("iskra", "meetings", run_meetings)
    registry.add_phase("iskra", "deals", run_deals)
    registry.add_phase("iskra", "numbers", run_numbers)
    registry.add_phase("iskra", "stats", run_stats)


# --- config helpers (read through credentials; orchestrator loads .env without exporting) -------
def _cfg(ctx: RunContext, key: str, default: str) -> str:
    try:
        v = ctx.credentials.optional(key)
    except Exception:  # noqa: BLE001
        v = None
    return v if v else default


def _month_start(today: date) -> str:
    return date(today.year, today.month, 1).isoformat()


def _backfill_since(ctx: RunContext, today: date) -> str:
    """First-run lower bound (ISO date). Default = current month start; widen via ISKRA_BACKFILL_SINCE
    once Sam confirms the backfill horizon (Open Question 4)."""
    return _cfg(ctx, "ISKRA_BACKFILL_SINCE", _month_start(today))


def _overlap_days(ctx: RunContext) -> int:
    try:
        return int(_cfg(ctx, "ISKRA_INCREMENTAL_OVERLAP_DAYS", "2"))
    except ValueError:
        return 2


def _watermark(conn, table: str, col: str) -> str | None:
    """Max timestamp already in the table, as an ISO string (for the incremental `since`)."""
    try:
        row = conn.execute(f"SELECT max({col}) FROM {table}").fetchone()
    except Exception:  # noqa: BLE001 — table may not exist yet on a fresh DB
        return None
    if not row or row[0] is None:
        return None
    v = row[0]
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _norm_ts(s: str) -> str:
    """Normalize a `since`/`stop_before` value to a full RFC3339 UTC timestamp. A date-only string
    becomes that day's start. Guarantees lexicographic comparability with the API's created_at
    (always +00:00 with microseconds) for the client-side early-stop."""
    if "T" not in s:
        return f"{s}T00:00:00+00:00"
    return s.replace("Z", "+00:00")


def _since_for(ctx: RunContext, conn, today: date, table: str, col: str) -> str:
    """Incremental `since`: watermark minus the overlap buffer, else the first-run backfill floor.
    Overlap re-pulls recent rows so late status/sentiment updates are re-captured; UPSERT dedups.
    Always returns a full RFC3339 timestamp."""
    wm = _watermark(conn, table, col)
    if wm is None:
        return _norm_ts(_backfill_since(ctx, today))
    try:
        wm_dt = datetime.fromisoformat(wm.replace("Z", "+00:00"))
    except ValueError:
        return _norm_ts(_backfill_since(ctx, today))
    # Emit as UTC (+00:00): the box's DuckDB session TZ is non-UTC (America/Asuncion), so the
    # watermark comes back with a -0x:00 offset. The client's early-stop does a LEXICOGRAPHIC
    # compare against the API's always-+00:00 created_at, which is only a valid instant comparison
    # when both carry the same offset. Normalize to UTC here so it holds under any box TZ.
    return (wm_dt - timedelta(days=_overlap_days(ctx))).astimezone(timezone.utc).isoformat()


def _as_str(v):
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    return str(v)


def _upsert(conn, table: str, cols: list[str], pk: str, rows: list[tuple]) -> int:
    """executemany UPSERT keyed on `pk`; every non-pk column (plus _loaded_at/_run_id) is refreshed
    from the incoming row on conflict. Returns the number of rows submitted."""
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in cols)
    update_cols = [c for c in cols if c != pk and c != "_loaded_at"] + ["_loaded_at"]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}, _loaded_at) "
        f"VALUES ({placeholders}, now()) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {set_clause}"
    )
    conn.executemany(sql, rows)
    return len(rows)


def _require_complete(walk, surface: str) -> None:
    """Audit E3: a cursor walk that stops at the page cap WITHOUT has_more=false is silent
    truncation = data loss. Never let it pass as a clean pull."""
    if not walk.completed:
        raise RuntimeError(
            f"iskra {surface}: cursor walk hit the page cap ({walk.pages} pages) without reaching "
            f"has_more=false — refusing to commit a truncated pull (audit E3)."
        )


def _reconcile(committed: int, expected, surface: str, tol_min: int = 10, tol_frac: float = 0.02):
    """Compare rows COMMITTED for a window vs Iskra's own stats/summary count. Returns
    (reconciled, delta). Loud warning when the gap exceeds tolerance (benign window-edge skew is
    within tolerance). `expected` may be None when stats is unavailable -> reconciled unknown."""
    if expected is None:
        return None, None
    delta = expected - committed
    tol = max(tol_min, int(tol_frac * expected))
    reconciled = abs(delta) <= tol
    if not reconciled:
        logger.warning(
            "iskra %s NOT RECONCILED: committed=%d stats=%d delta=%d (tol=%d) — possible silent drop",
            surface, committed, expected, delta, tol,
        )
    return reconciled, delta


# --- ingests -----------------------------------------------------------------------------------
def run_messages(ctx: RunContext) -> PhaseResult:
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    today = datetime.now(timezone.utc).date()
    since = _since_for(ctx, conn, today, "raw_iskra_messages", "created_at")

    # campaign_id/campaign_name/template_id/template_name are NEW v1 outbound identity fields
    # (DDL 1028). `body` (final rendered copy) is the live copy-attribution key; the four IDs exist
    # in the API contract but Iskra writes them 100% NULL today — captured now so they auto-fill the
    # moment Arseny populates them (the incremental overlap re-pull + UPSERT refreshes them in place).
    cols = ["id", "channel", "direction", "body", "campaign_id", "campaign_name",
            "template_id", "template_name", "status", "provider_message_id",
            "conversation_id", "contact_phone", "contact_name", "created_at", "_run_id"]
    with IskraClient(api_key) as cli:
        walk = cli.messages(since=since)
        _require_complete(walk, "messages")
        rows = [(m.get("id"), m.get("channel"), m.get("direction"), m.get("body"),
                 m.get("campaign_id"), m.get("campaign_name"), m.get("template_id"),
                 m.get("template_name"), m.get("status"), m.get("provider_message_id"),
                 m.get("conversation_id"), m.get("contact_phone"), m.get("contact_name"),
                 m.get("created_at"), run_id) for m in walk.items if m.get("id")]
        attempted = _upsert(conn, "raw_iskra_messages", cols, "id", rows)

        # Reconcile committed-in-window vs the stats/summary funnel for the SAME date window.
        # stats `to` is EXCLUSIVE ([from, to)), so to include `today` we pass today+1. The
        # committed window is the inclusive [recon_from, recon_to]. Verified 2026-06-18: with the
        # exclusive-end aligned, stats.messages_sent == committed outbound MESSAGES and
        # stats.replies == committed inbound MESSAGES (both raw message counts) to within a couple
        # rows of mid-pull skew.
        recon_from = since[:10]
        recon_to = today.isoformat()
        recon_to_excl = (today + timedelta(days=1)).isoformat()
        stats = None
        try:
            stats = cli.stats_summary(CHANNEL, recon_from, recon_to_excl).get(CHANNEL) or {}
        except Exception as exc:  # noqa: BLE001 — recon is best-effort; never fail the ingest on it
            logger.warning("messages recon: stats/summary unavailable: %s", exc)
        # UTC calendar date — the box runs a non-UTC session TZ (America/Asuncion), and Iskra's
        # stats/summary windows are UTC; a naive ::date would mis-bucket edge-of-day rows.
        committed_out = conn.execute(
            "SELECT count(*) FROM raw_iskra_messages WHERE direction='outbound' "
            "AND (created_at AT TIME ZONE 'UTC')::date BETWEEN ? AND ?", [recon_from, recon_to]).fetchone()[0]
        committed_in = conn.execute(
            "SELECT count(*) FROM raw_iskra_messages WHERE direction='inbound' "
            "AND (created_at AT TIME ZONE 'UTC')::date BETWEEN ? AND ?", [recon_from, recon_to]).fetchone()[0]
        # Sends are the HARD silent-drop guard (exact raw-message grain). Replies reconcile to the
        # same grain with the exclusive-end window; both warn loud past tolerance.
        rec_sent, d_sent = _reconcile(committed_out, (stats or {}).get("messages_sent"), "messages.sent")
        rec_rep, d_rep = _reconcile(committed_in, (stats or {}).get("replies"), "messages.replies")

    notes = {"since": since, "pages": walk.pages, "walked": len(walk.items), "upserted": attempted,
             "recon_window": [recon_from, recon_to],
             "committed_outbound": committed_out, "committed_inbound": committed_in,
             "stats_sent": (stats or {}).get("messages_sent"), "stats_replies": (stats or {}).get("replies"),
             "delta_sent": d_sent, "delta_replies": d_rep,
             "reconciled_sent": rec_sent, "reconciled_replies": rec_rep}
    logger.info("iskra messages: %s", notes)
    return PhaseResult(rows_in=len(walk.items), rows_out=attempted, notes=notes)


def run_conversations(ctx: RunContext) -> PhaseResult:
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    today = datetime.now(timezone.utc).date()
    since = _since_for(ctx, conn, today, "raw_iskra_conversations", "created_at")
    cols = ["id", "contact_phone", "contact_name", "last_message_text", "last_message_at",
            "unread_count", "assigned_user_id", "pipeline_id", "created_at", "_run_id"]
    with IskraClient(api_key) as cli:
        walk = cli.conversations(stop_before=since)
        _require_complete(walk, "conversations")
        rows = [(c.get("id"), c.get("contact_phone"), c.get("contact_name"),
                 c.get("last_message_text"), c.get("last_message_at"), c.get("unread_count"),
                 c.get("assigned_user_id"), c.get("pipeline_id"), c.get("created_at"), run_id)
                for c in walk.items if c.get("id")]
        n = _upsert(conn, "raw_iskra_conversations", cols, "id", rows)
    notes = {"since": since, "pages": walk.pages, "walked": len(walk.items), "upserted": n}
    logger.info("iskra conversations: %s", notes)
    return PhaseResult(rows_in=len(walk.items), rows_out=n, notes=notes)


def run_meetings(ctx: RunContext) -> PhaseResult:
    """Pull the COMPLETE meeting-tag set every run (NOT incremental). The v1 /meetings endpoint now
    cursor-paginates (the old 500-newest-only hard cap is lifted, 2026-06-27), so we walk it in full
    (since=None, limit=999) and UPSERT on conversation_id. The full set is only ~6,065 rows / 7
    requests, and the requirement is a COMPLETE meeting count — this is the W1e fix for the old
    500-cap under-count (it had stalled the warehouse at ~2.2k rows / ~54 booked vs the true
    ~6.1k / ~181). _require_complete fails loud on a truncated cursor walk (audit E3: no silent
    truncation). A full pull every night also self-heals any past gap and stays 100% complete via the
    conversation_id UPSERT, satisfying the 100%-or-wipe rule for the meeting-derived KPIs."""
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    today = datetime.now(timezone.utc).date()
    cols = ["conversation_id", "meeting_status", "meeting_evidence", "deal_outcome",
            "reply_sentiment", "summary", "tagged_at", "_run_id"]
    with IskraClient(api_key) as cli:
        walk = cli.meetings()                       # since=None -> the COMPLETE set
        _require_complete(walk, "meetings")         # a truncated cursor walk is data loss -> fail loud
        rows = [(m.get("conversation_id"), m.get("meeting_status"), m.get("meeting_evidence"),
                 m.get("deal_outcome"), m.get("reply_sentiment"), m.get("summary"),
                 m.get("tagged_at"), run_id) for m in walk.items if m.get("conversation_id")]
        n = _upsert(conn, "raw_iskra_meetings", cols, "conversation_id", rows)
        # OBSERVABILITY: per-conversation booked count (MTD) vs the agency stats funnel. The table is
        # now COMPLETE (full cursor walk), so this is a real coverage signal — no false silent-drop
        # warning. stats.meetings_booked is the cumulative MTD agency total; the conversation-tag
        # source can still legitimately differ (W1f/DDL 1023 chose the conversation-tag truth as
        # authoritative for the funnel view). The TRUTH for the agency total = raw_iskra_stats.
        recon_from, recon_to = _month_start(today), today.isoformat()
        recon_to_excl = (today + timedelta(days=1)).isoformat()  # stats `to` is exclusive
        booked = conn.execute(
            "SELECT count(*) FROM raw_iskra_meetings WHERE meeting_status='booked' "
            "AND (tagged_at AT TIME ZONE 'UTC')::date BETWEEN ? AND ?", [recon_from, recon_to]).fetchone()[0]
        total_rows = conn.execute("SELECT count(*) FROM raw_iskra_meetings").fetchone()[0]
        total_booked = conn.execute(
            "SELECT count(*) FROM raw_iskra_meetings WHERE meeting_status='booked'").fetchone()[0]
        stats_booked = None
        try:
            s = cli.stats_summary(CHANNEL, recon_from, recon_to_excl)
            stats_booked = s.get("meetings_booked")
        except Exception as exc:  # noqa: BLE001
            logger.warning("meetings recon: stats/summary unavailable: %s", exc)
    booked_coverage = (round(booked / stats_booked, 3) if stats_booked else None)
    notes = {"pages": walk.pages, "walked": len(walk.items), "upserted": n,
             "pull_mode": "complete (full cursor walk @ limit=999, since=None)",
             "total_rows_in_table": total_rows, "total_booked_in_table": total_booked,
             "booked_in_table_mtd": booked, "stats_meetings_booked_mtd": stats_booked,
             "booked_coverage": booked_coverage}
    logger.info("iskra meetings: %s", notes)
    return PhaseResult(rows_in=len(walk.items), rows_out=n, notes=notes)


def run_deals(ctx: RunContext) -> PhaseResult:
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    today = datetime.now(timezone.utc).date()
    since = _since_for(ctx, conn, today, "raw_iskra_deals", "created_at")
    cols = ["id", "title", "stage_id", "pipeline_id", "contact_name", "contact_phone",
            "amount", "currency", "conversation_id", "created_at", "updated_at", "_run_id"]
    with IskraClient(api_key) as cli:
        walk = cli.deals(stop_before=since)
        _require_complete(walk, "deals")
        rows = [(d.get("id"), d.get("title"), d.get("stage_id"), d.get("pipeline_id"),
                 d.get("contact_name"), d.get("contact_phone"), d.get("amount"), d.get("currency"),
                 d.get("conversation_id"), d.get("created_at"), d.get("updated_at"), run_id)
                for d in walk.items if d.get("id")]
        n = _upsert(conn, "raw_iskra_deals", cols, "id", rows)
    notes = {"since": since, "pages": walk.pages, "walked": len(walk.items), "upserted": n,
             "stages": sorted({d.get("stage_id") for d in walk.items if d.get("stage_id")})[:20]}
    logger.info("iskra deals: %s", notes)
    return PhaseResult(rows_in=len(walk.items), rows_out=n, notes=notes)


def run_numbers(ctx: RunContext) -> PhaseResult:
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    with IskraClient(api_key) as cli:
        walk = cli.numbers()
        _require_complete(walk, "numbers")
        nrows = [(n.get("id"), n.get("phone_number"), n.get("label"), n.get("display_name"),
                  n.get("status"), _as_str(n.get("quality_rating")), _as_str(n.get("messaging_limit")),
                  n.get("daily_send_limit"), n.get("warmup_day"), n.get("country_code"),
                  n.get("workspace_id"), n.get("provider_app_id"), n.get("business_manager_id"),
                  n.get("last_health_sync_at"), n.get("created_at"), n.get("updated_at"), run_id)
                 for n in walk.items if n.get("id")]
        # APPEND a fresh per-run snapshot (DELETE just THIS run_id so a re-run is idempotent).
        conn.execute("DELETE FROM raw_iskra_numbers WHERE _run_id = ?", [run_id])
        if nrows:
            conn.executemany(
                "INSERT INTO raw_iskra_numbers (id, phone_number, label, display_name, status, "
                "quality_rating, messaging_limit, daily_send_limit, warmup_day, country_code, "
                "workspace_id, provider_app_id, business_manager_id, last_health_sync_at, "
                "created_at, updated_at, _run_id, _loaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())", nrows)

        snap = cli.numbers_snapshot() or {}
        by_s = snap.get("by_status") or {}
        by_q = snap.get("by_quality") or {}
        conn.execute("DELETE FROM raw_iskra_numbers_snapshot WHERE _run_id = ?", [run_id])
        conn.execute(
            "INSERT INTO raw_iskra_numbers_snapshot (captured_at, total, n_banned, n_restricted, "
            "n_inactive, n_warming, n_ready, q_green, q_yellow, q_red, q_unknown, total_daily_cap, "
            "raw_json, _run_id, _loaded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())",
            [snap.get("captured_at"), snap.get("total"), by_s.get("banned"), by_s.get("restricted"),
             by_s.get("inactive"), by_s.get("warming"), by_s.get("ready"), by_q.get("GREEN"),
             by_q.get("YELLOW"), by_q.get("RED"), by_q.get("UNKNOWN"), snap.get("total_daily_cap"),
             json.dumps(snap), run_id])
        # Reconcile: numbers pulled vs the aggregate total.
        rec, delta = _reconcile(len(nrows), snap.get("total"), "numbers.total", tol_min=2, tol_frac=0.05)
    notes = {"pages": walk.pages, "numbers": len(nrows), "snapshot_total": snap.get("total"),
             "total_daily_cap": snap.get("total_daily_cap"), "by_status": by_s,
             "delta_total": delta, "reconciled_total": rec}
    logger.info("iskra numbers: %s", notes)
    return PhaseResult(rows_in=len(walk.items), rows_out=len(nrows), notes=notes)


def run_stats(ctx: RunContext) -> PhaseResult:
    """The agency funnel for a window — stored verbatim as the reconciliation SOT row. Refreshes
    a wide window each run (month-to-date by default; widen via ISKRA_STATS_FROM) so we never carry
    Sendivo's 7-day rolling-window staleness (SMS gap G9)."""
    conn, run_id = ctx.db, ctx.run_id
    api_key = ctx.credentials.require("ISKRA_API_KEY")
    today = datetime.now(timezone.utc).date()
    frm = _cfg(ctx, "ISKRA_STATS_FROM", _month_start(today))
    to = today.isoformat()                                   # inclusive last day we represent
    to_excl = (today + timedelta(days=1)).isoformat()        # stats `to` is EXCLUSIVE -> +1 to include today
    with IskraClient(api_key) as cli:
        payload = cli.stats_summary(CHANNEL, frm, to_excl) or {}
    wa = payload.get(CHANNEL) or {}
    conn.execute("INSERT INTO raw_iskra_stats (channel, window_from, window_to, messages_sent, "
                 "messages_delivered, delivery_rate, replies, reply_rate, opportunities, "
                 "meetings_booked, deals_won, captured_at, raw_json, _run_id, _loaded_at) "
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, ?, now())",
                 [CHANNEL, frm, to, wa.get("messages_sent"), wa.get("messages_delivered"),
                  wa.get("delivery_rate"), wa.get("replies"), wa.get("reply_rate"),
                  payload.get("opportunities"), payload.get("meetings_booked"),
                  payload.get("deals_won"), json.dumps(payload), run_id])
    notes = {"window": [frm, to], "messages_sent": wa.get("messages_sent"),
             "replies": wa.get("replies"), "opportunities": payload.get("opportunities"),
             "meetings_booked": payload.get("meetings_booked"), "deals_won": payload.get("deals_won")}
    logger.info("iskra stats: %s", notes)
    return PhaseResult(rows_in=1, rows_out=1, notes=notes)
