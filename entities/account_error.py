"""account_error: nightly disconnect-reason sync -> core.account_error (DDL 112, PK=email).

Fills the empty table so the Inbox Hub can show WHY an inbox is down: for each DISCONNECTED
inbox (Instantly status<0 returns a structured status_message) record email + error_string +
error_code. Healthy inboxes carry no row.

Collision-safe: disconnected inboxes from ALL workspaces are gathered into ONE dict keyed by
email FIRST (so if an email is disconnected in two workspaces in the same run it resolves
deterministically to a single row, never a silent cross-workspace overwrite mid-loop). Then:
upsert every row, then sweep rows in the successfully-pulled workspaces that were NOT refreshed
this run (= reconnected). Upsert-then-sweep, and the sweep is scoped to workspaces that
actually succeeded, so neither a mid-run failure nor a skipped workspace can drop live rows.
Registers under the 'instantly' phase (nightly).
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_error")


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "account_error", run_account_error_ingest)


def _parse(sm):
    if isinstance(sm, dict):
        return sm.get("e_message") or sm.get("message"), sm.get("code")
    return (sm.strip(), None) if isinstance(sm, str) and sm.strip() else (None, None)


def run_account_error_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        return PhaseResult(notes={"reason": "no_keys"})
    now = datetime.now(timezone.utc)
    rows, done_ws, failures, seen = {}, [], [], set()
    for slug in sorted(keys):
        try:
            with InstantlyClient(keys[slug]) as client:
                wsid = (client.get_current_workspace() or {}).get("id")
                if not wsid or wsid in seen:
                    continue
                seen.add(wsid)
                for st in (-1, -3):  # connection_error, sending_error
                    for a in client.list_accounts(status=st, workspace_id=wsid):
                        email = (a.get("email") or "").strip().lower()
                        if "@" not in email:
                            continue
                        err, code = _parse(a.get("status_message"))
                        rows[email] = (email, wsid, True, err, code, now, now, ctx.run_id)
                done_ws.append(wsid)
        except InstantlyError as exc:
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_error %s", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    for r in rows.values():
        ctx.db.execute(
            "INSERT INTO core.account_error (email, workspace_uuid, has_errors, error_string, "
            "error_code, checked_at, _loaded_at, _run_id) VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT (email) DO UPDATE SET workspace_uuid=excluded.workspace_uuid, "
            "has_errors=excluded.has_errors, error_string=excluded.error_string, "
            "error_code=excluded.error_code, checked_at=excluded.checked_at, "
            "_loaded_at=excluded._loaded_at, _run_id=excluded._run_id", list(r))
    # sweep reconnected rows — only in workspaces we actually pulled, only after all upserts
    if done_ws:
        ph = ",".join("?" * len(done_ws))
        ctx.db.execute(
            f"DELETE FROM core.account_error WHERE _run_id <> ? AND workspace_uuid IN ({ph})",
            [ctx.run_id, *done_ws])
    return PhaseResult(notes={"disconnected_with_reason": len(rows),
                              "workspaces_done": len(done_ws), "failures": failures})
