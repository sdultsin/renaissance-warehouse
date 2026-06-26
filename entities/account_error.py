"""account_error: nightly disconnect-reason sync — fills core.account_error (DDL 112).

That table shipped empty (its generator was never built). This pulls the live error for
every DISCONNECTED inbox from Instantly (status<0 returns a structured `status_message`,
e.g. {"code":"EAUTH","e_message":"can't create new access token"}), so the Inbox Hub can
show WHY an inbox is down — not just that it is. Healthy inboxes carry no row.

Per workspace key:
  1. GET /accounts?status=-1 (connection_error) and ?status=-3 (sending_error)
  2. email + status_message -> error_string (e_message) + error_code (code)
  3. FULL-REPLACE this workspace's rows (delete+insert) so a reconnected inbox drops out.

Registers under the 'instantly' phase (runs nightly with the other Instantly syncs).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_error")

_DISCONNECTED_STATUSES = (-1, -3)  # connection_error, sending_error


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "account_error", run_account_error_ingest)


def _parse_msg(sm):
    """status_message is a dict {code,command,e_message} or a plain string or None."""
    if isinstance(sm, dict):
        return (sm.get("e_message") or sm.get("message") or None, sm.get("code"))
    if isinstance(sm, str) and sm.strip():
        return (sm.strip(), None)
    return (None, None)


def run_account_error_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No Instantly workspace keys — skipping account_error")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    total = 0
    workspaces_done: list[str] = []
    failures: list[dict] = []
    seen: set[str] = set()

    for slug in sorted(keys.keys()):
        try:
            with InstantlyClient(keys[slug]) as client:
                ws = client.get_current_workspace()
                wsid = ws.get("id")
                if not wsid or wsid in seen:
                    if not wsid:
                        failures.append({"slug": slug, "error": "missing_workspace_id"})
                    continue
                seen.add(wsid)

                rows = {}
                for st in _DISCONNECTED_STATUSES:
                    for a in client.list_accounts(status=st, workspace_id=wsid):
                        email = (a.get("email") or "").strip().lower()
                        if "@" not in email:
                            continue
                        err, code = _parse_msg(a.get("status_message"))
                        rows[email] = (email, wsid, True, err, code, now, now, ctx.run_id)

                ctx.db.execute("DELETE FROM core.account_error WHERE workspace_uuid = ?", [wsid])
                for r in rows.values():
                    ctx.db.execute(
                        "INSERT INTO core.account_error "
                        "(email, workspace_uuid, has_errors, error_string, error_code, "
                        " checked_at, _loaded_at, _run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT DO NOTHING",
                        list(r),
                    )
                total += len(rows)
                workspaces_done.append(slug)
                logger.info("account_error %s: %d disconnected inboxes with reason", slug, len(rows))
        except InstantlyError as exc:
            logger.error("account_error %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_error %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(notes={"disconnected_with_reason": total, "workspaces_done": workspaces_done, "failures": failures})
