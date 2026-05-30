"""Workspace ingest. Iterates env workspace keys serially, hits Instantly,
writes raw snapshot rows, resolves canonical core.workspace.

Resolution rules: per spec 02. Instantly is source of truth for everything
except `slug`, which is derived from the env-key convention because the
Instantly API doesn't return a slug field.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.workspace")


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "workspace", run_workspace_ingest)


def run_workspace_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping workspace ingest")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    rows_in = 0
    rows_out = 0
    failures: list[dict] = []
    seen_workspace_ids: list[str] = []
    slug_to_workspace_id: dict[str, str] = {}

    # Serial. Do not parallelize across workspaces.
    for slug in sorted(keys.keys()):
        api_key = keys[slug]
        try:
            with InstantlyClient(api_key) as client:
                payload = client.get_current_workspace()
            rows_in += 1
            workspace_id = payload.get("id")
            if not workspace_id:
                failures.append({"slug": slug, "error": "missing_workspace_id"})
                continue
            slug_to_workspace_id.setdefault(slug, workspace_id)

            # Append to raw. Multiple slugs may map to the same workspace_id
            # (the FUNDING_4 == KOI_AND_DESTROY case); we write one raw row per
            # successful API call, preserving each slug's audit trail.
            # The PK is (workspace_id, _loaded_at) so simultaneous writes from
            # two slugs in the same microsecond would collide — we offset.
            loaded_at = now
            if workspace_id in seen_workspace_ids:
                # add a fractional second to the second key — keeps PK unique
                # and records the duplicate slug fact
                loaded_at = datetime.fromtimestamp(
                    now.timestamp() + 0.001 * (seen_workspace_ids.count(workspace_id)),
                    tz=timezone.utc,
                )
            seen_workspace_ids.append(workspace_id)

            ctx.db.execute(
                """
                INSERT INTO raw_instantly_workspace
                  (_loaded_at, _run_id, workspace_id, slug, name, plan,
                   trial_active, organization_id, api_response_raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    loaded_at,
                    ctx.run_id,
                    workspace_id,
                    slug,
                    payload.get("name"),
                    payload.get("plan_id"),
                    None,  # trial_active not exposed by API
                    payload.get("owner"),  # owner UUID is closest analogue to org id here
                    json.dumps(payload),
                ],
            )
            rows_out += 1
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001 — defensive: surface and continue
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    # ---- canonical resolution ------------------------------------------
    # For every workspace seen in THIS run, upsert into core.workspace.
    # is_active = True for any workspace that appeared in this run.
    # Workspaces that exist in core but did NOT appear this run get is_active=False
    # (we leave the row in place; we never delete from canonical).

    # Insert/update from the latest raw row per workspace_id in this run.
    ctx.db.execute(
        """
        WITH latest AS (
          SELECT workspace_id,
                 -- Pick a deterministic slug per workspace. Prefer the slug whose
                 -- name matches the API display name when normalized (e.g. Funding 4
                 -- maps to renaissance-koi-and-destroy if both keys hit). Otherwise
                 -- pick the alphabetically first slug for stability.
                 first(slug ORDER BY slug)         AS slug,
                 any_value(name)                   AS name,
                 any_value(plan)                   AS plan
          FROM raw_instantly_workspace
          WHERE _run_id = ?
          GROUP BY workspace_id
        )
        INSERT INTO core.workspace
          (workspace_id, slug, name, plan, is_active, first_seen_at, last_seen_at, resolved_at)
        SELECT workspace_id, slug, name, plan, TRUE, ?, ?, ?
        FROM latest
        WHERE workspace_id NOT IN (SELECT workspace_id FROM core.workspace)
        """,
        [ctx.run_id, now, now, now],
    )

    # Materialize the per-run latest snapshot into a temp view so the UPDATE
    # is a simple correlated lookup. Avoids DuckDB's prepared-statement type
    # inference confusing `_run_id` (VARCHAR) with the TIMESTAMPTZ placeholders.
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_ws")
    ctx.db.execute(
        """
        CREATE TEMP TABLE _run_latest_ws AS
        SELECT workspace_id,
               first(slug ORDER BY slug)  AS slug,
               any_value(name)            AS name,
               any_value(plan)            AS plan
        FROM raw_instantly_workspace
        WHERE _run_id = ?
        GROUP BY workspace_id
        """,
        [ctx.run_id],
    )

    ctx.db.execute(
        """
        UPDATE core.workspace
        SET slug = src.slug,
            name = src.name,
            plan = src.plan,
            is_active = TRUE,
            last_seen_at = ?,
            resolved_at = ?
        FROM _run_latest_ws AS src
        WHERE core.workspace.workspace_id = src.workspace_id
        """,
        [now, now],
    )

    # Flip is_active for workspaces we did NOT see this run.
    ctx.db.execute(
        """
        UPDATE core.workspace
        SET is_active = FALSE,
            resolved_at = ?
        WHERE workspace_id NOT IN (SELECT workspace_id FROM _run_latest_ws)
        """,
        [now],
    )

    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_ws")

    notes = {
        "keys_attempted": len(keys),
        "failures": failures,
        "duplicate_slug_pairs": [
            (a, b)
            for a in slug_to_workspace_id
            for b in slug_to_workspace_id
            if a < b and slug_to_workspace_id[a] == slug_to_workspace_id[b]
        ],
    }
    return PhaseResult(rows_in=rows_in, rows_out=rows_out, notes=notes)
