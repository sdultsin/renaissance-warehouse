"""core.sending_account canonical entity — resolved from raw_account_truth_accounts.

Reads the most-recent account_truth snapshot mirrored into raw_account_truth_accounts
and projects one canonical row per inbox. Registers under the 'canonical' phase.
Idempotent full rebuild each run (like entities/meeting.py).

Classification / lifecycle derivation rules (spec 06), constrained by what a single
account_truth snapshot can actually tell us:

  esp            <- infra_type   (Google->google, Outlook->outlook, OTD->otd, else NULL)
  infra_provider <- provider_code (1=OTD -> 'OTD'; the specific vendor brand for
                    other Outlook/Google inboxes is NOT in account_truth, needs
                    domain/tag resolution -> NULL for now (GAP)).
  lifecycle_state:
     Missing Current Inventory -> retired
     Paused / Connection Error -> paused
     Active + warmup_status=1   -> active      (warmed, in service)
     Active + warmup_status=-1  -> warmed      (degraded warmup; conservative)
     Active + warmup_status=0   -> warming
  status         <- status_label normalized
  warmup_phase   <- warmup_status label

Transition timestamps other than created_at are left NULL — a single snapshot can't
observe when warmup started/ended or when an account was paused. Those fill in as
nightly snapshot-diffs detect transitions (documented follow-up, see GAPS.md).
daily_limit_used is Instantly-supplement-only and stays NULL in v1.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.sending_account")

RAW = "raw_account_truth_accounts"


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "sending_account", run_sending_account)


def run_sending_account(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # No raw snapshot yet -> no-op (e.g. canonical phase run before account_truth phase).
    have_raw = db.execute(
        f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{RAW}'"
    ).fetchone()[0]
    if not have_raw or db.execute(f"SELECT count(*) FROM {RAW}").fetchone()[0] == 0:
        logger.warning("%s empty/absent — skipping core.sending_account build", RAW)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no raw snapshot"})

    # Idempotent full rebuild from the most-recent snapshot run.
    db.execute("DELETE FROM core.sending_account")
    db.execute(
        f"""
        INSERT INTO core.sending_account (
          account_id, email, domain, workspace_slug, workspace_id,
          esp, infra_provider, lifecycle_state, rotation_state,
          created_at, warmup_started_at, warmup_completed_at, rampup_started_at,
          rampup_completed_at, paused_at, retired_at,
          status, warmup_phase, warmup_score, daily_limit, daily_limit_used,
          cost_per_day_usd_estimated, vendor_billing_cycle,
          is_active, first_seen_at, last_seen_at, resolved_at
        )
        WITH latest AS (
          SELECT * FROM {RAW}
          WHERE _run_id = (SELECT _run_id FROM {RAW} ORDER BY _loaded_at DESC LIMIT 1)
        ), ranked AS (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY email
              ORDER BY CASE WHEN status_label = 'Missing Current Inventory' THEN 1 ELSE 0 END,
                       daily_limit DESC NULLS LAST
            ) AS rn
          FROM latest
        )
        SELECT
          email                                              AS account_id,
          email,
          COALESCE(domain, split_part(email, '@', 2))        AS domain,
          workspace_slug,
          w.workspace_id,
          CASE infra_type
            WHEN 'Google'  THEN 'google'
            WHEN 'Outlook' THEN 'outlook'
            WHEN 'OTD'     THEN 'otd'
            ELSE NULL
          END                                                AS esp,
          CASE WHEN provider_code = 1 THEN 'OTD' ELSE NULL END AS infra_provider,
          CASE
            WHEN status_label = 'Missing Current Inventory' THEN 'retired'
            WHEN status_label = 'Paused'                    THEN 'paused'
            WHEN status_label = 'Connection Error'          THEN 'paused'
            WHEN status_label = 'Active' AND warmup_status = 1  THEN 'active'
            WHEN status_label = 'Active' AND warmup_status = -1 THEN 'warmed'
            WHEN status_label = 'Active'                     THEN 'warming'
            ELSE 'warming'
          END                                                AS lifecycle_state,
          NULL                                               AS rotation_state,
          TRY_CAST(created_at AS TIMESTAMPTZ)                AS created_at,
          NULL, NULL, NULL, NULL, NULL,                      -- warmup/rampup/paused transitions
          CASE WHEN status_label = 'Missing Current Inventory'
               THEN TRY_CAST(updated_at AS TIMESTAMPTZ) END  AS retired_at,
          CASE status_label
            WHEN 'Active'                    THEN 'active'
            WHEN 'Paused'                    THEN 'paused'
            WHEN 'Connection Error'          THEN 'connection_error'
            WHEN 'Missing Current Inventory' THEN 'missing'
            ELSE lower(status_label)
          END                                                AS status,
          CASE warmup_status WHEN 1 THEN 'warmed' WHEN 0 THEN 'not_warmed'
               WHEN -1 THEN 'degraded' ELSE NULL END         AS warmup_phase,
          warmup_score,
          daily_limit,
          NULL                                               AS daily_limit_used,
          NULL                                               AS cost_per_day_usd_estimated,
          NULL                                               AS vendor_billing_cycle,
          (status_label <> 'Missing Current Inventory')      AS is_active,
          now()                                              AS first_seen_at,
          now()                                              AS last_seen_at,
          now()                                              AS resolved_at
        FROM ranked
        LEFT JOIN core.workspace w ON w.slug = ranked.workspace_slug
        WHERE rn = 1 AND email IS NOT NULL
        """
    )

    # Seed 'created' lifecycle events (append-only; PK prevents dupes on re-run).
    db.execute(
        """
        INSERT OR IGNORE INTO core.sending_account_state_event
          (account_id, event_type, event_at, previous_state, new_state, notes, _detected_at)
        SELECT account_id, 'created', created_at, NULL, lifecycle_state,
               'seeded from account_truth snapshot', now()
        FROM core.sending_account
        WHERE created_at IS NOT NULL
        """
    )

    n = db.execute("SELECT count(*) FROM core.sending_account").fetchone()[0]
    n_active = db.execute(
        "SELECT count(*) FROM core.sending_account WHERE is_active"
    ).fetchone()[0]
    n_events = db.execute(
        "SELECT count(*) FROM core.sending_account_state_event"
    ).fetchone()[0]
    logger.info(
        "core.sending_account rebuilt: %d rows (%d active); %d state events",
        n, n_active, n_events,
    )
    return PhaseResult(
        rows_in=n, rows_out=n,
        notes={"active": n_active, "state_events": n_events},
    )
