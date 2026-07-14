"""core.sending_account canonical entity — CENSUS-DERIVED registry (WS3, v105).

Rebuilt from core.account_census (the live Instantly /accounts census), NOT the retired
raw_account_truth_accounts snapshot. is_active = membership in the latest census
(retired_at NULL); departed accounts are tombstoned (retired_at stamped day-after-last-cold-send).
status uses the DISAMBIGUATED connection axis (conn_state: conn_active/conn_paused/
connection_error/sending_error) and warmup_state the warmup axis (warmup_on/off/banned),
both from core.v_account_census_state. first_cold_send_at is backfilled here (= MIN daily
date WHERE actual_sends>0 per account; WS4's cold_start reads it). Registers under the
'canonical' phase (runs AFTER the account_census phase). Idempotent full rebuild each run.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.sending_account")

RECENT_DAYS = 3   # safety-guard window (matches ASSERT 4)


def register(registry: Registry) -> None:
    # [2026-07-14] Moved 'canonical' -> 'portal_core' (PASS A). This ingest reads ONLY
    # core.account_census / core.sending_account_* / core.workspace — never core.reply,
    # core.domain or anything the dns_sweep/replies/CRM phases produce — so it can be built
    # before them. That lets the serving snapshot with same-day fleet health publish at
    # ~03:30 ET instead of ~09:40 ET. Runs exactly once per night; canonical ingests that
    # read core.sending_account now see it FRESHER (built earlier the same night), not staler.
    registry.add_phase("portal_core", "sending_account", run_sending_account)


def run_sending_account(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # --- Fail-loud precondition (B3): census AND summary must exist, else NO-OP
    #     (do NOT rebuild on an inflated/absent source) ---
    have_census = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='account_census'").fetchone()[0]
    have_summary = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='account_census_summary'").fetchone()[0]
    if not have_census or db.execute("SELECT count(*) FROM core.account_census").fetchone()[0] == 0:
        logger.warning("core.account_census empty/absent — skipping sending_account rebuild (fail-safe)")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no census"})
    if not have_summary or db.execute(
            "SELECT count(*) FROM core.account_census_summary "
            "WHERE census_date=(SELECT max(census_date) FROM core.account_census)").fetchone()[0] == 0:
        raise RuntimeError(
            "core.account_census_summary missing latest-date row — ASSERT 1 ground truth absent; abort")

    LATEST = "(SELECT max(census_date) FROM core.account_census)"

    # Carry-forward prior labels + historical universe BEFORE the DELETE (so esp/infra survive).
    db.execute("CREATE TEMP TABLE sending_account_PRE AS SELECT * FROM core.sending_account")
    db.execute("DELETE FROM core.sending_account")
    db.execute(f"""
    INSERT INTO core.sending_account (
      account_id, email, domain, workspace_slug, workspace_id,
      esp, infra_provider, lifecycle_state, rotation_state,
      created_at, status, warmup_state, warmup_phase, warmup_score, daily_limit,
      is_active, has_errors, first_seen_at, last_seen_at, resolved_at,
      _snapshot_date, retired_at, inventory_source, census_date_resolved, first_cold_send_at
    )
    WITH cur AS (                                    -- latest live census, DISAMBIGUATED state labels
        SELECT lower(email) AS email_lc, * FROM core.v_account_census_state
    ),
    hist AS (                                         -- prior warehouse rows (labels to carry forward)
        SELECT lower(email) AS email_lc, * FROM sending_account_PRE
    ),
    dly AS (                                          -- per-email daily rollup: esp + first/last cold-send date
        SELECT lower(account_id) AS email_lc,
               any_value(esp)                              AS esp,
               any_value(workspace_slug)                   AS workspace_slug,
               MAX(date)                                   AS last_row_date,
               MAX(date) FILTER (WHERE actual_sends > 0)   AS last_cold_send_date,
               MIN(date) FILTER (WHERE actual_sends > 0)   AS first_cold_send_date,  -- WS4 cold_start reads this
               MIN(date)                                   AS first_seen_date
        FROM core.sending_account_daily GROUP BY 1
    ),
    universe AS (                                     -- census ∪ daily ∪ prior rows (the REAL universe — B1)
        SELECT email_lc FROM cur
        UNION SELECT email_lc FROM dly
        UNION SELECT email_lc FROM hist
    )
    SELECT
        u.email_lc                                                        AS account_id,
        COALESCE(c.email, h.email, u.email_lc)                            AS email,
        COALESCE(c.domain, h.domain, split_part(u.email_lc,'@',2))        AS domain,
        COALESCE(c.workspace_slug, h.workspace_slug, dl.workspace_slug)   AS workspace_slug,
        w.workspace_id,
        COALESCE(h.esp, dl.esp)                                           AS esp,            -- B2: daily esp fallback
        h.infra_provider,                                                                   -- WS4 owns; pass-through
        COALESCE(h.lifecycle_state, 'unknown')                           AS lifecycle_state,
        NULL                                                             AS rotation_state,
        COALESCE(TRY_CAST(c.timestamp_created AS TIMESTAMPTZ), h.created_at) AS created_at,
        -- status: DISAMBIGUATED connection axis for live; else carry the prior VARCHAR status.
        COALESCE(c.conn_state, h.status)                                AS status,
        c.warmup_state                                                  AS warmup_state,   -- disambiguated warmup axis
        h.warmup_phase,
        COALESCE(c.stat_warmup_score, h.warmup_score)                  AS warmup_score,
        COALESCE(c.daily_limit, h.daily_limit)                         AS daily_limit,
        (c.email_lc IS NOT NULL)                                       AS is_active,         -- LIVE iff in latest census
        COALESCE(h.has_errors, FALSE)                                  AS has_errors,
        COALESCE(h.first_seen_at, dl.first_seen_date::timestamptz, now()) AS first_seen_at,
        CASE WHEN c.email_lc IS NOT NULL THEN now()
             ELSE COALESCE(dl.last_row_date::timestamptz, h.last_seen_at) END AS last_seen_at,
        now()                                                          AS resolved_at,
        {LATEST}                                                       AS _snapshot_date,
        -- retired_at (C2): NULL if in latest census; else day-after-last-cold-send, else census date (bootstrap)
        CASE WHEN c.email_lc IS NOT NULL THEN NULL
             ELSE COALESCE(dl.last_cold_send_date + INTERVAL 1 DAY, {LATEST}) END::DATE AS retired_at,
        CASE WHEN c.email_lc IS NOT NULL THEN 'instantly_census'
             WHEN dl.last_cold_send_date IS NOT NULL THEN 'census_diff_departed'
             ELSE 'bootstrap_departed' END                            AS inventory_source,
        {LATEST}                                                       AS census_date_resolved,
        -- first_cold_send_at BACKFILL (WS3 owns; WS4's cold_start reads this):
        COALESCE(h.first_cold_send_at, dl.first_cold_send_date::timestamptz) AS first_cold_send_at
    FROM universe u
    LEFT JOIN cur  c  ON c.email_lc  = u.email_lc
    LEFT JOIN hist h  ON h.email_lc  = u.email_lc
    LEFT JOIN dly  dl ON dl.email_lc = u.email_lc
    LEFT JOIN core.workspace w ON w.slug = COALESCE(c.workspace_slug, h.workspace_slug, dl.workspace_slug)
    """)

    # Seed 'created' lifecycle events (append-only; PK prevents dupes on re-run). Unchanged from v1.
    db.execute(
        """
        INSERT OR IGNORE INTO core.sending_account_state_event
          (account_id, event_type, event_at, previous_state, new_state, notes, _detected_at)
        SELECT account_id, 'created', created_at, NULL, lifecycle_state,
               'seeded from census-derived rebuild', now()
        FROM core.sending_account
        WHERE created_at IS NOT NULL
        """
    )

    n = db.execute("SELECT count(*) FROM core.sending_account").fetchone()[0]
    n_active = db.execute("SELECT count(*) FROM core.sending_account WHERE is_active").fetchone()[0]
    n_events = db.execute("SELECT count(*) FROM core.sending_account_state_event").fetchone()[0]
    logger.info(
        "core.sending_account rebuilt (census-derived): %d rows (%d active); %d state events",
        n, n_active, n_events,
    )
    return PhaseResult(
        rows_in=n, rows_out=n,
        notes={"active": n_active, "state_events": n_events},
    )
