"""No-show campaign member backfill (Sam 2026-07-18 — WAREHOUSE = source of truth for
ALL no-show data; Google Sheets are visualizations only).

Loads core.no_show_campaign_member from a git-ignored JSONL seed that folds two feeds:
  origin='instantly_campaign' — membership of the 15 no-show / lifecycle Warm-Leads
      campaigns (fresh POST /leads/list {campaign} download 2026-07-18).
  origin='partner_file'       — the partner no-show EXPORT rows already in the RG Master
      No-show sheet (ramir_no_show_resources parsed data), rg_status from the box ledger.

⚠ PII (real lead emails/phones): the seed dir seed_data/no-show-backfill/ is git-ignored
(seed_data/ is fully ignored); this loader reads it locally, nothing is committed. Same
pattern as entities/partner_feedback.py.

Idempotent: clears the load_source(s) present in the seed then re-inserts, so re-running
(nightly or manual) is safe and the table survives warehouse rebuilds. Registers under the
existing 'sheets' phase — no core/config.py PHASE_ORDER edit needed.

Schema: sql/ddl/1140_no_show_campaign_member.sql.
"""
from __future__ import annotations

import glob
import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.no_show_campaign_member")

_SEED_DIR = REPO_ROOT / "seed_data" / "no-show-backfill"
_DDL = REPO_ROOT / "sql" / "ddl" / "1140_no_show_campaign_member.sql"

_COLS = [
    "origin", "campaign_id", "source_tab", "lead_email", "campaign_name", "partner",
    "bucket", "first_name", "last_name", "company_name", "phone", "lead_status",
    "rg_status", "lead_id", "ts_created", "ts_updated", "payload", "load_source",
]


def run_no_show_campaign_member(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent CREATE TABLE / VIEW IF NOT EXISTS

    seeds = sorted(glob.glob(str(_SEED_DIR / "*.jsonl")))
    if not seeds:
        logger.warning("No no-show backfill seed in %s — skipping", _SEED_DIR)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_seed"})

    # Stage every seed file (union). read_json infers all fields; payload stays a JSON
    # string in the seed and is CAST to JSON on insert.
    db.execute(
        "CREATE OR REPLACE TEMP TABLE _nscm_raw AS "
        "SELECT * FROM read_json_auto(?, format='newline_delimited', "
        "union_by_name=true, maximum_object_size=52428800)",
        [seeds],
    )
    # Normalize + guard the FOUR PK columns (all NOT NULL): drop rows missing
    # origin/campaign_id/lead_email, coalesce source_tab->'', lower(trim) the email, and
    # de-duplicate on the exact PK grain so the INSERT can never FATAL on NOT NULL / PK
    # (the seed is built clean, but the loader must be self-defending for re-runs).
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE _nscm_stage AS
        SELECT * FROM (
            -- read_json_auto infers UUID-shaped strings (campaign_id, lead_id) as UUID;
            -- CAST every column to its target type up front so type inference can never
            -- break the guard/insert (trim(UUID) etc.).
            SELECT CAST(origin AS VARCHAR)                  AS origin,
                   CAST(campaign_id AS VARCHAR)             AS campaign_id,
                   coalesce(CAST(source_tab AS VARCHAR), '') AS source_tab,
                   lower(trim(CAST(lead_email AS VARCHAR)))  AS lead_email,
                   CAST(campaign_name AS VARCHAR)           AS campaign_name,
                   CAST(partner AS VARCHAR)                 AS partner,
                   CAST(bucket AS VARCHAR)                  AS bucket,
                   CAST(first_name AS VARCHAR)              AS first_name,
                   CAST(last_name AS VARCHAR)               AS last_name,
                   CAST(company_name AS VARCHAR)            AS company_name,
                   CAST(phone AS VARCHAR)                   AS phone,
                   lead_status, rg_status,
                   CAST(lead_id AS VARCHAR)                 AS lead_id,
                   ts_created, ts_updated, payload,
                   CAST(load_source AS VARCHAR)             AS load_source
            FROM _nscm_raw
            WHERE origin IS NOT NULL AND trim(CAST(origin AS VARCHAR)) <> ''
              AND campaign_id IS NOT NULL AND trim(CAST(campaign_id AS VARCHAR)) <> ''
              AND lead_email IS NOT NULL AND trim(CAST(lead_email AS VARCHAR)) <> ''
        )
        QUALIFY row_number() OVER (
            PARTITION BY origin, campaign_id, source_tab, lead_email
            ORDER BY ts_updated DESC NULLS LAST, load_source
        ) = 1
        """
    )
    raw_n = db.execute("SELECT count(*) FROM _nscm_raw").fetchone()[0]
    stage_n = db.execute("SELECT count(*) FROM _nscm_stage").fetchone()[0]
    if raw_n != stage_n:
        logger.warning("no_show_campaign_member: %d seed rows -> %d after guard/dedup (%d dropped)",
                       raw_n, stage_n, raw_n - stage_n)

    # Idempotent + atomic: clear only the load_source(s) this seed carries, then insert,
    # in ONE transaction so any failure rolls back (never a partial/empty load_source).
    db.execute("BEGIN TRANSACTION")
    try:
        db.execute(
            "DELETE FROM core.no_show_campaign_member WHERE load_source IN "
            "(SELECT DISTINCT load_source FROM _nscm_stage)"
        )
        db.execute(
            f"""
            INSERT INTO core.no_show_campaign_member ({', '.join(_COLS)}, loaded_at)
            SELECT origin, campaign_id, source_tab, lead_email,
                   campaign_name, partner, bucket,
                   first_name, last_name, company_name, phone,
                   TRY_CAST(lead_status AS INTEGER)    AS lead_status,
                   rg_status, lead_id,
                   TRY_CAST(ts_created AS TIMESTAMPTZ)  AS ts_created,
                   TRY_CAST(ts_updated AS TIMESTAMPTZ)  AS ts_updated,
                   TRY_CAST(payload AS JSON)           AS payload,
                   load_source, now()
            FROM _nscm_stage
            """
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise

    total = db.execute("SELECT count(*) FROM core.no_show_campaign_member").fetchone()[0]
    by_origin = dict(
        db.execute(
            "SELECT origin, count(*) FROM core.no_show_campaign_member GROUP BY origin"
        ).fetchall()
    )
    logger.info("no_show_campaign_member: %d rows total, by origin=%s", total, by_origin)
    return PhaseResult(rows_in=total, rows_out=total, notes={"by_origin": by_origin, "seeds": len(seeds)})


def register(registry: Registry) -> None:
    # Ride the existing 'sheets' phase — no PHASE_ORDER edit needed.
    registry.add_phase("sheets", "no_show_campaign_member", run_no_show_campaign_member)
