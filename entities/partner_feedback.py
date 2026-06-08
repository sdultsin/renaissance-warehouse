"""Partner disposition feedback ingest (Spec 16 — BI/Lead-Intent layer, object #1).

Loads funding-partner sales-rep feedback on leads we booked/delivered (what happened
after hand-off: No show / DNQ / LIVE OPPORTUNITY / ...) from a manual xlsx drop, normalized
to seed_data/partner-feedback/*__lead_detail.csv. No live Google Sheet (Sam, 2026-06-08) —
each reporting period is a new CSV in that dir.

⚠ PII: lead emails. The seed dir + *.xlsx are git-ignored; this loader reads them locally
but nothing is committed.

Pipeline:
  raw_partner_lead_feedback  — idempotent UPSERT per (lead_email, source_period)
  core.lead_disposition      — latest disposition per lead, raw string → disposition_class
  v_disposition_funnel       — distribution by class (first insight surface)

Registers under the existing 'sheets' phase, so core/config.py PHASE_ORDER is untouched.
Schema lives in sql/ddl/41_partner_feedback.sql.
"""
from __future__ import annotations

import glob
import logging
import os

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.partner_feedback")

_SEED_DIR = REPO_ROOT / "seed_data" / "partner-feedback"
_DDL = REPO_ROOT / "sql" / "ddl" / "41_partner_feedback.sql"

# Raw disposition string -> tidy disposition_class. Anything unmapped -> 'unknown'
# (never silently wrong; add the mapping rather than guessing).
_CLASS_MAP = {
    "LIVE OPPORTUNITY": "live",
    "Pipeline (soft)": "live",
    "No show": "no_show",
    "DNQ": "disqualified",
    "Not interested": "disqualified",
    "Reschedule": "reschedule",
    "Rebooked": "reschedule",
    "Bad contact info": "bad_data",
    "Data issue": "bad_data",
    "Unreachable": "bad_data",
    "Already in system": "duplicate",
    "Disputed booking": "duplicate",
    "Cancelled": "cancelled",
    "No note": "unknown",
}

_RAW_COLS = [
    "lead_email", "business_name", "industry", "id_confidence",
    "rep", "disposition", "rep_notes", "source_period",
]


def _class_case_sql() -> str:
    """Build a SQL CASE expression for disposition -> disposition_class."""
    whens = " ".join(
        f"WHEN disposition = '{k.replace(chr(39), chr(39)*2)}' THEN '{v}'"
        for k, v in _CLASS_MAP.items()
    )
    return f"CASE {whens} ELSE 'unknown' END"


def run_partner_feedback(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent CREATE TABLE IF NOT EXISTS

    csvs = sorted(glob.glob(str(_SEED_DIR / "*__lead_detail.csv")))
    if not csvs:
        logger.warning("No partner-feedback CSVs in %s — skipping", _SEED_DIR)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_csv"})

    loaded: dict[str, int] = {}
    for csv_path in csvs:
        # Read the normalized CSV (header: row_num,lead_email,...,source_period).
        db.execute("CREATE OR REPLACE TEMP TABLE _pf_stage AS SELECT * FROM read_csv(?, header=true, all_varchar=true)", [csv_path])
        # Idempotent: clear the period(s) present in this file, then insert.
        db.execute(
            "DELETE FROM raw_partner_lead_feedback WHERE source_period IN "
            "(SELECT DISTINCT source_period FROM _pf_stage)"
        )
        db.execute(
            f"""
            INSERT INTO raw_partner_lead_feedback ({', '.join(_RAW_COLS)}, _loaded_at, _run_id)
            SELECT lower(trim(lead_email)), business_name, industry, id_confidence,
                   rep, disposition, rep_notes, source_period, now(), ?
            FROM _pf_stage
            WHERE lead_email IS NOT NULL AND trim(lead_email) <> ''
            """,
            [ctx.run_id],
        )
        n = db.execute(
            "SELECT count(*) FROM _pf_stage WHERE lead_email IS NOT NULL AND trim(lead_email) <> ''"
        ).fetchone()[0]
        loaded[os.path.basename(csv_path)] = n

    # ── Canonical: latest disposition per (lead_email, source_period) ──
    db.execute("DELETE FROM core.lead_disposition")
    db.execute(
        f"""
        INSERT INTO core.lead_disposition
        SELECT lead_email, source_period, disposition,
               {_class_case_sql()} AS disposition_class,
               rep, business_name, industry, id_confidence, rep_notes, now()
        FROM raw_partner_lead_feedback
        """
    )

    # ── First insight surface ──
    db.execute(
        """
        CREATE OR REPLACE VIEW v_disposition_funnel AS
        SELECT source_period,
               disposition_class,
               count(*) AS leads,
               round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY source_period), 1) AS pct
        FROM core.lead_disposition
        GROUP BY source_period, disposition_class
        ORDER BY source_period, leads DESC
        """
    )

    total = sum(loaded.values())
    canon = db.execute("SELECT count(*) FROM core.lead_disposition").fetchone()[0]
    logger.info("partner_feedback: %d raw rows from %d file(s), %d canonical", total, len(csvs), canon)
    return PhaseResult(rows_in=total, rows_out=canon, notes={"files": loaded})


def register(registry: Registry) -> None:
    # Ride the existing 'sheets' phase — no PHASE_ORDER edit needed.
    registry.add_phase("sheets", "partner_feedback", run_partner_feedback)
