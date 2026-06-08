"""core.lead — the canonical lead spine (Spec 16, WS-F).

ONE row per SIGNAL lead: any lead we have a reply, a call, a partner disposition, an
opportunity, or a meeting on. NOT the ~27M scraped universe — only leads with a signal.

IDENTITY (spec 16 §1 — DETERMINISTIC only, NO fuzzy matching):
  * exact email (lower/trim) when an email exists;
  * the E.164 phone for phone-only Sendivo leads (a core.call row whose lead_email is NULL).
  lead_key = md5(coalesce(lower(email), phone_e164)) — a stable surrogate so the same
  resolved identity collapses to one row across every source signal AND across re-runs.

Sources unioned (email identities):
  core.reply.lead_email, core.call.lead_email, core.lead_disposition.lead_email,
  core.opportunity.lead_email.
Sources unioned (phone-only identities):
  core.call.phone_e164 WHERE lead_email IS NULL (Sendivo SMS leads have no email).

⚠ core.meeting carries NO lead email/phone (it is Slack-success-channel derived, keyed by
  meeting_id) so it contributes no identity to the spine — intentionally omitted. See the
  header of sql/ddl/44_lead.sql.

Scraped attrs (first_name/company/segment/industry/lead_source) LEFT JOIN to the lead-DB
mirror for the signal subset. The mirror is a local duckdb on the droplet; its table is NOT
present in this repo's DDL, so — per the spec's correctness>completeness rule — the join is
gated behind a flag and the attr columns stay NULL until the parent confirms the mirror's
name/columns. See TODO(parent) below. We never guess a table name.

Idempotent: DELETE + INSERT full rebuild each run. Registers under the existing 'canonical'
phase (no core/config.py PHASE_ORDER edit).
"""
from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.lead_spine")

_DDL = REPO_ROOT / "sql" / "ddl" / "44_lead.sql"

# Signal sources that carry an email identity: (table, email_col, first_seen_ts_expr).
# first_seen_ts_expr is a SQL expression evaluated against that table for an arrival time.
_EMAIL_SOURCES = [
    ("core.reply", "lead_email", "reply_timestamp"),
    ("core.call", "lead_email", "occurred_at"),
    ("core.lead_disposition", "lead_email", "resolved_at"),
    ("core.opportunity", "lead_email", "opened_at"),
]

# ── Scraped-attr enrichment from the lead-DB mirror ───────────────────────────
# TODO(parent): wire scraped attrs from the lead mirror once its table name/columns are
# confirmed. The mirror is a local duckdb on the droplet (see memory: lead_mirror.duckdb /
# `mca_lead` etc.) but no such table/view is declared in this repo's sql/ddl/*, so we cannot
# confirm a name or column set from the DDL. Until then attrs stay NULL and the join is OFF.
# To enable: set _ENRICH_MIRROR_TABLE to the confirmed table (e.g. 'lead_mirror' /
# 'raw_leads_mirror') and map the columns in _MIRROR_COLS, then flip _ENABLE_MIRROR_ENRICH.
_ENABLE_MIRROR_ENRICH = False
_ENRICH_MIRROR_TABLE = None  # e.g. "lead_mirror" — must match exactly; never guess.
_MIRROR_COLS = {
    # warehouse column : mirror column (only used when _ENABLE_MIRROR_ENRICH is True)
    "first_name": "first_name",
    "company": "company",
    "segment": "segment",
    "industry": "industry",
    "lead_source": "lead_source",
}


def register(registry: Registry) -> None:
    # Ride the existing 'canonical' phase (already references the lead spine in PHASE_ORDER).
    registry.add_phase("canonical", "lead_spine", run)


def _table_exists(db, qualified: str) -> bool:
    """qualified = 'schema.table' or 'table'. Defensive (sources may be absent)."""
    if "." in qualified:
        schema, table = qualified.split(".", 1)
        n = db.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()[0]
    else:
        n = db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [qualified],
        ).fetchone()[0]
    return n > 0


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent CREATE TABLE/INDEX IF NOT EXISTS

    # ── Build the union of all signal-lead identities into a staging CTE set ──
    # email identities (one normalized row per (key, source-arrival-ts))
    email_selects = []
    for table, col, ts_expr in _EMAIL_SOURCES:
        if not _table_exists(db, table):
            logger.warning("lead_spine: %s absent — skipping that signal source", table)
            continue
        email_selects.append(
            f"""
            SELECT lower(trim({col})) AS email,
                   CAST(NULL AS VARCHAR) AS phone_e164,
                   {ts_expr} AS seen_at
            FROM {table}
            WHERE {col} IS NOT NULL AND trim({col}) <> ''
            """
        )

    # phone-only identities: core.call rows with NULL email but an E.164 phone (Sendivo).
    phone_selects = []
    if _table_exists(db, "core.call"):
        phone_selects.append(
            """
            SELECT CAST(NULL AS VARCHAR) AS email,
                   phone_e164,
                   occurred_at AS seen_at
            FROM core.call
            WHERE (lead_email IS NULL OR trim(lead_email) = '')
              AND phone_e164 IS NOT NULL AND trim(phone_e164) <> ''
            """
        )

    all_selects = email_selects + phone_selects
    if not all_selects:
        logger.warning("lead_spine: no signal sources present — core.lead left empty")
        db.execute("DELETE FROM core.lead")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_sources"})

    union_sql = "\nUNION ALL\n".join(all_selects)

    # Resolve identity -> lead_key, collapse to one row per key, take earliest seen_at.
    # resolution_confidence: 'email' when keyed by email, else 'phone' (phone-only).
    db.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _lead_stage AS
        WITH signals AS (
            {union_sql}
        ),
        keyed AS (
            SELECT
                md5(coalesce(email, phone_e164))           AS lead_key,
                email,
                phone_e164,
                CASE WHEN email IS NOT NULL THEN 'email' ELSE 'phone' END AS resolution_confidence,
                seen_at
            FROM signals
        )
        SELECT
            lead_key,
            -- email/phone are functionally determined by lead_key; max() picks the single value.
            max(email)                 AS email,
            max(phone_e164)            AS phone_e164,
            max(resolution_confidence) AS resolution_confidence,
            min(seen_at)               AS first_seen_at
        FROM keyed
        GROUP BY lead_key
        """
    )

    # ── Idempotent rebuild ──
    db.execute("DELETE FROM core.lead")

    enriched = (
        _ENABLE_MIRROR_ENRICH
        and _ENRICH_MIRROR_TABLE
        and _table_exists(db, _ENRICH_MIRROR_TABLE)
    )
    if enriched:
        # LEFT JOIN the confirmed lead mirror by exact email (deterministic).
        m = _MIRROR_COLS
        db.execute(
            f"""
            INSERT INTO core.lead
              (lead_key, email, phone_e164, first_name, company, segment, industry,
               lead_source, resolution_confidence, first_seen_at, resolved_at)
            SELECT
              s.lead_key, s.email, s.phone_e164,
              lm.{m['first_name']}, lm.{m['company']}, lm.{m['segment']},
              lm.{m['industry']}, lm.{m['lead_source']},
              CASE
                WHEN s.resolution_confidence = 'phone' THEN 'phone'
                WHEN lm.{m['first_name']} IS NULL AND lm.{m['company']} IS NULL
                     THEN 'unmatched'
                ELSE 'email'
              END AS resolution_confidence,
              s.first_seen_at, now()
            FROM _lead_stage s
            LEFT JOIN {_ENRICH_MIRROR_TABLE} lm
              ON s.email IS NOT NULL AND lm.email = s.email
            """
        )
    else:
        # Mirror not confirmed — attrs NULL, confidence reflects key type only.
        db.execute(
            """
            INSERT INTO core.lead
              (lead_key, email, phone_e164, first_name, company, segment, industry,
               lead_source, resolution_confidence, first_seen_at, resolved_at)
            SELECT
              lead_key, email, phone_e164,
              NULL, NULL, NULL, NULL, NULL,
              resolution_confidence, first_seen_at, now()
            FROM _lead_stage
            """
        )

    n = db.execute("SELECT count(*) FROM core.lead").fetchone()[0]
    by_conf = dict(
        db.execute(
            "SELECT resolution_confidence, count(*) FROM core.lead GROUP BY 1"
        ).fetchall()
    )
    logger.info(
        "core.lead rebuilt: %d signal leads (%s); mirror_enrich=%s",
        n, by_conf, bool(enriched),
    )
    return PhaseResult(
        rows_in=n, rows_out=n,
        notes={"by_confidence": by_conf, "mirror_enrich": bool(enriched)},
    )
