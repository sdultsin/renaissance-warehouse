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
import os

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
# The lead mirror is a SEPARATE droplet DuckDB (mirror.leads_current, ~27.7M leads, the
# nightly mirror of the lead DB). We ATTACH it READ-ONLY and LEFT JOIN by email (100% keyed,
# deterministic) to populate the scraped identity attrs. ATTACH (not copy) = consolidation
# without duplicating the 21.8 GB file or touching the single-writer lock.
# Verified columns: first_name/enrich_first_name, company_name, general_industry/
# specific_industry, source_list_name, source, email, phone.
_LEAD_MIRROR_DB = os.environ.get(
    "LEAD_MIRROR_DB", "/root/renaissance-worker/jobs/lead-mirror/lead_mirror.duckdb"
)
# Opt-in: the in-line 247k×27.7M cross-ATTACH join is too heavy to hold the nightly writer
# lock by default. The sustainable delivery is to materialize the mirror into the warehouse
# (core.lead as the full dimension) so enrichment is a fast in-warehouse join. Until that
# lands, set LEAD_SPINE_ENRICH=1 to do the cross-ATTACH enrich.
_ENABLE_MIRROR_ENRICH = os.environ.get("LEAD_SPINE_ENRICH", "0") == "1"


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

    enriched = False

    # Path 1: pre-materialized core.lead_attrs (fast in-warehouse join, no ATTACH).
    # prebuild_lead_attrs.py populates this before the nightly (cron 02:30 UTC).
    if not enriched and _table_exists(db, "core.lead_attrs"):
        n_attrs = db.execute("SELECT count(*) FROM core.lead_attrs").fetchone()[0]
        if n_attrs > 0:
            try:
                db.execute("""
                    INSERT INTO core.lead
                      (lead_key, email, phone_e164, first_name, company, segment, industry,
                       lead_source, resolution_confidence, first_seen_at, resolved_at)
                    SELECT
                      s.lead_key, s.email, s.phone_e164,
                      la.first_name,
                      la.company_name                                        AS company,
                      la.source_list_name                                    AS segment,
                      coalesce(la.specific_industry, la.general_industry)   AS industry,
                      la.source                                              AS lead_source,
                      CASE
                        WHEN s.resolution_confidence = 'phone' THEN 'phone'
                        WHEN la.email IS NULL THEN 'unmatched'
                        ELSE 'email'
                      END                                                    AS resolution_confidence,
                      s.first_seen_at, now()
                    FROM _lead_stage s
                    LEFT JOIN core.lead_attrs la
                      ON s.email IS NOT NULL AND la.email = s.email
                """)
                enriched = True
                logger.info("lead_spine: enriched via core.lead_attrs (%d rows)", n_attrs)
            except Exception as exc:
                logger.warning("lead_attrs enrich failed (%s) — falling back", str(exc)[:160])
                db.execute("DELETE FROM core.lead")

    # Path 2: direct ATTACH to lead_mirror (heavy; only if lead_attrs absent/empty).
    if not enriched and _ENABLE_MIRROR_ENRICH and os.path.exists(_LEAD_MIRROR_DB):
        try:
            db.execute("DETACH DATABASE IF EXISTS leadmirror")
            db.execute(f"ATTACH '{_LEAD_MIRROR_DB}' AS leadmirror (READ_ONLY)")
            # one row per email in the mirror (leads_current is already current-snapshot)
            db.execute(
                """
                INSERT INTO core.lead
                  (lead_key, email, phone_e164, first_name, company, segment, industry,
                   lead_source, resolution_confidence, first_seen_at, resolved_at)
                SELECT
                  s.lead_key, s.email, s.phone_e164,
                  coalesce(lm.enrich_first_name, lm.first_name)        AS first_name,
                  lm.company_name                                       AS company,
                  lm.source_list_name                                   AS segment,
                  coalesce(lm.specific_industry, lm.general_industry)   AS industry,
                  lm.source                                             AS lead_source,
                  CASE
                    WHEN s.resolution_confidence = 'phone' THEN 'phone'
                    WHEN lm.email IS NULL THEN 'unmatched'
                    ELSE 'email'
                  END                                                   AS resolution_confidence,
                  s.first_seen_at, now()
                FROM _lead_stage s
                LEFT JOIN leadmirror.mirror.leads_current lm
                  ON s.email IS NOT NULL AND lm.email = s.email
                """
            )
            db.execute("DETACH DATABASE IF EXISTS leadmirror")
            enriched = True
        except Exception as exc:  # mirror locked/missing -> fall back to attrs-NULL, never fail the build
            logger.warning("lead mirror enrich failed (%s) — attrs NULL this run", str(exc)[:160])
            db.execute("DELETE FROM core.lead")
            try:
                db.execute("DETACH DATABASE IF EXISTS leadmirror")
            except Exception:
                pass
    if not enriched:
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
