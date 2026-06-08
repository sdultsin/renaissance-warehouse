"""WS-I — derived.lead_intel + the first BI insight surfaces (Spec 16 — BI/Lead-Intent layer).

THE PAYOFF LAYER. Folds every downstream signal onto ONE wide row per signal lead
(core.lead): reply behaviour (core.reply ⋈ core.reply_intent), partner disposition
(core.lead_disposition), and funnel state (core.opportunity + core.conversion_event). The
BI views + chatbot read this single surface instead of re-joining 5 source tables per
question.

Runs in the `derived` phase (registry.add_phase('derived', 'lead_intel', run)) — the slot
already exists in core/config.py PHASE_ORDER, so PHASE_ORDER is untouched. Parent runs
canonical → intent → derived, so core.lead / core.reply / core.reply_intent /
core.opportunity / core.conversion_event are populated before us. We are DEFENSIVE anyway:
all joins are LEFT joins and tolerate empty signal tables. A full DELETE+INSERT rebuild +
CREATE OR REPLACE VIEW make the whole phase idempotent.

profile_summary (LLM synthesis) is DEFERRED — left NULL. The column exists so adding it
later is a backfill, not a schema change.

⚠ PII: lead_email + reply text + partner notes are PII; nothing is written to a git file.

Schema = sql/ddl/46_lead_intel.sql.
"""
from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.lead_intel")

_DDL = REPO_ROOT / "sql" / "ddl" / "46_lead_intel.sql"


def register(registry: Registry) -> None:
    registry.add_phase("derived", "lead_intel", run)


def _build(db) -> dict:
    """DELETE+INSERT rebuild of derived.lead_intel, then (re)build the insight views.

    Shared by run() and the local in-memory test. Assumes the source tables exist (the DDL
    applier / test harness creates them); every reference is a LEFT JOIN so emptiness is fine.
    """
    # ── reply behaviour, aggregated per lead_email over human replies ──────────
    # dominant_intent = modal primary_intent; top_objection_type = modal objection_type
    # among objection rows. last_* taken from the most-recent reply (window rank).
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE _li_reply AS
        WITH r AS (
            SELECT
                rep.lead_email,
                rep.reply_timestamp,
                rep.reply_text,
                ri.primary_intent,
                ri.intent_tags,
                ri.sentiment,
                ri.is_question,
                ri.is_objection,
                ri.objection_type,
                row_number() OVER (
                    PARTITION BY rep.lead_email ORDER BY rep.reply_timestamp DESC NULLS LAST
                ) AS rn_desc
            FROM core.reply rep
            LEFT JOIN core.reply_intent ri ON ri.reply_id = rep.reply_id
            WHERE rep.lead_email IS NOT NULL
              AND COALESCE(rep.is_auto_reply, false) = false
        ),
        -- modal primary_intent per lead
        intent_mode AS (
            SELECT lead_email, primary_intent AS dominant_intent
            FROM (
                SELECT lead_email, primary_intent,
                       row_number() OVER (
                           PARTITION BY lead_email
                           ORDER BY count(*) DESC, primary_intent
                       ) AS rk
                FROM r
                WHERE primary_intent IS NOT NULL
                GROUP BY lead_email, primary_intent
            ) WHERE rk = 1
        ),
        -- modal objection_type per lead (objection rows only)
        obj_mode AS (
            SELECT lead_email, objection_type AS top_objection_type
            FROM (
                SELECT lead_email, objection_type,
                       row_number() OVER (
                           PARTITION BY lead_email
                           ORDER BY count(*) DESC, objection_type
                       ) AS rk
                FROM r
                WHERE COALESCE(is_objection, false) = true AND objection_type IS NOT NULL
                GROUP BY lead_email, objection_type
            ) WHERE rk = 1
        ),
        -- distinct union of primary_intent + exploded intent_tags
        tags AS (
            SELECT lead_email, array_agg(DISTINCT tag) AS all_intent_tags
            FROM (
                SELECT lead_email, primary_intent AS tag FROM r WHERE primary_intent IS NOT NULL
                UNION ALL
                SELECT lead_email, unnest(intent_tags) AS tag FROM r WHERE intent_tags IS NOT NULL
            ) WHERE tag IS NOT NULL
            GROUP BY lead_email
        ),
        agg AS (
            SELECT
                lead_email,
                count(*)                                            AS n_replies,
                min(reply_timestamp)                                AS first_reply_at,
                max(reply_timestamp)                                AS last_reply_at,
                bool_or(COALESCE(is_question, false))               AS has_question,
                bool_or(COALESCE(is_objection, false))              AS has_objection,
                max(reply_text)  FILTER (WHERE rn_desc = 1)         AS last_reply_text,
                max(sentiment)   FILTER (WHERE rn_desc = 1)         AS last_sentiment
            FROM r
            GROUP BY lead_email
        )
        SELECT a.lead_email, a.n_replies, a.first_reply_at, a.last_reply_at,
               im.dominant_intent, t.all_intent_tags, a.has_question, a.has_objection,
               om.top_objection_type, a.last_reply_text, a.last_sentiment
        FROM agg a
        LEFT JOIN intent_mode im ON im.lead_email = a.lead_email
        LEFT JOIN obj_mode    om ON om.lead_email = a.lead_email
        LEFT JOIN tags        t  ON t.lead_email  = a.lead_email
        """
    )

    # ── latest partner disposition per lead_email (max source_period) ──────────
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE _li_disp AS
        SELECT lead_email, disposition AS partner_disposition, disposition_class,
               rep AS partner_rep, business_name AS partner_business_name,
               id_confidence AS partner_id_confidence, rep_notes AS partner_notes
        FROM (
            SELECT *, row_number() OVER (
                       PARTITION BY lead_email ORDER BY source_period DESC, resolved_at DESC NULLS LAST
                   ) AS rk
            FROM core.lead_disposition
            WHERE lead_email IS NOT NULL
        ) WHERE rk = 1
        """
    )

    # ── opportunity flag per lead_email ────────────────────────────────────────
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE _li_opp AS
        SELECT DISTINCT lead_email FROM core.opportunity WHERE lead_email IS NOT NULL
        """
    )

    # ── conversion (meeting) per lead — join on lead_key, fallback lead_email ───
    # latest conversion event's agent. Keyed both ways so phone-only events still land.
    db.execute(
        """
        CREATE OR REPLACE TEMP TABLE _li_conv AS
        SELECT lead_key, lead_email, conversion_agent
        FROM (
            SELECT lead_key, lead_email, conversion_agent,
                   row_number() OVER (
                       PARTITION BY COALESCE(lead_key, lead_email)
                       ORDER BY occurred_at DESC NULLS LAST
                   ) AS rk
            FROM core.conversion_event
            WHERE lead_key IS NOT NULL OR lead_email IS NOT NULL
        ) WHERE rk = 1
        """
    )

    # ── assemble: one row per core.lead ────────────────────────────────────────
    db.execute("DELETE FROM derived.lead_intel")
    db.execute(
        """
        INSERT INTO derived.lead_intel
        SELECT
            l.lead_key,
            l.email                        AS lead_email,
            l.phone_e164,
            l.first_name,
            l.company,
            l.segment,
            l.industry,
            l.lead_source,

            COALESCE(rb.n_replies, 0)      AS n_replies,
            rb.first_reply_at,
            rb.last_reply_at,
            rb.dominant_intent,
            rb.all_intent_tags,
            COALESCE(rb.has_question, false)  AS has_question,
            COALESCE(rb.has_objection, false) AS has_objection,
            rb.top_objection_type,
            rb.last_reply_text,
            rb.last_sentiment,

            d.partner_disposition,
            d.disposition_class,
            d.partner_rep,
            d.partner_business_name,
            d.partner_id_confidence,
            d.partner_notes,

            (o.lead_email IS NOT NULL)     AS is_opportunity,
            (cv.conversion_agent IS NOT NULL) AS is_meeting,
            cv.conversion_agent,

            -- funnel_stage: highest stage reached
            CASE
                WHEN cv.conversion_agent IS NOT NULL THEN 'meeting'
                WHEN o.lead_email IS NOT NULL        THEN 'opportunity'
                WHEN COALESCE(rb.n_replies, 0) > 0   THEN 'replied'
                ELSE 'lead'
            END                            AS funnel_stage,

            -- engagement_score: deterministic 0–100 composite
            LEAST(100,
                COALESCE(rb.n_replies, 0) * 10
                + CASE WHEN COALESCE(rb.has_question, false) THEN 10 ELSE 0 END
                + CASE WHEN rb.dominant_intent IN ('interested', 'info_request') THEN 25 ELSE 0 END
                + CASE WHEN rb.last_sentiment = 'positive' THEN 10 ELSE 0 END
                + CASE WHEN o.lead_email IS NOT NULL THEN 25 ELSE 0 END
                + CASE WHEN cv.conversion_agent IS NOT NULL THEN 30 ELSE 0 END
                + CASE WHEN d.disposition_class = 'live' THEN 20 ELSE 0 END
            )::INTEGER                     AS engagement_score,

            NULL                           AS profile_summary,  -- DEFERRED (LLM follow-up)
            now()                          AS resolved_at
        FROM core.lead l
        LEFT JOIN _li_reply rb ON rb.lead_email = l.email
        LEFT JOIN _li_disp  d  ON d.lead_email  = l.email
        LEFT JOIN _li_opp   o  ON o.lead_email  = l.email
        LEFT JOIN _li_conv  cv ON (cv.lead_key = l.lead_key)
                               OR (cv.lead_key IS NULL AND cv.lead_email = l.email)
        """
    )

    # ── insight VIEWS ──────────────────────────────────────────────────────────
    # v_intent_distribution — primary_intent counts/rates, sliced by campaign + segment.
    # Sourced from the reply grain (one row per reply) so a lead with N replies counts N
    # times here (distribution of intents, not of leads). Campaign comes from core.reply;
    # segment from derived.lead_intel via lead_email.
    db.execute(
        """
        CREATE OR REPLACE VIEW v_intent_distribution AS
        WITH base AS (
            SELECT
                rep.campaign_id,
                li.segment,
                ri.primary_intent
            FROM core.reply rep
            JOIN core.reply_intent ri ON ri.reply_id = rep.reply_id
            LEFT JOIN derived.lead_intel li ON li.lead_email = rep.lead_email
            WHERE COALESCE(rep.is_auto_reply, false) = false
              AND ri.primary_intent IS NOT NULL
        )
        SELECT
            campaign_id,
            segment,
            primary_intent,
            count(*) AS replies,
            round(100.0 * count(*)
                  / sum(count(*)) OVER (PARTITION BY campaign_id, segment), 1) AS pct
        FROM base
        GROUP BY campaign_id, segment, primary_intent
        ORDER BY campaign_id, segment, replies DESC
        """
    )

    # v_objection_library — objection_type × example summaries × frequency.
    db.execute(
        """
        CREATE OR REPLACE VIEW v_objection_library AS
        SELECT
            COALESCE(ri.objection_type, 'unspecified') AS objection_type,
            count(*)                                   AS occurrences,
            count(DISTINCT rep.lead_email)             AS leads,
            list(DISTINCT ri.summary)
                FILTER (WHERE ri.summary IS NOT NULL)  AS example_summaries
        FROM core.reply_intent ri
        JOIN core.reply rep ON rep.reply_id = ri.reply_id
        WHERE COALESCE(ri.is_objection, false) = true
        GROUP BY 1
        ORDER BY occurrences DESC
        """
    )

    # v_question_library — top questions: reply summaries where is_question.
    db.execute(
        """
        CREATE OR REPLACE VIEW v_question_library AS
        SELECT
            COALESCE(ri.summary, '(no summary)') AS question_summary,
            COALESCE(ri.primary_intent, 'unknown') AS primary_intent,
            count(*)                       AS occurrences,
            count(DISTINCT rep.lead_email) AS leads,
            max(rep.reply_timestamp)       AS last_asked_at
        FROM core.reply_intent ri
        JOIN core.reply rep ON rep.reply_id = ri.reply_id
        WHERE COALESCE(ri.is_question, false) = true
        GROUP BY 1, 2
        ORDER BY occurrences DESC, last_asked_at DESC NULLS LAST
        """
    )

    n_rows = db.execute("SELECT count(*) FROM derived.lead_intel").fetchone()[0]
    n_replied = db.execute("SELECT count(*) FROM derived.lead_intel WHERE n_replies > 0").fetchone()[0]
    n_disp = db.execute(
        "SELECT count(*) FROM derived.lead_intel WHERE partner_disposition IS NOT NULL"
    ).fetchone()[0]
    n_meeting = db.execute("SELECT count(*) FROM derived.lead_intel WHERE is_meeting").fetchone()[0]
    return {"leads": n_rows, "with_reply": n_replied, "with_disposition": n_disp,
            "with_meeting": n_meeting}


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent: creates derived schema + derived.lead_intel
    stats = _build(db)
    logger.info(
        "lead_intel: %d leads (%d replied, %d partner-disposition, %d meeting)",
        stats["leads"], stats["with_reply"], stats["with_disposition"], stats["with_meeting"])
    return PhaseResult(rows_in=stats["leads"], rows_out=stats["leads"], notes=stats)
