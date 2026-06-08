"""Canonical reply fact (Spec 16 — BI/Lead-Intent layer, WS-C, object: Reply).

Builds core.reply: ONE row per inbound human reply, consolidating

  * raw_instantly_email      — PRIMARY / current source (direct-Instantly ingest, Instantly-wins)
  * raw_pipeline_reply_data  — historical FALLBACK for pre-cutover replies that predate the
                               direct-Instantly ingest (the pipeline-Supabase mirror, being retired)

deduped on (lead_email, thread_id, reply_timestamp). Carries the recovered `variant`
(consumed from v_reply_enriched — NOT re-implemented; NULL when that view is absent or the
variant is unrecoverable) and a derived `is_auto_reply`.

Idempotent: full DELETE + INSERT rebuild every run (the source raw tables are the system of
record; this is a cheap projection over ~130k + ~410k rows). Re-running converges, never dups.

⚠ PII: reply_text + lead_email. Nothing here is written to a git-tracked file.

Registers under the `canonical` phase. Schema = sql/ddl/43_reply_intent.sql.
"""
from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.reply_canonical")

_DDL = REPO_ROOT / "sql" / "ddl" / "43_reply_intent.sql"

# Heuristic auto-reply detector (raw_instantly_email has no native auto flag; ue_type is
# constant 2 = "received"). Mirrors the kind of signal the dead pipeline classifier's
# `label_auto`/`body_auto` produced. Lower-cased LIKE match over subject || ' ' || reply_text.
_AUTO_PATTERNS = [
    "out of office", "out of the office", "automatic reply", "auto-reply", "autoreply",
    "auto reply", "on vacation", "annual leave", "away from my desk", "currently away",
    "away from the office", "i am currently out", "i'm currently out", "will be out of",
    "delivery has failed", "undeliverable", "mail delivery", "address not found",
    "could not be delivered", "message blocked", "no longer with", "no longer employed",
    "has left the company", "thank you for your email and i will",
]


def _auto_reply_sql(subj: str, body: str) -> str:
    """SQL boolean expression: true if the subject/body looks like an autoresponder/bounce."""
    blob = f"lower(coalesce({subj},'') || ' ' || coalesce({body},''))"
    likes = " OR ".join(
        f"{blob} LIKE '%{p.replace(chr(39), chr(39) * 2)}%'" for p in _AUTO_PATTERNS
    )
    return f"({likes})"


def _has_view(db, name: str) -> bool:
    return db.execute(
        "SELECT count(*) FROM duckdb_views() WHERE view_name = ?", [name]
    ).fetchone()[0] > 0


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent CREATE TABLE IF NOT EXISTS

    # --- variant recovery: consume v_reply_enriched if it exists (do NOT re-implement) ---
    use_enriched = _has_view(db, "v_reply_enriched")
    if use_enriched:
        # v_reply_enriched recovers `variant` per Instantly reply. Join on email_id when the
        # view exposes it, else on the dedup key. We probe its columns to stay decoupled.
        cols = {
            r[0]
            for r in db.execute(
                "SELECT column_name FROM duckdb_columns() WHERE table_name = 'v_reply_enriched'"
            ).fetchall()
        }
        if "email_id" in cols and "variant" in cols:
            variant_join = (
                "LEFT JOIN (SELECT DISTINCT email_id, CAST(variant AS VARCHAR) AS variant "
                "FROM v_reply_enriched WHERE email_id IS NOT NULL) ve ON ve.email_id = i.email_id"
            )
            variant_expr = "ve.variant"
            variant_note = "v_reply_enriched (email_id join)"
        elif {"lead_email", "reply_timestamp", "variant"} <= cols:
            variant_join = (
                "LEFT JOIN (SELECT DISTINCT lead_email, reply_timestamp, CAST(variant AS VARCHAR) AS variant "
                "FROM v_reply_enriched) ve "
                "ON ve.lead_email = i.lead_email AND ve.reply_timestamp = i.reply_timestamp"
            )
            variant_expr = "ve.variant"
            variant_note = "v_reply_enriched (lead/ts join)"
        else:
            variant_join = ""
            variant_expr = "CAST(NULL AS VARCHAR)"
            variant_note = "v_reply_enriched present but lacks usable cols -> variant NULL"
    else:
        variant_join = ""
        variant_expr = "CAST(NULL AS VARCHAR)"
        variant_note = "v_reply_enriched ABSENT -> variant NULL (per spec)"

    logger.info("variant recovery: %s", variant_note)

    auto_instantly = _auto_reply_sql("subject", "reply_text")
    auto_pipeline = _auto_reply_sql("subject", "reply_text")

    # --- rebuild ---
    db.execute("DELETE FROM core.reply")

    # Instantly side (PRIMARY). reply_id = email_id. Dedup within Instantly on the 3-tuple,
    # keeping the newest-loaded email_id per key (QUALIFY row_number).
    db.execute(
        f"""
        INSERT INTO core.reply
        WITH inst AS (
            SELECT
                i.email_id,
                lower(trim(i.lead_email))                       AS lead_email,
                i.campaign_id, i.workspace_id, i.step,
                i.subject, i.reply_text, i.reply_timestamp, i.thread_id,
                {variant_expr}                                  AS variant,
                {auto_instantly}                                AS is_auto_reply
            FROM raw_instantly_email i
            {variant_join}
            WHERE i.lead_email IS NOT NULL AND trim(i.lead_email) <> ''
            QUALIFY row_number() OVER (
                PARTITION BY lower(trim(i.lead_email)), i.thread_id, i.reply_timestamp
                ORDER BY i._loaded_at DESC, i.email_id
            ) = 1
        )
        SELECT
            email_id AS reply_id, lead_email, campaign_id, workspace_id, step, variant,
            subject, reply_text, reply_timestamp, is_auto_reply,
            'instantly' AS source, now() AS _loaded_at, ? AS _run_id
        FROM inst
        """,
        [ctx.run_id],
    )

    # Pipeline side (historical FALLBACK). Only insert rows NOT already covered by an Instantly
    # reply (same lead_email + reply_timestamp — raw_pipeline_reply_data has no thread_id, so we
    # use the loosest safe cross-source guard to avoid double-counting the same physical reply).
    # reply_id = stable md5 hash over the dedup key (no email_id upstream).
    db.execute(
        f"""
        INSERT INTO core.reply
        WITH pipe AS (
            SELECT
                lower(trim(p.lead_email))                       AS lead_email,
                p.campaign_id, p.workspace_id, p.step,
                CAST(p.variant AS VARCHAR)                      AS variant,
                p.subject, p.reply_text, p.reply_timestamp,
                {auto_pipeline}                                 AS is_auto_reply
            FROM raw_pipeline_reply_data p
            WHERE p.lead_email IS NOT NULL AND trim(p.lead_email) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM core.reply r
                  WHERE r.lead_email = lower(trim(p.lead_email))
                    AND r.reply_timestamp = p.reply_timestamp
              )
            QUALIFY row_number() OVER (
                PARTITION BY lower(trim(p.lead_email)), p.reply_timestamp
                ORDER BY p.reply_timestamp
            ) = 1
        )
        SELECT
            md5(lead_email || '|' || coalesce(CAST(reply_timestamp AS VARCHAR), '')) AS reply_id,
            lead_email, campaign_id, workspace_id, step, variant,
            subject, reply_text, reply_timestamp, is_auto_reply,
            'pipeline' AS source, now() AS _loaded_at, ? AS _run_id
        FROM pipe
        """,
        [ctx.run_id],
    )

    total = db.execute("SELECT count(*) FROM core.reply").fetchone()[0]
    by_source = dict(
        db.execute("SELECT source, count(*) FROM core.reply GROUP BY 1").fetchall()
    )
    auto_n = db.execute(
        "SELECT count(*) FROM core.reply WHERE is_auto_reply"
    ).fetchone()[0]
    # exact-dup guard (must be 0)
    dups = db.execute(
        "SELECT count(*) FROM (SELECT lead_email, reply_timestamp, count(*) c "
        "FROM core.reply GROUP BY 1,2 HAVING count(*) > 1)"
    ).fetchone()[0]

    logger.info(
        "core.reply rebuilt: %d total (%s), auto=%d, cross-source dup groups=%d",
        total, by_source, auto_n, dups,
    )

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={
            "by_source": by_source,
            "auto_reply": auto_n,
            "variant_recovery": variant_note,
            "dup_groups": dups,
        },
    )


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "reply_canonical", run)
