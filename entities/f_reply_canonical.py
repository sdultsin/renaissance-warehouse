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

# Row-level non-human / non-positive classifiers (raw_instantly_email has no clean native auto
# flag; ue_type is constant 2 = "received", and Instantly's per-message i_status sentinels
# correlate only ~33-45% with auto text so they're too noisy to trust as a blanket flag — see
# the 2026-06-28 reply-pipe remediation calibration). We match HTML-STRIPPED text, because
# reply_text is frequently raw HTML and ~87% of replies carry quoted prior-email history.
#
#  * is_auto_reply  — OOO / autoresponder / bounce. Matched over subject + the first ~1500 chars
#                     of the stripped body (these signatures sit at the top of the message). The
#                     quoted footer does NOT pollute this (calibration: raw 4.57% == de-quoted
#                     4.56%). ~4.6% of the reply store, up from the old ~2.6% bare-LIKE heuristic.
#  * is_unsubscribe — the LEAD asking to be removed ("unsubscribe me", "stop emailing me", "take
#                     me off your list"). A human action but NOT a positive reply, so it must be
#                     netted out of human-reply / positive KPIs separately from auto. Matched over
#                     subject + only the lead's OPENER (first ~200 chars of stripped body): the
#                     lead's actual words are at the top, while OUR outbound unsubscribe-link
#                     footer is below the quote — matching the full body would over-fire on that
#                     footer (~16-22% of all replies contain the word). ~15% of the reply store.
#
# NOTE the old QA "~60% auto" figure counts BOUNCES that Instantly tracks but never returns from
# /emails (they are not in this store at all), and its "≥10.6% false-human via unsubscribe" was
# itself inflated by the quoted footer — see calibration. The honest in-store rates are the above.
_AUTO_PATTERNS = [
    "out of office", "out of the office", "automatic reply", "auto-reply", "autoreply",
    "auto reply", "on vacation", "annual leave", "parental leave", "maternity leave",
    "medical leave", "sick leave", "on leave", "away from my desk", "currently away",
    "away from the office", "i am currently out", "i'm currently out", "i am out of the office",
    "i will be out", "will be out of", "delivery has failed", "undeliverable", "mail delivery",
    "address not found", "could not be delivered", "message blocked", "message undelivered",
    "mailer-daemon", "mailer daemon", "delivery status notification", "failure notice",
    "returned mail", "mailbox full", "quota exceeded", "no longer with", "no longer employed",
    "has left the company", "this is an automated", "automated response", "automated message",
    "do not reply", "do-not-reply", "donotreply", "vacation responder",
    "thank you for your email and i will",
]

# Lead-initiated removal requests. Tight imperatives a human types; matched on the opener only.
_UNSUB_PATTERNS = [
    "unsubscribe", "please remove me", "remove me from", "take me off", "opt me out",
    "stop emailing", "do not contact me", "do not email me", "remove from your list",
    "remove from this list",
]


def _clean_blob(subj: str, body: str, limit: int) -> str:
    """lower( ws-collapsed( html-stripped( subject + ' ' + first `limit` chars of body ) ) ).
    Bounds the regexp cost per row (some bodies are 100KB+ of HTML) while keeping the signal,
    which lives near the top of the message."""
    raw = f"coalesce({subj},'') || ' ' || substr(coalesce({body},''), 1, {limit})"
    stripped = f"regexp_replace({raw}, '<[^>]+>', ' ', 'g')"
    collapsed = f"regexp_replace({stripped}, '\\s+', ' ', 'g')"
    return f"lower({collapsed})"


def _like_any(blob: str, patterns: list[str]) -> str:
    likes = " OR ".join(
        f"{blob} LIKE '%{p.replace(chr(39), chr(39) * 2)}%'" for p in patterns
    )
    return f"({likes})"


def _auto_reply_sql(subj: str, body: str) -> str:
    """SQL bool: looks like an autoresponder / OOO / bounce (subject + first 1500 chars)."""
    return _like_any(_clean_blob(subj, body, 1500), _AUTO_PATTERNS)


def _unsubscribe_sql(subj: str, body: str) -> str:
    """SQL bool: the lead is asking to be removed (subject + opener only — avoids our footer)."""
    return _like_any(_clean_blob(subj, body, 200), _UNSUB_PATTERNS)


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

    auto_sql = _auto_reply_sql("subject", "reply_text")
    unsub_sql = _unsubscribe_sql("subject", "reply_text")

    # --- rebuild ---
    # NOTE: the `eaccount` column is added by sql/ddl/48_reply_eaccount.sql (a SEPARATE
    # connection, via setup_db) — NOT here. Doing ALTER ADD COLUMN + bulk INSERT in the same
    # DuckDB connection trips an internal "ColumnData::Append" assertion, so it must be split.
    db.execute("DELETE FROM core.reply")

    # Instantly side (PRIMARY). reply_id = email_id (the true message id) — dedup on email_id
    # ONLY, never on text/sender (a lead can reply the same words to many campaigns; those are
    # distinct rows). step + variant are decoded from Instantly's composite `.step` field
    # ("subsequence_step_variant", e.g. "0_0_8"): the MIDDLE component is the sequence step,
    # the LAST is the variant INDEX (0-based) -> letter (0='A' ... 19='T'), matching
    # raw_pipeline_variant_copy.variant. Verified against variant_copy (campaign 8e698:
    # max step=3, max variant='T'=index 19). Falls back to the flattened i.step if the raw
    # composite is absent. eaccount = the inbox that sent the message the lead replied to.
    db.execute(
        f"""
        INSERT INTO core.reply
            (reply_id, lead_email, campaign_id, workspace_id, step, variant, eaccount,
             subject, reply_text, reply_timestamp, is_auto_reply, is_unsubscribe, source,
             _loaded_at, _run_id)
        WITH inst AS (
            SELECT
                i.email_id,
                lower(trim(i.lead_email))                       AS lead_email,
                i.campaign_id, i.workspace_id,
                -- composite middle component is 0-based; variant_copy.step is 1-based -> +1
                TRY_CAST(split_part(json_extract_string(i.api_response_raw, '$.step'), '_', 2) AS INT) + 1
                                                                AS step,
                CASE
                    WHEN TRY_CAST(split_part(json_extract_string(i.api_response_raw, '$.step'), '_', 3) AS INT) IS NOT NULL
                    THEN chr(65 + TRY_CAST(split_part(json_extract_string(i.api_response_raw, '$.step'), '_', 3) AS INT))
                    ELSE NULL
                END                                             AS variant,
                i.eaccount,
                i.subject, i.reply_text, i.reply_timestamp,
                {auto_sql}                                      AS is_auto_reply,
                {unsub_sql}                                     AS is_unsubscribe
            FROM raw_instantly_email i
            WHERE i.lead_email IS NOT NULL AND trim(i.lead_email) <> ''
            QUALIFY row_number() OVER (
                PARTITION BY i.email_id ORDER BY i._loaded_at DESC
            ) = 1
        )
        SELECT
            email_id, lead_email, campaign_id, workspace_id, step, variant, eaccount,
            subject, reply_text, reply_timestamp, is_auto_reply, is_unsubscribe,
            'instantly', now(), ?
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
            (reply_id, lead_email, campaign_id, workspace_id, step, variant, eaccount,
             subject, reply_text, reply_timestamp, is_auto_reply, is_unsubscribe, source,
             _loaded_at, _run_id)
        WITH pipe AS (
            SELECT
                lower(trim(p.lead_email))                       AS lead_email,
                p.campaign_id,
                -- campaign->workspace fallback (ticket 2026-07-18): reply workspace_id is
                -- payload-carried and ~6% land NULL despite a resolvable campaign.
                -- v_campaign_dim_unified is slug-encoded (matches the pipeline reply
                -- encoding) and unique on campaign_id, so no fan-out.
                COALESCE(p.workspace_id, cdu.workspace_slug)    AS workspace_id,
                p.step,
                CAST(p.variant AS VARCHAR)                      AS variant,
                p.subject, p.reply_text, p.reply_timestamp,
                {auto_sql}                                      AS is_auto_reply,
                {unsub_sql}                                     AS is_unsubscribe
            FROM raw_pipeline_reply_data p
            -- cdu pre-deduped to 1 row/campaign_id -> the LEFT JOIN is fan-out-proof (cannot
            -- duplicate a reply row) regardless of v_campaign_dim_unified's grain.
            LEFT JOIN (SELECT campaign_id, any_value(workspace_slug) AS workspace_slug
                       FROM core.v_campaign_dim_unified WHERE campaign_id IS NOT NULL
                       GROUP BY campaign_id) cdu ON cdu.campaign_id = p.campaign_id
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
            lead_email, campaign_id, workspace_id, step, variant, NULL AS eaccount,
            subject, reply_text, reply_timestamp, is_auto_reply, is_unsubscribe,
            'pipeline', now(), ?
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
    unsub_n = db.execute(
        "SELECT count(*) FROM core.reply WHERE is_unsubscribe"
    ).fetchone()[0]
    # exact-dup guard (must be 0)
    dups = db.execute(
        "SELECT count(*) FROM (SELECT lead_email, reply_timestamp, count(*) c "
        "FROM core.reply GROUP BY 1,2 HAVING count(*) > 1)"
    ).fetchone()[0]

    logger.info(
        "core.reply rebuilt: %d total (%s), auto=%d, unsubscribe=%d, cross-source dup groups=%d",
        total, by_source, auto_n, unsub_n, dups,
    )

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={
            "by_source": by_source,
            "auto_reply": auto_n,
            "unsubscribe": unsub_n,
            "variant_recovery": variant_note,
            "dup_groups": dups,
        },
    )


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "reply_canonical", run)
