"""core.meeting canonical entity — split-sourced at the 2026-06-01 cutover (WS-E re-platform).

  posted_at <  2026-06-01  -> Slack scrape (raw_pipeline_meetings_booked_raw), source='slack'.
                              LEGACY: left untouched (the sheet may not be accurate pre-June-1).
  posted_at >= 2026-06-01  -> Funding-Form Google Sheet (raw_sheets_funding_form_data),
                              source='sheet'. SOURCE OF TRUTH: it carries an explicit Channel
                              (Email/SMS/WhatsApp/Call/LinkedIn), Campaign Manager, Campaign Name
                              and lead Email per booking — so meetings attribute DIRECTLY (no fuzzy
                              keyword splitting on raw_text, the P2 over-count root cause) and link
                              to leads by email (which the Slack source could never do).

The sheet load happens in the 'sheets' phase (04:15) BEFORE this 'canonical' phase (05:30); the
meetings_refresh.sh cron stages+loads the sheet immediately before rebuilding. Idempotent full
rebuild each run (core.meeting is a pure projection of its two raw sources).

Campaign attribution for sheet rows = main.norm_campaign_name() join to the campaign universe
(raw_pipeline_campaigns); ~97.5% of email submissions resolve directly. The residual (genuine
non-campaign labels / truncated names) land with campaign_id NULL and match_method='unmatched' —
those are the campaigns flagged as carrying inaccurate data (per the DoD), and the 4-tier fallback
matcher is the safety net for them.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.meeting")

RAW = "raw_pipeline_meetings_booked_raw"
FF = "main.raw_sheets_funding_form_data"
CUTOVER = "2026-06-01"  # sheet is canonical on/after this date; Slack stays untouched before it
# NOTE (boundary): Slack posted_at is TIMESTAMPTZ→DATE in UTC; the sheet's Submission time is a naive
# local timestamp→DATE. The two branches are disjoint (< vs >=) so DOUBLE-counting is impossible. A
# near-midnight booking on 2026-05-31 could land in neither branch (single-digit, one-time gap on the
# transition day only). Accepted: pre-cutover data is explicitly "may not be accurate"; not worth a
# TZ-normalization pass for a handful of boundary-day meetings.

# Funding-Form 'Data' tab column positions (0-based), confirmed 2026-06-13. Keep in sync with
# scripts/stage_funding_form.py EXPECTED_HEADER (the producer fails loud on column drift).
_C_SUBMISSION_ID = 1
_C_SUBMISSION_TIME = 2
_C_CHANNEL = 3
_C_PARTNER = 4
_C_ADVISOR = 5          # "<PARTNER_PREFIX>: <Full Name>" e.g. "BTC: Jett Lurvey" (portal gap dim, DDL 70)
_C_EMAIL = 9
_C_INBOX_MANAGER = 15   # Inbox Manager; some rows are first-name-only -> resolved to full names (DDL 70)
_C_CAMPAIGN_MANAGER = 17
_C_CAMPAIGN_NAME = 18

# Advisor partner-prefix -> canonical partner name (matches the portal whName keys + the
# generator's PARTNER_NORM). The advisor cell is "<prefix>: <full name>"; the prefix is the
# partner shorthand. Unknown prefixes pass through unchanged (advisor_partner = the prefix).
_ADVISOR_PARTNER_MAP = {
    "BTC": "Big Think Capital",
    "GQ": "GoQualifi",
    "GBC": "GreenBridge Capital",
    "Llama": "Llama",
    "Infusion": "Infusion",
    "Capfront": "Capfront",
    "Clarify": "Clarify",
}

# Inbox-Manager first-name-only -> full-name normalization (captured 2026-06-14 from the live
# Funding-Form; every first name resolves 1:1 EXCEPT "Jamie" which is ambiguous (Jamie Isla vs
# Jamie Solis) and is intentionally left unresolved). Applied so all-time IM leaderboards don't
# split a person across their first-name and full-name spellings.
_IM_FIRST_TO_FULL = {
    "Anjanette": "Anjanette Manayao",
    "April": "April Bagahansol",
    "Erwell": "Erwell Pacot",
    "Frank": "Frank Intong",
    "Jamil": "Jamil Matias",
    "Jessica": "Jessica Dumlao",
    "Kenneth": "Kenneth Bondoc",
    "Larrabel": "Larrabel Cardoza",
    "Madel": "Madel Pantaleon",
    "Monique": "Monique Andrade",
    "Nikko": "Nikko Macarandan",
    "Norman": "Norman Pascua",
    "Ramir": "Ramir Velasquez",
    "Robert": "Robert Bat-og",
    "William": "William Isla",
    # "Jamie" deliberately OMITTED — ambiguous (Jamie Isla / Jamie Solis); leave as-is.
}


def _advisor_partner_case(col_expr: str) -> str:
    """Build a SQL CASE mapping the advisor prefix (text before the first ':') to a partner."""
    pfx = f"trim(split_part({col_expr}, ':', 1))"
    whens = "\n".join(
        f"            WHEN {pfx} = '{k}' THEN '{v}'" for k, v in _ADVISOR_PARTNER_MAP.items()
    )
    # Unknown prefix -> the prefix itself (so it's never silently dropped). No advisor -> NULL.
    return (
        f"CASE WHEN {col_expr} IS NULL THEN NULL\n{whens}\n"
        f"            ELSE NULLIF({pfx}, '') END"
    )


def _im_norm_case(col_expr: str) -> str:
    """Build a SQL CASE resolving first-name-only inbox managers to full names."""
    whens = "\n".join(
        f"            WHEN {col_expr} = '{k}' THEN '{v}'" for k, v in _IM_FIRST_TO_FULL.items()
    )
    return f"CASE\n{whens}\n            ELSE {col_expr} END"


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "meeting", run_meeting)


def run_meeting(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # Idempotent full rebuild — core.meeting is a pure projection of its raw sources.
    db.execute("DELETE FROM core.meeting")

    # -- 1. Pre-cutover Slack rows (date < 2026-06-01) — unchanged legacy behavior, channel/
    #       lead_email left NULL (the Slack source has neither). Date-grain boundary so there is
    #       no overlap/gap with the sheet rows.
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, match_method, match_confidence, is_duplicate_of,
           cost_per_meeting_usd_estimated, raw_text)
        SELECT
          COALESCE(channel_id,'') || ':' || COALESCE(message_ts,'') || ':' ||
            COALESCE(CAST(line_index AS VARCHAR),'')  AS meeting_id,
          'slack'              AS source,
          CAST(id AS VARCHAR)  AS source_event_id,
          posted_at,
          partner,
          campaign_id,
          campaign_name_raw,
          NULL                 AS cm,            -- filled from campaign join below
          match_method,
          match_confidence,
          NULL                 AS is_duplicate_of,
          NULL                 AS cost_per_meeting_usd_estimated,
          raw_text
        FROM {RAW}
        WHERE id IS NOT NULL
          AND CAST(posted_at AS DATE) < DATE '{CUTOVER}'
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY COALESCE(channel_id,'') || ':' || COALESCE(message_ts,'') || ':' ||
            COALESCE(CAST(line_index AS VARCHAR),'')
          ORDER BY posted_at
        ) = 1
        """
    )

    # -- 2. Post-cutover sheet rows (date >= 2026-06-01). Parse row_json by position, type the
    #       timestamp, dedup on Submission ID (synthetic md5 key for the ~12 blank IDs), and
    #       resolve campaign_id via the normalized-name join (arg_max -> most recent campaign of
    #       that normalized name). cm = uppercased Campaign Manager to align with the dim's casing.
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, channel, lead_email, match_method, match_confidence,
           is_duplicate_of, cost_per_meeting_usd_estimated, raw_text,
           advisor, advisor_name, advisor_partner, inbox_manager)
        WITH ff AS (
          SELECT
            NULLIF(json_extract_string(row_json, '$[{_C_SUBMISSION_ID}]'),'')   AS submission_id,
            json_extract_string(row_json, '$[{_C_SUBMISSION_TIME}]')            AS submission_time,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_CHANNEL}]')),'')   AS channel,
            NULLIF(json_extract_string(row_json, '$[{_C_PARTNER}]'),'')         AS partner,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_ADVISOR}]')),'')   AS advisor,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_INBOX_MANAGER}]')),'') AS inbox_manager_raw,
            NULLIF(lower(trim(json_extract_string(row_json, '$[{_C_EMAIL}]'))),'') AS lead_email,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_CAMPAIGN_MANAGER}]')),'') AS campaign_manager,
            NULLIF(json_extract_string(row_json, '$[{_C_CAMPAIGN_NAME}]'),'')   AS campaign_name,
            row_json
          FROM {FF}
          -- sheets_mirror only purges the CURRENT run's rows (last-known-good semantics), so this
          -- raw table ACCUMULATES one snapshot per run. Read ONLY the latest snapshot so corrections
          -- (same Submission ID, edited cells) win and the projection stays a true idempotent rebuild.
          WHERE _run_id = (SELECT _run_id FROM {FF} ORDER BY _loaded_at DESC LIMIT 1)
            AND row_index > 0
        ),
        ff_typed AS (
          SELECT *, TRY_CAST(submission_time AS TIMESTAMP) AS posted_ts FROM ff
          WHERE TRY_CAST(submission_time AS TIMESTAMP) IS NOT NULL
            AND CAST(TRY_CAST(submission_time AS TIMESTAMP) AS DATE) >= DATE '{CUTOVER}'
        ),
        ff_keyed AS (
          SELECT *, 'sheet:' || COALESCE(submission_id, md5(row_json)) AS meeting_id FROM ff_typed
        ),
        ff_dedup AS (
          SELECT * FROM ff_keyed
          QUALIFY ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY posted_ts) = 1
        ),
        camp AS (  -- one campaign per normalized name: most-recent, fully deterministic tiebreak
          SELECT nk, campaign_id FROM (
            SELECT main.norm_campaign_name(name) AS nk, campaign_id,
                   ROW_NUMBER() OVER (PARTITION BY main.norm_campaign_name(name)
                                      ORDER BY instantly_created_at DESC NULLS LAST, campaign_id DESC) AS rn
            FROM main.raw_pipeline_campaigns
            WHERE name IS NOT NULL AND name <> ''
          ) WHERE rn = 1
        ),
        matched AS (
          SELECT f.*, c.campaign_id AS matched_cid
          FROM ff_dedup f
          LEFT JOIN camp c ON c.nk = main.norm_campaign_name(f.campaign_name)
        )
        SELECT
          meeting_id,
          'sheet'                                  AS source,
          submission_id                            AS source_event_id,
          posted_ts                                AS posted_at,
          partner,
          matched_cid                              AS campaign_id,
          campaign_name                            AS campaign_name_raw,
          upper(campaign_manager)                  AS cm,
          channel,
          lead_email,
          CASE WHEN matched_cid IS NOT NULL THEN 'sheet_norm' ELSE 'unmatched' END AS match_method,
          CASE WHEN matched_cid IS NOT NULL THEN 1.0 ELSE NULL END                 AS match_confidence,
          NULL                                     AS is_duplicate_of,
          NULL                                     AS cost_per_meeting_usd_estimated,
          NULL                                     AS raw_text,
          advisor                                  AS advisor,            -- raw "<prefix>: <name>"
          NULLIF(trim(regexp_replace(advisor, '^[^:]*:', '')), '') AS advisor_name,  -- name after the prefix
          {_advisor_partner_case('advisor')}       AS advisor_partner,
          {_im_norm_case('inbox_manager_raw')}     AS inbox_manager
        FROM matched
        """
    )

    # -- 2b. Campaign-id BACKFILL for sheet email rows the norm-name join missed (handoff B2).
    #        The sheet's Campaign Name is sometimes truncated ("... - (EYVE" with no closing paren,
    #        which defeats norm_campaign_name's trailing "- (Name)" strip), renamed, or a genuine
    #        non-campaign label ("No Campaign", a pasted email). But a sheet meeting is booked OFF a
    #        lead's reply, so recover the campaign_id from that lead's reply: prefer the most-recent
    #        reply AT/BEFORE the meeting (the one that triggered the booking), else the nearest reply.
    #        Only touches source='sheet' channel='Email' rows still NULL after the name join, and only
    #        where the lead has a real reply carrying a campaign_id. Empirically lifts sheet email
    #        attribution ~95.2% -> ~99.5% (verified read-only 2026-06-18). match_method records the
    #        provenance so it is never confused with an exact name match. Pre-cutover Slack rows have
    #        no lead_email and are untouched (their NULLs are the irreducible '(unattributed)' bucket).
    db.execute(
        """
        UPDATE core.meeting AS m
        SET campaign_id = b.campaign_id,
            match_method = 'email_reply_backfill',
            match_confidence = 0.9
        FROM (
          SELECT meeting_id, campaign_id FROM (
            SELECT m2.meeting_id, r.campaign_id,
                   ROW_NUMBER() OVER (
                     PARTITION BY m2.meeting_id
                     ORDER BY (r.reply_timestamp <= m2.posted_at) DESC,                 -- replies at/before the meeting first
                              CASE WHEN r.reply_timestamp <= m2.posted_at
                                   THEN m2.posted_at - r.reply_timestamp END ASC NULLS LAST,  -- then nearest before
                              r.reply_timestamp DESC                                     -- else nearest after
                   ) AS rn
            FROM core.meeting m2
            JOIN main.raw_pipeline_reply_data r ON lower(r.lead_email) = m2.lead_email
            WHERE m2.source = 'sheet' AND m2.channel = 'Email' AND m2.campaign_id IS NULL
              AND m2.lead_email IS NOT NULL AND m2.lead_email <> ''
              AND r.campaign_id IS NOT NULL
          ) WHERE rn = 1
        ) AS b
        WHERE m.meeting_id = b.meeting_id
        """
    )

    # CM attribution fallback (slack rows + any sheet row with a blank Campaign Manager but a
    # resolved campaign): campaign join first (authoritative), then regex on raw_text. Sheet rows
    # already carry cm and have raw_text NULL, so the regex passes only touch Slack rows.
    db.execute(
        """
        UPDATE core.meeting AS m
        SET cm = c.cm
        FROM core.campaign c
        WHERE m.campaign_id = c.campaign_id AND m.cm IS NULL AND c.cm IS NOT NULL
        """
    )
    db.execute(
        r"""
        UPDATE core.meeting
        SET cm = upper(regexp_extract(raw_text, '\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\b', 1))
        WHERE cm IS NULL
          AND raw_text IS NOT NULL
          AND regexp_extract(raw_text, '\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\b', 1) <> ''
        """
    )
    # Mixed-case fallback for the "<CM>: <campaign>" Slack line ("Ido: MCA …").
    db.execute(
        r"""
        UPDATE core.meeting
        SET cm = upper(regexp_extract(raw_text, '(?i)\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\s*:', 1))
        WHERE cm IS NULL
          AND raw_text IS NOT NULL
          AND regexp_extract(raw_text, '(?i)\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\s*:', 1) <> ''
        """
    )

    n = db.execute("SELECT count(*) FROM core.meeting").fetchone()[0]
    by_source = db.execute(
        "SELECT source, count(*) FROM core.meeting GROUP BY 1 ORDER BY 1"
    ).fetchall()
    sheet_unmatched = db.execute(
        "SELECT count(*) FROM core.meeting WHERE source='sheet' AND channel='Email' AND campaign_id IS NULL"
    ).fetchone()[0]
    sheet_backfilled = db.execute(
        "SELECT count(*) FROM core.meeting WHERE match_method='email_reply_backfill'"
    ).fetchone()[0]
    sheet_email_total = db.execute(
        "SELECT count(*) FROM core.meeting WHERE source='sheet' AND channel='Email'"
    ).fetchone()[0]
    sheet_attr_pct = round(100.0 * (sheet_email_total - sheet_unmatched) / sheet_email_total, 2) if sheet_email_total else None
    logger.info("core.meeting rebuilt: %d rows %s; sheet email-meetings unmatched=%d "
                "(reply-backfilled=%d, sheet-email-attribution=%.2f%%)",
                n, dict(by_source), sheet_unmatched, sheet_backfilled, sheet_attr_pct or 0.0)
    return PhaseResult(
        rows_in=n, rows_out=n,
        notes={"by_source": dict(by_source), "sheet_email_unmatched": sheet_unmatched,
               "sheet_email_backfilled": sheet_backfilled, "sheet_email_attr_pct": sheet_attr_pct,
               "cutover": CUTOVER},
    )
