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
non-campaign labels / truncated names) land with campaign_id NULL and match_method='unmatched'.
The real match_method vocabulary (no "4-tier matcher" exists — DATA-2 docstring fix 2026-07-01):
sheet-era: 'sheet_norm' (norm-name join) | 'unmatched' | 'partner_sheet' (step 2.5) |
'email_reply_backfill' (step 2b) | 'sms_phone_sendivo' (step 2e); slack-era rows carry the legacy
matcher values ('exact' | 'alias' | 'normalized' | 'manual*' | 'llm_*' | ..., see 20_meeting.sql).

WS5 v3 [2026-06-21] — canonical business-date + workspace-normalized D6 attribution + D7 SMS split:
  * meeting_date  = col A "Date" (the business date Grace + the sheet pivots use); the sheet rows
                    now GATE on col A and downstream day-buckets move to meeting_date. posted_at is
                    KEPT as col C (submission timestamp) for back-compat/audit only.
  * workspace_*   = col O "Workspace", resolved through core.workspace_alias (NOT a hand CASE —
                    design RB3/D6). The alias table maps every name variant ("Funding 2" ==
                    "Funding 2 (Ido)") -> one (canonical_current_name, warehouse_slug, cm).
  * cm_workspace  = alias.cm — the WORKSPACE-credited CM (D6). Non-NULL ONLY for the 5 funding-CM
                    workspaces. This is the portal's ONLY CM-credit source, which is what makes
                    IDO = Funding-2-only hold. The raw col-17 `cm` is kept UNCHANGED as the audit
                    truth (Grace types "IDO" on warm-leads/SMS/DFY work — the leak class D6 kills:
                    Net Jun-19 portal IDO email 24 -> 8).
  * program/offer/sendivo_sub_account (D7) — SMS splits Funding (Sendivo Renaissance 1) vs Pre-IPO
                    (Renaissance 2) off the anchored col-O label; email offer inherited from
                    core.campaign.offer post-insert.
  HARD DEP: core.workspace_alias (created+seeded self-contained in the 107 DDL, block 107.0a) MUST
            exist before this phase runs.

Pre-IPO partner desks [2026-06-25] — step 2.5 adds a THIRD class of source='sheet' rows from the
  partner booking sheets (raw_sheets_summit_ventures_leads / raw_sheets_collins_preipo_leads), the
  missing Pre-IPO MEETING source (the Funding-Form is Business-Funding-only). offer/program='Pre-IPO',
  channel per-row from "Sending Account", meeting_id namespaced 'summit:'/'collins:' so the Funding-Form-
  only logic ('sheet:%' guards on 2b/2c and the attribution health metrics) never touches them. See the
  PARTNER_BOOKING_SHEETS block + DDL partner_booking_sheets.sql + scripts/stage_partner_booking_sheets.py.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.meeting")

RAW = "raw_pipeline_meetings_booked_raw"
FF = "main.raw_sheets_funding_form_data"
IMB = "raw_im_bookings"                       # the bookings-portal mirror (entities/im_bookings.py)
IMB_SOURCE_TAG = "portal_im_bookings_nightly" # the live (not frozen 2026-05-31) snapshot tag
CUTOVER = "2026-06-01"  # sheet is canonical on/after this date; Slack stays untouched before it
IMB_CUTOVER = "2026-06-29"  # im_bookings is the Funding booking source on/after this date.
# NOTE (boundary): Slack posted_at is TIMESTAMPTZ→DATE in UTC; the sheet's Submission time is a naive
# local timestamp→DATE. The two branches are disjoint (< vs >=) so DOUBLE-counting is impossible. A
# near-midnight booking on 2026-05-31 could land in neither branch (single-digit, one-time gap on the
# transition day only). Accepted: pre-cutover data is explicitly "may not be accurate"; not worth a
# TZ-normalization pass for a handful of boundary-day meetings.
# WS5 v3: the sheet branch now gates on col A (meeting_date), with a col-C-date fallback. CUTOVER stays
# 2026-06-01 so the slack(<=May-31) / sheet(>=Jun-01) branches never overlap on either date column.
#
# im_bookings cutover [2026-06-30, handoffs/2026-06-29-meeting-source-rewire-im-bookings-BUILD.md] —
# THREE date-disjoint Funding sources now feed source='sheet', so no booking is ever double-counted:
#   meeting_date <  2026-06-01  -> Slack (step 1, source='slack')
#   meeting_date in [06-01, 06-28] -> Funding-Form Google Sheet (step 2, meeting_id 'sheet:%') — FROZEN:
#                                  the sheet is RETIRED, so this reads its last-good snapshot for history.
#   meeting_date >= 2026-06-29  -> bookings-portal im_bookings (step 2-IMB, meeting_id 'imb:%'). The
#                                  portal is now the channel-rich canonical Funding source (native
#                                  channel/advisor/workspace/email/phone per booking). source stays
#                                  'sheet' (40+ views gate on it — hard contract). FORWARD-ONLY: we do
#                                  NOT rebuild pre-06-29 history from im_bookings (it is materially
#                                  incomplete before the cutover — would silently delete real meetings).
#   (Pre-IPO partner desks, step 2.5, are unchanged — im_bookings is 100% Funding.)

# -- Pre-IPO partner booking desks (2026-06-25, deliverables/2026-06-25-partner-booking-sheets-ingest) --
# Pre-IPO meetings are logged by partner desks in their OWN Google Sheets — the master Funding-Form is
# Business-Funding-only (verified: core.meeting had ZERO Pre-IPO meetings). These land as additional
# source='sheet' rows so every existing source='sheet' funnel / SMS-dashboard / omnichannel view picks
# them up automatically, distinguished by `partner` and a namespaced meeting_id ('summit:'/'collins:' vs
# the Funding-Form 'sheet:'). offer/program='Pre-IPO' for ALL rows (partner-desk level; corroborated by
# core.v_channel_offer mapping Summit's funding4doctors_llc -> Pre-IPO + Collins's explicit Pre-IPO
# campaigns/investor-qual columns). channel is per-row from the "Sending Account" cell. campaign_id stays
# NULL (the cell is a partner SMS blast/script name, not an Instantly campaign) -> the Funding-Form-only
# backfills (2b/2c) are gated OFF partner rows by the 'sheet:%' meeting_id prefix. NOT gated on CUTOVER:
# there is no Slack/Funding-Form coverage of Pre-IPO (verified 0 overlap), so pre-Jun-1 Collins rows have
# no competing source to double-count against.  (raw_table, partner_label, id_prefix)
PARTNER_BOOKING_SHEETS = [
    ("main.raw_sheets_summit_ventures_leads", "Summit Ventures",             "summit"),
    ("main.raw_sheets_collins_preipo_leads",  "Collins Investment Partners", "collins"),
]
# Partner-sheet column positions (0-based) — Summit & Collins share the same leading layout.
_P_DATE = 0
_P_EMAIL = 3
_P_SENDING_ACCOUNT = 6
_P_CAMPAIGN = 7
_P_BOOKED_DATE = 8
_P_ADVISOR = 10

# Funding-Form 'Data' tab column positions (0-based), confirmed 2026-06-13. Keep in sync with
# scripts/stage_funding_form.py EXPECTED_HEADER (the producer fails loud on column drift).
_C_DATE = 0             # col A "Date" — canonical business date (the sheet pivots + Grace use this)
_C_SUBMISSION_ID = 1
_C_SUBMISSION_TIME = 2
_C_CHANNEL = 3
_C_PARTNER = 4
_C_ADVISOR = 5          # "<PARTNER_PREFIX>: <Full Name>" e.g. "BTC: Jett Lurvey" (portal gap dim, DDL 70)
_C_EMAIL = 9
_C_WORKSPACE = 14       # col O "Workspace" — drives workspace_slug/cm/program/offer via core.workspace_alias
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


def _partner_channel_case(col_expr: str) -> str:
    """Per-row channel from the partner sheet's "Sending Account" cell. Returns the canonical
    Email/SMS/WhatsApp vocabulary; an unrecognized value -> '(unmapped)' (never guessed)."""
    return (
        f"CASE WHEN lower({col_expr}) = 'sms' THEN 'SMS'\n"
        f"             WHEN lower({col_expr}) = 'whatsapp' THEN 'WhatsApp'\n"
        f"             WHEN {col_expr} = 'Email' OR {col_expr} LIKE '%@%' THEN 'Email'\n"
        f"             ELSE '(unmapped)' END"
    )


def _partner_union_sql() -> str:
    """UNION ALL of each partner sheet's LATEST snapshot (partner label + id prefix + row_json).
    Mirrors the Funding-Form 'read only the latest _run_id' rule so corrections win and the projection
    stays a true idempotent rebuild."""
    parts = []
    for table, partner, pfx in PARTNER_BOOKING_SHEETS:
        parts.append(
            f"  SELECT '{partner}' AS partner, '{pfx}' AS pfx, row_json\n"
            f"    FROM {table}\n"
            f"   WHERE _run_id = (SELECT _run_id FROM {table} ORDER BY _loaded_at DESC LIMIT 1)\n"
            f"     AND row_index > 0"
        )
    return "\n  UNION ALL\n".join(parts)


def _im_dedup_pairs(rows: list) -> list:
    """email-OR-phone UNION-FIND dedup for im_bookings meetings (Sam's rule, 2026-06-30): within a
    single booking date, collapse rows that share *either* a normalized email OR a normalized phone
    (a connected component, NOT a COALESCE(email,phone) partition — so emailA/phoneP + emailB/phoneP
    merge via the shared phone). Canonical = the EARLIEST created_at (NULLS LAST, then id asc); every
    other member is a duplicate. Different-day bookings are never merged. SOFT-FLAG only: returns the
    (duplicate_meeting_id, canonical_meeting_id) pairs to write into is_duplicate_of — nothing is
    deleted (the daily report already filters is_duplicate_of IS NULL).

      rows: (meeting_id, meeting_date, email_k, phone_k, created_ts, id) — email_k/phone_k already
            normalized (lower-trim email; last-10-digits phone); '' / None when absent.
    """
    from collections import defaultdict

    parent: dict = {}

    def find(x):
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:       # path-compress
            parent[x], x = root, parent[x]
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for r in rows:
        find(r[0])

    by_date: dict = defaultdict(list)
    for r in rows:
        by_date[r[1]].append(r)
    for _dt, group in by_date.items():
        first_by_email: dict = {}
        first_by_phone: dict = {}
        for mid, _md, ek, pk, _cts, _id in group:
            if ek:
                if ek in first_by_email:
                    union(first_by_email[ek], mid)
                else:
                    first_by_email[ek] = mid
            if pk:
                if pk in first_by_phone:
                    union(first_by_phone[pk], mid)
                else:
                    first_by_phone[pk] = mid

    info = {r[0]: r for r in rows}
    comps: dict = defaultdict(list)
    for r in rows:
        comps[find(r[0])].append(r[0])

    pairs = []
    for _root, members in comps.items():
        if len(members) < 2:
            continue
        # earliest created_at canonical (NULLS LAST), deterministic id tiebreak.
        members.sort(key=lambda mid: (info[mid][4] is None, info[mid][4], info[mid][5]))
        canonical = members[0]
        for mid in members[1:]:
            pairs.append((mid, canonical))
    return pairs


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "meeting", run_meeting)


def run_meeting(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # Idempotent full rebuild — core.meeting is a pure projection of its raw sources.
    db.execute("DELETE FROM core.meeting")

    # -- 1. Pre-cutover Slack rows (date < 2026-06-01) — unchanged legacy behavior, channel/
    #       lead_email left NULL (the Slack source has neither). Date-grain boundary so there is
    #       no overlap/gap with the sheet rows. WS5: the new canonical columns (meeting_date,
    #       workspace_*, cm_workspace, program/offer/sendivo_sub_account) are left NULL for slack
    #       rows — they have no col-A date and no col-O workspace; the day-bucket COALESCE in the
    #       migrated views keeps them bucketing on posted_at, and portal CM credit falls back to
    #       the legacy resolver for source='slack' (all-time history unbroken).
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
    #
    #       WS5 v3 changes (D6/D7):
    #         (a) extract col A (date_display) and col O (workspace_name);
    #         (b) GATE the sheet branch on col A (meeting_date) — with a col-C-date fallback — so
    #             the cutover floor sits on the same column day-buckets land on;
    #         (c) resolve col O -> (warehouse_slug, canonical_current_name, cm) via a LEFT JOIN to
    #             core.workspace_alias (ws_resolved CTE) — the D6-mandated mechanism, NOT a CASE;
    #         (d) derive the Sendivo sub-account + program/offer (D7) off the anchored col-O label.
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, channel, lead_email, match_method, match_confidence,
           is_duplicate_of, cost_per_meeting_usd_estimated, raw_text,
           advisor, advisor_name, advisor_partner, inbox_manager,
           meeting_date, submission_ts, workspace_name, workspace_slug, workspace_canonical,
           cm_workspace, program, offer, sendivo_sub_account)
        WITH ff AS (
          SELECT
            NULLIF(json_extract_string(row_json, '$[{_C_SUBMISSION_ID}]'),'')   AS submission_id,
            json_extract_string(row_json, '$[{_C_SUBMISSION_TIME}]')            AS submission_time,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_DATE}]')),'')      AS date_display,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_CHANNEL}]')),'')   AS channel,
            NULLIF(json_extract_string(row_json, '$[{_C_PARTNER}]'),'')         AS partner,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_ADVISOR}]')),'')   AS advisor,
            NULLIF(trim(json_extract_string(row_json, '$[{_C_WORKSPACE}]')),'') AS workspace_name,
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
          -- WS5: posted_ts = col C (audit/back-compat). meeting_date = CANONICAL col A, with a
          --   col-C-date fallback (so a blank/unparseable col A never drops the row). The cutover
          --   GATE moves to meeting_date so the floor sits on the same column the views bucket on.
          -- im_bookings cutover: the FF branch is now FROZEN to [CUTOVER, IMB_CUTOVER) — meetings on
          --   or after IMB_CUTOVER come from im_bookings (step 2-IMB). The sheet is retired, so this
          --   reads its last-good snapshot for the 06-01..06-28 history only.
          SELECT *,
            TRY_CAST(submission_time AS TIMESTAMP) AS posted_ts,
            COALESCE(
              CAST(TRY_STRPTIME(date_display, ['%b %-d, %Y','%B %-d, %Y']) AS DATE),
              CAST(TRY_CAST(submission_time AS TIMESTAMP) AS DATE)
            ) AS meeting_date
          FROM ff
          WHERE TRY_CAST(submission_time AS TIMESTAMP) IS NOT NULL
            AND COALESCE(
                  CAST(TRY_STRPTIME(date_display, ['%b %-d, %Y','%B %-d, %Y']) AS DATE),
                  CAST(TRY_CAST(submission_time AS TIMESTAMP) AS DATE)
                ) >= DATE '{CUTOVER}'
            AND COALESCE(
                  CAST(TRY_STRPTIME(date_display, ['%b %-d, %Y','%B %-d, %Y']) AS DATE),
                  CAST(TRY_CAST(submission_time AS TIMESTAMP) AS DATE)
                ) <  DATE '{IMB_CUTOVER}'
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
        ),
        ws_resolved AS (
          -- D6: resolve the raw col-O label -> canonical workspace via core.workspace_alias
          --   (the canonical crosswalk, NOT a hand CASE — design RB3). a.cm is the
          --   WORKSPACE-credited CM (NULL = no CM credited), which is what kills the IDO leak:
          --   warm-leads / SMS / DFY rows resolve to a non-funding-CM workspace -> ws_cm NULL.
          -- D7: the Sendivo sub-account for SMS is read off the anchored col-O label.
          SELECT f.*,
                 a.warehouse_slug          AS ws_slug,
                 a.canonical_current_name  AS ws_canon,
                 a.cm                       AS ws_cm,
                 sub.sub_account_name       AS sub_acct
          FROM matched f
          LEFT JOIN core.workspace_alias a ON a.alias_name = f.workspace_name
          LEFT JOIN (VALUES ('Sendivo (Renaissance 1)','Renaissance 1'),
                            ('Sendivo (Renaissance 2)','Renaissance 2'))
               sub(lbl, sub_account_name) ON sub.lbl = f.workspace_name
        )
        SELECT
          meeting_id,
          'sheet'                                  AS source,
          submission_id                            AS source_event_id,
          posted_ts                                AS posted_at,          -- col C (back-compat)
          partner,
          matched_cid                              AS campaign_id,
          campaign_name                            AS campaign_name_raw,
          upper(campaign_manager)                  AS cm,                 -- raw col-17 (audit truth, UNCHANGED)
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
          {_im_norm_case('inbox_manager_raw')}     AS inbox_manager,
          -- WS5 canonical columns:
          meeting_date                             AS meeting_date,       -- CANONICAL (col A)
          posted_ts                                AS submission_ts,      -- audit (col C)
          workspace_name                           AS workspace_name,     -- raw col-O label
          ws_slug                                  AS workspace_slug,     -- alias.warehouse_slug
          ws_canon                                 AS workspace_canonical,-- alias.canonical_current_name
          ws_cm                                    AS cm_workspace,       -- WORKSPACE-credited CM (D6)
          CASE WHEN sub_acct = 'Renaissance 2' THEN 'Pre-IPO' ELSE 'Funding' END AS program,
          CASE WHEN channel = 'SMS' AND sub_acct = 'Renaissance 2' THEN 'Pre-IPO'
               WHEN channel = 'SMS'                                THEN 'Business Funding'  -- Ren1/Ren3 -> Funding
               ELSE NULL END                       AS offer,             -- email offer set in the post-step below
          sub_acct                                 AS sendivo_sub_account
        FROM ws_resolved
        """
    )

    # -- 2-IMB. Post-cutover FUNDING rows from the bookings-portal im_bookings mirror (meeting_date >=
    #          IMB_CUTOVER). The Funding-Form Google Sheet is retired; im_bookings carries a NATIVE
    #          channel / advisor / workspace / lead email / phone per booking, so meetings attribute
    #          DIRECTLY (no row_json positions). source STAYS 'sheet' (40+ views gate on it), meeting_id
    #          namespaced 'imb:<id>'. Reuses the SAME campaign norm-name join, core.workspace_alias D6
    #          resolution, and D7 sub-account/program/offer derivation as step 2 (im_bookings is 100%
    #          Funding offer; the workspace_alias + sub_acct map default program='Funding' correctly).
    #          Reads ONLY the latest LIVE snapshot, excludes cancelled bookings (deleted_at), and is a
    #          true idempotent rebuild. The email-OR-phone union-find dedup (Sam's rule) runs as a
    #          second pass right after, flagging is_duplicate_of.
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, channel, lead_email, match_method, match_confidence,
           is_duplicate_of, cost_per_meeting_usd_estimated, raw_text,
           advisor, advisor_name, advisor_partner, inbox_manager,
           meeting_date, submission_ts, workspace_name, workspace_slug, workspace_canonical,
           cm_workspace, program, offer, sendivo_sub_account)
        WITH imb AS (
          SELECT
            id,
            NULLIF(trim(channel), '')                            AS channel,
            NULLIF(trim(partner), '')                            AS partner,
            NULLIF(trim(advisor), '')                            AS advisor,
            NULLIF(trim(workspace), '')                          AS workspace_name,
            NULLIF(trim(inbox_manager), '')                      AS inbox_manager_raw,
            NULLIF(lower(trim(email)), '')                       AS lead_email,
            NULLIF(trim(campaign_manager), '')                   AS campaign_manager,
            NULLIF(trim(campaign), '')                           AS campaign_name,
            TRY_CAST("date" AS DATE)                             AS meeting_date
          FROM {IMB}
          -- the live (not frozen 2026-05-31) snapshot, latest generation only.
          WHERE _source = '{IMB_SOURCE_TAG}'
            AND _snapshot_date = (SELECT max(_snapshot_date) FROM {IMB}
                                  WHERE _source = '{IMB_SOURCE_TAG}')
            AND deleted_at IS NULL                  -- cancelled bookings excluded (never count a dead booking)
            AND TRY_CAST("date" AS DATE) >= DATE '{IMB_CUTOVER}'
        ),
        camp AS (  -- one campaign per normalized name: most-recent, fully deterministic tiebreak (== step 2)
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
          FROM imb f
          LEFT JOIN camp c ON c.nk = main.norm_campaign_name(f.campaign_name)
        ),
        ws_resolved AS (
          -- D6 workspace_alias resolution + D7 Sendivo sub-account — identical mechanism to step 2.
          SELECT f.*,
                 a.warehouse_slug          AS ws_slug,
                 a.canonical_current_name  AS ws_canon,
                 a.cm                       AS ws_cm,
                 sub.sub_account_name       AS sub_acct
          FROM matched f
          LEFT JOIN core.workspace_alias a ON a.alias_name = f.workspace_name
          LEFT JOIN (VALUES ('Sendivo (Renaissance 1)','Renaissance 1'),
                            ('Sendivo (Renaissance 2)','Renaissance 2'))
               sub(lbl, sub_account_name) ON sub.lbl = f.workspace_name
        )
        SELECT
          'imb:' || CAST(id AS VARCHAR)            AS meeting_id,
          'sheet'                                  AS source,            -- KEEP (hard contract: 40+ views)
          CAST(id AS VARCHAR)                      AS source_event_id,
          CAST(meeting_date AS TIMESTAMP)          AS posted_at,         -- portal booking carries no time-of-day; date = booking day
          partner,
          matched_cid                              AS campaign_id,
          campaign_name                            AS campaign_name_raw,
          upper(campaign_manager)                  AS cm,                -- raw audit CM (UNCHANGED)
          channel,                                                       -- native portal channel (Email/SMS/Call/WhatsApp)
          lead_email,
          CASE WHEN matched_cid IS NOT NULL THEN 'sheet_norm' ELSE 'unmatched' END AS match_method,
          CASE WHEN matched_cid IS NOT NULL THEN 1.0 ELSE NULL END                 AS match_confidence,
          NULL                                     AS is_duplicate_of,   -- set by the union-find pass below
          NULL                                     AS cost_per_meeting_usd_estimated,
          NULL                                     AS raw_text,
          advisor                                  AS advisor,           -- raw "<prefix>: <name>"
          NULLIF(trim(regexp_replace(advisor, '^[^:]*:', '')), '') AS advisor_name,
          {_advisor_partner_case('advisor')}       AS advisor_partner,
          {_im_norm_case('inbox_manager_raw')}     AS inbox_manager,     -- im_bookings is full-name; passthrough-safe
          meeting_date                             AS meeting_date,      -- CANONICAL (im_bookings.date = booking date)
          CAST(meeting_date AS TIMESTAMP)          AS submission_ts,
          workspace_name                           AS workspace_name,
          ws_slug                                  AS workspace_slug,
          ws_canon                                 AS workspace_canonical,
          ws_cm                                    AS cm_workspace,      -- WORKSPACE-credited CM (D6)
          CASE WHEN sub_acct = 'Renaissance 2' THEN 'Pre-IPO' ELSE 'Funding' END AS program,
          CASE WHEN channel = 'SMS' AND sub_acct = 'Renaissance 2' THEN 'Pre-IPO'
               WHEN channel = 'SMS'                                THEN 'Business Funding'
               ELSE NULL END                       AS offer,            -- email offer set in 2c; Call/WhatsApp in 2c2
          sub_acct                                 AS sendivo_sub_account
        FROM ws_resolved
        """
    )

    # -- 2-IMB dedup: email-OR-phone UNION-FIND over the im_bookings rows just inserted (Sam's rule
    #    2026-06-30). Soft-flag duplicates (same booking date sharing email OR phone) via is_duplicate_of
    #    = the earliest-created canonical meeting_id; never hard-delete. Pure no-op when there are no
    #    same-date email/phone collisions (the current ≥06-29 window has none — a go-forward guard).
    dup_rows = db.execute(
        f"""
        SELECT 'imb:' || CAST(id AS VARCHAR)                                        AS meeting_id,
               TRY_CAST("date" AS DATE)                                             AS meeting_date,
               NULLIF(lower(trim(email)), '')                                       AS email_k,
               NULLIF(right(regexp_replace(coalesce(phone,''),'[^0-9]','','g'),10),'') AS phone_k,
               TRY_CAST(created_at AS TIMESTAMP)                                     AS created_ts,
               id
        FROM {IMB}
        WHERE _source = '{IMB_SOURCE_TAG}'
          AND _snapshot_date = (SELECT max(_snapshot_date) FROM {IMB}
                                WHERE _source = '{IMB_SOURCE_TAG}')
          AND deleted_at IS NULL
          AND TRY_CAST("date" AS DATE) >= DATE '{IMB_CUTOVER}'
        """
    ).fetchall()
    dup_pairs = _im_dedup_pairs(dup_rows)
    if dup_pairs:
        db.executemany(
            "UPDATE core.meeting SET is_duplicate_of = ? WHERE meeting_id = ?",
            [(canonical, dup) for dup, canonical in dup_pairs],
        )
    logger.info("core.meeting im_bookings dedup: %d duplicate booking(s) soft-flagged "
                "(email-OR-phone union-find, same booking date)", len(dup_pairs))

    # -- 2.5 PRE-IPO PARTNER BOOKING DESKS (Summit Ventures SMS + Collins email/SMS/WhatsApp). Additional
    #        source='sheet' rows (so every source='sheet' view picks them up), offer/program='Pre-IPO',
    #        channel per-row from "Sending Account", campaign_id NULL (partner blast/script name kept in
    #        campaign_name_raw for meetings-by-blast). meeting_id namespaced 'summit:'/'collins:' (never
    #        collides with the Funding-Form 'sheet:'). Runs AFTER step 2 so the dedup-vs-Funding-Form
    #        NOT EXISTS sees the just-inserted Funding-Form rows. NOT gated on CUTOVER (no competing
    #        Slack/Funding-Form Pre-IPO source — verified 0 overlap).
    #        WORKSPACE (DATA-2 B3, 2026-07-01): Collins/Summit partner-desk rows now attribute to the
    #        synthetic Sendivo (Renaissance 2) workspace — Sendivo Pre-IPO-family sends are 100%
    #        Renaissance 2 (0 from Ren1/Ren3) and the 28 already-attributed Pre-IPO siblings all carry
    #        'Sendivo (Renaissance 2)' / slug 'sendivo-renaissance-2' (mirrored exactly here). This is
    #        the deterministic partner+offer desk rule (no phone join); it covers the 433 June rows AND
    #        the 91 Apr/May Collins rows (524 total — Sam-sanctioned Apr/May extension, 2026-07-01).
    #        NB: slug 'sendivo-renaissance-2' is a SYNTHETIC label, NOT core.workspace slug
    #        'renaissance-2' (= Funding 5 / Eyver) — dim row seeded by the DATA-2 core.workspace DDL.
    #        campaign_id stays NULL by design (Sendivo has no registry in core.campaign);
    #        match_method stays 'partner_sheet' (view 1018 offer logic + run metrics key on it).
    #        cm_workspace stays NULL so these never leak CM credit onto a funding workspace.
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, channel, lead_email, match_method, match_confidence,
           is_duplicate_of, cost_per_meeting_usd_estimated, raw_text,
           advisor, advisor_name, advisor_partner, inbox_manager,
           meeting_date, submission_ts, workspace_name, workspace_slug, workspace_canonical,
           cm_workspace, program, offer, sendivo_sub_account)
        WITH ps_raw AS (
{_partner_union_sql()}
        ),
        ps AS (
          SELECT partner, pfx,
            NULLIF(lower(trim(json_extract_string(row_json, '$[{_P_EMAIL}]'))),'')        AS lead_email,
            trim(json_extract_string(row_json, '$[{_P_SENDING_ACCOUNT}]'))                AS sending_account,
            NULLIF(trim(json_extract_string(row_json, '$[{_P_CAMPAIGN}]')),'')            AS campaign_name,
            NULLIF(trim(json_extract_string(row_json, '$[{_P_BOOKED_DATE}]')),'')         AS booked_raw,
            NULLIF(trim(json_extract_string(row_json, '$[{_P_ADVISOR}]')),'')             AS advisor,
            TRY_STRPTIME(trim(json_extract_string(row_json, '$[{_P_DATE}]')), '%m/%d/%Y')::DATE AS meeting_date
          FROM ps_raw
        ),
        ps_typed AS (
          SELECT *, {_partner_channel_case('sending_account')} AS channel
          FROM ps
          WHERE meeting_date IS NOT NULL          -- 100%-or-wipe: a row with an unparseable booked-on date is dropped
        ),
        ps_keyed AS (
          SELECT *,
            pfx || ':' || md5(
              COALESCE(lead_email,'')   || '|' || COALESCE(meeting_date::VARCHAR,'') || '|' ||
              COALESCE(campaign_name,'')|| '|' || COALESCE(booked_raw,'')            || '|' || channel
            ) AS meeting_id
          FROM ps_typed
        ),
        ps_dedup AS (   -- collapse byte-identical rows (a true duplicate sheet entry); genuine repeats
          SELECT * FROM ps_keyed     -- (same lead, different date/campaign) keep distinct meeting_ids
          QUALIFY ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY campaign_name) = 1
        )
        SELECT
          d.meeting_id,
          'sheet'                            AS source,
          NULL                               AS source_event_id,
          CAST(d.meeting_date AS TIMESTAMP)  AS posted_at,           -- partner sheets carry no time-of-day
          d.partner,
          NULL                               AS campaign_id,         -- partner blast/script, not an Instantly campaign
          d.campaign_name                    AS campaign_name_raw,   -- kept for meetings-by-blast attribution
          NULL                               AS cm,                  -- not a Renaissance-CM workspace
          d.channel,
          d.lead_email,
          'partner_sheet'                    AS match_method,
          NULL                               AS match_confidence,
          NULL                               AS is_duplicate_of,
          NULL                               AS cost_per_meeting_usd_estimated,
          NULL                               AS raw_text,
          d.advisor                          AS advisor,             -- bare name (no "<prefix>:" form)
          d.advisor                          AS advisor_name,
          d.partner                          AS advisor_partner,     -- the booking desk
          NULL                               AS inbox_manager,       -- partner-side setter stays in the raw table, not an IM
          d.meeting_date                     AS meeting_date,        -- canonical (col A "Date" = booked-on)
          CAST(d.meeting_date AS TIMESTAMP)  AS submission_ts,
          'Sendivo (Renaissance 2)'          AS workspace_name,      -- B3 desk rule: Pre-IPO desks send 100% via Sendivo Ren2
          'sendivo-renaissance-2'            AS workspace_slug,      -- SYNTHETIC slug (NOT 'renaissance-2' = Funding 5)
          'Sendivo (Renaissance 2)'          AS workspace_canonical, -- mirrors the 28 already-attributed Pre-IPO siblings
          NULL                               AS cm_workspace,        -- no CM credit (prevents Pre-IPO leaking onto a funding CM)
          'Pre-IPO'                          AS program,
          'Pre-IPO'                          AS offer,
          CASE WHEN d.channel = 'SMS' THEN 'Renaissance 2' END AS sendivo_sub_account  -- 107 contract: SMS-only; NULL otherwise
        FROM ps_dedup d
        -- DEDUP vs the Funding rows inserted in steps 2 + 2-IMB: drop a partner row whose (lead_email,
        -- meeting_date) already exists as a Funding meeting (same booking logged twice). 0 today
        -- (verified net-new); a go-forward guard. Funding rows carry meeting_id 'sheet:%' (frozen FF
        -- ≤06-28) or 'imb:%' (im_bookings ≥06-29).
        WHERE NOT EXISTS (
          SELECT 1 FROM core.meeting ff
          WHERE (ff.meeting_id LIKE 'sheet:%' OR ff.meeting_id LIKE 'imb:%')
            AND d.lead_email IS NOT NULL
            AND ff.lead_email = d.lead_email
            AND ff.meeting_date = d.meeting_date
        )
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
              AND (m2.meeting_id LIKE 'sheet:%' OR m2.meeting_id LIKE 'imb:%')   -- Funding rows (FF ≤06-28 + im_bookings ≥06-29); partner Pre-IPO rows (no Instantly campaign) exempt
              AND m2.lead_email IS NOT NULL AND m2.lead_email <> ''
              AND r.campaign_id IS NOT NULL
          ) WHERE rn = 1
        ) AS b
        WHERE m.meeting_id = b.meeting_id
        """
    )

    # -- 2e. SMS phone -> Sendivo send-log -> campaign_name + sub_account (DATA-2, 2026-07-01).
    #        Funding SMS meetings booked off a Sendivo blast have NO Instantly campaign, so the
    #        norm-name join leaves them match_method='unmatched'. Recover the originating blast by
    #        phone: resolve the lead's phone10 (imb rows via raw_im_bookings.id = source_event_id;
    #        frozen-FF 'sheet:' rows via the lead_email -> im_bookings fallback, nearest booking
    #        date), then take the MOST RECENT rich-send-log message sent AT/BEFORE the meeting date
    #        (strict — never attribute a send AFTER the booking; the "nearest after" fallback the
    #        investigator drafted was dropped per the B5 verifier: 7 impossible-causation rows).
    #        SETS campaign_name_raw (Sendivo campaign label; campaign_id stays NULL — Sendivo has no
    #        registry in core.campaign, so this is NOT campaign-id attribution), workspace_* only
    #        when the row has NO label at all (name-anchored fill; sub_account -> 'Sendivo
    #        (Renaissance N)' via core.workspace_alias, never a hand CASE),
    #        match_method='sms_phone_sendivo', match_confidence=0.7. Only CAMPAIGN-BEARING send-log
    #        rows count (a row never leaves 'unmatched' without gaining an attribution), de-duped to
    #        the latest load per sendivo_log_id (the mirror re-pulls a 6h overlap).
    #        GUARDS (verifier-mandated): channel='SMS' + match_method='unmatched' + campaign NULL +
    #        offer <> 'Pre-IPO' (Pre-IPO desk rows belong to the step-2.5 B3 rule; without this, 11
    #        Collins/Summit SMS bookings leak into the Funding attribution path). Yield is bounded by
    #        the rich send-log window (raw_sendivo_outbound_message starts 2026-06-26) — a DATA gap,
    #        not a logic gap; coverage grows as the send-log accumulates (idempotent full rebuild).
    db.execute(
        f"""
        UPDATE core.meeting AS m
        SET campaign_name_raw   = COALESCE(m.campaign_name_raw, b.campaign_name),
            -- workspace fill is NAME-anchored: only a row with NO label at all takes the resolved
            -- triple (an existing label wins in full — never a mixed name/slug pair from two sources).
            workspace_name      = COALESCE(m.workspace_name, b.ws_name),
            workspace_slug      = CASE WHEN m.workspace_name IS NULL OR m.workspace_name = ''
                                       THEN b.ws_slug ELSE m.workspace_slug END,
            workspace_canonical = CASE WHEN m.workspace_name IS NULL OR m.workspace_name = ''
                                       THEN b.ws_canon ELSE m.workspace_canonical END,
            sendivo_sub_account = COALESCE(m.sendivo_sub_account, b.sub_account_name),
            match_method        = 'sms_phone_sendivo',
            match_confidence    = 0.7
        FROM (
          WITH tgt AS (
            SELECT meeting_id, meeting_date, source_event_id, lead_email
            FROM core.meeting
            WHERE (is_duplicate_of IS NULL OR is_duplicate_of = '')
              AND (campaign_id IS NULL OR campaign_id = '')
              AND channel = 'SMS'
              AND match_method = 'unmatched'
              AND COALESCE(offer, '') <> 'Pre-IPO'
              AND (meeting_id LIKE 'sheet:%' OR meeting_id LIKE 'imb:%')
          ),
          rib AS (  -- live portal bookings, latest snapshot only (same pin as step 2-IMB)
            SELECT id,
                   NULLIF(lower(trim(email)), '') AS email_k,
                   NULLIF(right(regexp_replace(CAST(phone AS VARCHAR), '[^0-9]', '', 'g'), 10), '') AS phone10,
                   TRY_CAST("date" AS DATE) AS booking_date
            FROM {IMB}
            WHERE _source = '{IMB_SOURCE_TAG}'
              AND _snapshot_date = (SELECT max(_snapshot_date) FROM {IMB}
                                    WHERE _source = '{IMB_SOURCE_TAG}')
              AND phone IS NOT NULL
          ),
          ph AS (   -- phone via booking id (imb rows), else via lead_email (frozen-FF rows)
            SELECT t.meeting_id, t.meeting_date, r.phone10,
                   0 AS dist, r.id AS tiebreak
            FROM tgt t JOIN rib r ON CAST(r.id AS VARCHAR) = t.source_event_id
            WHERE t.meeting_id LIKE 'imb:%'
            UNION ALL
            SELECT t.meeting_id, t.meeting_date, r.phone10,
                   abs(datediff('day', r.booking_date, t.meeting_date)) AS dist, r.id AS tiebreak
            FROM tgt t JOIN rib r ON r.email_k = t.lead_email
            WHERE t.meeting_id LIKE 'sheet:%' AND t.lead_email IS NOT NULL
          ),
          ph1 AS (  -- one phone per meeting: nearest same-lead booking, deterministic tiebreak
            SELECT meeting_id, meeting_date, phone10 FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY meeting_id ORDER BY dist, tiebreak) AS prn
              FROM ph WHERE phone10 IS NOT NULL AND length(phone10) = 10
            ) WHERE prn = 1
          ),
          slog AS (  -- de-dup the append-only send-log mirror (overlap re-pulls carry duplicate
                     -- sendivo_log_ids; latest load wins — same pattern as v_sendivo_outbound_blast,
                     -- which is NOT reused here because it filters blast_id IS NOT NULL while this
                     -- step needs CAMPAIGN-bearing rows), and require a campaign_name so a row never
                     -- exits the 'unmatched' bucket without gaining an attribution.
            SELECT phone10, campaign_name, sub_account_name, sent_at, sendivo_log_id FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY sendivo_log_id ORDER BY _loaded_at DESC) AS lrn
              FROM main.raw_sendivo_outbound_message
              WHERE campaign_name IS NOT NULL
            ) WHERE lrn = 1
          ),
          ranked AS (  -- most-recent send AT/BEFORE the meeting; rn=1 collapses multi-blast phones (no fan-out)
            SELECT p.meeting_id, s.campaign_name, s.sub_account_name,
                   ROW_NUMBER() OVER (PARTITION BY p.meeting_id
                                      ORDER BY s.sent_at DESC, s.sendivo_log_id) AS rn
            FROM ph1 p
            JOIN slog s
              ON s.phone10 = p.phone10
             AND CAST(s.sent_at AS DATE) <= p.meeting_date
          )
          SELECT r.meeting_id, r.campaign_name, r.sub_account_name,
                 a.alias_name              AS ws_name,
                 a.warehouse_slug          AS ws_slug,
                 a.canonical_current_name  AS ws_canon
          FROM ranked r
          LEFT JOIN core.workspace_alias a
            ON a.alias_name = 'Sendivo (' || r.sub_account_name || ')'
          WHERE r.rn = 1
        ) AS b
        WHERE m.meeting_id = b.meeting_id
        """
    )

    # -- 2f. HISTORICAL campaign -> workspace backfill (DATA-2 B4, 2026-07-01). Pre-cutover Slack
    #        rows carry a campaign_id but were inserted with workspace_* NULL (the Slack source has
    #        no col-O). Resolve workspace from the campaign dimension — 3-tier, first hit wins:
    #        (1) raw_pipeline_campaigns.workspace_id, which is a SLUG (joining it as a UUID returns
    #            0 — the verified trap), incl. the one display-form label 'Renaissance 2' normalized
    #            to slug 'renaissance-2' (= Funding 5; 2 campaigns / 44 meetings);
    #        (2) raw_instantly_campaign.workspace_id (UUID);
    #        (3) core.campaign.workspace_id (UUID).
    #        ROW_NUMBER-by-priority => exactly one workspace per campaign (verified no fan-out),
    #        slug+name always from the same row.
    #        Only fills workspace-NULL non-dup rows with a campaign_id; June rows are untouched by
    #        construction (every June workspace-NULL row is also campaign-NULL). Resolves 7,435 of
    #        8,831 all-time + ~1,394 more once the 4 cancelled slugs (outlook-2, renaissance-6/7,
    #        erc-2) are re-seeded in core.workspace (the DATA-7 dim DDL shipped with this change).
    db.execute(
        """
        UPDATE core.meeting AS m
        SET workspace_slug      = p.slug,
            workspace_name      = p.name,
            workspace_canonical = p.name   -- core.workspace.name IS the canonical current display name;
                                           -- without it v_meeting_canonical buckets these '(unmapped)'
        FROM (
          SELECT campaign_id, slug, name FROM (
            -- rn=1 picks slug+name from the SAME row, fully deterministic on pri ties
            -- (arg_min per-column could mix two workspaces if a campaign ever relabeled).
            SELECT campaign_id, slug, name,
                   ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY pri, slug, name) AS rn
            FROM (
              SELECT DISTINCT rpc.campaign_id, w.slug, w.name, 1 AS pri
                FROM main.raw_pipeline_campaigns rpc
                JOIN core.workspace w
                  ON (CASE WHEN rpc.workspace_id = 'Renaissance 2' THEN 'renaissance-2'
                           ELSE rpc.workspace_id END) = w.slug
              UNION
              SELECT DISTINCT ric.campaign_id, w.slug, w.name, 2 AS pri
                FROM main.raw_instantly_campaign ric
                JOIN core.workspace w ON ric.workspace_id = w.workspace_id
              UNION
              SELECT DISTINCT c.campaign_id, w.slug, w.name, 3 AS pri
                FROM core.campaign c
                JOIN core.workspace w ON c.workspace_id = w.workspace_id
            )
          ) WHERE rn = 1
        ) AS p
        WHERE m.campaign_id = p.campaign_id
          AND (m.is_duplicate_of IS NULL OR m.is_duplicate_of = '')
          AND m.campaign_id <> ''
          AND (m.workspace_name IS NULL OR m.workspace_name = '')
        """
    )

    # CM attribution fallback (slack rows + any sheet row with a blank Campaign Manager but a
    # resolved campaign): campaign join first (authoritative), then regex on raw_text. Sheet rows
    # already carry cm and have raw_text NULL, so the regex passes only touch Slack rows.
    # NB (WS5): this fills the RAW audit `cm` only. The portal credits a CM off `cm_workspace`
    #   (= alias.cm), which is set in the sheet INSERT and intentionally NOT touched here — so these
    #   raw-cm fallbacks never leak a CM credit onto a non-funding workspace.
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

    # -- 2c. Email offer inherited from the campaign (D7 email side). core.campaign.offer is LIVE
    #        (WS7), so this UPDATE is safe to run now: email meetings get their offer from the
    #        resolved campaign. Until a campaign carries a non-NULL offer the meeting's offer stays
    #        NULL — the campaign->workspace->CM chain is still complete via the alias. SMS offers
    #        are already set in the INSERT off the Sendivo sub-account and are left untouched.
    db.execute(
        """
        UPDATE core.meeting AS m
        SET offer = c.offer
        FROM core.campaign c
        WHERE m.campaign_id = c.campaign_id AND m.channel = 'Email'
          AND m.source = 'sheet' AND (m.meeting_id LIKE 'sheet:%' OR m.meeting_id LIKE 'imb:%')  -- Funding rows only; never clobber the partner Pre-IPO tag
          AND c.offer IS NOT NULL
        """
    )

    # -- 2c2. Call + WhatsApp channel offer = Business Funding. These are Business-Funding-ONLY
    #         outreach channels: SDR phone calls (Close / Big Think Capital desk) dial funding leads —
    #         there is no Pre-IPO call program; and the WhatsApp outreach program is funding-only
    #         (core.v_whatsapp_conversation_offer carries only 'Business Funding'/NULL across 44k+
    #         conversations — zero Pre-IPO). Pre-IPO bookings on these channels come exclusively via the
    #         Collins/Summit partner sheets, which already carry offer='Pre-IPO' (match_method=
    #         'partner_sheet') — so the `offer IS NULL` guard preserves them and makes this idempotent.
    #         Before this, Call/WhatsApp meetings were 100%/98% offer=NULL and silently dropped from
    #         every per-offer meeting rollup (DW-TICKET-2 / ticket B9, 2026-06-29).
    db.execute(
        """
        UPDATE core.meeting
        SET offer = 'Business Funding'
        WHERE channel IN ('Call', 'WhatsApp') AND offer IS NULL
        """
    )

    # -- 2c3. Residual program-derived offer fallback (RC-5 / DW-ticket-T2 + B9; completes #106's 2c2).
    #         2c2 above tags Call/WhatsApp; this catches everything that still carries a program but no
    #         offer: the sheet Email meetings 2c missed (campaign_id not in core.campaign — 149 of the
    #         174 MTD NULLs), LinkedIn, and any future channel. program<->offer is 1:1 wherever both
    #         are set (Funding->Business Funding, Pre-IPO->Pre-IPO; zero cross-contamination, verified
    #         read-only 2026-06-29); the Funding-Form (Business-Funding-only) + partner desks are
    #         program-authoritative for offer. ADDITIVE: fills offer IS NULL only — never clobbers the
    #         precise campaign offer (2c) or 2c2; program-NULL pre-cutover / unattributed rows stay NULL.
    db.execute(
        """
        UPDATE core.meeting
        SET offer = CASE program
                      WHEN 'Funding' THEN 'Business Funding'
                      WHEN 'Pre-IPO' THEN 'Pre-IPO'
                    END
        WHERE offer IS NULL AND program IN ('Funding', 'Pre-IPO')
        """
    )

    # -- 2d. WARN on any workspace label (Funding-Form col-O OR im_bookings.workspace) that did NOT
    #        resolve via core.workspace_alias, so a new/renamed/dirty workspace surfaces loudly instead
    #        of silently landing in the '(unmapped)' reporting segment. (im_bookings carries dirtier
    #        labels than the FF col-O — e.g. 'Sendivo R1' / 'R1' / 'Sendivo R3' — which are SMS
    #        sub-accounts that correctly carry cm_workspace=NULL; seed them in workspace_alias to also
    #        resolve workspace_slug for segmentation. The empty/NULL label is expected '(unmapped)'.)
    unmapped = db.execute(
        "SELECT DISTINCT workspace_name FROM core.meeting "
        "WHERE source='sheet' AND workspace_name IS NOT NULL AND workspace_slug IS NULL"
    ).fetchall()
    if unmapped:
        logger.warning(
            "core.meeting: workspace labels unresolved by core.workspace_alias "
            "(add to the WS1 seed or the 107.0 supplemental seed): %s",
            [u[0] for u in unmapped],
        )

    n = db.execute("SELECT count(*) FROM core.meeting").fetchone()[0]
    by_source = db.execute(
        "SELECT source, count(*) FROM core.meeting GROUP BY 1 ORDER BY 1"
    ).fetchall()
    # Funding email-attribution health — scoped to the Funding rows (frozen FF 'sheet:%' ≤06-28 +
    # im_bookings 'imb:%' ≥06-29) so the partner Pre-IPO email rows (intentionally campaign_id NULL)
    # don't pollute the Funding attribution %.
    sheet_unmatched = db.execute(
        "SELECT count(*) FROM core.meeting WHERE source='sheet' AND channel='Email' AND campaign_id IS NULL "
        "AND (meeting_id LIKE 'sheet:%' OR meeting_id LIKE 'imb:%')"
    ).fetchone()[0]
    sheet_backfilled = db.execute(
        "SELECT count(*) FROM core.meeting WHERE match_method='email_reply_backfill'"
    ).fetchone()[0]
    sheet_email_total = db.execute(
        "SELECT count(*) FROM core.meeting WHERE source='sheet' AND channel='Email' "
        "AND (meeting_id LIKE 'sheet:%' OR meeting_id LIKE 'imb:%')"
    ).fetchone()[0]
    sheet_attr_pct = round(100.0 * (sheet_email_total - sheet_unmatched) / sheet_email_total, 2) if sheet_email_total else None
    # im_bookings cutover footprint (>= IMB_CUTOVER) — meetings by channel + soft-flagged duplicates.
    imb_by_channel = db.execute(
        "SELECT channel, count(*) FROM core.meeting WHERE meeting_id LIKE 'imb:%' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    imb_total = sum(c for _, c in imb_by_channel)
    imb_dupes = db.execute(
        "SELECT count(*) FROM core.meeting WHERE meeting_id LIKE 'imb:%' AND is_duplicate_of IS NOT NULL"
    ).fetchone()[0]
    # DATA-2 attribution extensions (2026-07-01) — observable step yields.
    sms_phone_matched = db.execute(
        "SELECT count(*) FROM core.meeting WHERE match_method='sms_phone_sendivo'"
    ).fetchone()[0]
    slack_ws_backfilled = db.execute(
        "SELECT count(*) FROM core.meeting WHERE source='slack' "
        "AND workspace_name IS NOT NULL AND workspace_name <> ''"
    ).fetchone()[0]
    # Pre-IPO partner-desk rows (Summit + Collins) — net-new meetings by channel, all offer='Pre-IPO'.
    partner_by_channel = db.execute(
        "SELECT channel, count(*) FROM core.meeting WHERE match_method='partner_sheet' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    partner_total = sum(c for _, c in partner_by_channel)
    partner_unmapped = db.execute(
        "SELECT count(*) FROM core.meeting WHERE match_method='partner_sheet' AND channel='(unmapped)'"
    ).fetchone()[0]
    if partner_unmapped:
        logger.warning("core.meeting: %d partner-sheet rows have an unmapped channel "
                       "(unrecognized 'Sending Account') — they fall out of all channel-scoped views",
                       partner_unmapped)
    logger.info("core.meeting rebuilt: %d rows %s; Funding email-meetings unmatched=%d "
                "(reply-backfilled=%d, email-attribution=%.2f%%); im_bookings meetings=%d %s "
                "(soft-flagged dupes=%d); partner Pre-IPO meetings=%d %s; "
                "SMS phone->sendivo matched=%d; slack ws-backfilled=%d",
                n, dict(by_source), sheet_unmatched, sheet_backfilled, sheet_attr_pct or 0.0,
                imb_total, dict(imb_by_channel), imb_dupes,
                partner_total, dict(partner_by_channel),
                sms_phone_matched, slack_ws_backfilled)
    return PhaseResult(
        rows_in=n, rows_out=n,
        notes={"by_source": dict(by_source), "sheet_email_unmatched": sheet_unmatched,
               "sheet_email_backfilled": sheet_backfilled, "sheet_email_attr_pct": sheet_attr_pct,
               "cutover": CUTOVER, "imb_cutover": IMB_CUTOVER,
               "imb_meetings": imb_total, "imb_by_channel": dict(imb_by_channel),
               "imb_soft_flagged_dupes": imb_dupes,
               "partner_preipo_meetings": partner_total,
               "partner_preipo_by_channel": dict(partner_by_channel),
               "sms_phone_sendivo_matched": sms_phone_matched,
               "slack_ws_backfilled": slack_ws_backfilled},
    )
