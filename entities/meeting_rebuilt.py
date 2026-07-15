"""core.meeting_rebuilt — Slack-era meeting rebuild from the portal archive (DDL 1106).

WHY (meetings-truth-reconciliation.md, 2026-07-14/15): the Slack feed behind pre-June
core.meeting missed 9–33% of email meetings weekly (May worst: ~831 email meetings = 31%
missing), carries zero lead identity and no channel labels. Portal raw_im_bookings deduped
by email/phone + day (NEVER row id — standing Sam rule) is the measured superset with
identity + channel/workspace/partner. This entity materializes that portal truth for the
slack era ONLY (meeting_date < 2026-06-01, which also folds the BTC client-sheet backfill:
portal rows source='btc_sheet_backfill_20260713', 1,240 rows, 2024-01-15→2025-12-06).

core.meeting and entities/meeting.py are NOT touched (reversibility is a charter rule).
Consumers read core.v_meeting_truth (DDL 1106), whose post-cutover era reads core.meeting
LIVE — this table only serves the pre-2026-06-01 era. Idempotent full rebuild per run
(pure projection of raw_im_bookings; DELETE + INSERT in one transaction).

CLASSIFICATION RULES (measured in the reconciliation; validated 2026-07-15 against its
weekly series — exact on 15/18 weeks, ±2 rows on 3 weeks from portal snapshot drift):

* channel_norm: native `channel` column when present (rare pre-June); else workspace label
  (SMS/Sendivo/Sendblue -> SMS; LinkedIn -> LinkedIn; GHL -> Call; ISKRA -> WhatsApp);
  else campaign string (Phone/GHL -> Call; sendivo|sms|text blast|new premium|500k part|
  mca data -> SMS — the measured Sendivo blast names that leaked into the email pool;
  whatsapp|iskra -> WhatsApp; linkedin -> LinkedIn); else Email.

* in_funding_scope (workspace-label grain, per the reconciliation): RE Wholesale /
  Section 125 / R&D Credit / Pre-IPO / Infinite Savings -> FALSE. TARIFFS DISAMBIGUATION:
  the portal RETROACTIVELY collapsed the booking-time label 'Tariffs + Funding' (erc-1,
  2026-04-16→04-30, 213 rows, recon-verified 0 tariff campaign strings => funding) into
  plain 'Tariffs' in every later snapshot. The frozen darcy_portal_im_bookings capture
  (≤2026-05-02) preserves the booking-time label, so tariffs-labeled rows are classed
  funding IFF they appear as 'Tariffs + Funding' in that capture; the residual
  (18 pre-May + 5 May rows) stays non-funding. Do NOT "simplify" this to a plain label
  rule — it re-breaks April (+/-110 meetings in wk 2026-04-13 alone). The join is by
  (lower(email), booking day), NOT by id: raw_im_bookings.id is NOT stable across the
  three _source generations (measured 2026-07-15: id overlap 0/213, email+day 213/213 —
  renovation-ledger fact; booking_id is a known decoy too, DDL 1091).

* is_ours: FALSE when workspace/campaign/partner mentions ISKRA (partner-outbound, not
  our sending).

* campaign attribution: best-effort main.norm_campaign_name join against
  raw_pipeline_campaigns where the normalized string maps to exactly ONE campaign
  (match_method='portal_norm'); the rest stay campaign_id NULL / 'unmatched'. This is
  a lower-bound attribution — never read attributed counts as total meetings.

EXPECTED WEEKLY email-funding-ours (validation vs meetings-truth-reconciliation.csv,
weeks 2026-01-26..2026-05-25): 723 · 732 · 686±2 · 964 · 910 · 1005 · 1020 · 1108±1 ·
1272 · 1068±1 · 940 · 1098 · 1419 · 1035±2 · 551 · 565 · 645 · 625±3.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.meeting_rebuilt")

CUTOVER = "2026-06-01"   # era boundary: this table serves ONLY meeting_date < CUTOVER

_REBUILD = f"""
INSERT INTO core.meeting_rebuilt
  (meeting_key, era, meeting_date, posted_at, lead_email, lead_phone, channel_raw,
   channel_norm, channel_basis, workspace_label_raw, workspace_slug, offer,
   in_funding_scope, is_ours, partner, advisor, campaign_string, campaign_id,
   match_method, cm, source, portal_source, raw_text, _run_id)
WITH latest AS (
  SELECT max(_snapshot_date) AS d FROM raw_im_bookings
  WHERE _source = 'portal_im_bookings_nightly'
),
tariffs_funding_era AS (  -- booking-time labels preserved only in the frozen darcy capture.
  -- Keyed by email+day, NOT id: portal row ids are NOT stable across _source generations
  -- (measured: id overlap 0/213; email+day 213/213).
  SELECT DISTINCT lower(trim(email)) AS em, TRY_CAST(date AS DATE) AS dy
  FROM raw_im_bookings
  WHERE _source = 'darcy_portal_im_bookings' AND workspace = 'Tariffs + Funding'
    AND COALESCE(trim(email), '') <> ''
),
base AS (
  SELECT b.id, b.email, b.phone, b.workspace, b.campaign, b.channel, b.partner, b.advisor,
         b.offer, b.source AS portal_source, b.campaign_manager,
         TRY_CAST(b.date AS DATE) AS booking_day,
         COALESCE(NULLIF(lower(trim(b.email)), ''),
                  NULLIF(regexp_replace(COALESCE(b.phone, ''), '[^0-9]', '', 'g'), '')) AS lead_key,
         (tf.em IS NOT NULL) AS is_tariffs_funding_era
  FROM raw_im_bookings b
  CROSS JOIN latest
  LEFT JOIN tariffs_funding_era tf
    ON tf.em = lower(trim(b.email)) AND tf.dy = TRY_CAST(b.date AS DATE)
  WHERE b._source = 'portal_im_bookings_nightly'
    AND b._snapshot_date = latest.d
    AND TRY_CAST(b.date AS DATE) IS NOT NULL
    AND TRY_CAST(b.date AS DATE) < DATE '{CUTOVER}'
),
dedup AS (  -- email/phone + day; NEVER row id (standing rule). Prefer the email-bearing row.
  SELECT *, row_number() OVER (
           PARTITION BY COALESCE(lead_key, 'row:' || CAST(id AS VARCHAR)), booking_day
           ORDER BY (CASE WHEN email IS NULL OR trim(email) = '' THEN 1 ELSE 0 END), id
         ) AS rn
  FROM base
),
one AS (SELECT * FROM dedup WHERE rn = 1),
cls AS (
  SELECT *,
    CASE
      WHEN COALESCE(channel, '') <> '' THEN
        CASE WHEN lower(channel) = 'email' THEN 'Email'
             WHEN lower(channel) IN ('sms', 'text') THEN 'SMS'
             WHEN lower(channel) = 'whatsapp' THEN 'WhatsApp'
             WHEN lower(channel) = 'linkedin' THEN 'LinkedIn'
             WHEN lower(channel) IN ('call', 'phone') THEN 'Call'
             ELSE 'Other' END
      WHEN regexp_matches(lower(COALESCE(workspace, '')), '^sms$|sendivo|sendblue') THEN 'SMS'
      WHEN regexp_matches(lower(COALESCE(workspace, '')), 'linkedin') THEN 'LinkedIn'
      WHEN regexp_matches(lower(COALESCE(workspace, '')), '^ghl$') THEN 'Call'
      WHEN regexp_matches(lower(COALESCE(workspace, '')), 'iskra') THEN 'WhatsApp'
      WHEN lower(COALESCE(campaign, '')) = 'phone'
        OR regexp_matches(lower(COALESCE(campaign, '')), '\\bghl\\b') THEN 'Call'
      WHEN regexp_matches(lower(COALESCE(campaign, '')),
             'sendivo|\\bsms\\b|text blast|new premium|500k part|mca data') THEN 'SMS'
      WHEN regexp_matches(lower(COALESCE(campaign, '')), 'whatsapp|iskra') THEN 'WhatsApp'
      WHEN regexp_matches(lower(COALESCE(campaign, '')), 'linkedin') THEN 'LinkedIn'
      ELSE 'Email'
    END AS channel_norm,
    CASE
      WHEN COALESCE(channel, '') <> '' THEN 'native_channel'
      WHEN regexp_matches(lower(COALESCE(workspace, '')), '^sms$|sendivo|sendblue|linkedin|^ghl$|iskra')
        THEN 'workspace_label'
      WHEN lower(COALESCE(campaign, '')) = 'phone'
        OR regexp_matches(lower(COALESCE(campaign, '')),
             '\\bghl\\b|sendivo|\\bsms\\b|text blast|new premium|500k part|mca data|whatsapp|iskra|linkedin')
        THEN 'campaign_string'
      ELSE 'default_email'
    END AS channel_basis,
    CASE
      WHEN lower(COALESCE(workspace, '')) LIKE 'tariffs%' THEN is_tariffs_funding_era
      WHEN regexp_matches(lower(COALESCE(workspace, '')),
             '^re wholesale$|^section 125$|r&d credit|pre-ipo|^infinite savings$') THEN FALSE
      ELSE TRUE
    END AS in_funding_scope_c,
    NOT regexp_matches(lower(COALESCE(workspace, '') || ' ' || COALESCE(campaign, '') || ' '
                             || COALESCE(partner, '')), 'iskra') AS is_ours_c
  FROM one
)
SELECT
  'portal:' || COALESCE(lead_key, 'row:' || CAST(id AS VARCHAR)) || ':'
             || strftime(booking_day, '%Y-%m-%d')            AS meeting_key,
  'slack_era_portal'                                          AS era,
  booking_day                                                 AS meeting_date,
  CAST(booking_day AS TIMESTAMPTZ)                            AS posted_at,
  NULLIF(lower(trim(email)), '')                              AS lead_email,
  NULLIF(regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g'), '') AS lead_phone,
  NULLIF(channel, '')                                         AS channel_raw,
  channel_norm,
  channel_basis,
  workspace                                                   AS workspace_label_raw,
  NULL                                                        AS workspace_slug,  -- filled below
  CASE WHEN lower(COALESCE(workspace, '')) LIKE 'tariffs%' AND NOT is_tariffs_funding_era
       THEN 'tariffs'
       WHEN in_funding_scope_c THEN 'business_funding'
       ELSE lower(COALESCE(workspace, 'unknown')) END         AS offer,
  in_funding_scope_c                                          AS in_funding_scope,
  is_ours_c                                                   AS is_ours,
  partner,
  advisor,
  NULLIF(trim(campaign), '')                                  AS campaign_string,
  NULL                                                        AS campaign_id,     -- filled below
  'unmatched'                                                 AS match_method,
  NULLIF(trim(campaign_manager), '')                          AS cm,
  'portal_rebuild'                                            AS source,
  portal_source,
  NULL                                                        AS raw_text,
  ?                                                           AS _run_id
FROM cls
"""

_SLUG_FILL = """
UPDATE core.meeting_rebuilt mr
SET workspace_slug = wn.warehouse_slug
FROM core.v_workspace_slug_norm wn
WHERE mr.workspace_slug IS NULL
  AND mr.workspace_label_raw IS NOT NULL
  AND wn.alias_lower = lower(trim(mr.workspace_label_raw))
"""

# Best-effort campaign attribution: normalized portal string -> UNIQUE campaign name.
_CAMPAIGN_ATTR = """
UPDATE core.meeting_rebuilt mr
SET campaign_id = u.campaign_id, match_method = 'portal_norm'
FROM (
  SELECT main.norm_campaign_name(name) AS nname, min(campaign_id) AS campaign_id
  FROM main.raw_pipeline_campaigns
  WHERE COALESCE(name, '') <> ''
  GROUP BY 1
  HAVING count(DISTINCT campaign_id) = 1
) u
WHERE mr.campaign_id IS NULL
  AND mr.campaign_string IS NOT NULL
  AND main.norm_campaign_name(mr.campaign_string) = u.nname
"""


def _table_exists(conn, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
    )


def run(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn, "core", "meeting_rebuilt"):
        logger.warning("meeting_rebuilt SKIP: core.meeting_rebuilt missing (DDL 1106 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "table missing"})

    src_rows = conn.execute(
        "SELECT count(*) FROM raw_im_bookings WHERE _source = 'portal_im_bookings_nightly'"
    ).fetchone()[0]
    if src_rows < 3_000:  # same broken-pull guard class as entities/im_bookings.py
        logger.warning("meeting_rebuilt SKIP: nightly im_bookings snapshot suspiciously small (%d rows) — keeping last-good rebuild.", src_rows)
        return PhaseResult(rows_in=src_rows, rows_out=0, notes={"skipped": "source snapshot too small"})

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.meeting_rebuilt")
        conn.execute(_REBUILD, [ctx.run_id])
        conn.execute(_SLUG_FILL)
        conn.execute(_CAMPAIGN_ATTR)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n, n_email_funding, n_attr = conn.execute(
        """
        SELECT count(*),
               count(*) FILTER (channel_norm = 'Email' AND in_funding_scope AND is_ours),
               count(*) FILTER (campaign_id IS NOT NULL)
        FROM core.meeting_rebuilt
        """
    ).fetchone()
    logger.info("meeting_rebuilt: %d slack-era portal meetings (%d email-funding-ours, %d campaign-attributed).",
                n, n_email_funding, n_attr)
    return PhaseResult(rows_in=src_rows, rows_out=n,
                       notes={"rows": n, "email_funding_ours": n_email_funding, "campaign_attributed": n_attr})


def register(registry: Registry) -> None:
    # 'canonical' phase; file sorts after meeting.py so it runs after core.meeting's rebuild.
    registry.add_phase("canonical", "meeting_rebuilt", run)
