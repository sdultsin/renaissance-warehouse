"""core.campaign_infra — persistent campaign→sending-infra registry (nightly, 'derived').

Feeds the registry created by sql/ddl/1071_campaign_infra.sql (campaign-truth build,
TKT-1 + TKT-2 unified — DESIGN §2). One durable row per campaign_id ever seen,
upsert-once / NEVER truncated. Five steps each night:

  a) UNIVERSE — ensure a registry row exists for every campaign in
     raw_instantly_campaign_dim (DDL 1061 durable dim), core.account_campaign
     (live census), every campaign with sent>0 in
     raw_pipeline_campaign_daily_metrics since 2026-05-01, AND every campaign
     in core.campaign_sending_tag (the frozen ≤2026-06-15 tag table) — so
     pre-May frozen-history campaigns get registry rows and the
     frozen_tag_table derivation applies (100%-or-wipe: label what's provable).
     The frozen-tag source NEVER contributes liveness (last_seen_live_at reads
     dim/census only) and is gated on a raw_pipeline_campaigns identity row:
     frozen-tag campaigns absent from raw_pipeline_campaigns are skipped with
     a logged count. Identity (name / workspace / status) comes from the
     latest raw_pipeline_campaigns row per campaign, overridden by the fresher
     dim/census surfaces where present. workspace_slug resolves through
     core.workspace (slug first, display name fallback); an unresolvable raw
     workspace id is kept in derivation_note.
  b) DIM_TAG — classify raw_instantly_campaign_dim.tag_labels (a JSON array
     string, e.g. '["Outreach Today Active"]') per the Sam-specified tag map.
     Only infra tags count: a plain RG#### / RG####-#### batch tag is NOT an
     infra family and is skipped; any unrecognized raw tag becomes
     (vendor=<raw tag>, esp='unknown') — surfaced, never guessed.
  c) CENSUS_MAJORITY — core.account_campaign ⋈ core.account_tags majority
     Active-family per campaign (the TKT-2 acceptance-query CTE shape);
     n_accounts (distinct census inboxes) recorded. NOTE: core.account_tags is
     refreshed by the LAST phase ('account_tags_late'), so census reads the
     previous night's tags — acceptable, dim_tag outranks census anyway.
  d) FROZEN_TAG_TABLE — core.campaign_sending_tag (frozen 2026-06-15) families
     for campaigns still unknown; frozen-era raw tags ('Outlook', 'Outlook PP',
     'Google', 'MailIn', 'Gmail', …) → (that raw tag, 'unknown').
  e) MERGE + IMMUTABILITY — an existing row's (infra_vendor, derivation_source)
     may only be overwritten by a STRICTLY higher-precedence source, or when the
     current vendor is 'unknown'; every upgrade appends to derivation_note.
     Precedence: manual(7) > dim_tag(6) > census_majority(5) >
     manifest_pump_day(4) > frozen_tag_table(3) > rg_partner(2) >
     name_heuristic(1) > unknown(0).
     Cross-check: dim_tag vs census_majority both resolved and DISAGREE → keep
     dim_tag, mixed_detail gains 'disagrees:census=<fam>', count logged.
  f) LIVENESS — last_seen_live_at / last_seen_name / campaign_status refreshed
     for campaigns present in the dim (last_seen_at within _LIVE_WINDOW_DAYS)
     or the live census this run.
  g) RECIPIENT MIX — aggregate the warehouse-local
     raw_pipeline_contact_frequency_campaign_daily ([2026-07-18 wave-2, MOF-10]
     repointed from the retiring pipeline-Supabase attach; inbox-fed + full
     history backfilled) (campaign × lead_domain × day) LEFT JOIN core.recipient_domain into
     per-campaign send buckets (google / microsoft / other / unknown; the tiny
     'isp'/'yahoo'/'apple' classes fold into 'other'; NULL rd → 'unknown').
     Default incremental scope = campaigns with any send in the last 45 days
     (their FULL history is aggregated so recipient_sends_total stays
     cumulative); env CAMPAIGN_INFRA_RECIP_FULL=1 → all campaigns (the one-off
     backfill run). Label rule: sends_total>=100 AND dominant-known/sends_total
     >=0.8 → dominant ESP; >=100 AND the KNOWN buckets genuinely disagree
     (dominant-known/known-total < 0.8) → 'mixed'; else 'unknown' — when
     unknown-domain coverage is what prevents a >=0.8 share, the label is
     'unknown' (a coverage gap), NEVER a definite 'mixed'. Unknown-domain sends
     stay in the unknown bucket AND the dominant-share denominator
     (100%-or-wipe: never redistributed).
     A labeled recipient_esp is never downgraded to 'unknown' (zero-window
     campaigns are skipped entirely; a weak 'unknown' recompute never clobbers
     a real label, e.g. a manifest_pump_day one). Recomputed stats are
     otherwise allowed to change — they self-improve as the DNS ESP backfill
     lands.

Write style: UPDATE ... FROM staged + INSERT ... ANTI JOIN — NO ON CONFLICT DO
UPDATE (the DuckDB ART-index INTERNAL duplicate-key abort class documented in
scripts/backfill_account_tags_full.py). All writes in explicit small
transactions; staging temp tables are built under autocommit (and the pg
catalog is DETACHed before the recipient write transaction opens).

Verified read-only on serving snapshot warehouse_20260703_043558_874.duckdb:
  * universe = 1,879 campaigns (dim 272 ∪ census 135 ∪ sent>0-since-May 1,842
    ∪ frozen-tag +2). The frozen tag table holds 325 distinct campaigns, 8 of
    them beyond the previous 1,877-campaign universe; 2 of those 8 carry a
    raw_pipeline_campaigns identity and are ADDED, the other 6 are skipped
    (logged) — no identity surface anywhere.
  * 1,836 (+2 frozen-tag) carry a raw_pipeline_campaigns identity, all
    slug-resolve.
  * dim_tag: OTD 168 · Reseller 19 · MilkBox 7 · raw-others 48 (Google 43,
    I-Google / MailIn / Outlook / 'Instantly - Pre-warms' / ai-sdr-… 1 each).
  * census_majority: OTD 112 · Reseller 20 (8 mixed) — account_campaign emails
    verified already-lowercase (0 rows differ from lower()).
  * dim vs census both-resolved 57, disagree 4.
  * frozen: OTD 102 · Reseller 15 · raw Outlook 83 / Google 28 / MailIn 16 /
    'Outlook PP' 16 / Gmail 3 / … (RG batch tags, incl. 'RG2160-2169' ranges,
    excluded).
  * recipient join shape: 50k-domain sample from raw_pipeline_lead_events →
    google 18,448 / other 12,315 / unknown 10,955 / microsoft 8,282 (pg-side
    aggregation not reachable read-only; verified by shape + esp_matrix
    precedent).

One-off full backfill (NOT the nightly path; takes the writer flock itself):
    CAMPAIGN_INFRA_RECIP_FULL=1 python -m entities.campaign_infra
"""

from __future__ import annotations

import json
import logging
import os
import sys

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.campaign_infra")

# Campaigns with sends on/after this date belong in the registry universe even
# if they have since been deleted upstream (DESIGN: all 809 June campaigns
# either derived or explicit unknown).
UNIVERSE_SENT_SINCE = "2026-05-01"

# A dim row is "live this run" if the nightly analytics sync touched it this
# recently (the dim is never wiped, so old last_seen_at = deleted upstream).
_LIVE_WINDOW_DAYS = int(os.environ.get("CAMPAIGN_INFRA_LIVE_WINDOW_DAYS", "3"))

# Recipient-mix incremental scope (days of send activity that qualify a
# campaign for recompute). CAMPAIGN_INFRA_RECIP_FULL=1 recomputes everything.
_RECIP_WINDOW_DAYS = int(os.environ.get("CAMPAIGN_INFRA_RECIP_WINDOW_DAYS", "45"))


def _src_rank(col: str) -> str:
    """Precedence rank of a derivation_source column/literal (higher wins)."""
    return (
        f"CASE {col} WHEN 'manual' THEN 7 WHEN 'dim_tag' THEN 6 "
        "WHEN 'census_majority' THEN 5 WHEN 'manifest_pump_day' THEN 4 "
        "WHEN 'frozen_tag_table' THEN 3 WHEN 'rg_partner' THEN 2 "
        "WHEN 'name_heuristic' THEN 1 ELSE 0 END"
    )


def _fam_case(tag: str) -> str:
    """Tag → infra family (vendor). RG batch tags (RG####, RG####-####) are NOT
    an infra family; any other unrecognized raw tag IS surfaced as its own
    vendor with esp 'unknown' (never guessed)."""
    return (
        "CASE "
        f"WHEN {tag} IS NULL THEN NULL "
        f"WHEN regexp_matches({tag}, '^RG[0-9]+(-[0-9]+)?$') THEN NULL "
        f"WHEN {tag} ILIKE 'Outreach Today%' OR {tag} = 'OTD' THEN 'OTD' "
        f"WHEN {tag} ILIKE 'Reseller%' THEN 'Reseller' "
        f"WHEN {tag} ILIKE 'Milkbox%' THEN 'MilkBox' "
        f"WHEN {tag} ILIKE 'Cheap Inboxes%' THEN 'CheapInboxes' "
        f"WHEN {tag} ILIKE 'Google Panel%' THEN 'GooglePanel' "
        f"WHEN {tag} ILIKE 'MS Panel%' THEN 'MSPanel' "
        f"ELSE {tag} END"
    )


def _esp_case(vendor: str) -> str:
    return (
        f"CASE {vendor} WHEN 'OTD' THEN 'OTD' WHEN 'Reseller' THEN 'google' "
        "WHEN 'MilkBox' THEN 'outlook' ELSE 'unknown' END"
    )


def _fam_rank(vendor: str) -> str:
    """Order for the multi-family pick (§1b rule: first by this order + mixed_infra)."""
    return (
        f"CASE {vendor} WHEN 'OTD' THEN 1 WHEN 'Reseller' THEN 2 "
        "WHEN 'MilkBox' THEN 3 WHEN 'CheapInboxes' THEN 4 "
        "WHEN 'GooglePanel' THEN 5 WHEN 'MSPanel' THEN 6 ELSE 7 END"
    )


# Shared shape for the dim/frozen "classified tags -> one family per campaign" pick.
_PICK_SQL = f"""
    fams AS (
        SELECT campaign_id, vendor,
               min(tag)                 AS matched_tag,
               {_esp_case('vendor')}    AS esp,
               {_fam_rank('vendor')}    AS rnk
        FROM cls
        WHERE vendor IS NOT NULL
        GROUP BY campaign_id, vendor
    ),
    pick AS (
        SELECT campaign_id, vendor, esp, matched_tag,
               ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY rnk, vendor) AS rn,
               COUNT(*)    OVER (PARTITION BY campaign_id)                       AS n_fams
        FROM fams
    ),
    detail AS (
        SELECT campaign_id,
               array_to_string(list_sort(list_distinct(array_agg(vendor))), '+') AS fam_list
        FROM fams GROUP BY campaign_id
    )
    SELECT p.campaign_id, p.vendor, p.esp, p.matched_tag,
           p.n_fams > 1 AS mixed,
           CASE WHEN p.n_fams > 1 THEN 'families:' || d.fam_list END AS mixed_detail
    FROM pick p
    JOIN detail d USING (campaign_id)
    WHERE p.rn = 1
"""


def register(registry: Registry) -> None:
    registry.add_phase("derived", "campaign_infra", run_campaign_infra)


def _stage_universe(conn) -> int:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _ci_universe AS
        WITH ids AS (
            SELECT campaign_id FROM raw_instantly_campaign_dim
            UNION
            SELECT campaign_id FROM core.account_campaign WHERE campaign_id IS NOT NULL
            UNION
            SELECT campaign_id FROM raw_pipeline_campaign_daily_metrics
            WHERE sent > 0 AND date >= DATE '{UNIVERSE_SENT_SINCE}'
            UNION
            -- 4th source (campaign-truth, 2026-07-03): the frozen ≤2026-06-15 tag
            -- table, so pre-May frozen-history campaigns get registry rows and the
            -- frozen_tag_table derivation applies. Gated on a raw_pipeline_campaigns
            -- identity (absent -> skipped + logged after staging); NEVER contributes
            -- liveness (last_seen_live_at reads dim/census only in _refresh_liveness).
            SELECT campaign_id FROM core.campaign_sending_tag
            WHERE campaign_id IS NOT NULL
              AND campaign_id IN (SELECT campaign_id FROM raw_pipeline_campaigns
                                  WHERE campaign_id IS NOT NULL)
        ),
        rpc AS (  -- latest identity row per campaign (names are unstable; take newest)
            SELECT DISTINCT ON (campaign_id) campaign_id, name, workspace_id,
                   TRY_CAST(status AS INTEGER) AS status
            FROM raw_pipeline_campaigns
            ORDER BY campaign_id, _loaded_at DESC
        ),
        cen AS (
            SELECT DISTINCT ON (campaign_id) campaign_id, campaign_name, workspace_slug,
                   campaign_status
            FROM core.account_campaign WHERE campaign_id IS NOT NULL
            ORDER BY campaign_id, _synced_at DESC
        )
        SELECT ids.campaign_id,
               COALESCE(dim.campaign_name, cen.campaign_name, rpc.name)          AS name,
               COALESCE(dim.workspace_slug, cen.workspace_slug,
                        ws.slug, wn.slug)                                        AS workspace_slug,
               COALESCE(dim.campaign_status, cen.campaign_status, rpc.status)    AS campaign_status,
               rpc.workspace_id                                                  AS raw_workspace_id
        FROM ids
        LEFT JOIN rpc ON rpc.campaign_id = ids.campaign_id
        LEFT JOIN raw_instantly_campaign_dim dim ON dim.campaign_id = ids.campaign_id
        LEFT JOIN cen ON cen.campaign_id = ids.campaign_id
        LEFT JOIN core.workspace ws ON ws.slug = rpc.workspace_id
        LEFT JOIN core.workspace wn ON wn.name = rpc.workspace_id
        """
    )
    # Frozen-tag campaigns with NO raw_pipeline_campaigns identity are skipped from
    # the universe (nothing to anchor a registry row on) — surfaced, never silent.
    skipped = conn.execute(
        """
        SELECT count(DISTINCT t.campaign_id)
        FROM core.campaign_sending_tag t
        WHERE t.campaign_id IS NOT NULL
          AND t.campaign_id NOT IN (SELECT campaign_id FROM _ci_universe)
        """
    ).fetchone()[0]
    if skipped:
        logger.info(
            "campaign_infra: %d frozen-tag campaign(s) skipped from the universe "
            "(no raw_pipeline_campaigns identity)", skipped)
    return conn.execute("SELECT count(*) FROM _ci_universe").fetchone()[0]


def _stage_derivations(conn) -> dict:
    # (b) dim_tag — tag_labels is a JSON array string ('["Outreach Today Active"]').
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _ci_dim AS
        WITH exploded AS (
            SELECT campaign_id, unnest(from_json(tag_labels, '["VARCHAR"]')) AS tag
            FROM raw_instantly_campaign_dim
            WHERE tag_labels IS NOT NULL
        ),
        cls AS (
            SELECT campaign_id, tag, {_fam_case('tag')} AS vendor FROM exploded
        ),
        {_PICK_SQL}
        """
    )
    # (c) census_majority — the TKT-2 acceptance-query CTE shape. account_tags.email
    # is written lowercase by entities/account_tags.py; account_campaign emails
    # verified already-lowercase (lower() kept as a guard). Workspace-scoped join so
    # a same-named inbox in another workspace can never cross-classify.
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _ci_census AS
        WITH acct AS (
            SELECT DISTINCT ac.campaign_id, lower(ac.account_email) AS account_email,
                CASE
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'Outreach Today Active%')) > 0 THEN 'OTD'
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'Milkbox Active%'))         > 0 THEN 'MilkBox'
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'Reseller Active%'))        > 0 THEN 'Reseller'
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'Cheap Inboxes Active%'))   > 0 THEN 'CheapInboxes'
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'Google Panel Active%'))    > 0 THEN 'GooglePanel'
                    WHEN len(list_filter(tg.tags_arr, x -> x ILIKE 'MS Panel Active%'))        > 0 THEN 'MSPanel'
                END AS fam
            FROM core.account_campaign ac
            JOIN core.account_tags tg
              ON tg.email = lower(ac.account_email)
             AND tg.workspace_slug = ac.workspace_slug
            WHERE ac.campaign_id IS NOT NULL
        ),
        maj AS (
            SELECT campaign_id, mode(fam) AS vendor, count(DISTINCT fam) AS n_fams,
                   array_to_string(list_sort(list_distinct(array_agg(fam))), '+') AS fam_list
            FROM acct WHERE fam IS NOT NULL GROUP BY campaign_id
        ),
        cnt AS (
            SELECT campaign_id, count(DISTINCT account_email) AS n_accounts
            FROM acct GROUP BY campaign_id
        )
        SELECT m.campaign_id, m.vendor, {_esp_case('m.vendor')} AS esp,
               CASE m.vendor
                   WHEN 'OTD' THEN 'Outreach Today Active'
                   WHEN 'Reseller' THEN 'Reseller Active'
                   WHEN 'MilkBox' THEN 'MilkBox Active *'
                   WHEN 'CheapInboxes' THEN 'Cheap Inboxes Active'
                   WHEN 'GooglePanel' THEN 'Google Panel Active'
                   WHEN 'MSPanel' THEN 'MS Panel Active'
               END AS matched_tag,
               m.n_fams > 1 AS mixed,
               CASE WHEN m.n_fams > 1 THEN 'families:' || m.fam_list END AS mixed_detail,
               c.n_accounts
        FROM maj m
        JOIN cnt c USING (campaign_id)
        """
    )
    # (d) frozen_tag_table — same classification over the frozen campaign-tag sync.
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _ci_frozen AS
        WITH cls AS (
            SELECT campaign_id, tag_name AS tag, {_fam_case('tag_name')} AS vendor
            FROM core.campaign_sending_tag
        ),
        {_PICK_SQL}
        """
    )
    # (e) best-per-campaign by source precedence + dim-vs-census cross-check.
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE _ci_best AS
        WITH unioned AS (
            SELECT campaign_id, vendor, esp, matched_tag, mixed, mixed_detail,
                   NULL::INTEGER AS n_accounts, 'dim_tag' AS source, 6 AS src_rank
            FROM _ci_dim
            UNION ALL
            SELECT campaign_id, vendor, esp, matched_tag, mixed, mixed_detail,
                   n_accounts, 'census_majority', 5
            FROM _ci_census
            UNION ALL
            SELECT campaign_id, vendor, esp, matched_tag, mixed, mixed_detail,
                   NULL::INTEGER, 'frozen_tag_table', 3
            FROM _ci_frozen
        ),
        pick AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY src_rank DESC) AS rn
            FROM unioned
        ),
        dis AS (  -- dim and census both resolve and disagree -> keep dim, flag it
            SELECT d.campaign_id, c.vendor AS census_vendor
            FROM _ci_dim d
            JOIN _ci_census c USING (campaign_id)
            WHERE d.vendor <> c.vendor
        )
        SELECT p.campaign_id, p.vendor, p.esp, p.matched_tag,
               p.mixed,
               NULLIF(concat_ws(';', p.mixed_detail,
                                'disagrees:census=' || dis.census_vendor), '') AS mixed_detail,
               COALESCE(p.n_accounts, cen.n_accounts) AS n_accounts,
               p.source, p.src_rank
        FROM pick p
        LEFT JOIN dis ON dis.campaign_id = p.campaign_id AND p.source = 'dim_tag'
        LEFT JOIN _ci_census cen ON cen.campaign_id = p.campaign_id
        WHERE p.rn = 1
        """
    )
    counts = dict(
        conn.execute("SELECT source, count(*) FROM _ci_best GROUP BY 1").fetchall()
    )
    disagreements = conn.execute(
        "SELECT count(*) FROM _ci_best WHERE mixed_detail LIKE '%disagrees:census=%'"
    ).fetchone()[0]
    raw_vendors = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT vendor FROM _ci_best WHERE vendor NOT IN "
            "('OTD','Reseller','MilkBox','CheapInboxes','GooglePanel','MSPanel') "
            "ORDER BY 1"
        ).fetchall()
    ]
    if disagreements:
        rows = conn.execute(
            "SELECT campaign_id, vendor, mixed_detail FROM _ci_best "
            "WHERE mixed_detail LIKE '%disagrees:census=%'"
        ).fetchall()
        logger.warning("campaign_infra: %d dim-vs-census disagreements (kept dim): %s",
                       disagreements, rows[:20])
    return {"derived_by_source": counts, "disagreements": disagreements,
            "raw_tag_vendors": raw_vendors}


def _merge(conn, run_id: str) -> dict:
    conn.execute("BEGIN")
    try:
        # New campaigns: anti-join INSERT (registry rows are NEVER re-inserted).
        inserted = conn.execute(
            """
            INSERT INTO core.campaign_infra
              (campaign_id, workspace_slug, first_seen_name, last_seen_name,
               campaign_status, derivation_note, first_seen_at, _loaded_at, _run_id)
            SELECT u.campaign_id, u.workspace_slug, u.name, u.name, u.campaign_status,
                   CASE WHEN u.workspace_slug IS NULL AND u.raw_workspace_id IS NOT NULL
                        THEN 'raw_ws=' || u.raw_workspace_id END,
                   now(), now(), ?
            FROM _ci_universe u
            ANTI JOIN core.campaign_infra ci ON ci.campaign_id = u.campaign_id
            """,
            [run_id],
        ).fetchone()[0]
        # Sending-arm upgrade: STRICTLY higher precedence, or current unknown.
        upgraded = conn.execute(
            f"""
            UPDATE core.campaign_infra AS ci SET
                infra_vendor      = b.vendor,
                infra_esp         = b.esp,
                matched_tag       = b.matched_tag,
                mixed_infra       = b.mixed,
                mixed_detail      = b.mixed_detail,
                n_accounts        = COALESCE(b.n_accounts, ci.n_accounts),
                derivation_source = b.source,
                derivation_note   = concat_ws(' | ', ci.derivation_note,
                                        ci.derivation_source || '->' || b.source ||
                                        ' [' || strftime(now(), '%Y-%m-%d') || ']'),
                _loaded_at        = now(),
                _run_id           = ?
            FROM _ci_best b
            WHERE b.campaign_id = ci.campaign_id
              AND (ci.infra_vendor = 'unknown'
                   OR b.src_rank > {_src_rank('ci.derivation_source')})
            """,
            [run_id],
        ).fetchone()[0]
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"inserted": inserted, "upgraded": upgraded}


def _refresh_liveness(conn, run_id: str) -> int:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _ci_live AS
        WITH u AS (
            SELECT campaign_id, campaign_name AS name, campaign_status,
                   last_seen_at AS live_at
            FROM raw_instantly_campaign_dim
            WHERE last_seen_at >= now() - INTERVAL {_LIVE_WINDOW_DAYS} DAY
            UNION ALL
            SELECT campaign_id, campaign_name, campaign_status, _synced_at
            FROM core.account_campaign WHERE campaign_id IS NOT NULL
        )
        SELECT DISTINCT ON (campaign_id) campaign_id, name, campaign_status, live_at
        FROM u ORDER BY campaign_id, live_at DESC
        """
    )
    conn.execute("BEGIN")
    try:
        n = conn.execute(
            """
            UPDATE core.campaign_infra AS ci SET
                last_seen_live_at = greatest(COALESCE(ci.last_seen_live_at, l.live_at), l.live_at),
                last_seen_name    = COALESCE(l.name, ci.last_seen_name),
                campaign_status   = COALESCE(l.campaign_status, ci.campaign_status),
                _loaded_at        = now(),
                _run_id           = ?
            FROM _ci_live l
            WHERE l.campaign_id = ci.campaign_id
            """,
            [run_id],
        ).fetchone()[0]
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return n


def _recipient_mix(conn, run_id: str) -> dict:
    # [2026-07-18 wave-2, MOF-10] Repointed from pg.public.contact_frequency_campaign_daily
    # (retiring pipeline Supabase, postgres_scanner attach) to the warehouse-local
    # raw_pipeline_contact_frequency_campaign_daily. The local table carries the FULL
    # history (backfilled 2026-07-18, _run_id='cf-history-backfill-20260718') so
    # recipient_sends_total stays cumulative — the invariant below is preserved.
    full = os.environ.get("CAMPAIGN_INFRA_RECIP_FULL", "") == "1"
    try:
        # Incremental scope = CAMPAIGNS with recent sends; their FULL history is
        # aggregated so recipient_sends_total stays cumulative (never a window
        # stat that would shrink vs the backfill run). Campaigns with zero
        # qualifying sends are absent from the staging -> skipped, never touched.
        campaign_filter = "" if full else (
            "WHERE cf.campaign_id IN ("
            "  SELECT DISTINCT campaign_id FROM raw_pipeline_contact_frequency_campaign_daily"
            f"  WHERE send_date >= current_date - INTERVAL {_RECIP_WINDOW_DAYS} DAY)"
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _ci_recip AS
            WITH agg AS (
                SELECT CAST(cf.campaign_id AS VARCHAR) AS campaign_id,
                       SUM(cf.sent_count)::BIGINT AS sends_total,
                       SUM(CASE WHEN rd.recipient_esp = 'google'
                                THEN cf.sent_count ELSE 0 END)::BIGINT AS s_google,
                       SUM(CASE WHEN rd.recipient_esp = 'microsoft'
                                THEN cf.sent_count ELSE 0 END)::BIGINT AS s_microsoft,
                       SUM(CASE WHEN rd.recipient_esp IS NOT NULL
                                 AND rd.recipient_esp NOT IN ('google', 'microsoft')
                                THEN cf.sent_count ELSE 0 END)::BIGINT AS s_other,
                       SUM(CASE WHEN rd.recipient_esp IS NULL
                                THEN cf.sent_count ELSE 0 END)::BIGINT AS s_unknown
                FROM raw_pipeline_contact_frequency_campaign_daily cf
                LEFT JOIN core.recipient_domain rd ON rd.domain = lower(cf.lead_domain)
                {campaign_filter}
                GROUP BY 1
                HAVING SUM(cf.sent_count) > 0
            )
            SELECT campaign_id, sends_total, s_google, s_microsoft, s_other, s_unknown,
                   round(greatest(s_google, s_microsoft, s_other)::DOUBLE
                         / sends_total, 4) AS share,
                   CASE
                     WHEN sends_total >= 100
                          AND greatest(s_google, s_microsoft, s_other)::DOUBLE
                              / sends_total >= 0.8
                     THEN CASE greatest(s_google, s_microsoft, s_other)
                              WHEN s_google THEN 'google'
                              WHEN s_microsoft THEN 'microsoft'
                              ELSE 'other' END
                     WHEN sends_total >= 100
                          AND (s_google + s_microsoft + s_other) > 0
                          AND greatest(s_google, s_microsoft, s_other)::DOUBLE
                              / (s_google + s_microsoft + s_other) < 0.8
                     THEN 'mixed'      -- known buckets genuinely disagree
                     ELSE 'unknown'    -- unknown-dominated or <100 sends: don't
                                       -- assert what the data can't prove
                   END AS label
            FROM agg
            """
        )
    finally:
        pass  # warehouse-local only since 2026-07-18 — nothing attached to detach

    conn.execute("BEGIN")
    try:
        updated = conn.execute(
            """
            UPDATE core.campaign_infra AS ci SET
                recipient_esp             = r.label,
                recipient_esp_share       = r.share,
                recipient_sends_total     = r.sends_total,
                recip_sends_google        = r.s_google,
                recip_sends_microsoft     = r.s_microsoft,
                recip_sends_other         = r.s_other,
                recip_sends_unknown       = r.s_unknown,
                recipient_esp_source      = 'send_mix',
                recipient_esp_computed_at = now(),
                _loaded_at                = now(),
                _run_id                   = ?
            FROM _ci_recip r
            WHERE r.campaign_id = ci.campaign_id
              -- never DOWNGRADE a real label to 'unknown' (protects
              -- manifest_pump_day labels and earlier stronger measurements)
              AND NOT (r.label = 'unknown' AND ci.recipient_esp <> 'unknown')
            """,
            [run_id],
        ).fetchone()[0]
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    staged, unmatched = conn.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE ci.campaign_id IS NULL)
        FROM _ci_recip r
        LEFT JOIN core.campaign_infra ci ON ci.campaign_id = r.campaign_id
        """
    ).fetchone()
    labels = dict(conn.execute(
        "SELECT label, count(*) FROM _ci_recip GROUP BY 1").fetchall())
    if unmatched:
        # sends in pg for campaigns outside the registry universe (pre-May,
        # never in dim/census) — visible, not silently dropped.
        logger.warning("campaign_infra: %d recipient-mix campaigns not in registry "
                       "(pg sends outside the universe window)", unmatched)
    return {"mode": "full" if full else f"incremental_{_RECIP_WINDOW_DAYS}d",
            "campaigns_staged": staged, "campaigns_updated": updated,
            "campaigns_unmatched": unmatched, "labels": labels}


def run_campaign_infra(ctx: RunContext) -> PhaseResult:
    conn = ctx.db

    universe = _stage_universe(conn)
    derivation = _stage_derivations(conn)
    merge = _merge(conn, ctx.run_id)
    live = _refresh_liveness(conn, ctx.run_id)
    recip = _recipient_mix(conn, ctx.run_id)

    registry_total = conn.execute("SELECT count(*) FROM core.campaign_infra").fetchone()[0]
    by_source = dict(conn.execute(
        "SELECT derivation_source, count(*) FROM core.campaign_infra GROUP BY 1"
    ).fetchall())
    for t in ("_ci_universe", "_ci_dim", "_ci_census", "_ci_frozen", "_ci_best",
              "_ci_live", "_ci_recip"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")

    logger.info(
        "campaign_infra: universe=%d registry=%d inserted=%d upgraded=%d live=%d "
        "disagreements=%d recip=%s",
        universe, registry_total, merge["inserted"], merge["upgraded"], live,
        derivation["disagreements"], recip,
    )
    return PhaseResult(
        rows_in=universe,
        rows_out=registry_total,
        notes={
            "universe": universe,
            "registry_total": registry_total,
            "inserted": merge["inserted"],
            "upgraded": merge["upgraded"],
            "liveness_refreshed": live,
            "derived_this_run_by_source": derivation["derived_by_source"],
            "registry_by_source": by_source,
            "dim_census_disagreements": derivation["disagreements"],
            "raw_tag_vendors_surfaced": derivation["raw_tag_vendors"],
            "recipient_mix": recip,
        },
    )


def main() -> int:
    """One-off run (e.g. the CAMPAIGN_INFRA_RECIP_FULL=1 backfill). NOT the
    nightly path — opens its own writer connection (core.db.connect() acquires
    the box flock, acquire-or-wait)."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from datetime import datetime, timezone

    from core import db as db_module
    from core.credentials import load_credentials

    run_id = f"campaign_infra_oneoff_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    conn = db_module.connect()
    try:
        ctx = RunContext(run_id=run_id, db=conn, credentials=load_credentials())
        result = run_campaign_infra(ctx)
        print(json.dumps(result.notes, indent=2, default=str))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
