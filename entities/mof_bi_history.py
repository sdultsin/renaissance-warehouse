"""MOF-BI durable batch — history-escrow seeder + nightly hygiene (DDLs 1101–1109).

Three idempotent jobs, all registered in the 'canonical' phase:

1. HISTORY ESCROW SEED (DDL 1101 tables). Loads the one-time Instantly day-grain
   analytics history (fetched 2026-07-15 via the API, incl. deleted campaigns) from the
   parquet files at <repo>/seed_data/mof_bi_20260715/ into the four
   raw_instantly_*_history tables. INSERT … ON CONFLICT DO NOTHING — append-only /
   frozen: the first nightly run is the initial backfill (variant_copy precedent),
   every later run is a no-op. seed_data/ is gitignored BY DESIGN (public-repo "data
   never in repo" rule) — the dir is copied onto the droplet checkout at ship time
   (untracked files survive the hourly `git reset --hard` guard); the durable master
   copy lives in the parent repo at
   Renaissance/deliverables/2026-07-14-cold-email-bi/warehouse-seed/mof_bi_20260715/,
   and once loaded the WAREHOUSE TABLES are the durable home (they ride the nightly
   publish + MotherDuck migration; the original droplet escrow dies with the box
   ~2026-07-25). Missing seed files degrade to a logged warning (never a raise — the
   DDL-92 nightly-killer class).

   Expected initial load (validation):
     ws_daily        2,829 rows loaded · sum(sent)=192,412,822
                    (parquet holds 2,837 incl. 8 identical-value boundary duplicates)
     campaign_daily 20,664 rows · 1,711 campaigns
     campaign_steps 16,183 rows · 1,680 campaigns · sum(sent)=100,985,165
                    (19,059 in the parquet; 2,876 null-step reply-residue rows excluded)
     campaign_dim      288 rows

2. OFFER-SCOPE AUTO-APPEND (DDL 1103 table). Appends campaign_ids newly seen in
   raw_instantly_campaign_dim but absent from core.campaign_offer_scope, applying the
   OFFER-TAG-SPEC convention (untagged = business funding) with a tag-hint waterfall.
   confidence='low', classified_by='default_untagged_new', evidence marks the auto-append
   — human/copy re-classification can overwrite later (frozen Sam-validated rows are
   never touched: ON CONFLICT DO NOTHING + anti-join).

3. WORKSPACE-SLUG NORMALIZATION (DDL 1107 columns). Populates workspace_slug_norm on
   core.reply and main.raw_instantly_email_message from core.v_workspace_slug_norm for
   rows where it is still NULL (covers both the one-time backfill and each night's new
   rows WITHOUT touching the hot loaders' INSERT column lists — post-insert enrichment,
   the entities/meeting.py offer-inheritance precedent). Raw workspace_id values are
   NEVER rewritten in place. Known coverage note: the-eagles has NO rows in
   raw_instantly_email_message (verified 2026-07-15) — nothing is fabricated for it.
"""
from __future__ import annotations

import logging
from pathlib import Path

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.mof_bi_history")

SEED_DIR = REPO_ROOT / "seed_data" / "mof_bi_20260715"

_METRICS = (
    "sent, contacted, new_leads_contacted, opened, unique_opened, clicks, unique_clicks, "
    "replies, unique_replies, replies_automatic, unique_replies_automatic, "
    "opportunities, unique_opportunities"
)
_STEP_METRICS = (
    "sent, opened, unique_opened, clicks, unique_clicks, "
    "replies, unique_replies, replies_automatic, unique_replies_automatic"
)

# (table, parquet file, insert SQL built over read_parquet(?))
_SEEDS = [
    (
        "raw_instantly_ws_daily_history",
        "ws_daily.parquet",
        f"""
        INSERT INTO main.raw_instantly_ws_daily_history
          (workspace_slug, date, {_METRICS}, fetched_at)
        SELECT workspace_slug, date, {_METRICS}, fetched_at
        FROM read_parquet(?)
        -- 8 identical-value Jan-1 boundary duplicates exist in the parquet (fetch-window
        -- overlap); QUALIFY keeps one deterministically -> 2,829 rows land.
        QUALIFY row_number() OVER (PARTITION BY workspace_slug, date ORDER BY sent DESC) = 1
        ON CONFLICT (workspace_slug, date) DO NOTHING
        """,
    ),
    (
        "raw_instantly_campaign_daily_history",
        "campaign_daily.parquet",
        f"""
        INSERT INTO main.raw_instantly_campaign_daily_history
          (campaign_id, date, workspace_slug, {_METRICS}, fetched_at)
        SELECT CAST(campaign_id AS VARCHAR), date, workspace_slug, {_METRICS}, fetched_at
        FROM read_parquet(?)
        ON CONFLICT (campaign_id, date) DO NOTHING
        """,
    ),
    (
        "raw_instantly_campaign_steps_history",
        "campaign_steps.parquet",
        f"""
        INSERT INTO main.raw_instantly_campaign_steps_history
          (campaign_id, step, variant, workspace_slug, {_STEP_METRICS}, fetched_at)
        SELECT CAST(campaign_id AS VARCHAR), step, variant, workspace_slug, {_STEP_METRICS}, fetched_at
        FROM read_parquet(?)
        -- 2,876 unattributed-reply residue rows (NULL/'null' step or variant; sent=0,
        -- sum(replies)=10,078; 1,253 would collide on the PK) are reply-attribution noise
        -- from the API, not variant analytics — excluded here, retained in the committed parquet.
        WHERE step IS NOT NULL AND step <> 'null' AND variant IS NOT NULL AND variant <> 'null'
        ON CONFLICT (campaign_id, step, variant, fetched_at) DO NOTHING
        """,
    ),
    (
        "raw_instantly_campaign_dim_history",
        "campaign_dim.parquet",
        """
        INSERT INTO main.raw_instantly_campaign_dim_history
          (campaign_id, workspace_slug, name, status, timestamp_created, source, fetched_at)
        SELECT CAST(campaign_id AS VARCHAR), workspace_slug, name,
               CAST(status AS INTEGER), CAST(timestamp_created AS TIMESTAMPTZ), source,
               DATE '2026-07-15'
        FROM read_parquet(?)
        ON CONFLICT (campaign_id) DO NOTHING
        """,
    ),
]

_OFFER_SCOPE_APPEND = """
INSERT INTO core.campaign_offer_scope
  (campaign_id, campaign_name, workspace_slug, offer_class, in_funding_scope,
   classified_by, confidence, evidence, owner_cm, owner_basis, source_table, _source)
SELECT d.campaign_id,
       d.campaign_name,
       d.workspace_slug,
       CASE
         WHEN lower(COALESCE(d.tag_labels, '')) LIKE '%pre-ipo%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%pre ipo%'   THEN 'pre_ipo'
         WHEN lower(COALESCE(d.tag_labels, '')) LIKE '%section 125%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%s125%'      THEN 'section_125'
         WHEN lower(COALESCE(d.tag_labels, '')) LIKE '%tariff%'    THEN 'tariffs'
         ELSE 'business_funding'
       END,
       CASE
         WHEN lower(COALESCE(d.tag_labels, '')) LIKE '%pre-ipo%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%pre ipo%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%section 125%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%s125%'
           OR lower(COALESCE(d.tag_labels, '')) LIKE '%tariff%'    THEN FALSE
         ELSE TRUE
       END,
       'default_untagged_new',
       'low',
       'auto-append (nightly, entities/mof_bi_history.py): new campaign first seen '
         || CAST(d.first_seen_at AS VARCHAR)
         || '; OFFER-TAG-SPEC untagged=Funding convention; tags=' || COALESCE(d.tag_labels, '(none)'),
       CASE
         WHEN regexp_extract(COALESCE(d.campaign_name, ''), '\\(([A-Z]{2,10})\\)', 1) <> ''
           THEN regexp_extract(COALESCE(d.campaign_name, ''), '\\(([A-Z]{2,10})\\)', 1)
         WHEN d.workspace_slug = 'warm-leads' THEN 'WL'
         ELSE 'IDO'
       END,
       CASE
         WHEN regexp_extract(COALESCE(d.campaign_name, ''), '\\(([A-Z]{2,10})\\)', 1) <> '' THEN 'name'
         WHEN d.workspace_slug = 'warm-leads' THEN 'workspace'
         ELSE 'default'
       END,
       'raw_instantly_campaign_dim(auto-append)',
       'mof_bi_history_auto_append'
FROM raw_instantly_campaign_dim d
WHERE NOT EXISTS (SELECT 1 FROM core.campaign_offer_scope s WHERE s.campaign_id = d.campaign_id)
ON CONFLICT (campaign_id) DO NOTHING
"""

_SLUG_NORM_UPDATES = [
    (
        "core.reply",
        """
        UPDATE core.reply r
        SET workspace_slug_norm = wn.warehouse_slug
        FROM core.v_workspace_slug_norm wn
        WHERE r.workspace_slug_norm IS NULL
          AND r.workspace_id IS NOT NULL
          AND wn.alias_lower = lower(r.workspace_id)
        """,
    ),
    (
        "main.raw_instantly_email_message",
        """
        UPDATE main.raw_instantly_email_message m
        SET workspace_slug_norm = wn.warehouse_slug
        FROM core.v_workspace_slug_norm wn
        WHERE m.workspace_slug_norm IS NULL
          AND m.workspace_id IS NOT NULL
          AND wn.alias_lower = lower(m.workspace_id)
        """,
    ),
]


def _table_exists(conn, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
    )


def run(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    notes: dict = {}
    total = 0

    # 1. history escrow seed (no-op after the first successful load)
    for table, fname, sql in _SEEDS:
        if not _table_exists(conn, "main", table):
            logger.warning("mof_bi_history SKIP %s: table missing (DDL 1101 not applied yet).", table)
            notes[table] = "skipped: table missing"
            continue
        path = SEED_DIR / fname
        if not path.exists():
            logger.warning("mof_bi_history SKIP %s: seed file %s missing (repo checkout stale?).", table, path)
            notes[table] = "skipped: seed file missing"
            continue
        before = conn.execute(f"SELECT count(*) FROM main.{table}").fetchone()[0]
        conn.execute(sql, [str(path)])
        after = conn.execute(f"SELECT count(*) FROM main.{table}").fetchone()[0]
        loaded = after - before
        total += loaded
        notes[table] = {"rows_before": before, "rows_loaded": loaded}
        if loaded:
            logger.info("mof_bi_history seeded main.%s: +%d rows (now %d)", table, loaded, after)

    # 2. offer-scope auto-append for newly seen campaigns
    if _table_exists(conn, "core", "campaign_offer_scope"):
        before = conn.execute("SELECT count(*) FROM core.campaign_offer_scope").fetchone()[0]
        conn.execute(_OFFER_SCOPE_APPEND)
        appended = conn.execute("SELECT count(*) FROM core.campaign_offer_scope").fetchone()[0] - before
        notes["campaign_offer_scope_auto_append"] = appended
        if appended:
            logger.info("mof_bi_history offer-scope auto-append: +%d new campaigns (low-confidence default).", appended)
    else:
        notes["campaign_offer_scope_auto_append"] = "skipped: table missing (DDL 1103)"

    # 3. workspace_slug_norm enrichment (incremental; only NULL rows are touched)
    for target, sql in _SLUG_NORM_UPDATES:
        schema, table = target.split(".")
        if not _table_exists(conn, schema, table):
            notes[f"slug_norm:{target}"] = "skipped: table missing"
            continue
        has_col = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='workspace_slug_norm'",
            [schema, table],
        ).fetchone()
        if not has_col:
            logger.warning("mof_bi_history SKIP slug-norm %s: column missing (DDL 1107 not applied yet).", target)
            notes[f"slug_norm:{target}"] = "skipped: column missing"
            continue
        conn.execute(sql)
        remaining = conn.execute(
            f"SELECT count(*) FROM {target} WHERE workspace_slug_norm IS NULL AND workspace_id IS NOT NULL"
        ).fetchone()[0]
        notes[f"slug_norm:{target}"] = {"unresolved_remaining": remaining}

    return PhaseResult(rows_in=total, rows_out=total, notes=notes)


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "mof_bi_history", run)
