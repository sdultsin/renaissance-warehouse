#!/usr/bin/env python3
"""One-off seed of core.campaign_infra from the pump-day manifest. [2026-07-03]

WHY: the 2026-07-02 pump-day ESP matrix (deliverables/2026-07-02-pump-day-esp-matrix)
plus the F2/F4 cm-work mutation lists proved sending arm AND recipient ESP for 94
campaigns — most of them DELETED from Instantly the same day, so no live tag or
census derivation can ever recover them. This is the only durable record of what
those campaigns were; the nightly entities/campaign_infra.py can't re-derive it.

WHAT: reads seed_data/campaign_infra_manifest_20260702.json and upserts into
core.campaign_infra with derivation_source='manifest_pump_day' (precedence rank 4):
  - infra fields (infra_vendor/infra_esp): written only where the existing row's
    derivation_source ranks BELOW manifest_pump_day (unknown/name_heuristic/
    rg_partner/frozen_tag_table) — NEVER overwrites dim_tag/census_majority/manual.
    Upgrades are logged in derivation_note.
  - recipient_esp (+recipient_esp_source='manifest_pump_day'): written ONLY where
    the row's recipient_esp is 'unknown' or the row is being created — a MEASURED
    send-mix label (recipient_esp_source='send_mix') always wins over the manifest.
  - missing rows are created with identity from the latest raw_pipeline_campaigns
    row (workspace_slug via core.workspace — NEVER raw workspace_name, stale codenames).

WRITE PATTERN: staged TEMP table → explicit UPDATE ... FROM + INSERT ... anti-join.
No INSERT ... ON CONFLICT DO UPDATE — that trips a DuckDB ART-index INTERNAL
"duplicate key" abort on some tables in this repo (see scripts/
backfill_account_tags_full.py, 2026-07-01). DRY by default.

Verified read-only on serving snapshot warehouse_20260703_043558_874.duckdb:
manifest = 94 campaigns (50 pump-day percamp rows — 42 OTD-MS→microsoft,
7 RESELLER-GOOG→google, 1 OTD-GEN→mixed; 7 GOOGLE-other arm rows skipped by design —
+ 38 delete_otd_general + 6 delete_milkbox from mutation_lists.json);
94/94 present in raw_pipeline_campaigns (identity join covers all), 56/94 still in
raw_instantly_campaign_dim (the rest deleted → exactly why this seed must exist).

USAGE (on the box):
  python3 scripts/seed_campaign_infra_manual.py           # DRY: report what would change
  python3 scripts/seed_campaign_infra_manual.py --apply   # write (takes the writer flock)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/root/renaissance-warehouse")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import db as core_db  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("seed_campaign_infra")

RUN_ID = "seed_manifest_pump_day_20260702"
MANIFEST = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "seed_data", "campaign_infra_manifest_20260702.json",
)

# Derivation precedence (DDL 1071 contract). manifest_pump_day may only overwrite
# infra fields on rows ranking strictly BELOW it.
PRECEDENCE_CASE = """CASE derivation_source
    WHEN 'manual' THEN 7 WHEN 'dim_tag' THEN 6 WHEN 'census_majority' THEN 5
    WHEN 'manifest_pump_day' THEN 4 WHEN 'frozen_tag_table' THEN 3
    WHEN 'rg_partner' THEN 2 WHEN 'name_heuristic' THEN 1 ELSE 0 END"""
MANIFEST_RANK = 4


def load_manifest() -> list[dict]:
    with open(MANIFEST) as f:
        doc = json.load(f)
    rows = doc["campaigns"]
    ids = {r["campaign_id"] for r in rows}
    if len(ids) != len(rows):
        raise SystemExit(f"manifest has duplicate campaign_ids ({len(rows)} rows, {len(ids)} ids)")
    log.info("manifest: %d campaigns from %s", len(rows), doc["_meta"]["sources"])
    return rows


def stage(con, rows: list[dict]) -> None:
    con.execute("""CREATE OR REPLACE TEMP TABLE _manifest_stage (
        campaign_id VARCHAR PRIMARY KEY, infra_vendor VARCHAR, infra_esp VARCHAR,
        recipient_esp VARCHAR, source_detail VARCHAR)""")
    con.executemany(
        "INSERT INTO _manifest_stage VALUES (?,?,?,?,?)",
        [(r["campaign_id"], r["infra_vendor"], r["infra_esp"], r["recipient_esp"],
          r["source_detail"]) for r in rows])


def report(con) -> dict:
    """Classify every manifest row against the current registry (read-only)."""
    counts = dict(con.execute(f"""
        SELECT bucket, count(*) FROM (
          SELECT CASE
            WHEN ci.campaign_id IS NULL THEN 'insert_new'
            WHEN ({PRECEDENCE_CASE.replace('derivation_source', 'ci.derivation_source')}) < {MANIFEST_RANK}
              THEN 'upgrade_infra'
            ELSE 'infra_kept_higher_precedence' END AS bucket
          FROM _manifest_stage s LEFT JOIN core.campaign_infra ci USING (campaign_id))
        GROUP BY bucket""").fetchall())
    recip = con.execute(f"""
        SELECT count(*) FROM _manifest_stage s
        JOIN core.campaign_infra ci USING (campaign_id)
        WHERE ci.recipient_esp = 'unknown' AND s.recipient_esp <> 'unknown'""").fetchone()[0]
    counts["recipient_fill_existing_unknown"] = recip
    for k, v in sorted(counts.items()):
        log.info("  %-32s %d", k, v)
    return counts


def apply_writes(con) -> None:
    now = datetime.now(timezone.utc)

    # 1) UPDATE infra fields where manifest outranks the existing derivation.
    con.execute(f"""
        UPDATE core.campaign_infra ci SET
            infra_vendor = s.infra_vendor,
            infra_esp = s.infra_esp,
            derivation_source = 'manifest_pump_day',
            derivation_note = trim(coalesce(ci.derivation_note || ' | ', '') ||
                'upgraded ' || coalesce(ci.derivation_source, 'unknown') ||
                '->manifest_pump_day [{RUN_ID}]: ' || s.source_detail),
            _loaded_at = ?, _run_id = ?
        FROM _manifest_stage s
        WHERE ci.campaign_id = s.campaign_id
          AND ({PRECEDENCE_CASE.replace('derivation_source', 'ci.derivation_source')}) < {MANIFEST_RANK}
    """, [now, RUN_ID])

    # 2) UPDATE recipient label ONLY where currently unknown (send_mix always wins).
    con.execute("""
        UPDATE core.campaign_infra ci SET
            recipient_esp = s.recipient_esp,
            recipient_esp_source = 'manifest_pump_day',
            recipient_esp_computed_at = ?,
            _loaded_at = ?, _run_id = ?
        FROM _manifest_stage s
        WHERE ci.campaign_id = s.campaign_id
          AND ci.recipient_esp = 'unknown' AND s.recipient_esp <> 'unknown'
    """, [now, now, RUN_ID])

    # 3) INSERT rows the registry has never seen — identity from the LATEST
    #    raw_pipeline_campaigns row; slug from core.workspace (canonical), never
    #    raw workspace_name (stale codenames). raw_pipeline_campaigns.workspace_id
    #    holds the workspace SLUG (matches core.workspace.slug, name as fallback,
    #    NEVER workspace_id) — same resolution as entities/campaign_infra.py
    #    _stage_universe. An unresolvable raw workspace id is kept as a
    #    derivation_note breadcrumb (DDL 1071 contract).
    con.execute(f"""
        INSERT INTO core.campaign_infra
            (campaign_id, workspace_slug, first_seen_name, last_seen_name, campaign_status,
             infra_vendor, infra_esp, derivation_source, derivation_note,
             recipient_esp, recipient_esp_source, recipient_esp_computed_at,
             first_seen_at, _loaded_at, _run_id)
        SELECT s.campaign_id, coalesce(ws.slug, wn.slug), rp.name, rp.name,
               TRY_CAST(rp.status AS INTEGER),
               s.infra_vendor, s.infra_esp, 'manifest_pump_day',
               'seeded [{RUN_ID}]: ' || s.source_detail ||
                   CASE WHEN ws.slug IS NULL AND wn.slug IS NULL
                             AND rp.workspace_id IS NOT NULL
                        THEN ' | raw_ws=' || rp.workspace_id ELSE '' END,
               s.recipient_esp, 'manifest_pump_day', ?, ?, ?, ?
        FROM _manifest_stage s
        LEFT JOIN (
            SELECT campaign_id, name, status, workspace_id,
                   row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) rn
            FROM main.raw_pipeline_campaigns
        ) rp ON rp.campaign_id = s.campaign_id AND rp.rn = 1
        LEFT JOIN core.workspace ws ON ws.slug = rp.workspace_id
        LEFT JOIN core.workspace wn ON wn.name = rp.workspace_id
        WHERE NOT EXISTS (SELECT 1 FROM core.campaign_infra ci
                          WHERE ci.campaign_id = s.campaign_id)
    """, [now, now, now, RUN_ID])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="write (default is DRY report)")
    args = ap.parse_args()

    rows = load_manifest()
    con = core_db.connect() if args.apply else core_db.connect(read_only=True)
    try:
        stage(con, rows)
        log.info("plan (%s):", "APPLY" if args.apply else "DRY")
        report(con)
        if not args.apply:
            log.info("DRY complete — no write. Re-run with --apply.")
            return
        pre = con.execute("SELECT count(*) FROM core.campaign_infra").fetchone()[0]
        apply_writes(con)
        post, touched = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE _run_id = ?) FROM core.campaign_infra",
            [RUN_ID]).fetchone()
        log.info("APPLY complete — table %d -> %d rows; %d rows carry run_id %s",
                 pre, post, touched, RUN_ID)
    finally:
        con.close()


if __name__ == "__main__":
    main()
