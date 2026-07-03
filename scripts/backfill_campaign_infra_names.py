#!/usr/bin/env python3
"""Name-heuristic infra backfill for core.campaign_infra — hard validation gate. [2026-07-03]

WHY: campaigns that predate the tag era (or were deleted before raw_instantly_campaign_dim
existed) have no tag/census derivation, but many carry the infra family in their NAME
("F2 - GEN - RESELLER-GOOG - ...", "OTD - AI GENERAL"). A name token is weak evidence,
so this backfill is gated: it may fire ONLY after proving >=99% agreement against
campaigns whose infra is independently tag-derived, and it ranks LOWEST in the
registry precedence (name_heuristic=1) so any real derivation overwrites it later.

FIX: fills infra_vendor/infra_esp on rows currently 'unknown' with
derivation_source='name_heuristic'. Ambiguous tokens are NEVER applied:
bare OUTLOOK (recipient-vs-infra unclear without OTD context) and GOOG/GOOGLE
(google can be the recipient OR the Reseller arm in names) are reported only.

TOKEN MAP (case-insensitive, word-ish boundaries):
  OTD / O2D / OUTREACH TODAY -> (OTD, OTD)         RESELLER -> (Reseller, google)
  MILKBOX / 'MB '            -> (MilkBox, outlook)  MAILIN   -> (MailIn, unknown)
  bare OUTLOOK -> AMBIGUOUS (report only)           GOOG/GOOGLE -> AMBIGUOUS (report only)

MEASURED LIVE AGREEMENT (classifier vs tag-derived truth, read-only, serving snapshot
warehouse_20260703_043558_874.duckdb, main.raw_instantly_campaign_dim, 272 campaigns,
195 with a known-family tag):
  truth OTD      (168): 102 classified OTD, 66 abstain (no family token)  -> 102/102 correct
  truth Reseller  (19):  18 classified Reseller, 1 AMBIG-google           ->  18/18 correct
  truth MilkBox    (7):   7 classified MilkBox                            ->   7/7 correct
  truth MailIn     (1):   0 classified (name has no MAILIN token), abstain
  Applied-token precision: OTD 100% (102/102) · Reseller 100% (18/18) · MilkBox 100% (7/7)
  Overall agreement on applied tokens: 127/127 = 100.0%  -> the >=99% gate CAN pass.
  Ambiguity proof: "ON - GOOGLE - ISAAC - BTC/GQ - (EYVER)" is tagged Reseller Active but
  its only token is GOOGLE — exactly why GOOG stays excluded from auto-apply.
  MailIn had ZERO validation support -> families without measured support are also
  excluded from apply at runtime (reported only).

WRITE PATTERN: explicit UPDATE ... FROM a staged classification — no ON CONFLICT
(DuckDB ART-index INTERNAL duplicate-key trap, see scripts/backfill_account_tags_full.py).
Validation is read-only; --apply takes the writer flock via core.db.connect().

USAGE (on the box):
  python3 scripts/backfill_campaign_infra_names.py --validate   # read-only report (default)
  python3 scripts/backfill_campaign_infra_names.py --apply      # refuses unless gate passes
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/root/renaissance-warehouse")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import db as core_db  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_infra_names")

RUN_ID = "backfill_name_heuristic_20260703"
GATE_PCT = 99.0

# Applied families -> (infra_vendor, infra_esp). Ambiguous classes are never applied.
FAMILY_ESP = {"OTD": "OTD", "Reseller": "google", "MilkBox": "outlook", "MailIn": "unknown"}
AMBIGUOUS = ("AMBIG-outlook", "AMBIG-google")

# The classifier — ONE definition used by both --validate and --apply. Order matters:
# family tokens first (so "RESELLER-GOOG" reads Reseller, not AMBIG-google).
CLASSIFY_SQL = """CASE
    WHEN regexp_matches(upper({name}), '(^|[^A-Z0-9])(OTD|O2D)([^A-Z0-9]|$)')
      OR upper({name}) LIKE '%OUTREACH TODAY%'                     THEN 'OTD'
    WHEN regexp_matches(upper({name}), '(^|[^A-Z0-9])RESELLER')    THEN 'Reseller'
    WHEN upper({name}) LIKE '%MILKBOX%'
      OR regexp_matches(upper({name}), '(^|[^A-Z0-9])MB ')         THEN 'MilkBox'
    WHEN upper({name}) LIKE '%MAILIN%'                             THEN 'MailIn'
    WHEN regexp_matches(upper({name}), '(^|[^A-Z0-9])OUTLOOK([^A-Z0-9]|$)') THEN 'AMBIG-outlook'
    WHEN regexp_matches(upper({name}), 'GOOG')                     THEN 'AMBIG-google'
    ELSE 'unclassified' END"""

NAME_COL = "coalesce(ci.last_seen_name, ci.first_seen_name)"


def validate(con) -> tuple[bool, dict[str, float]]:
    """Score the classifier against tag-derived registry truth. Returns (gate_ok, per-family precision)."""
    cls = CLASSIFY_SQL.format(name=NAME_COL)
    rows = con.execute(f"""
        SELECT ci.infra_vendor AS truth, {cls} AS name_class, count(*) c
        FROM core.campaign_infra ci
        WHERE ci.derivation_source IN ('dim_tag', 'census_majority', 'frozen_tag_table')
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
    log.info("validation matrix (truth = tag/census-derived infra_vendor):")
    applied_total = applied_correct = 0
    per_fam: dict[str, list[int]] = {}  # family -> [correct, classified]
    for truth, name_class, c in rows:
        log.info("  truth=%-12s name_class=%-14s %d", truth, name_class, c)
        if name_class in FAMILY_ESP:
            ok = (truth == name_class)
            per_fam.setdefault(name_class, [0, 0])
            per_fam[name_class][1] += c
            if ok:
                per_fam[name_class][0] += c
                applied_correct += c
            applied_total += c
    mism = con.execute(f"""
        SELECT ci.campaign_id, ci.workspace_slug, {NAME_COL}, ci.infra_vendor, {cls}
        FROM core.campaign_infra ci
        WHERE ci.derivation_source IN ('dim_tag', 'census_majority', 'frozen_tag_table')
          AND {cls} IN ({','.join(repr(f) for f in FAMILY_ESP)})
          AND {cls} <> ci.infra_vendor""").fetchall()
    for m in mism:
        log.warning("  MISMATCH %s ws=%s name=%r truth=%s classified=%s", *m)
    precision: dict[str, float] = {}
    ok = applied_total > 0
    for fam, (corr, tot) in sorted(per_fam.items()):
        precision[fam] = 100.0 * corr / tot
        log.info("  precision %-10s %d/%d = %.1f%%", fam, corr, tot, precision[fam])
        if precision[fam] < GATE_PCT:
            ok = False
    overall = 100.0 * applied_correct / applied_total if applied_total else 0.0
    log.info("  overall agreement on applied tokens: %d/%d = %.1f%%",
             applied_correct, applied_total, overall)
    if overall < GATE_PCT:
        ok = False
    log.info("GATE (>=%.0f%% overall AND per-token): %s", GATE_PCT, "PASS" if ok else "FAIL")
    return ok, precision


def apply(con, precision: dict[str, float]) -> None:
    now = datetime.now(timezone.utc)
    cls = CLASSIFY_SQL.format(name=NAME_COL)
    # Families provably >=99% in validation; unmeasured families (no validation rows)
    # are NOT applied — report them instead.
    applyable = [f for f in FAMILY_ESP if precision.get(f, 0.0) >= GATE_PCT]
    skipped_fams = [f for f in FAMILY_ESP if f not in applyable]
    esp_case = (f"CASE ({cls}) "
                + " ".join(f"WHEN '{f}' THEN '{FAMILY_ESP[f]}'" for f in applyable)
                + " END")
    fam_list = ",".join(repr(f) for f in applyable)

    plan = con.execute(f"""
        SELECT {cls} AS fam, count(*) FROM core.campaign_infra ci
        WHERE ci.infra_vendor = 'unknown'
          AND coalesce(ci.derivation_source, 'unknown') = 'unknown'
          AND {cls} <> 'unclassified'
        GROUP BY 1 ORDER BY 1""").fetchall()
    for fam, c in plan:
        action = ("APPLY" if fam in applyable else
                  "REPORT-ONLY (ambiguous)" if fam in AMBIGUOUS else
                  "REPORT-ONLY (no validation support)")
        log.info("  unknown campaigns classified %-14s %4d -> %s", fam, c, action)
    if skipped_fams:
        log.warning("families without >=%.0f%% measured support, NOT applied: %s",
                    GATE_PCT, skipped_fams)
    if not applyable:
        log.error("no applyable families — nothing to write")
        return

    con.execute(f"""
        UPDATE core.campaign_infra ci SET
            infra_vendor = ({cls}),
            infra_esp = ({esp_case}),
            derivation_source = 'name_heuristic',
            derivation_note = trim(coalesce(ci.derivation_note || ' | ', '') ||
                'name_heuristic [{RUN_ID}]: token=' || ({cls}) ||
                ' name=' || coalesce({NAME_COL}, '')),
            _loaded_at = ?, _run_id = ?
        WHERE ci.infra_vendor = 'unknown'
          AND coalesce(ci.derivation_source, 'unknown') = 'unknown'
          AND ({cls}) IN ({fam_list})
    """, [now, RUN_ID])
    n = con.execute("SELECT count(*) FROM core.campaign_infra WHERE _run_id = ?",
                    [RUN_ID]).fetchone()[0]
    log.info("APPLY complete — %d rows now carry run_id %s", n, RUN_ID)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--validate", action="store_true", help="read-only report (default)")
    mode.add_argument("--apply", action="store_true",
                      help="refuses unless validation gate passes (>=99%% overall + per-token)")
    args = ap.parse_args()

    con = core_db.connect() if args.apply else core_db.connect(read_only=True)
    try:
        gate_ok, precision = validate(con)
        if not args.apply:
            log.info("validate-only — no write.")
            return
        if not gate_ok:
            log.error("GATE FAILED — refusing to apply. Fix the classifier or the truth set.")
            sys.exit(1)
        apply(con, precision)
    finally:
        con.close()


if __name__ == "__main__":
    main()
