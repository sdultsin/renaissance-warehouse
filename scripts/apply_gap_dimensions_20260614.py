#!/usr/bin/env python3
"""ONE-SHOT apply+backfill of the 3 portal gap dimensions + the 4 staged DDL.

Run ONCE on the droplet, UNDER flock (/root/core/warehouse.write.lock), in a post-cutover idle
writer window (outside 03:30-05:45 UTC, no nightly/meetings_refresh running). Idempotent: every
DDL is version-guarded by apply_ddl_file; the meeting rebuild is a full projection; the SLA build
and credit load are DELETE+reinsert / UPSERT. Safe to re-run.

Steps (each verified, fail-loud):
  0. preflight: schema_version, no competing writer
  1. apply DDL 66 (SMS), 67 (ws soft-delete), 68 (ws views), 69 (SLA), 70 (portal gap dims)
  2. rebuild core.meeting  -> backfills advisor / advisor_name / advisor_partner / inbox_manager
  3. build core.sla_reply_time (+ daily snapshot, full backfill)
  4. load core.instantly_credit (via portal_credits.py; drops "The Eagles" free-trial junk row)

DDL is applied via core.db.apply_ddl_file (the warehouse's own mechanism). The meeting rebuild is
invoked through the orchestrator's canonical phase so it shares the exact production code path.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("apply_gap_dims")

DDL = REPO_ROOT / "sql" / "ddl"
APPLY_ORDER = [
    (66, "66_sms_failure_assets.sql"),
    (67, "67_workspace_soft_delete.sql"),
    (68, "68_workspace_fact_driven_views.sql"),
    (69, "69_sla_reply_time.sql"),
    (70, "70_portal_gap_dimensions.sql"),
]


def schema_version(conn) -> int:
    return conn.execute("SELECT COALESCE(max(version), -1) FROM core.schema_version").fetchone()[0]


def main() -> int:
    conn = db_module.connect()  # read-write; we hold the flock externally
    log.info("connected to %s", db_module.DB_PATH if hasattr(db_module, "DB_PATH") else "warehouse")

    sv0 = schema_version(conn)
    log.info("STEP 0 preflight: schema_version=%d", sv0)
    if sv0 != 65:
        log.warning("schema_version is %d (expected 65). Continuing — apply_ddl_file is version-guarded "
                    "and will no-op anything already applied.", sv0)

    # STEP 1 — apply DDL in order, verifying version advances per newly-applied file.
    for ver, fname in APPLY_ORDER:
        f = DDL / fname
        if not f.exists():
            log.error("MISSING DDL FILE: %s", f); return 2
        before = schema_version(conn)
        applied = db_module.apply_ddl_file(conn, f, version=ver)
        after = schema_version(conn)
        if applied:
            assert after >= ver, f"version did not advance to {ver} (now {after})"
            log.info("STEP 1 applied %s -> schema_version %d->%d", fname, before, after)
        else:
            log.info("STEP 1 already-applied %s (schema_version stays %d)", fname, after)
    sv1 = schema_version(conn)
    log.info("STEP 1 done: schema_version now %d", sv1)
    conn.close()  # release before the orchestrator opens its own connection

    # STEP 2 — rebuild core.meeting via the production canonical phase (backfills advisor/IM).
    log.info("STEP 2 rebuilding core.meeting via orchestrator canonical phase (ingest=meeting)")
    from core.orchestrator import main as orch_main
    rc = orch_main(["--phase", "canonical", "--ingest", "meeting"])
    if rc not in (0, None):
        log.error("STEP 2 meeting rebuild returned rc=%s", rc); return 3

    # verify the backfill landed
    conn = db_module.connect(read_only=True)
    adv = conn.execute("SELECT count(*) FROM core.meeting WHERE advisor IS NOT NULL").fetchone()[0]
    advp = conn.execute("SELECT count(DISTINCT advisor_partner) FROM core.meeting WHERE advisor_partner IS NOT NULL").fetchone()[0]
    imc = conn.execute("SELECT count(*) FROM core.meeting WHERE inbox_manager IS NOT NULL").fetchone()[0]
    imd = conn.execute("SELECT count(DISTINCT inbox_manager) FROM core.meeting WHERE inbox_manager IS NOT NULL").fetchone()[0]
    log.info("STEP 2 verified: meeting.advisor non-null=%d (distinct partners=%d); inbox_manager non-null=%d (distinct=%d)",
             adv, advp, imc, imd)
    conn.close()

    # STEP 3 — build SLA reply-time (full backfill).
    log.info("STEP 3 building core.sla_reply_time (full backfill)")
    from scripts.build_sla_reply_time import main as sla_main
    rc = sla_main(["--snapshot-all"])
    if rc not in (0, None):
        log.error("STEP 3 sla build returned rc=%s", rc); return 4
    conn = db_module.connect(read_only=True)
    sla_total = conn.execute("SELECT count(*) FROM core.sla_reply_time").fetchone()[0]
    sla_ans = conn.execute("SELECT count(*) FROM core.sla_reply_time WHERE response_latency_minutes IS NOT NULL").fetchone()[0]
    sla_daily = conn.execute("SELECT count(*) FROM core.sla_reply_time_daily").fetchone()[0]
    log.info("STEP 3 verified: sla_reply_time=%d (answered=%d), daily snapshot=%d", sla_total, sla_ans, sla_daily)
    conn.close()

    # STEP 4 — load Instantly credits.
    log.info("STEP 4 loading core.instantly_credit (portal_credits.py path; drops free-trial junk)")
    from scripts.load_instantly_credit import main as cred_main
    rc = cred_main([])
    if rc not in (0, None):
        log.error("STEP 4 credit load returned rc=%s", rc); return 5
    conn = db_module.connect(read_only=True)
    cred_rows = conn.execute("SELECT count(*) FROM core.instantly_credit").fetchone()[0]
    has_eagles = conn.execute("SELECT count(*) FROM core.instantly_credit WHERE lower(workspace) LIKE 'the eagles%' OR lower(plan)='free trial'").fetchone()[0]
    pool = conn.execute("SELECT count(*) FROM core.instantly_credit_pool").fetchone()[0]
    final_sv = conn.execute("SELECT max(version) FROM core.schema_version").fetchone()[0]
    conn.close()
    log.info("STEP 4 verified: instantly_credit rows=%d (eagles/free-trial leaked=%d), pool rows=%d", cred_rows, has_eagles, pool)
    if has_eagles:
        log.error("ANOMALY: free-trial junk row leaked into core.instantly_credit"); return 6

    log.info("ALL DONE. final schema_version=%d", final_sv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
