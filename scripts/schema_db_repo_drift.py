#!/usr/bin/env python3
"""schema_db_repo_drift.py — detect DB-schema-AHEAD-of-repo drift (the moderator apply-now blind spot).

WHY: `warehouse_git_divergence_guard.sh` catches git drift (box AHEAD/BEHIND/DIRTY vs origin/main),
but NOT the case the 2026-06-22 v96 incident exposed: a moderator `apply-now` physically applies a
DDL to the live warehouse (recorded in `core.schema_version`) WITHOUT the SQL ever being committed to
the repo. The git tree stays clean — the file was applied transiently — so the git guard sees nothing,
yet the live DB is now AHEAD of the repo and the next from-repo rebuild could silently DROP the change.

This is the missing signal: compare the versions APPLIED in the live DB against the DDL files COMMITTED
at origin/main, and flag any applied version with NO matching `sql/ddl/NN_*.sql` in the repo.

  ALERTING signal  : an applied `core.schema_version` row whose recorded DDL **filename** is not
                     committed under `sql/ddl/` at origin/main. We key on the FILENAME, not the
                     number, so a reused number can't mask the drift (kind=`file-not-in-repo` when the
                     number is also absent; `number-reused` when the number exists but under a
                     different committed file; `number-missing` when no filename was recorded).
  INFO (not alerted): `repo-ahead` — committed DDL not yet applied (NORMAL: applies on the next
                     nightly rebuild).

Read-only: opens the serving snapshot read_only, runs `git ls-tree` (no fetch, no writes, no tree
mutation). Designed to be (a) imported + unit-tested via compute_drift(), and (b) run by the guard,
which parses the final `DBDRIFT=<n> VERSIONS=<csv>` line. Fails SILENT (DBDRIFT=0, exit 0) on its own
errors — a guard helper must never false-alarm; the git signals still protect, and errors go to stderr.

Env:
  WAREHOUSE_CURRENT_DUCKDB  serving snapshot (read-only)   default /opt/duckdb/warehouse_current.duckdb
  WAREHOUSE_REPO_ROOT       repo checkout                  default /root/renaissance-warehouse
  SCHEMA_REPO_REF           ref to compare against         default origin/main
  SCHEMA_DRIFT_BASELINE     known-uncommitted versions to suppress (alert only on NEW drift)
                            default <repo>/scripts/schema_db_repo_drift_baseline.txt
"""
from __future__ import annotations

import os
import subprocess
import sys

CURRENT_DUCKDB = os.environ.get("WAREHOUSE_CURRENT_DUCKDB", "/opt/duckdb/warehouse_current.duckdb")
REPO_ROOT = os.environ.get("WAREHOUSE_REPO_ROOT", "/root/renaissance-warehouse")
REF = os.environ.get("SCHEMA_REPO_REF", "origin/main")
# Baseline = applied versions ALREADY known to be uncommitted (benign historical artifacts + residue
# being cleaned). They're EXCLUDED from the alerting DBDRIFT count (reported as `baselined`, info only)
# so the guard fires only on NEW drift — a watchdog that re-alerts a known, understood state is noise.
# Default = a version-controlled file next to this script; override with SCHEMA_DRIFT_BASELINE.
BASELINE_PATH = os.environ.get("SCHEMA_DRIFT_BASELINE",
                               os.path.join(REPO_ROOT, "scripts", "schema_db_repo_drift_baseline.txt"))


def load_baseline(path: str = None) -> set:
    """Parse the baseline file -> {version:int}. One number per line; `#` comments + blanks ignored;
    an inline `# ...` comment after a number is fine. Missing/unreadable file -> empty set (no baseline)."""
    p = path if path is not None else BASELINE_PATH
    out: set = set()
    try:
        with open(p) as f:
            for line in f:
                tok = line.split("#", 1)[0].strip()
                if tok.isdigit():
                    out.add(int(tok))
    except OSError:
        pass
    return out


def applied_versions(snapshot_path: str) -> dict:
    """{version:int -> sql_file:str} from core.schema_version in the serving snapshot (read-only)."""
    import duckdb  # imported lazily so import-for-test doesn't require duckdb
    con = duckdb.connect(os.path.realpath(snapshot_path), read_only=True)
    try:
        rows = con.execute("SELECT version, sql_file FROM core.schema_version").fetchall()
        return {int(v): (f or "") for v, f in rows if v is not None}
    finally:
        con.close()


def repo_ddl_versions(repo_root: str, ref: str) -> dict:
    """Committed sql/ddl/NN_*.sql at <ref> -> {"by_num": {version:int -> basename}, "basenames": set}.
    We keep BOTH: by_num (for repo-ahead) AND the full basename set, because the ALERTING signal keys
    on the applied FILENAME (not the number) so a reused number can't mask real drift."""
    p = subprocess.run(["git", "-C", repo_root, "ls-tree", "-r", "--name-only", ref, "sql/ddl/"],
                       capture_output=True, text=True, timeout=30)
    if p.returncode != 0:
        raise RuntimeError(f"git ls-tree {ref} sql/ddl/ failed: {(p.stderr or '').strip()[:200]}")
    by_num: dict = {}
    basenames: set = set()
    for line in p.stdout.splitlines():
        base = os.path.basename(line.strip())
        if not base.endswith(".sql"):
            continue
        basenames.add(base)
        stem = base.split("_", 1)[0]
        if stem.isdigit():
            by_num[int(stem)] = base  # if two files shared a number (shouldn't), last wins
    return {"by_num": by_num, "basenames": basenames}


def compute_drift(snapshot_path: str = CURRENT_DUCKDB, repo_root: str = REPO_ROOT,
                  ref: str = REF, baseline: set = None) -> dict:
    """Pure comparison: applied DB versions vs committed repo DDL. Returns a structured report.
    `missing` = the ALERTING signal: an applied DDL whose FILE isn't committed at <ref> (robust to
    number reuse — keying on the number alone would let a reused number mask the drift), EXCLUDING any
    version in `baseline` (those go to `baselined`, info only). `repo_ahead` (committed-not-yet-applied)
    is info-only."""
    if baseline is None:
        baseline = load_baseline()
    applied = applied_versions(snapshot_path)
    repo = repo_ddl_versions(repo_root, ref)
    by_num, basenames = repo["by_num"], repo["basenames"]
    missing, baselined = [], []
    for v, sql_file in sorted(applied.items()):
        base = os.path.basename(sql_file) if sql_file else ""
        drifted = (base and base not in basenames) or (not base and v not in by_num)
        if not drifted:
            continue
        kind = ("file-not-in-repo" if v not in by_num else "number-reused") if base else "number-missing"
        item = {"version": v, "applied_as": sql_file, "repo": by_num.get(v), "kind": kind}
        (baselined if v in baseline else missing).append(item)
    repo_ahead = sorted(v for v in by_num if v not in applied)
    return {"applied_count": len(applied), "repo_count": len(by_num),
            "max_applied": max(applied) if applied else 0, "max_repo": max(by_num) if by_num else 0,
            "missing": missing, "baselined": baselined, "repo_ahead": repo_ahead}


def _drift_versions(report: dict) -> list:
    return sorted({d["version"] for d in report["missing"]})


def main() -> int:
    try:
        report = compute_drift()
    except Exception as e:  # noqa: BLE001 — a guard helper must NEVER false-alarm on its own breakage
        # Fail-silent for the ALERT (DBDRIFT=0 so we don't false-alarm), but emit a distinct marker +
        # a stderr line (captured in the guard log) so 'no drift' is distinguishable from 'check broke'
        # — e.g. core.schema_version itself losing the version/sql_file column would land here.
        print(f"schema_db_repo_drift: check unavailable ({type(e).__name__}: {e})", file=sys.stderr)
        print("DBDRIFT=0 VERSIONS= DBDRIFT_ERROR=1")
        return 0
    vers = _drift_versions(report)
    for d in report["missing"]:
        repo_hint = f" (repo has '{d['repo']}' at that number)" if d.get("repo") else ""
        print(f"  DRIFT [{d['kind']}]: v{d['version']} applied as '{d['applied_as']}' "
              f"is not committed at {REF}{repo_hint}")
    if report.get("baselined"):
        print(f"  info  baselined (known-uncommitted, suppressed): "
              f"{','.join('v%d' % d['version'] for d in report['baselined'])}")
    if report["repo_ahead"]:
        print(f"  info  repo-ahead (committed, not yet applied — normal): "
              f"{','.join('v%d' % v for v in report['repo_ahead'][:20])}"
              f"{' …' if len(report['repo_ahead']) > 20 else ''}")
    print(f"  summary: applied={report['applied_count']} (max v{report['max_applied']}), "
          f"repo={report['repo_count']} (max v{report['max_repo']})")
    print(f"DBDRIFT={len(vers)} VERSIONS={','.join('v%d' % v for v in vers)}")
    return 2 if vers else 0


if __name__ == "__main__":
    raise SystemExit(main())
