#!/usr/bin/env python3
"""test_box_drift_guard.py — self-contained tests for the apply-now box-sync drift guard.

The guard is the fix for the 2026-06-27 fleet-wide divergence: `apply-now --pull-first` runs
`_git_pull_ff()` (best-effort, NEVER raises) so a diverged / dirty / feature-branch box silently
left the nightly rebuilding from STALE code. The guard turns that silent swallow into a LOUD,
BLOCKING failure (assert clean origin/main; else preserve + alert #cc-sam + block, fail-closed).

Covers the locally-runnable logic (no live Postgres / no box needed):
  * _box_is_noise          — runtime-noise filter (logs/snapshots/backups/env never count as drift)
  * _box_sync_status       — clean vs dirty / feature-branch / behind / ahead / noise-only, fail-closed
  * _auto_preserve_box_drift — NON-destructive: snapshots state, leaves the working tree intact
  * _alert_cc_sam          — graceful (returns False, never raises) when the alert script is absent;
                             returns True when the helper exits 0 (wiring), without posting to Slack

Builds throwaway git repos in a tmpdir; asserts; cleans up. Run:
    python3 moderator/tests/test_box_drift_guard.py
(plain stdlib; no duckdb / no PG.)
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.normpath(os.path.join(HERE, "..", "bin"))
SCRIPTS = os.path.normpath(os.path.join(HERE, "..", "..", "scripts"))
for p in (BIN, SCRIPTS):
    if p not in sys.path:
        sys.path.insert(0, p)

_PASS = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def _git(root, *args):
    return subprocess.run(["git", "-C", root, *args], capture_output=True, text=True)


def _init_repo(root, branch="main"):
    _git(root, "init", "-q", "-b", branch)
    _git(root, "config", "user.email", "t@t.t")
    _git(root, "config", "user.name", "t")
    _git(root, "config", "commit.gpgsign", "false")


def _commit(root, relpath, content: bytes, msg=None):
    full = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as f:
        f.write(content)
    _git(root, "add", relpath)
    _git(root, "commit", "-q", "-m", msg or f"add {relpath}")


def test_noise_filter():
    print("[guard] _box_is_noise — runtime noise never counts as writer drift")
    import moderator_apply as ap
    noise = ["logs/git_drift_guard.log", "data/x.parquet", ".worktrees/foo",
             "core/warehouse.duckdb", "core/warehouse.duckdb.wal",
             "moderator/bin/moderator_apply.py.bak-20260626T230621Z",
             ".env", ".env.instantly", "scripts/x.log", "snap.parquet",
             "celerybeat.beat", "tmp/y.tmp"]
    real = ["entities/iskra.py", "sql/ddl/1019_sendivo_blast_daily.sql",
            "sources/iskra.py", "scripts/classify_call_outcomes.py"]
    check("all runtime-noise paths filtered", all(ap._box_is_noise(p) for p in noise),
          str([p for p in noise if not ap._box_is_noise(p)]))
    check("all real-drift paths NOT filtered", not any(ap._box_is_noise(p) for p in real),
          str([p for p in real if ap._box_is_noise(p)]))


def test_box_sync_status(tmp):
    print("[guard] _box_sync_status — clean vs every drift shape (fail-closed)")
    import moderator_apply as ap

    # bare 'origin' the box pulls from (so HEAD..origin/main is meaningful)
    origin = os.path.join(tmp, "origin.git")
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", origin], capture_output=True)

    box = os.path.join(tmp, "box")
    os.makedirs(box)
    _init_repo(box, "main")
    _commit(box, "sql/ddl/100_a.sql", b"-- a\n")
    _git(box, "remote", "add", "origin", origin)
    _git(box, "push", "-q", "origin", "main")
    _git(box, "update-ref", "refs/remotes/origin/main", "HEAD")

    ap.WAREHOUSE_ROOT = box
    ap._GIT_REF = "origin/main"

    st = ap._box_sync_status()
    check("clean box on origin/main -> clean True", st["clean"] is True, str(st))
    check("clean reports branch=main ahead=0 behind=0 dirty=0",
          st["branch"] == "main" and st["ahead"] == 0 and st["behind"] == 0 and st["dirty"] == 0, str(st))

    # dirty (tracked edit) -> NOT clean
    with open(os.path.join(box, "sql/ddl/100_a.sql"), "ab") as f:
        f.write(b"-- edit\n")
    st = ap._box_sync_status()
    check("dirty tracked edit -> clean False, dirty>=1", st["clean"] is False and st["dirty"] >= 1, str(st))
    _git(box, "checkout", "-q", "--", "sql/ddl/100_a.sql")  # revert

    # runtime-noise-only dirty -> STILL clean (noise filtered)
    os.makedirs(os.path.join(box, "logs"), exist_ok=True)
    with open(os.path.join(box, "logs", "drift.log"), "w") as f:
        f.write("noise\n")
    st = ap._box_sync_status()
    check("noise-only dirty (logs/) -> still clean True", st["clean"] is True, str(st))

    # untracked REAL file -> NOT clean
    with open(os.path.join(box, "sql/ddl/101_new.sql"), "w") as f:
        f.write("-- new\n")
    st = ap._box_sync_status()
    check("untracked real DDL -> clean False", st["clean"] is False and st["dirty"] >= 1, str(st))
    os.remove(os.path.join(box, "sql/ddl/101_new.sql"))

    # feature branch -> NOT clean (wrong branch even if otherwise pristine)
    _git(box, "checkout", "-q", "-b", "ship/feature-x")
    st = ap._box_sync_status()
    check("on feature branch -> clean False (branch!=main)",
          st["clean"] is False and st["branch"] == "ship/feature-x", str(st))
    _git(box, "checkout", "-q", "main")

    # AHEAD (box has an unpushed commit) -> NOT clean
    _commit(box, "sql/ddl/102_local.sql", b"-- local only\n")
    st = ap._box_sync_status()
    check("box ahead of origin/main -> clean False, ahead>=1", st["clean"] is False and st["ahead"] >= 1, str(st))
    _git(box, "reset", "-q", "--hard", "origin/main")  # back to clean

    # BEHIND (origin advanced, box not pulled) -> NOT clean
    clone = os.path.join(tmp, "clone")
    subprocess.run(["git", "clone", "-q", origin, clone], capture_output=True)
    _git(clone, "config", "user.email", "t@t.t")
    _git(clone, "config", "user.name", "t")
    _commit(clone, "sql/ddl/103_remote.sql", b"-- remote\n")
    _git(clone, "push", "-q", "origin", "main")
    _git(box, "fetch", "-q", "origin")
    st = ap._box_sync_status()
    check("box behind origin/main -> clean False, behind>=1", st["clean"] is False and st["behind"] >= 1, str(st))

    # fail-closed: no git repo -> clean False (never declare an unverifiable box clean)
    ap.WAREHOUSE_ROOT = os.path.join(tmp, "nonexistent")
    st = ap._box_sync_status()
    check("no repo -> fail-closed clean False", st["clean"] is False, str(st))


def test_auto_preserve_non_destructive(tmp):
    print("[guard] _auto_preserve_box_drift — preserves state, NEVER mutates the working tree")
    import moderator_apply as ap

    box = os.path.join(tmp, "box_preserve")
    os.makedirs(box)
    _init_repo(box, "main")
    _commit(box, "sql/ddl/100_a.sql", b"-- a\n")
    _git(box, "update-ref", "refs/remotes/origin/main", "HEAD")
    # dirty it: a tracked edit + an untracked file (the exact shape the reconcile preserved)
    with open(os.path.join(box, "sql/ddl/100_a.sql"), "ab") as f:
        f.write(b"-- dirty edit\n")
    with open(os.path.join(box, "untracked_ddl.sql"), "w") as f:
        f.write("-- writer in-flight\n")

    ap.WAREHOUSE_ROOT = box
    ap._GIT_REF = "origin/main"
    ap._BOX_DRIFT_RESCUE_DIR = os.path.join(tmp, "rescue")

    before = _git(box, "status", "--porcelain").stdout
    drift = ap._box_sync_status()
    out = ap._auto_preserve_box_drift(drift)
    after = _git(box, "status", "--porcelain").stdout

    check("working tree UNCHANGED by preserve (non-destructive)", before == after, f"before={before!r} after={after!r}")
    check("untracked in-flight file still on disk", os.path.exists(os.path.join(box, "untracked_ddl.sql")))
    check("snapshot dir created", out["snapshot_dir"] and os.path.isdir(out["snapshot_dir"]), str(out))
    check("snapshot has working_tree.diff", out["snapshot_dir"] and
          os.path.exists(os.path.join(out["snapshot_dir"], "working_tree.diff")), str(out))
    check("tracked dirty state captured to a recoverable stash ref", bool(out["stash_ref"]), str(out))
    # second fire doesn't clobber the first snapshot dir
    out2 = ap._auto_preserve_box_drift(drift)
    check("repeated fire -> distinct snapshot dir (no clobber)", out2["snapshot_dir"] != out["snapshot_dir"], str(out2))


def test_alert_graceful(tmp):
    print("[guard] _alert_cc_sam — graceful (no raise); wiring returns helper exit status")
    import moderator_apply as ap

    ap._ALERT_SLACK_PY = os.path.join(tmp, "does_not_exist.py")
    check("absent alert script -> False, no raise", ap._alert_cc_sam("hi") is False)

    ok_py = os.path.join(tmp, "fake_alert_ok.py")
    with open(ok_py, "w") as f:
        f.write("import sys; sys.exit(0)\n")
    ap._ALERT_SLACK_PY = ok_py
    check("helper exit 0 -> True (wiring works, no real Slack post)", ap._alert_cc_sam("hi") is True)

    bad_py = os.path.join(tmp, "fake_alert_bad.py")
    with open(bad_py, "w") as f:
        f.write("import sys; sys.exit(1)\n")
    ap._ALERT_SLACK_PY = bad_py
    check("helper exit 1 -> False", ap._alert_cc_sam("hi") is False)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        sync = os.path.join(tmp, "sync")
        os.makedirs(sync, exist_ok=True)
        test_noise_filter()
        test_box_sync_status(sync)
        test_auto_preserve_non_destructive(tmp)
        test_alert_graceful(tmp)
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
