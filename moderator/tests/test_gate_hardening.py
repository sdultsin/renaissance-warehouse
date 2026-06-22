#!/usr/bin/env python3
"""test_gate_hardening.py — self-contained tests for the moderator-gate-hardening changes.

Covers the NON-trivial, locally-runnable logic (no live Postgres / no box needed):
  * schema_db_repo_drift.compute_drift  — Fix C: DB-applied-vs-repo-committed comparison.
  * moderator_apply._committed_in_repo  — Fix A: apply==commit / refuse-if-uncommitted git check.
  * moderator_engine._max_repo_ddl_version / _max_applied_version — Fix B allocator inputs.

Builds a throwaway git repo (with a refs/remotes/origin/main ref) + a throwaway DuckDB snapshot in a
tmpdir; asserts; cleans up. Run: `python3 moderator/tests/test_gate_hardening.py` (plain stdlib +
duckdb). The PG-bound parts (version_reservation INSERT/retry, the live-conn collision SELECT) are
verified by the deploy-time smoke test on the box — noted in the deliverable.
"""
from __future__ import annotations

import hashlib
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


def _init_repo(root):
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@t.t")
    _git(root, "config", "user.name", "t")
    _git(root, "config", "commit.gpgsign", "false")


def _commit(root, relpath, content: bytes):
    full = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "wb") as f:
        f.write(content)
    _git(root, "add", relpath)
    _git(root, "commit", "-q", "-m", f"add {relpath}")


def _make_snapshot(path, rows):
    import duckdb
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute("CREATE TABLE core.schema_version (version INTEGER PRIMARY KEY, "
                "applied_at TIMESTAMPTZ DEFAULT now(), sql_file VARCHAR NOT NULL)")
    for v, f in rows:
        con.execute("INSERT INTO core.schema_version (version, sql_file) VALUES (?, ?)", [v, f])
    con.close()


def test_drift(tmp):
    print("[Fix C] schema_db_repo_drift.compute_drift")
    import schema_db_repo_drift as drift

    repo = os.path.join(tmp, "repo")
    os.makedirs(repo)
    _init_repo(repo)
    sha = hashlib.sha256(b"x").hexdigest()
    _commit(repo, "sql/ddl/103_a.sql", b"-- a\n")
    _commit(repo, "sql/ddl/104_b.sql", b"-- b\n")
    _commit(repo, "sql/ddl/105_y.sql", b"-- y\n")   # repo has 105 as *_y.sql
    _commit(repo, "sql/ddl/106_z.sql", b"-- z\n")   # committed, NOT applied -> repo-ahead
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    snap = os.path.join(tmp, "snap.duckdb")
    _make_snapshot(snap, [(103, "103_a.sql"), (104, "104_b.sql"),
                          (105, "105_x.sql"),       # name-mismatch vs repo 105_y.sql
                          (96, "96_ghost.sql")])    # v96-class: applied, NOT in repo -> missing

    r = drift.compute_drift(snapshot_path=snap, repo_root=repo, ref="HEAD")
    missing = sorted(d["version"] for d in r["missing"])
    kinds = {d["version"]: d["kind"] for d in r["missing"]}
    # FILENAME-keyed alerting catches BOTH v96 (file not committed) AND v105 (number reused under a
    # different committed file) — the masking the number-keyed version would have downgraded to info.
    check("filename-keyed drift catches v96 AND reused-number v105", missing == [96, 105], f"got {missing}")
    check("v96 kind=file-not-in-repo", kinds.get(96) == "file-not-in-repo", str(kinds))
    check("v105 kind=number-reused (masking fix)", kinds.get(105) == "number-reused", str(kinds))
    check("repo-ahead lists v106 (info, not alerted)", r["repo_ahead"] == [106], f"got {r['repo_ahead']}")
    check("max_applied=105", r["max_applied"] == 105, f"got {r['max_applied']}")

    # Baseline suppresses known drift -> alert only on NEW: baseline {96} leaves only v105 alerting.
    rb = drift.compute_drift(snapshot_path=snap, repo_root=repo, ref="HEAD", baseline={96})
    check("baseline {96} -> v96 baselined, only v105 alerts",
          [d["version"] for d in rb["missing"]] == [105]
          and [d["version"] for d in rb["baselined"]] == [96], f"missing={rb['missing']} baselined={rb['baselined']}")
    # load_baseline parses numbers + ignores comments/blanks
    bfile = os.path.join(tmp, "baseline.txt")
    with open(bfile, "w") as fh:
        fh.write("# comment\n96  # inline\n\n105\nnotanumber\n")
    check("load_baseline parses {96,105}", drift.load_baseline(bfile) == {96, 105}, str(drift.load_baseline(bfile)))

    # Clean case: every applied file is committed under its exact name
    snap2 = os.path.join(tmp, "snap2.duckdb")
    _make_snapshot(snap2, [(103, "103_a.sql"), (104, "104_b.sql"), (105, "105_y.sql"), (106, "106_z.sql")])
    r2 = drift.compute_drift(snapshot_path=snap2, repo_root=repo, ref="HEAD")
    check("clean snapshot -> 0 drift", not r2["missing"], f"missing={r2['missing']}")


def test_committed(tmp):
    print("[Fix A] moderator_apply._committed_in_repo")
    import moderator_apply as ap

    repo = os.path.join(tmp, "repo_a")
    os.makedirs(repo)
    _init_repo(repo)
    content = b"-- @gate: add\nCREATE VIEW core.v_x AS SELECT 1;\n"
    sha = hashlib.sha256(content).hexdigest()
    _commit(repo, "sql/ddl/200_foo.sql", content)

    ap.WAREHOUSE_ROOT = repo
    ap._GIT_REF = "HEAD"     # test against HEAD (no remote needed)
    ap._APPLY_FETCH = False  # don't try to fetch a nonexistent origin

    ok = ap._committed_in_repo("200_foo.sql", sha)
    check("committed + sha match -> committed True", ok["committed"] is True, str(ok))
    bad = ap._committed_in_repo("200_foo.sql", "deadbeef" * 8)
    check("committed but sha mismatch -> committed False", bad["committed"] is False and bad["verifiable"], str(bad))
    miss = ap._committed_in_repo("201_missing.sql", sha)
    check("not committed -> committed False, verifiable True", miss["committed"] is False and miss["verifiable"], str(miss))
    none = ap._committed_in_repo(None, sha)
    check("no sql_file -> committed False", none["committed"] is False, str(none))

    # EOL-tolerant fallback (MED-1): committed blob is LF, the writer's content is CRLF -> still committed.
    crlf = content.replace(b"\n", b"\r\n").decode("utf-8")
    crlf_sha = hashlib.sha256(crlf.encode("utf-8")).hexdigest()
    eol = ap._committed_in_repo("200_foo.sql", crlf_sha, crlf)
    check("CRLF content vs LF blob -> committed True (modulo EOL)", eol["committed"] is True, str(eol))
    # but genuinely different SQL must still fail even with content given
    diff = ap._committed_in_repo("200_foo.sql", "ff" * 32, "CREATE VIEW core.v_y AS SELECT 2;\n")
    check("different SQL (content given) -> committed False", diff["committed"] is False, str(diff))

    ap.WAREHOUSE_ROOT = os.path.join(tmp, "nonexistent")
    nogit = ap._committed_in_repo("200_foo.sql", sha)
    check("no git repo -> verifiable False (caller fail-closes)", nogit["verifiable"] is False, str(nogit))
    # box-pull (apply-now --pull-first) degrades cleanly + never raises when there's no repo
    pull = ap._git_pull_ff()
    check("git pull-ff no-repo -> pulled False (degrades, no raise)", pull["pulled"] is False, str(pull))


def test_engine_maxes(tmp):
    print("[Fix B] moderator_engine max-version inputs")
    import moderator_common as mc
    import moderator_engine as eng

    repo = os.path.join(tmp, "repo_b")
    os.makedirs(repo)
    _init_repo(repo)
    _commit(repo, "sql/ddl/110_a.sql", b"-- a\n")
    _commit(repo, "sql/ddl/113_b.sql", b"-- b\n")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")  # _max_repo_ddl_version uses origin/main
    mc.WAREHOUSE_ROOT = repo
    check("_max_repo_ddl_version=113", eng._max_repo_ddl_version() == 113, str(eng._max_repo_ddl_version()))

    snap = os.path.join(tmp, "snap_b.duckdb")
    _make_snapshot(snap, [(96, "96_x.sql"), (108, "108_y.sql")])
    mc.DUCKDB_CURRENT = snap
    check("_max_applied_version=108", eng._max_applied_version() == 108, str(eng._max_applied_version()))
    # allocator base would be max(repo=113, applied=108, ...) -> next >= 114 (never reuses gaps 95/97-102)
    check("allocator base picks repo>applied (next>=114)", max(eng._max_repo_ddl_version(),
          eng._max_applied_version()) == 113)


def test_cli_contract(tmp):
    print("[Fix C] schema_db_repo_drift CLI <-> guard sed-parse contract")
    import re
    helper = os.path.join(SCRIPTS, "schema_db_repo_drift.py")
    repo = os.path.join(tmp, "repo_cli")
    os.makedirs(repo)
    _init_repo(repo)
    _commit(repo, "sql/ddl/113_a.sql", b"-- a\n")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    snap = os.path.join(tmp, "snap_cli.duckdb")
    _make_snapshot(snap, [(113, "113_a.sql"), (96, "96_ghost.sql")])
    env = dict(os.environ, WAREHOUSE_REPO_ROOT=repo, WAREHOUSE_CURRENT_DUCKDB=snap,
               SCHEMA_REPO_REF="origin/main")
    p = subprocess.run([sys.executable, helper], capture_output=True, text=True, env=env)
    m = re.search(r"^DBDRIFT=(\d+) VERSIONS=(.*)$", p.stdout, re.M)
    check("CLI emits a DBDRIFT line", bool(m), p.stdout)
    check("CLI exit=2 on drift", p.returncode == 2, f"got {p.returncode}")
    if m:
        check("guard parses DBDRIFT=1 VERSIONS=v96", int(m.group(1)) == 1 and m.group(2) == "v96",
              f"DBDRIFT={m.group(1)} VERSIONS={m.group(2)}")


def test_two_key_decide(tmp):
    """two_key_merge.decide() truth table + destructive detection + plain-English escalation guarantees.
    Pure logic — no Anthropic key, no Postgres, no box (the LLM reviewer is mocked out of the decision)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("two_key_merge", os.path.join(SCRIPTS, "two_key_merge.py"))
    tk = importlib.util.module_from_spec(spec); spec.loader.exec_module(tk)

    # (1) decide(): merge ONLY on gate-pass + approve + non-destructive; else escalate (tagged).
    cases = [
        ("pass", "approve", False, "merge", None, True),
        ("pass-with-warn", "approve", False, "merge", None, True),
        ("pass", "request_changes", False, "escalate", "disagreement", False),
        ("block", "approve", False, "escalate", "disagreement", False),
        ("pass", "approve", True, "escalate", "destructive", True),       # destructive trumps approve
        ("pass", "request_changes", True, "escalate", "destructive", False),
        ("pass", "unavailable", False, "escalate", "disagreement", False),  # unconfirmed key never merges
        ("block", "request_changes", False, "escalate", "disagreement", True),
    ]
    for g, r, dz, ea, ek, eag in cases:
        d = tk.decide(g, r, dz)
        check(f"decide gate={g}/rev={r}/destr={dz} -> {ea}/{ek}",
              d["action"] == ea and d["escalation_kind"] == ek and d["agreed"] == eag, str(d))

    # (2) detect_destructive(): drops/deletes/renames/type-changes are destructive; ADD/CREATE are not;
    #     a DROP mentioned only in a comment must NOT false-flag.
    dtests = [
        (["ALTER TABLE core.x ADD COLUMN IF NOT EXISTS y DOUBLE;"], False),
        (["DROP TABLE core.x;"], True),
        (["ALTER TABLE core.x DROP COLUMN y;"], True),
        (["DELETE FROM core.x WHERE id=1;"], True),
        (["ALTER TABLE core.x RENAME COLUMN a TO b;"], True),
        (["ALTER TABLE core.x ALTER COLUMN a SET DATA TYPE INTEGER;"], True),
        (["CREATE OR REPLACE VIEW core.v AS SELECT 1;"], False),
        (["-- IRREVERSIBLE: drop_column core.x.y drops data\nALTER TABLE core.x ADD COLUMN z INT;"], False),
    ]
    for sqls, exp in dtests:
        check(f"detect_destructive {sqls[0][:38]!r} -> {exp}",
              tk.detect_destructive(sqls)["destructive"] == exp, str(tk.detect_destructive(sqls)))

    # (3) plain-English escalation (DECISION 2026-06-22 refinement). Two genuinely different messages,
    #     each posted ON THE PR (so the AUTHOR is notified, not a generic "Sam approves"), never a diff.
    #     DESTRUCTIVE = an AUTHOR-INTENT CONFIRMATION (the only human action left in the system): the
    #     author may confirm their OWN intent by merging / YES, or ignore to block.
    esc_d = tk.decide("pass", "approve", True)
    txt = tk.plain_english_escalation(esc_d, pr_number=7, pr_title="t", gate_verdict="pass",
                                      reviewer={"verdict": "approve"},
                                      destructive={"destructive": True, "reasons": ["drops a table (and all its data)"]})
    check("destructive escalation = author-intent confirm (permanent + author + confirm-by-merge, no diff)",
          "permanently" in txt.lower() and "@author" in txt and "merging" in txt.lower()
          and "YES" in txt and "```" not in txt, txt)
    #     DISAGREEMENT = a BLOCK, NOT an approval request: it must NOT offer a "merge it anyway" / "YES to
    #     merge" coin-flip; it tells the AUTHOR to FIX (push a commit) or escalate to Sam.
    dis_d = tk.decide("pass", "request_changes", False)
    dtxt = tk.plain_english_escalation(dis_d, pr_number=8, pr_title="t", gate_verdict="pass",
                                       reviewer={"verdict": "request_changes", "summary": "the migration silently no-ops",
                                                 "reasons": ["x"]},
                                       destructive={"destructive": False, "reasons": []})
    check("disagreement escalation = BLOCK (author fixes/escalates; NO merge-anyway path; no diff)",
          "second independent reviewer" in dtxt.lower() and "```" not in dtxt
          and "blocked" in dtxt.lower() and "push a new commit" in dtxt.lower()
          and "escalate to sam" in dtxt.lower()
          and "merge it anyway" not in dtxt.lower() and "*yes*" not in dtxt.lower(), dtxt)
    #     unavailable reviewer is also a DISAGREEMENT block (an unconfirmed key never green-lights a merge).
    una_d = tk.decide("pass", "unavailable", False)
    utxt = tk.plain_english_escalation(una_d, pr_number=9, pr_title="t", gate_verdict="pass",
                                       reviewer={"verdict": "unavailable"},
                                       destructive={"destructive": False, "reasons": []})
    check("unavailable-reviewer escalation = BLOCK (no merge-anyway)",
          "blocked" in utxt.lower() and "could not be reached" in utxt.lower()
          and "*yes*" not in utxt.lower(), utxt)

    # (3b) the kill switch: TWO_KEY_AUTOMERGE must gate all action; only the literal 'on' enables it.
    #     Point RENAISSANCE_ENV at a nonexistent file so the .env fallback is hermetic (no local .env).
    _saved = os.environ.get("TWO_KEY_AUTOMERGE")
    _saved_env = os.environ.get("RENAISSANCE_ENV")
    os.environ["RENAISSANCE_ENV"] = os.path.join(tmp, "no_such.env")
    try:
        for val, exp in [("on", True), ("ON", True), (" On ", True), ("off", False),
                         ("", False), ("yes", False), ("1", False)]:
            os.environ["TWO_KEY_AUTOMERGE"] = val
            check(f"automerge_enabled({val!r}) -> {exp}", tk.automerge_enabled() == exp, val)
        os.environ.pop("TWO_KEY_AUTOMERGE", None)
        check("automerge_enabled() unset (no env file) -> False", tk.automerge_enabled() is False, "unset")
        # .env fallback: with no shell var, an `=on` in the env file enables it.
        envp = os.path.join(tmp, "wired.env")
        open(envp, "w").write("FOO=bar\nTWO_KEY_AUTOMERGE=on\n")
        os.environ["RENAISSANCE_ENV"] = envp
        check("automerge_enabled() reads .env fallback on", tk.automerge_enabled() is True, "env on")
        open(envp, "w").write("TWO_KEY_AUTOMERGE=off\n")
        check("automerge_enabled() reads .env fallback off", tk.automerge_enabled() is False, "env off")
        # shell var overrides the file.
        os.environ["TWO_KEY_AUTOMERGE"] = "on"
        check("shell var overrides .env (on beats file off)", tk.automerge_enabled() is True, "override")
    finally:
        if _saved is None:
            os.environ.pop("TWO_KEY_AUTOMERGE", None)
        else:
            os.environ["TWO_KEY_AUTOMERGE"] = _saved
        if _saved_env is None:
            os.environ.pop("RENAISSANCE_ENV", None)
        else:
            os.environ["RENAISSANCE_ENV"] = _saved_env

    # (4) agreement log + stats round-trip.
    logp = os.path.join(tmp, "agree.jsonl")
    tk.log_agreement({"agreed": True, "action": "merge", "merged": True}, logp)
    tk.log_agreement({"agreed": False, "action": "escalate", "merged": False}, logp)
    s = tk.agreement_stats(logp)
    check("agreement_stats counts total/agreed/escalated",
          s["total"] == 2 and s["agreed"] == 1 and s["escalated"] == 1 and s["agreement_rate_pct"] == 50.0, str(s))


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="gate_hardening_test_") as tmp:
        d = os.path.join(tmp, "d"); os.makedirs(d, exist_ok=True)
        test_drift(d)
        test_committed(tmp)
        test_engine_maxes(tmp)
        test_cli_contract(tmp)
        test_two_key_decide(tmp)
    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 1 if _FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
