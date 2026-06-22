#!/usr/bin/env python3
"""two_key_merge.py — the two-key auto-merge decider (DECISION 2026-06-22, Option B).

A schema-change PR auto-merges IFF ALL of:
  (1) the moderator GATE verdict is PASS (pass | pass-with-warn — i.e. not `block`/`error`), AND
  (2) the INDEPENDENT adversarial reviewer (scripts/independent_reviewer.py, claude-sonnet-4-6, a
      different model+lens from the gate's claude-opus-4-8 deep-review) APPROVEs, AND
  (3) the change is NON-DESTRUCTIVE (no DROP/DELETE/destructive-or-lossy DDL).
Otherwise → ESCALATE to a human (do NOT merge). Two escalation triggers only:
  (a) DESTRUCTIVE class — already the gate's posture; keep it.
  (b) DISAGREEMENT — gate passes but the reviewer flags (or the reviewer can't be confirmed).

Escalations a human sees are PLAIN ENGLISH with a recommended action (never a raw diff) — e.g.
"This change will permanently delete the `X` table. Reply YES to allow, or ignore to block." If the
escalation isn't plain-English + actionable, even the real ones decay into rubber-stamps.

Per-change we LOG {gate_verdict, reviewer_verdict, agreed, destructive, merged, ...} to an append-only
JSONL (the agreement log). Sam's plan is to collapse to single-key (gate only) in ~2 weeks once the two
keys demonstrably agree across real changes — that decision needs this agreement-rate data, so we
capture it from day one and surface a count in the Slack ship report.

SAFE BY DEFAULT. This module DECIDES and LOGS; it never merges by itself. The caller (the ship flow /
CI) only runs `gh pr merge` when decision.action == 'merge' AND auto-merge has been explicitly enabled
(TWO_KEY_AUTOMERGE=on — held OFF until the PR is reviewed + the GitHub setting is turned on by a human).
Until then everything degrades to today's behavior: gate runs, PR opens, a human merges.

Stdlib-only except the reviewer's `anthropic` import (lazy, inside independent_reviewer). Importable +
unit-testable: decide() is a pure function of the three inputs; run_two_key() wires capture+review+log.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import independent_reviewer as ir  # noqa: E402

# Append-only agreement log. Default lives beside the other warehouse guard logs on the box; overridable.
AGREEMENT_LOG = os.environ.get(
    "TWO_KEY_AGREEMENT_LOG",
    os.path.join(os.environ.get("WAREHOUSE_REPO_ROOT", "/root/renaissance-warehouse"),
                 "logs", "two_key_agreement.jsonl"))

# Gate verdicts that count as the gate's KEY = PASS. The gate emits: pass | pass-with-warn | block.
_GATE_PASS = {"pass", "pass-with-warn"}


# ── the one-command kill switch ───────────────────────────────────────────────────────────────────
def _env_file_automerge() -> str | None:
    """Read TWO_KEY_AUTOMERGE from the canonical Renaissance .env (RENAISSANCE_ENV) as a fallback, so the
    kill switch has ONE home that works on any writer machine without exporting a shell var. os.environ
    still wins (a shell flip overrides the file)."""
    path = os.environ.get("RENAISSANCE_ENV", "/Users/sam/Documents/Claude Code/Renaissance/.env")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("TWO_KEY_AUTOMERGE="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        return None
    return None


def automerge_enabled() -> bool:
    """The SINGLE kill switch. TWO_KEY_AUTOMERGE=on (case-insensitive) → the two-key flow may actually
    merge / post on the PR. Anything else (unset, off, blank, garbage) → degrade to today's manual
    behavior: decide + log + print only, never touch the PR. `=off` is the one-flip rollback. Source of
    truth: os.environ first (the box sets it in /etc/environment; a shell can flip it), then the
    canonical .env (RENAISSANCE_ENV) as a fallback so writer machines need no shell export."""
    val = os.environ.get("TWO_KEY_AUTOMERGE")
    if val is None or val.strip() == "":
        val = _env_file_automerge() or ""
    return val.strip().lower() == "on"


# ── destructive detection (independent, deterministic) ────────────────────────────────────────────
# We reuse the SAME taxonomy the gate's classify_ddl uses (DESTRUCTIVE | BREAKING-RENAME | LOCK-REWRITE |
# DATA-DEPENDENT are the risky classes; ADD is safe) but compute it here with a self-contained regex
# sweep so the two-key decision does not depend on importing the gate engine on the writer's laptop / in
# CI (which needs the droplet's catalog). A destructive-CLASS change always escalates regardless of the
# two verdicts — matching the gate's existing "destructive → pause for the human" behavior.
_DESTRUCTIVE_PATTERNS = [
    (r"\bDROP\s+TABLE\b", "drops a table (and all its data)"),
    (r"\bDROP\s+VIEW\b", "drops a view"),
    (r"\bALTER\s+TABLE\b[^;]*\bDROP\s+(?:COLUMN\b)?", "drops a column (and its data)"),
    (r"\bTRUNCATE\b", "empties a table"),
    (r"\bDELETE\s+FROM\b", "deletes rows of data"),
    (r"\bUPDATE\s+[\w\".]+\s+SET\b", "rewrites existing rows of data"),
    (r"\bALTER\s+TABLE\b[^;]*\bRENAME\s+(?:COLUMN\s+)?[\w\"]+\s+TO\b", "renames a column (can break readers)"),
    (r"\bALTER\s+TABLE\b[^;]*\bRENAME\s+TO\b", "renames a table (can break readers)"),
    (r"\bALTER\s+TABLE\b[^;]*\bALTER\s+(?:COLUMN\s+)?[\w\"]+\s+(?:SET\s+DATA\s+)?TYPE\b", "changes a column's type (can lose/rewrite data)"),
    (r"\bDROP\s+SCHEMA\b", "drops an entire schema"),
]


def _strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* */ block comments so a DROP/DELETE mentioned only in a comment
    (e.g. an `-- IRREVERSIBLE: drop_column …` down-migration note) does not false-flag as destructive."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def detect_destructive(ddl_contents: list[str]) -> dict:
    """Scan submitted DDL text for destructive/lossy ops. Returns {destructive: bool, reasons: [plain
    English, …]}. Comment-stripped first so a documented down-migration note doesn't false-positive."""
    reasons: list[str] = []
    for sql in ddl_contents or []:
        body = _strip_sql_comments(sql or "")
        for pat, english in _DESTRUCTIVE_PATTERNS:
            if re.search(pat, body, re.IGNORECASE):
                if english not in reasons:
                    reasons.append(english)
    return {"destructive": bool(reasons), "reasons": reasons}


# ── the pure decision ─────────────────────────────────────────────────────────────────────────────
def decide(gate_verdict: str, reviewer_verdict: str, destructive: bool) -> dict:
    """Pure 3-input decision. Returns {action: 'merge'|'escalate', escalation_kind, agreed, ...}.

    merge  iff  gate∈PASS  AND  reviewer == 'approve'  AND  not destructive.
    escalate otherwise, tagged: 'destructive' (trigger a), else 'disagreement' (trigger b — covers a
    reviewer request_changes, an unavailable reviewer, or a gate block)."""
    gate_pass = gate_verdict in _GATE_PASS
    reviewer_approve = reviewer_verdict == "approve"
    # "agreed" = the two KEYS reached the same green/red call (both green, or both red). An unavailable
    # reviewer is NOT agreement — we couldn't confirm the second key.
    agreed = (reviewer_verdict in ("approve", "request_changes")) and (gate_pass == reviewer_approve)

    if gate_pass and reviewer_approve and not destructive:
        return {"action": "merge", "escalation_kind": None, "agreed": agreed,
                "gate_pass": gate_pass, "reviewer_approve": reviewer_approve}
    kind = "destructive" if destructive else "disagreement"
    return {"action": "escalate", "escalation_kind": kind, "agreed": agreed,
            "gate_pass": gate_pass, "reviewer_approve": reviewer_approve}


# ── plain-English escalation (what a NON-TECHNICAL human sees; never a raw diff) ───────────────────
# Two genuinely different escalations, by trigger (Sam, 2026-06-22):
#   DESTRUCTIVE → an AUTHOR-INTENT CONFIRMATION. Posted ON THE PR so the PERSON WHO WROTE IT is the one
#     notified (not a generic "Sam approves this"). The only remaining human action in the whole system:
#     the author confirming their OWN intent on an irreversible change. Confirm by merging / reply YES,
#     or ignore to block. (The change is correct + the second key approved; the only open question is
#     "did you, the author, mean to permanently delete this?" — which only the author can answer.)
#   DISAGREEMENT → a BLOCK, NOT an approval request. The two automatic checks split (or the second key
#     couldn't be confirmed), so we do NOT route anyone a yes/no coin-flip they can't adjudicate. We post
#     the reviewer's plain-English concern on the PR for the AUTHOR to FIX (push a new commit) or escalate
#     to Sam. There is deliberately NO "merge it anyway" path here — that path was the rubber-stamp.
def plain_english_escalation(decision: dict, *, pr_number, pr_title: str, gate_verdict: str,
                             reviewer: dict, destructive: dict) -> str:
    """Build the human-facing message posted ON THE PR (so the author is the one notified). Leads with
    what's happening + the recommended action; no diff, no jargon. Branches on the escalation trigger."""
    if decision["escalation_kind"] == "destructive":
        # AUTHOR-INTENT CONFIRMATION — addressed to the author of this PR, on the PR itself.
        what = "; ".join(destructive.get("reasons") or ["makes a permanent/irreversible change"])
        head = f"⚠️ *This change permanently deletes data — please confirm you meant to*  (PR #{pr_number}: {pr_title or 'untitled'})"
        return (f"{head}\n"
                f"@author — both automatic checks were happy with this change, but it **{what}**. "
                f"Because that is irreversible, we did NOT auto-merge it: only YOU can confirm you "
                f"intended to delete this.\n"
                f"• If you DID mean to: confirm by **merging this PR** (or reply *YES* here) — it will go live.\n"
                f"• If you did NOT: **ignore / close this PR** — nothing changes, nothing is deleted.")
    # DISAGREEMENT → BLOCK. No "merge anyway"; the author fixes it or escalates.
    head = f"🛑 *Held — the two automatic checks disagreed, so this was NOT merged*  (PR #{pr_number}: {pr_title or 'untitled'})"
    if reviewer.get("verdict") == "request_changes":
        why = reviewer.get("summary") or "; ".join(reviewer.get("reasons") or []) \
            or "a possible problem"
        lead = (f"The safety gate passed, but the second independent reviewer would NOT approve it: {why}")
    elif reviewer.get("verdict") == "unavailable":
        lead = ("The safety gate passed, but the second independent reviewer could not be reached, so "
                "we could not confirm a second opinion — and we never merge on a single unconfirmed check.")
    else:  # gate blocked / non-pass while reviewer approved, or any other split
        lead = ("The two automatic checks did not agree on this change "
                f"(safety gate: {gate_verdict}; independent reviewer: {reviewer.get('verdict')}).")
    return (f"{head}\n"
            f"@author — {lead}\n"
            f"This is **blocked** on purpose: we don't ask a human to rubber-stamp a check the system "
            f"isn't sure about. Nothing was merged.\n"
            f"• To proceed: **address the concern above and push a new commit** to this PR — the two-key "
            f"check re-runs automatically.\n"
            f"• If you believe the reviewer is wrong: reply here and **escalate to Sam** to make the call.")


# ── agreement log (append-only JSONL; the data that unlocks single-key later) ──────────────────────
def log_agreement(record: dict, path: str = None) -> None:
    p = path or AGREEMENT_LOG
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception:
        pass  # logging must never take down the decision


def agreement_stats(path: str = None) -> dict:
    """Summarise the agreement log for the Slack ship report / the ~2-week single-key decision."""
    p = path or AGREEMENT_LOG
    total = agreed = merged = escalated = 0
    try:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                total += 1
                agreed += 1 if r.get("agreed") else 0
                merged += 1 if r.get("merged") else 0
                escalated += 1 if r.get("action") == "escalate" else 0
    except OSError:
        pass
    rate = round(100.0 * agreed / total, 1) if total else None
    return {"total": total, "agreed": agreed, "agreement_rate_pct": rate,
            "merged": merged, "escalated": escalated}


# ── orchestration: capture diff → run reviewer → decide → log → emit ──────────────────────────────
def run_two_key(*, gate_verdict: str, ddl_contents: list[str], pr_number=None, pr_title: str = "",
                pr_body: str = "", diff: str = None, files: list[str] = None,
                log_path: str = None) -> dict:
    """Full two-key evaluation for one PR. Does NOT merge — returns the decision + the (already-written)
    agreement record + a ready-to-send plain-English escalation when action=='escalate'. The caller
    performs the merge only if decision['action']=='merge' AND auto-merge is enabled."""
    files = files or []
    destructive = detect_destructive(ddl_contents)
    if diff is None:
        ctx = ir.capture_pr_context(files=files)
        diff, pr_title, pr_body = ctx["diff"], (pr_title or ctx["title"]), (pr_body or ctx["body"])
        files = files or ctx["files"]
    reviewer = ir.review_pr(pr_title, pr_body, diff, files)
    decision = decide(gate_verdict, reviewer["verdict"], destructive["destructive"])
    merged = False  # the decider never merges; the caller sets this true after a real merge if it does
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pr_number": pr_number, "pr_title": pr_title, "files": files,
        "gate_verdict": gate_verdict,
        "reviewer_verdict": reviewer["verdict"], "reviewer_model": reviewer.get("model"),
        "reviewer_summary": reviewer.get("summary", ""),
        "destructive": destructive["destructive"], "destructive_reasons": destructive["reasons"],
        "agreed": decision["agreed"], "action": decision["action"],
        "escalation_kind": decision["escalation_kind"], "merged": merged,
    }
    log_agreement(record, log_path)
    out = {"decision": decision, "reviewer": reviewer, "destructive": destructive, "record": record}
    if decision["action"] == "escalate":
        out["escalation_text"] = plain_english_escalation(
            decision, pr_number=pr_number, pr_title=pr_title, gate_verdict=gate_verdict,
            reviewer=reviewer, destructive=destructive)
    return out


# ── CLI (the ship flow / CI entry point) ──────────────────────────────────────────────────────────
def _read_files(paths: list[str]) -> list[str]:
    out = []
    for p in paths or []:
        try:
            out.append(open(p, "rb").read().decode("utf-8"))
        except Exception:
            pass
    return out


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Two-key auto-merge decider")
    ap.add_argument("--gate-verdict", required=True,
                    help="the moderator gate verdict for this change (pass|pass-with-warn|block)")
    ap.add_argument("--files", nargs="*", default=[], help="the gated DDL/py files (for diff + destructive scan)")
    ap.add_argument("--pr-number")
    ap.add_argument("--pr-title", default=os.environ.get("TWO_KEY_PR_TITLE", ""))
    ap.add_argument("--pr-body", default=os.environ.get("TWO_KEY_PR_BODY", ""))
    ap.add_argument("--diff-file", help="read the PR diff from this file instead of `git diff`")
    ap.add_argument("--stats", action="store_true", help="also print agreement-log stats")
    ap.add_argument("--json", action="store_true", help="emit the full decision as JSON")
    args = ap.parse_args(argv)

    ddl_contents = _read_files([f for f in args.files if f.endswith(".sql")])
    diff = None
    if args.diff_file:
        try:
            diff = open(args.diff_file).read()
        except Exception:
            diff = None
    out = run_two_key(gate_verdict=args.gate_verdict, ddl_contents=ddl_contents,
                      pr_number=args.pr_number, pr_title=args.pr_title, pr_body=args.pr_body,
                      diff=diff, files=args.files)
    d = out["decision"]
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(f"two-key: gate={args.gate_verdict} reviewer={out['reviewer']['verdict']} "
              f"destructive={out['destructive']['destructive']} agreed={d['agreed']} "
              f"-> ACTION={d['action'].upper()}"
              + (f" ({d['escalation_kind']})" if d['escalation_kind'] else ""))
        if d["action"] == "escalate":
            print("\n--- PLAIN-ENGLISH ESCALATION (send to the human; NEVER a diff) ---")
            print(out["escalation_text"])
        if args.stats:
            print("\nagreement-log:", json.dumps(agreement_stats()))
    # exit 0 = merge-eligible; exit 10 = escalate (caller branches on this without parsing stdout).
    return 0 if d["action"] == "merge" else 10


if __name__ == "__main__":
    raise SystemExit(main())
