#!/usr/bin/env python3
"""independent_reviewer.py — the SECOND key in two-key auto-merge (DECISION 2026-06-22).

WHY a second reviewer at all (Sam, verbatim): *"none of us are super technical, so if a human is
going to say yes every time, there's no point."* A non-technical human clicking "yes" on every merge
is theater — latency + false confidence, zero real review. So the per-change reviewer is NOT a human;
it's a second, **independent** machine reviewer. A PR auto-merges iff ALL of: (1) the moderator gate
PASSes, (2) THIS reviewer APPROVEs, (3) the change is non-destructive. Otherwise → escalate to Sam in
plain English. See scripts/two_key_merge.py for the decision + agreement-log + escalation wiring.

INDEPENDENCE IS THE WHOLE POINT. If this were just the gate's logic twice, it buys nothing. So this
module is deliberately independent of moderator/bin/moderator_engine.py's deep-review along every axis:
  * DIFFERENT MODEL — gate deep-review = claude-opus-4-8 (moderator_common.LLM_MODEL); this reviewer
    = claude-sonnet-4-6 (REVIEWER_MODEL below). A different model family makes a shared blind spot far
    less likely than running the same model twice.
  * DIFFERENT LENS/PROMPT — the gate is a *schema moderator* (canonical names, alias dupes, consumer
    breakage vs the live catalog/rules). This reviewer is an *adversarial code reviewer* in the
    /code-review / cc-reviewer mould: correctness, safety, and "does this diff actually do what the
    change was asked to do?" It is given the PR diff + title/body, NOT the catalog or the rule set.
  * DIFFERENT SUBSTRATE — this is a standalone client-side module (runs in the writer's Claude / CI),
    not the droplet gate service. It does not import schema_gate_lib, the rules, or the catalog.

It returns a structured APPROVE / REQUEST_CHANGES verdict (+ plain-English reasons + concerns) so the
two-key decider can compute agreement and write a plain-English escalation if the two keys disagree.

Stdlib + `anthropic` only (the same SDK the gate already uses). Never raises: any API/transport failure
becomes verdict='unavailable', which the two-key decider treats as "cannot confirm the second key" →
escalate, never auto-merge (fail-safe: an unavailable independent reviewer must not silently green-light
a merge). Model overridable via TWO_KEY_REVIEWER_MODEL for forward-compat.
"""
from __future__ import annotations

import json
import os
import subprocess

# Different model from the gate's deep-review (claude-opus-4-8) — independence by construction.
REVIEWER_MODEL = os.environ.get("TWO_KEY_REVIEWER_MODEL", "claude-sonnet-4-6")
REVIEWER_TIMEOUT_S = float(os.environ.get("TWO_KEY_REVIEWER_TIMEOUT_S", "90"))
_MAX_DIFF_CHARS = int(os.environ.get("TWO_KEY_REVIEWER_MAX_DIFF_CHARS", "60000"))


def _anthropic_key() -> str:
    return os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_KEY", "")


# A genuinely different lens from the schema moderator: an adversarial *code reviewer*, not a schema-rule
# checker. It judges the actual change as a diff — correctness, safety, intent-match — the cc-reviewer /
# /code-review posture. It is told the gate already owns mechanical schema rules so it does not re-litigate
# them; its value is the orthogonal read.
_REVIEWER_SYSTEM = (
    "You are an independent adversarial CODE REVIEWER for the Renaissance DuckDB warehouse — the SECOND "
    "of two independent checks that must BOTH approve before a schema-change PR auto-merges (the first is "
    "a separate deterministic+LLM schema gate, which you are deliberately independent of). ~100 syncs read "
    "this shared warehouse, so a bad merge breaks production for five editors.\n\n"
    "Your lens is correctness + safety + intent-match on the ACTUAL diff — NOT schema style rules (a "
    "separate gate already owns canonical names, alias dupes, and catalog-consumer breakage; do not "
    "re-litigate those). Ask:\n"
    "  1. CORRECTNESS — does the SQL/Python actually do what the PR title/description says? Logic errors, "
    "wrong joins/filters, off-by-one, a migration that silently no-ops, a column list that won't match.\n"
    "  2. SAFETY — could this corrupt or lose data, lock a big table, break an INSERT contract, or apply "
    "out of order? Anything irreversible that the description does not call out and justify.\n"
    "  3. INTENT-MATCH — is the change scoped to what was asked, or does it smuggle in unrelated/unexpected "
    "edits a reviewer should question?\n\n"
    "Default to APPROVE for a clean, well-scoped, non-destructive change that matches its description. "
    "REQUEST_CHANGES ONLY for a concrete, explained problem in one of the three areas — never for style, "
    "and never just because you are uncertain (say so in a concern instead of blocking). Write every reason "
    "and concern in PLAIN ENGLISH a NON-TECHNICAL reader can act on (a human only ever sees these when the "
    "two keys disagree). Respond ONLY via the report_review tool."
)

_REVIEWER_TOOL = {
    "name": "report_review",
    "description": "Report the independent code-review verdict for the proposed schema-change PR.",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["approve", "request_changes"],
                        "description": "approve = correct, safe, well-scoped, matches its description. "
                                       "request_changes = a concrete correctness/safety/intent problem."},
            "summary": {"type": "string",
                        "description": "ONE plain-English sentence a non-technical reader understands: what "
                                       "this change does and whether it looks safe to merge."},
            "reasons": {"type": "array", "items": {"type": "string"},
                        "description": "Plain-English reasons for the verdict. If request_changes, each is a "
                                       "specific problem + what should change. Keep jargon out."},
            "concerns": {"type": "array", "items": {"type": "string"},
                         "description": "Non-blocking worries worth a human's eyes (uncertainty, things you "
                                        "could not verify). Do NOT block on these — list them here."},
        },
        "required": ["verdict", "summary", "reasons"],
    },
}


def _build_prompt(pr_title: str, pr_body: str, diff: str, files: list[str]) -> str:
    diff = diff or "(no diff captured)"
    if len(diff) > _MAX_DIFF_CHARS:
        diff = diff[:_MAX_DIFF_CHARS] + "\n… (diff truncated for length) …"
    parts = [
        "# Pull request under review (independent second-key code review)\n",
        f"## Title\n{pr_title or '(none)'}\n",
        f"## Description\n{pr_body or '(none)'}\n",
        f"## Changed files\n" + ("\n".join(f"- {p}" for p in files) if files else "(none listed)") + "\n",
        "## Diff\n```diff\n" + diff + "\n```\n",
        "\nReview this as an adversarial code reviewer (correctness, safety, intent-match). Return your "
        "verdict via the report_review tool. Remember: a separate schema gate already owns mechanical "
        "schema-naming/dupe/consumer rules — focus on whether this change is correct, safe, and does what "
        "the description says.",
    ]
    return "\n".join(parts)


def review_pr(pr_title: str, pr_body: str, diff: str, files: list[str] | None = None) -> dict:
    """Run the independent adversarial code review over the PR diff.

    Returns {verdict: 'approve'|'request_changes'|'unavailable', summary, reasons:[...], concerns:[...],
    model, error?}. NEVER raises — an API/SDK/transport failure becomes verdict='unavailable' so the
    two-key decider fail-safes to escalate (an unconfirmed second key must never auto-merge)."""
    files = files or []
    key = _anthropic_key()
    if not key:
        return {"verdict": "unavailable", "summary": "", "reasons": [],
                "concerns": ["independent reviewer could not run: no ANTHROPIC_API_KEY configured"],
                "model": REVIEWER_MODEL, "error": "no ANTHROPIC_API_KEY"}
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key, timeout=REVIEWER_TIMEOUT_S)
        resp = client.messages.create(
            model=REVIEWER_MODEL, max_tokens=2000, system=_REVIEWER_SYSTEM,
            tools=[_REVIEWER_TOOL], tool_choice={"type": "tool", "name": "report_review"},
            messages=[{"role": "user", "content": _build_prompt(pr_title, pr_body, diff, files)}],
        )
        payload = None
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use" and block.name == "report_review":
                payload = block.input
                break
        if payload is None:
            return {"verdict": "unavailable", "summary": "", "reasons": [],
                    "concerns": ["independent reviewer returned no structured verdict"],
                    "model": REVIEWER_MODEL, "error": "no structured verdict"}
        verdict = "request_changes" if payload.get("verdict") == "request_changes" else "approve"
        return {"verdict": verdict, "summary": payload.get("summary", ""),
                "reasons": list(payload.get("reasons") or []),
                "concerns": list(payload.get("concerns") or []), "model": REVIEWER_MODEL}
    except Exception as e:  # noqa: BLE001 — any failure → unavailable (decider fail-safes to escalate)
        return {"verdict": "unavailable", "summary": "", "reasons": [],
                "concerns": [f"independent reviewer error: {type(e).__name__}: {e}"],
                "model": REVIEWER_MODEL, "error": f"{type(e).__name__}: {e}"}


# ── PR diff capture (best-effort; works from a git checkout or `gh`) ──────────────────────────────
def capture_pr_context(base: str = "origin/main", head: str = "HEAD",
                       files: list[str] | None = None) -> dict:
    """Best-effort {title, body, diff, files} for the change under review. Prefers an explicit file
    list (the gated ship files); falls back to the base..head diff. Title/body come from the env
    (CI / ship flow set them) — this module never shells out to gh, keeping it dependency-light."""
    title = os.environ.get("TWO_KEY_PR_TITLE", "")
    body = os.environ.get("TWO_KEY_PR_BODY", "")
    diff = ""
    try:
        cmd = ["git", "diff", f"{base}...{head}"]
        if files:
            cmd += ["--"] + files
        diff = subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout
    except Exception:
        diff = ""
    if not files:
        try:
            out = subprocess.run(["git", "diff", "--name-only", f"{base}...{head}"],
                                 capture_output=True, text=True, timeout=30).stdout
            files = [l.strip() for l in out.splitlines() if l.strip()]
        except Exception:
            files = []
    return {"title": title, "body": body, "diff": diff, "files": files or []}


if __name__ == "__main__":  # quick manual smoke: read a diff on stdin, print the verdict
    import sys
    raw = sys.stdin.read()
    res = review_pr(os.environ.get("TWO_KEY_PR_TITLE", "manual smoke"),
                    os.environ.get("TWO_KEY_PR_BODY", ""), raw, [])
    print(json.dumps(res, indent=2))
