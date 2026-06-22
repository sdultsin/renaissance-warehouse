# The one-command writer front door — "review these changes and ship them if they're good"

This is the **single write path** to the DuckDB warehouse for every editor (David / Darcy / Thomas /
Sam) and their agents. The human says it **once**; their agent does everything below **invisibly**.
The writer must never see a branch, pick a version number, or ask "is someone else writing right now?"
Concurrency is handled by the gate's single-writer flock — writers never hand-coordinate. **Everyone
uses this same door, including Sam — there is no privileged bypass.** The gate being the only write
path is exactly what makes drift/corruption impossible (the 2026-06-22 v96 incident happened on a path
that skipped the commit).

The agent (you) drives `scripts/moderator_client.py` (`doctor` first if unset — token/URL self-resolve,
no SSH/Tailscale/minting). The hardened gate (apply==commit, auto-version, DB-vs-repo guard) is what
makes the final carry-through safe to fully automate.

## When to use
The writer says any of: "review and ship these changes", "apply these to the warehouse", "ship this
DDL if it's good", or edits a `sql/ddl/*.sql` (or an `entities|sources|scripts/*.py` that changes an
INSERT column list) and asks to make it live. Then run this flow end-to-end. Do **not** stop half-way
and hand git/PR/version steps back to the human.

## The flow

### 1. Identify the change
The changed `sql/ddl/*.sql` and/or `entities|sources|scripts/*.py` files (from the writer's working
tree / what they point at).

### 2. Assign the version number — never ask the human to pick
For each NEW or unnumbered DDL, get the number from the authority (not by eyeballing `sql/ddl/`):
```
python scripts/moderator_client.py next-version      # reserves the next free number, e.g. 114
```
Rename the file to `sql/ddl/<that-number>_<name>.sql`. (Already-numbered files: leave the name; a
collision is auto-fixed in step 3.) Tag intent at the top: `-- @gate: add | rename A->B | drop |
alter-type` and `-- Depends on NN`.

### 3. Gate review + auto-revise loop (cap K=3) — escalate ONLY on a judgment call
```
python scripts/moderator_client.py review --files sql/ddl/<N>_<name>.sql [entity.py ...]
```
- **Clean / pass-with-warn →** go to step 4.
- **Block → classify each finding:**
  - **Auto-fix yourself, then re-review (≤3 rounds):** formatting, missing `IF NOT EXISTS`, a banned
    pattern with a prescribed fix, a **version collision** (run `next-version` again, renumber +
    rename), a non-canonical column name / missing alias, missing `-- @gate:` / `-- Depends on` tags,
    numbering/label fixes. These are mechanical/confident — apply them in the working tree and re-run
    `review`.
  - **ESCALATE to the human (pause — this is a feature, not friction):** a **destructive / irreversible**
    migration — `DROP` of a column/table that has consumers, a data-deleting `UPDATE`/`DELETE`, a lossy
    type change (e.g. `VARCHAR -> INTEGER`) — **or** any fix you can't make confidently / that would
    change the intended logic. Surface: *"here's the issue + my proposed fix — confirm?"* Do **not**
    silently auto-revise-and-ship a substantive logic change. (For a rename/drop with consumers the
    prescribed fix is expand/contract — propose it, don't bare-rename.)
- **Still blocked after K=3 rounds →** escalate to the `orchestrator` on the parent bus (a genuine
  taxonomy / business fork), with the findings.

### 4. Record the pass
```
python scripts/moderator_client.py loop --files sql/ddl/<N>_<name>.sql   # review (clean) -> record-pass
```
`loop` records the content-hash-bound `approval_ledger` row — the only thing the apply tooth trusts.

### 5. Carry through to live — automatic, the human says nothing after step 1
1. **Commit on a fresh branch off origin/main** (the writer never sees it):
   ```
   git fetch origin main
   git switch -c ship/<short-slug> origin/main
   git add sql/ddl/<N>_<name>.sql [entity.py ...]
   git commit -m "ddl <N>: <what it does>"
   git push -u origin ship/<short-slug>
   ```
   (If the writer had other unrelated uncommitted changes, set them aside first — `git stash` — and
   restore after; ship ONLY the gated files so the PR is clean.)
2. **PR + TWO-KEY auto-merge** (gate AND an independent reviewer must both approve + non-destructive):
   ```
   gh pr create --base main --fill
   python scripts/moderator_client.py two-key --files sql/ddl/<N>_<name>.sql [entity.py ...] \
       --pr-number <N> --pr-title "<title>"   # exit 0 = merge-eligible, exit 10 = escalate
   ```
   `two-key` re-asks the moderator **gate** (`/review`, claude-opus-4-8) for the verdict, runs an
   **independent adversarial reviewer** (`scripts/independent_reviewer.py`, **claude-sonnet-4-6** — a
   different model AND a different lens: code-review correctness/safety/intent-match, not the schema-rule
   lens), checks the change is **non-destructive**, logs the agreement record, and decides:
   - **exit 0 (merge-eligible: gate PASS + reviewer APPROVE + non-destructive)** → merge **only if
     auto-merge is enabled** (`TWO_KEY_AUTOMERGE=on` + the GitHub auto-merge setting / branch protection):
     ```
     gh pr merge --auto --squash --delete-branch
     ```
     **Until auto-merge is enabled (default today):** report *"PR #N open + two-key green — awaiting
     merge-enable"* and stop; a human merges. Never skip the merge to apply directly — apply-now refuses
     uncommitted content (the backstop).
   - **exit 10 (escalate: destructive, OR the two keys disagree)** → do **NOT** merge. `two-key` printed a
     **plain-English** escalation with a recommended action (e.g. *"This change will permanently delete
     table X. Reply YES to allow, or ignore to block."*) — post **that text** (never the diff) to the human
     (`#cc-sam` / route via the orchestrator) and stop.
   The agreement log (`logs/two_key_agreement.jsonl`, overridable via `TWO_KEY_AGREEMENT_LOG`) records
   `{gate_verdict, reviewer_verdict, agreed?, destructive?, merged?}` per change — the data Sam uses to
   collapse to single-key (gate only) in ~2 weeks once the two keys consistently agree (`two-key`'s
   footer prints the running agreement rate; surface it in the Slack ship report).
3. **Wait for the merge to actually land** on origin/main (poll `gh pr view <N> --json state`).
4. **Apply live + keep the box (and nightly) in sync:**
   ```
   python scripts/moderator_client.py apply-enqueue --files sql/ddl/<N>_<name>.sql
   python scripts/moderator_client.py apply-now --pull-first
   ```
   `--pull-first` does a box-side `git pull --ff-only origin main` so the box checkout carries the
   merged DDL (the nightly rebuild then keeps it), then applies under the single-writer flock and
   re-promotes the serving snapshot. It self-queues behind the nightly / another writer — **this is the
   concurrency handling; you never coordinate manually.**
5. **Report:** *"Shipped — live as v\<N\> (serving snapshot \<id\>)."* That's the only thing the human
   sees on success.

## Invisible safety backstops (you don't run these; they just hold)
- **apply==commit:** apply-now refuses any content not committed at origin/main → the live DB can't get
  ahead of the repo.
- **auto-version + collision block:** two concurrent writers can't take the same number; a number-clash
  is refused loudly, never silently swallowed.
- **DB-vs-repo drift guard (hourly):** alerts if anything ever lands in the live DB without a committed
  file — a tripwire on this whole flow.
- **two-key fail-safe:** a flaky/unreachable gate or independent reviewer is treated as a NON-pass /
  `unavailable` → it ESCALATES, never auto-merges. An unconfirmed key can never green-light a merge.

## Two-key auto-merge (DECISION 2026-06-22 — Option B)
A PR auto-merges **iff ALL of**: (1) moderator **gate** = PASS, (2) the **independent reviewer** =
APPROVE, and (3) the change is **non-destructive**. Otherwise → **escalate to Sam** in plain English
(never a diff), with two triggers only: **destructive** (a permanent/irreversible change) or
**disagreement** (gate and reviewer split, or the reviewer can't be confirmed). The reviewer is
genuinely independent of the gate — different model (sonnet-4-6 vs the gate's opus-4-8) AND a different
lens (adversarial code review, not schema rules) — so two independent checks must agree. This is
**strictly safer than single-gate auto-merge** on day one. Reversible: `TWO_KEY_AUTOMERGE=off` falls back
to today's manual-gate behavior with one flip.

## KNOWN DEPENDENCY (orchestrator/Sam — flagged 2026-06-22)
Enabling fully-automatic merge still needs the GitHub repo config: branch protection on `main` +
auto-merge enabled + the `moderator-gate` workflow as a **required** status check, writers' machines
having merge creds, **and the flip `TWO_KEY_AUTOMERGE=on`**. **All of that is HELD for human review** —
until then step 5.2 degrades to "PR open + two-key green, awaiting merge-enable" and a human merges.
Everything else (version, review/revise, the two-key decision + agreement log + plain-English escalation,
box-pull, lock, apply, promote) is automatic now.
