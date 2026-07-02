# Git sync discipline — keep origin and the box from drifting [2026-06-20]

**Why this exists:** on 2026-06-20, `origin/main` was found ~131 files / ~12k lines **behind** the running
box (`renaissance-worker:/root/renaissance-warehouse`). Root cause: chats edited the box directly and
never pushed up. This is the standing rule + the automated guard that prevent a repeat.

## The rule (everyone: Darcy / David / Thomas + their agents)

**`origin/main` is the source of truth for code. The box is the runtime, not a divergent fork.**

1. **Edit → PR → merge to `origin/main`** (the Schema Moderator gate reviews schema/DDL changes).
2. **The box PULLS** `origin/main` — never the reverse. Do **not** commit on the box and leave it unpushed,
   and do **not** leave the box working tree dirty.
3. Genuine box-local emergency hotfix? Allowed, but **PR it back same day**. The guard makes any un-pushed
   box commit (or dirty tree) loud within the hour.
4. **A moderator `apply-now` is bound to the repo too — `apply == commit`.** apply-now refuses to apply a
   DDL whose exact content isn't already committed at `origin/main:sql/ddl/<file>` (`MODERATOR_REQUIRE_COMMITTED=enforce`,
   the default). So the live DB can never get **ahead** of the repo via apply-now: commit + PR + merge +
   box-pull FIRST, then `apply-now`. (This closes the 2026-06-22 v96 hole, where a DDL was applied to the
   live warehouse but never committed — a drift `git status` can't see.) Always take a DDL **number** from
   `moderator_client.py next-version` (the single authority across all writers) — never eyeball `sql/ddl/`.

## The automated guard — `scripts/warehouse_git_divergence_guard.sh`

Box cron at **:05 each hour** (offset from 03:15 delta-sync / 03:30 nightly / 06:35 b2b load). Each run
`git fetch origin` then checks three signals and alerts Slack (the warehouse alert channel) on **real drift only**:

| Signal | Meaning | Threshold |
|---|---|---|
| `AHEAD` = `git rev-list --count origin/main..HEAD` | box has commits not on origin (the exact 2026-06-20 failure) | **> 0** → alert |
| `BEHIND` = `git rev-list --count HEAD..origin/main` | box hasn't pulled origin | **> 5** → alert |
| `DIRTY` = `git status --porcelain` (minus runtime-generated paths) | uncommitted box edits | **> 0** → alert |
| `DBDRIFT` = applied `core.schema_version` versions with no committed `sql/ddl/NN_*.sql` at origin/main (`scripts/schema_db_repo_drift.py`) | live DB schema **ahead of** the repo — a moderator `apply-now` that skipped the commit (the 2026-06-22 v96 blind spot `git status` can't see) | **> 0** → alert |

The `DBDRIFT` check reads the serving snapshot read-only and is **fail-silent on its own errors** (a guard
helper must never false-alarm). It keys on the applied DDL **filename** (not the number), so it alerts
whenever an applied DDL file isn't committed under `sql/ddl/` at origin/main — robust even if the number
was reused (which would otherwise mask the drift). `repo-ahead` (committed-not-yet-applied — normal) is
info-only.

**Alert dedup (no #cc-sam spam):** a state file `/root/.warehouse-git-drift-state` holds the last-alerted
drift signature + timestamp. The guard alerts **once per distinct drift signature**, re-alerts only when the
signature **changes** (drift worsens/shifts) or after a **daily** cooldown — never every run. When drift
clears, the state is reset so the next real drift alerts fresh.

**Phase 1 = alert-only** (no auto-pull). Phase 2 (future): auto `git pull --ff-only` when the box is *only*
BEHIND and clean, alarming only on AHEAD/DIRTY/conflict.

Cron line (recorded in `/root/backups/crontab-*.txt`):
```
5 * * * * cd /root/renaissance-warehouse && /root/renaissance-warehouse/scripts/warehouse_git_divergence_guard.sh >> /root/renaissance-warehouse/logs/git_drift_guard.log 2>&1
```

## The persistence-step guard — `apply-now --pull-first` loud-fails on a drifted box [2026-06-28]

**Why this exists:** on 2026-06-27 the box sat on a writer's **feature branch**, ~21 commits **behind**
origin/main, with a **dirty tree** carrying several writers' uncommitted DDLs — and *3 separate chats* hit
the fallout before anyone noticed. The hourly divergence guard above had been alerting, but the failure was
**silent at the chokepoint that matters**: `apply-now --pull-first` runs `_git_pull_ff()` (a box-side
`git pull --ff-only origin main` so the nightly rebuild carries the just-merged DDL), and that helper is
**best-effort and NEVER raises**. On a diverged/dirty tree the ff-only pull just **fails quietly**, the
apply proceeds, and the **nightly keeps rebuilding from STALE code** — silently dropping merged DDLs (the
v96 nightly-drop class, fleet-wide). The silent swallow is what hid the divergence for ~a week.

**The guard (in `moderator/bin/moderator_apply.py`, the ONE schema chokepoint every ship goes through):**
after the `pull_first` pull, it **asserts the box is on clean `origin/main`** (branch == main, ahead == 0,
behind == 0, no dirty tracked files — runtime noise like `logs/`, `*.duckdb`, `*.bak`, `.env`, `.worktrees/`
is filtered, same list as the hourly guard). If the box is **not** clean it:

1. **Auto-preserves** the box state — snapshots `git status`/`diff` to `/root/box-drift-rescue/<head>/` and
   saves the dirty tracked state to a `refs/box-drift-rescue/*` stash-ref. **Non-destructive: it never
   resets — the working tree is left fully intact on disk**, so no writer's in-flight work is lost.
2. **Alerts `#cc-sam`** via `scripts/alert_slack.py` (the same durable channel as the hourly guard).
3. **BLOCKS the apply (fail-closed)** — returns `ok:false` with a clear remediation message and applies
   **nothing** this run. Applying live while the nightly stays stale is exactly the failure we refuse.

This is **fail-closed**: if git can't be consulted at all, the box is treated as *not* clean (we never apply
against a box we couldn't verify). Override only for local dev / a sanctioned manual window:
`MODERATOR_BOX_DRIFT_GUARD=off`. Tests: `moderator/tests/test_box_drift_guard.py` (20 cases, no PG/box).

**If the guard fires (or the hourly guard alerts):** the box has drifted — do **not** force the ship.
Reconcile the box back to clean `origin/main` first: **preserve-then-reset** per
`handoffs/2026-06-28-warehouse-box-git-reconcile.md` (commit all dirty+untracked to a pushed rescue branch,
then `reset --hard origin/main` — nothing is discarded; the reset is git-only and never touches the DuckDB
data). Then re-run the ship.

## Two more standing rules (the landmines this round surfaced)

- **The box stays on `origin/main`, clean — always.** Never edit DDLs directly on the box, never leave it on
  a feature branch, never leave an unpushed commit or a dirty tree. Ship is **edit → PR → merge to
  origin/main → the box pulls main** (`git pull --ff-only`, never push from the box). Worktrees for in-flight
  ship branches live **outside** the main checkout (`/root/wt-*`), never inside it.
- **Index renames use expand/contract — DROP the old index in the SAME diff.** Never
  `CREATE UNIQUE INDEX IF NOT EXISTS` to "rename": it leaves the old index in place, so you end with **two**
  unique indexes on the same key — the 24-day landmine that caused the 2026-06-26 nightly FATAL. (Already
  enforced by the ship-gate moderator, but stated here so writers author it right the first time.)
