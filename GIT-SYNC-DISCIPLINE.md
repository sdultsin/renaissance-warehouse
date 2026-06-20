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

## The automated guard — `scripts/warehouse_git_divergence_guard.sh`

Box cron at **:05 each hour** (offset from 03:15 delta-sync / 03:30 nightly / 06:35 b2b load). Each run
`git fetch origin` then checks three signals and alerts Slack (the warehouse alert channel) on **real drift only**:

| Signal | Meaning | Threshold |
|---|---|---|
| `AHEAD` = `git rev-list --count origin/main..HEAD` | box has commits not on origin (the exact 2026-06-20 failure) | **> 0** → alert |
| `BEHIND` = `git rev-list --count HEAD..origin/main` | box hasn't pulled origin | **> 5** → alert |
| `DIRTY` = `git status --porcelain` (minus runtime-generated paths) | uncommitted box edits | **> 0** → alert |

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
