# Deploy discipline — /root/renaissance-warehouse (LIVE warehouse runtime)

**This dir runs production:** nightly sync (03:30Z), sendivo, transcribe, portal-feed,
KPI feed, and ~20 cron jobs. It was historically edited DIRECTLY on the box, so it
silently ran ahead of the canonical **PUBLIC** GitHub repo
`sdultsin/renaissance-warehouse`. On **2026-06-17** it was converted to a real git
clone tracking `origin/main` so drift can't be silent anymore.

## How to deploy a change (the ONLY supported path)
1. Make the change in the repo, commit, and push to `origin/main` (off-box, or via PR).
2. On the box: `cd /root/renaissance-warehouse && git pull`
3. Restart/let-cron-pick-up as needed. That's it.

## Do NOT edit files directly on the box
Direct edits are exactly what caused the drift. If you must hotfix live, commit the
hotfix back to `origin/main` the same day, or the box and repo diverge again.

## Public repo — sensitive content is .gitignored on purpose
`.gitignore` excludes: `.env*`, `*.duckdb*`, `seed_data/` (vendor/cost/partner
reference), `*.xlsx` (PII), `*.csv/parquet/jsonl`, `*.bak*`, `_deploy_tmp/`,
`backups/`, `.venv/`, `logs/`. **NEVER `git add -A && push`** — you would publish
secrets/cost data to a public repo. Cost SQL (`14/15_cost_seed*.sql`) was deliberately
sanitized to no-op stubs in the repo; the box still holds the old inlined versions —
do not re-publish them.

## Known divergence (as of 2026-06-17 conversion — NOT yet reconciled)
The box is AHEAD of the repo on ~100+ files of real production code/DDL/specs/docs
that were never committed (e.g. DDL 49-79, transcribe_calls.py, sendivo watchdogs,
kpi_dashboard_data.py logic). The repo is AHEAD on a few (sanitized cost SQL,
`.gitignore`, `requirements.txt`, 5 files the box lacks). Reconciling this (deciding
per-file what is safe to publish to the PUBLIC repo vs keep box-local) is a pending
decision — see the handoff. Until then, `git status` shows the full picture.

## Drift guard
`scripts/warehouse_drift_guard.sh` (cron 08:00 + 20:00 UTC) fetches origin/main and
alerts #cc-sam if the box falls BEHIND origin/main (undeployed commits) or if NEW
direct-edit drift appears beyond the recorded `.drift_baseline`. Read-only.

## Reverting the conversion (if ever needed)
Pre-conversion snapshot: `/root/backups/renaissance-warehouse-predeploy-*.tar.gz`.
To undo just the git layer: `rm -rf /root/renaissance-warehouse/.git` (working tree is
untouched by the conversion). To fully restore: extract the tarball.
