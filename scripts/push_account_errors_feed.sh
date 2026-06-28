#!/usr/bin/env bash
# Hourly publish of the Account Errors lens feed. Reuses the EXISTING live-accounts poller output
# (read-only) — NO new Instantly poll, NO warehouse write. Sync-first so pushes stay fast-forward
# alongside the nightly 07:30 portal-feed-refresh; adds ONLY the account-errors feed file.
set -uo pipefail
REPO=/root/portal
PY=/root/Renaissance-venv/bin/python
GEN=/root/renaissance-warehouse/scripts/account_errors_dashboard_data.py
FEED=dashboards/lens-account-errors/data/latest.json
export GIT_SSH_COMMAND="ssh -i /root/.ssh/portal_deploy_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
cd "$REPO" || exit 2
git config --global --add safe.directory "$REPO" 2>/dev/null

# 1) sync to origin BEFORE regenerating so the push is always a fast-forward (mirrors refresh_portal_feed.sh)
git fetch -q origin && git reset --hard -q origin/main || echo "WARN: could not sync to origin (continuing)"

# 2) regenerate the feed from the latest hourly poll snapshot (read-only)
CORE_DB_PATH=/opt/duckdb/warehouse_current.duckdb "$PY" "$GEN" --out "$FEED" || { echo "GEN FAILED"; exit 1; }

# 3) commit + push ONLY this feed (never portal_data.js / other lens feeds — those belong to nightly)
git add "$FEED"
if git diff --cached --quiet; then echo "no change"; exit 0; fi
git commit -q -m "account-errors hourly feed $(date -u +%FT%TZ) [conductor]" && \
  git push origin main 2>/tmp/ae_hourly_push.err && echo "pushed -> Pages redeploy" || { echo "PUSH FAILED:"; cat /tmp/ae_hourly_push.err; exit 1; }
