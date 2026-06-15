#!/usr/bin/env bash
# refresh_portal_feed.sh — regenerate the Renaissance Portal warehouse feed and publish it.
# Runs daily via the conductor (07:30 UTC, after the 07:00 funding-form/meetings refresh).
# READ-ONLY on the warehouse: reads the gated SERVING SNAPSHOT (/opt/duckdb/warehouse_current
# .duckdb), never the live writer. The ONLY write is the portal repo commit+push.
set -uo pipefail

REPO=/root/portal
WH=/root/renaissance-warehouse
SNAP=/opt/duckdb/warehouse_current.duckdb          # gated read-only serving snapshot
PY=$WH/.venv/bin/python                            # project venv (has pytz for esp matrix)
CRED_JSON=/root/portal_credits.json
ENVF=$WH/.env.instantly
export GIT_SSH_COMMAND="ssh -i /root/.ssh/portal_deploy_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

echo "==== portal feed refresh $(date -u +%FT%TZ) ===="

# 1) Instantly Lead Credits (read-only billing API). Non-fatal: keep last-known-good on failure.
echo "[1/4] credits (Instantly billing plan-details, read-only)"
INSTANTLY_ENV_FILE="$ENVF" "$PY" "$WH/scripts/portal_credits.py" > "$CRED_JSON.tmp" 2>>/root/portal_credits.err \
  && mv -f "$CRED_JSON.tmp" "$CRED_JSON" || echo "  WARN credits puller failed — keeping last-known-good"

# 2) Generate the feed from the SERVING SNAPSHOT (read-only). Fatal if this fails.
echo "[2/4] generate portal_data.js (read-only, serving snapshot)"
CORE_DB_PATH="$SNAP" PORTAL_CREDITS_JSON="$CRED_JSON" "$PY" "$WH/scripts/portal_data.py" \
  > "$REPO/portal_data.js.tmp" 2>/root/portal_data.err
if [ $? -ne 0 ] || [ ! -s "$REPO/portal_data.js.tmp" ]; then
  echo "  ERROR generator failed — see /root/portal_data.err; aborting (portal stays on last feed)"; cat /root/portal_data.err >&2; exit 1
fi
mv -f "$REPO/portal_data.js.tmp" "$REPO/portal_data.js"
echo "  generated $(wc -c < "$REPO/portal_data.js") bytes"

# 3) Commit if changed.
cd "$REPO" || exit 1
git config --global --add safe.directory "$REPO" 2>/dev/null
if git diff --quiet -- portal_data.js; then
  echo "[3/4] no change in portal_data.js — nothing to publish"; exit 0
fi
git add portal_data.js
git commit -q -m "portal data refresh $(date -u +%FT%TZ) [conductor]" && echo "[3/4] committed"

# 4) Push (GitHub Pages auto-deploys). Non-fatal so a missing credential doesn't fail the job;
#    the commit is preserved and pushes on the next run once the deploy key/credential is live.
echo "[4/4] push to origin"
if git push origin main 2>/tmp/portal_push.err; then
  echo "  pushed -> GitHub Pages will redeploy"
else
  echo "  WARN push failed (no git credential on the box yet) — commit is queued locally:"; cat /tmp/portal_push.err >&2
  echo "  -> add the deploy key to generalrenaissance/Renaissance-Portal (see PORTAL-PUSH notes); the queued commit will go out next run."
fi
echo "==== done $(date -u +%FT%TZ) ===="
