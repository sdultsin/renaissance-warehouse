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

# 0) Sync the portal checkout to origin BEFORE regenerating, so the eventual push is always a
#    fast-forward even if a client push (e.g. an index.html change) landed since the last run.
#    Safe: the only tracked files this job touches are regenerated below; portal_credits.json
#    is untracked and survives the reset. Without this the box diverges on any client push and
#    its pushes fail indefinitely (it never pulls).
cd "$REPO" && git fetch -q origin 2>/dev/null && git reset --hard -q origin/main 2>/dev/null \
  && echo "  synced to origin $(git rev-parse --short HEAD)" || echo "  WARN could not sync to origin (continuing)"

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

# 2b) Regenerate the 3 in-portal Lens dashboard feeds from the SAME serving snapshot
#     (read-only) straight into the portal checkout so they ride this same nightly commit.
#     CORE_DB_PATH=$SNAP is REQUIRED — the Lens generators default to the LOCKED live writer
#     and fail otherwise (that was the old standalone-cron breakage, see
#     handoffs/2026-06-17-portal-dashboard-freshness.md). Non-fatal each: a bad Lens feed
#     must never block the portal_data.js publish (keep last-known-good).
echo "[2b/4] regenerate Lens dashboard feeds (read-only, serving snapshot)"
lensgen () {  # $1 = generator script, $2 = dest path in the portal repo
  mkdir -p "$(dirname "$2")"
  if CORE_DB_PATH="$SNAP" "$PY" "$WH/scripts/$1" > "$2.tmp" 2>>/root/lens_feeds.err && [ -s "$2.tmp" ]; then
    mv -f "$2.tmp" "$2"; echo "  ok $(basename "$(dirname "$2")") $(wc -c < "$2")B"
  else
    rm -f "$2.tmp"; echo "  WARN $1 failed — keeping last-known-good $2" >&2
  fi
}
lensgen dashboard_data.py     "$REPO/dashboards/lens-overview/data.json"
lensgen kpi_dashboard_data.py "$REPO/dashboards/lens-kpi/data.json"
CORE_DB_PATH="$SNAP" "$PY" "$WH/scripts/sms_campaign_dashboard_data.py" --out "$REPO/dashboards/lens-sms/data/latest.json" >>/root/lens_feeds.err 2>&1 \
  && echo "  ok lens-sms $(wc -c < "$REPO/dashboards/lens-sms/data/latest.json")B" || echo "  WARN sms feed failed — keeping last-known-good" >&2
# lens-sending-truth: the CORRECTED capacity cube from core.account_label (phantom-free MX-infra
# census + DDL-1003-healed limits) + per-day actuals. Retires the old bespoke account_truth.duckdb
# pipeline (which left the cube frozen at 2026-06-16/17). Writes gzip straight into the portal repo.
if CORE_DB_PATH="$SNAP" "$PY" "$WH/scripts/sending_truth_dashboard_data.py" \
     --out "$REPO/dashboards/lens-sending-truth/data.json.gz" >>/root/lens_feeds.err 2>&1 \
   && [ -s "$REPO/dashboards/lens-sending-truth/data.json.gz" ]; then
  echo "  ok lens-sending-truth $(wc -c < "$REPO/dashboards/lens-sending-truth/data.json.gz")B"
else
  echo "  WARN sending-truth feed failed — keeping last-known-good" >&2
fi

# 3) Commit if changed.
cd "$REPO" || exit 1
git config --global --add safe.directory "$REPO" 2>/dev/null
git add portal_data.js dashboards/lens-overview/data.json dashboards/lens-kpi/data.json dashboards/lens-sms/data/latest.json dashboards/lens-sending-truth/data.json.gz 2>/dev/null
if git diff --cached --quiet; then
  echo "[3/4] no change in portal feed or Lens data — nothing to publish"; exit 0
fi
git commit -q -m "portal data + Lens dashboard refresh $(date -u +%FT%TZ) [conductor]" && echo "[3/4] committed"

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
