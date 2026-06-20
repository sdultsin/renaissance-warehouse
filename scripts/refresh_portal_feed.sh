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

# 2b) Regenerate the 5 in-portal Lens dashboard feeds from the SAME serving snapshot
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
# sending-truth cube: committed GZIPPED (data.json.gz, ~1.3MB) instead of raw (~17.8MB) so the
# nightly git commit stays small (the cube regenerates every night -> ~17MB/day of history
# otherwise). The generator gzips when --json-out ends in .gz; app.js inflates client-side via
# DecompressionStream. Direct call (not lensgen) so we can pass --json-out .gz; same read-only
# serving-snapshot source + last-known-good contract (only swap in a non-empty fresh cube).
ST_GZ="$REPO/dashboards/lens-sending-truth/data.json.gz"
mkdir -p "$(dirname "$ST_GZ")"
if CORE_DB_PATH="$SNAP" "$PY" "$WH/scripts/sending_truth_dashboard_data.py" --gzip --json-out "$ST_GZ.tmp" 2>>/root/lens_feeds.err && [ -s "$ST_GZ.tmp" ]; then
  mv -f "$ST_GZ.tmp" "$ST_GZ"; echo "  ok lens-sending-truth $(wc -c < "$ST_GZ")B (gz)"
else
  rm -f "$ST_GZ.tmp"; echo "  WARN sending_truth_dashboard_data.py failed — keeping last-known-good $ST_GZ" >&2
fi
CORE_DB_PATH="$SNAP" "$PY" "$WH/scripts/sms_campaign_dashboard_data.py" --out "$REPO/dashboards/lens-sms/data/latest.json" >>/root/lens_feeds.err 2>&1 \
  && echo "  ok lens-sms $(wc -c < "$REPO/dashboards/lens-sms/data/latest.json")B" || echo "  WARN sms feed failed — keeping last-known-good" >&2

# campaign-performance is the ODD ONE OUT: its generator lives in /root/lens/scripts (it imports
# the proven daily_performance shaping module) and needs the LENS venv (duckdb), so it can't use
# $PY/lensgen. Same serving-snapshot source (--db $SNAP), same read-only contract. Writes the
# file itself via --json-out. Non-fatal: keep last-known-good on failure.
LENS_PY="/root/lens/backend/.venv/bin/python"
CP_DIR="$REPO/dashboards/lens-campaign-performance/data"; mkdir -p "$CP_DIR"
CP_OUT="$CP_DIR/latest.json"
if CORE_DB_PATH="$SNAP" "$LENS_PY" /root/lens/scripts/daily_performance_warehouse.py \
     --days 35 --db "$SNAP" --json-out "$CP_OUT" >>/root/lens_feeds.err 2>&1 && [ -s "$CP_OUT" ]; then
  echo "  ok lens-campaign-performance $(wc -c < "$CP_OUT")B"
else
  echo "  WARN campaign-performance feed failed — keeping last-known-good" >&2
fi
# The generator also drops a sibling unmapped-campaigns-<date>.md beside latest.json; it is not
# part of the served dashboard. Prune it so the tracked data/ dir stays {latest.json} only.
rm -f "$CP_DIR"/unmapped-campaigns-*.md 2>/dev/null || true

# 2c) Per-workspace dataset for the campaign-performance Workspaces tab (full totals,
#     all workspaces, no active-CM filter) from Pipeline (= Instantly) + email meetings.
#     Non-fatal: keep last-known-good on failure.
if "" "/scripts/gen_workspaces.py" >>/root/lens_feeds.err 2>&1; then
  echo "  ok lens-campaign-performance/workspaces.json"
else
  echo "  WARN gen_workspaces.py failed — keeping last-known-good" >&2
fi

# 3) Commit if changed.
cd "$REPO" || exit 1
git config --global --add safe.directory "$REPO" 2>/dev/null
git add portal_data.js \
  dashboards/lens-overview/data.json \
  dashboards/lens-kpi/data.json \
  dashboards/lens-sms/data/latest.json \
  dashboards/lens-sending-truth/data.json.gz \
  dashboards/lens-campaign-performance/data/latest.json \
  dashboards/lens-campaign-performance/data/workspaces.json 2>/dev/null
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
