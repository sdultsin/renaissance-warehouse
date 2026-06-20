#!/bin/bash
# warehouse_drift_guard.sh — detect repo<->box drift for /root/renaissance-warehouse.
#
# WHY: this dir is the LIVE warehouse runtime (nightly + sendivo + transcribe +
# portal-feed). It was historically edited DIRECTLY on the box, so it silently ran
# ahead of the canonical PUBLIC GitHub repo (sdultsin/renaissance-warehouse). On
# 2026-06-17 it was converted to a real clone tracking origin/main so `git pull` is
# the deploy path and drift is VISIBLE via `git status`. This guard makes the drift
# ACTIVELY MONITORED instead of silent: it alerts #cc-sam when NEW drift appears.
#
# Read-only. Never commits, never pushes, never modifies the working tree.
# Reversible: delete this file + its two cron lines.
#
# Detects:
#   (1) BEHIND: local HEAD is behind origin/main  -> undeployed commits exist; run `git pull`.
#   (2) NEW DRIFT: the set of locally-modified/added/deleted tracked files changed vs
#       the recorded baseline -> someone edited the box directly again (the thing we
#       are trying to prevent). Baseline = the known divergence captured at conversion.
#
# Gated: alerts only on the 2nd consecutive detection (avoids transient fetch blips).
#
# Cron (UTC):
#   0 8  * * * /root/renaissance-warehouse/scripts/warehouse_drift_guard.sh >> /root/renaissance-warehouse/logs/drift_guard.log 2>&1
#   0 20 * * * /root/renaissance-warehouse/scripts/warehouse_drift_guard.sh >> /root/renaissance-warehouse/logs/drift_guard.log 2>&1
set -u

REPO_DIR="${REPO_DIR:-/root/renaissance-warehouse}"
ENV_FILE="${MONITOR_ENV:-/root/monitors/.env}"
CH="${SLACK_CHANNEL:-C0AR0EA21C1}"          # #cc-sam
MENTION="${SLACK_MENTION:-<@U0AM2CQHW9E>}"  # Sam
BASELINE="${DRIFT_BASELINE:-/root/renaissance-warehouse/.drift_baseline}"
STATE="${DRIFT_STATE:-/tmp/warehouse-drift-guard.state}"   # consecutive-failure counter
NOW_TS="$(date -u +%FT%TZ)"

post_slack() {
  local msg="$1" TOKEN
  TOKEN="$(grep -E '^CC_SLACK_BOT_TOKEN=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'"'"')"
  [ -z "$TOKEN" ] && { echo "$NOW_TS WARN no CC_SLACK_BOT_TOKEN in $ENV_FILE — cannot alert" >&2; return 0; }
  curl -s -X POST https://slack.com/api/chat.postMessage \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Content-Type: application/json; charset=utf-8' \
    --data "$(python3 -c 'import json,sys;print(json.dumps({"channel":sys.argv[1],"text":sys.argv[2]}))' "$CH" "$msg")" \
    >/dev/null 2>&1
}

cd "$REPO_DIR" 2>/dev/null || { echo "$NOW_TS ERROR cannot cd $REPO_DIR"; exit 1; }
[ -d .git ] || { echo "$NOW_TS ERROR $REPO_DIR is not a git repo (conversion lost?)"; exit 1; }

# read-only fetch
git fetch -q origin main 2>/dev/null || { echo "$NOW_TS WARN git fetch failed (network?) — skipping this run"; exit 0; }

# Settle the index stat-cache so the porcelain fingerprint is deterministic.
# (git lazily refreshes .git/index mtimes on the first status after some ops;
# refreshing up front avoids a one-shot false "fp changed" flicker.) Read-only.
git update-index -q --refresh >/dev/null 2>&1 || true
git status --porcelain >/dev/null 2>&1 || true

LOCAL="$(git rev-parse HEAD 2>/dev/null)"
REMOTE="$(git rev-parse origin/main 2>/dev/null)"
BEHIND="$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)"

# Current drift fingerprint = sorted "STATUS path" of ALL non-ignored changes.
# Includes untracked-but-not-ignored files on purpose: most of the box-ahead
# production code (DDL 49-79, transcribe_calls.py, sendivo/transcribe watchdogs,
# new entities) is currently UNTRACKED, so a new direct edit/add there must still
# trip the guard. Ignored runtime/secret artifacts (.env*, seed_data/, .venv/,
# logs/, *.bak*, _deploy_tmp/, backups/, *.duckdb) are excluded via .gitignore.
CUR_FP="$(git status --porcelain 2>/dev/null | sort | sha256sum | cut -d' ' -f1)"
CUR_COUNT="$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')"

# seed baseline on first run (records the known-at-conversion drift; not an alert)
if [ ! -f "$BASELINE" ]; then
  echo "$CUR_FP" > "$BASELINE"
  echo "$NOW_TS INIT baseline seeded fp=$CUR_FP count=$CUR_COUNT"
  exit 0
fi
BASE_FP="$(cat "$BASELINE" 2>/dev/null)"

PROBLEMS=""
[ "$BEHIND" -gt 0 ] 2>/dev/null && PROBLEMS="${PROBLEMS}- BEHIND origin/main by ${BEHIND} commit(s) — undeployed commits exist; run \`git -C ${REPO_DIR} pull\`.\n"
if [ "$CUR_FP" != "$BASE_FP" ]; then
  PROBLEMS="${PROBLEMS}- NEW direct-edit drift detected (tracked-file change-set differs from recorded baseline; now ${CUR_COUNT} changed). Someone edited the box directly again — review with \`git -C ${REPO_DIR} status\` / \`git -C ${REPO_DIR} diff\`, then commit-to-repo or revert.\n"
fi

if [ -z "$PROBLEMS" ]; then
  echo "$NOW_TS OK  HEAD=$LOCAL == origin/main; drift fingerprint unchanged (count=$CUR_COUNT)"
  echo 0 > "$STATE"
  exit 0
fi

# gated: require 2 consecutive detections
CONSEC="$(cat "$STATE" 2>/dev/null || echo 0)"; CONSEC=$((CONSEC+1)); echo "$CONSEC" > "$STATE"
echo "$NOW_TS DRIFT (consec=$CONSEC) behind=$BEHIND fp_changed=$([ "$CUR_FP" != "$BASE_FP" ] && echo yes || echo no)"
if [ "$CONSEC" -ge 2 ]; then
  post_slack "$(printf '%s :twisted_rightwards_arrows: *Warehouse repo<->box DRIFT* (%s)\n%s\nDir: %s (PUBLIC repo — do NOT blindly commit; sensitive cost/seed/PII is .gitignored for a reason). Owner: warehouse-ops.' "$MENTION" "$NOW_TS" "$PROBLEMS" "$REPO_DIR")"
  echo "$NOW_TS ALERTED #cc-sam"
fi
exit 0
