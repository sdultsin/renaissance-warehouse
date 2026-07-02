#!/usr/bin/env bash
# refresh_infra_batch.sh — WEEKLY re-runnable mirror refresh of the infra-batch
# layer (core.infra_batch / core.sending_account_batch incl. rg_tag_1/rg_tag_2).
#
# Pipeline:
#   1. export_infra_batch.py   → fresh account_batch.parquet + batch_sheet.parquet
#      (a TRUE MIRROR of the source Google Sheets; rebuilt from scratch each run).
#   2. build_infra_batch.sql under the single-writer flock → DELETE+INSERT reload
#      (adds appear, removes drop — full reconcile, not append/upsert).
#   3. publish_serving.sh      → promote the read-side serving snapshot.
#
# Failure-aware watchdog: every gate (export, count guards, build, promote) that
# fails posts a #cc-sam alert and aborts WITHOUT promoting — a stale-but-correct
# table beats a fresh-but-broken one. A healthy run posts ONE completion line.
# Count guards refuse to promote a snapshot that drops memberships or zeroes RG
# attribution vs the prior run.
#
# Scheduled WEEKLY via cron, OUTSIDE the 03:30-05:45 UTC nightly/backup window
# (see crontab line installed by this work). The flock makes it self-sequence
# behind the nightly if they ever overlap.
#
# Manual run:  /root/renaissance-warehouse/scripts/refresh_infra_batch.sh
# Env knobs:
#   INFRA_BATCH_OUTPUT_DIR  (default /root/core/build/infra-batch)
#   CORE_DB_PATH            (default /root/core/warehouse.duckdb)
#   INFRA_BATCH_MIN_ROWS    (floor on memberships; default 2000000)
#   INFRA_BATCH_MAX_DROP_PCT(max allowed membership drop vs prior; default 15)
#   INFRA_BATCH_REQUIRE_ALL (1 = abort if any known source sheet is unresolved)
#   GOOGLE_SHEETS_TOKEN     (default ~/.config/mcp-google-sheets/token.json)
#   EMAIL_ACCOUNTS_ID_MAP   (optional name->id override JSON)
set -uo pipefail

REPO="${INFRA_BATCH_REPO:-/root/renaissance-warehouse}"
PY="${INFRA_BATCH_PY:-$REPO/.venv/bin/python}"
OUT_DIR="${INFRA_BATCH_OUTPUT_DIR:-/root/core/build/infra-batch}"
DB="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
MIN_ROWS="${INFRA_BATCH_MIN_ROWS:-2000000}"
MAX_DROP_PCT="${INFRA_BATCH_MAX_DROP_PCT:-15}"
LOG_DIR="${INFRA_BATCH_LOG_DIR:-$REPO/logs}"
LOG="$LOG_DIR/refresh_infra_batch.log"
LOCK_SH="$REPO/scripts/with_warehouse_lock.sh"
ALERT="$PY $REPO/scripts/alert_slack.py"

mkdir -p "$LOG_DIR" "$OUT_DIR"
exec >>"$LOG" 2>&1
ts() { date -u +%FT%TZ; }
log() { echo "$(ts) $*"; }
alert() { $ALERT "$1" || true; }
die() { log "FATAL: $1"; alert ":rotating_light: infra-batch weekly refresh FAILED — $1 (serving NOT refreshed; table left stale-but-correct)."; exit 1; }

cd "$REPO" || die "repo not found at $REPO"
log "=== infra-batch weekly refresh start ==="

# Keep the box checkout current so we run the merged exporter + build SQL.
git pull --ff-only origin main >/dev/null 2>&1 || log "WARN git pull --ff-only failed (continuing with local checkout)"

# Prior-run baseline (for the drop guard). Empty on first run.
PRIOR_ROWS="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM core.sending_account_batch" 2>/dev/null || echo 0)"
PRIOR_ROWS="${PRIOR_ROWS:-0}"
log "prior memberships=$PRIOR_ROWS"

# ── 1. EXPORT (with retry on transient Sheets/Drive failure) ─────────────────
# Default REQUIRE_ALL=1: the registry's known sheet set is the coverage contract,
# so a sheet that fails to resolve/read fails the run (no silent partial mirror).
# Set INFRA_BATCH_REQUIRE_ALL=0 only for a deliberate partial run.
REQUIRE_ALL_FLAG="--require-all"
[ "${INFRA_BATCH_REQUIRE_ALL:-1}" = "0" ] && REQUIRE_ALL_FLAG=""
ID_MAP_FLAG=""
[ -n "${EMAIL_ACCOUNTS_ID_MAP:-}" ] && ID_MAP_FLAG="--id-map ${EMAIL_ACCOUNTS_ID_MAP}"

export_ok=0
for attempt in 1 2 3; do
  log "export attempt $attempt ..."
  if $PY scripts/export_infra_batch.py --output-dir "$OUT_DIR" \
        $REQUIRE_ALL_FLAG $ID_MAP_FLAG; then
    export_ok=1; break
  fi
  log "WARN export attempt $attempt failed; retry in 60s"
  sleep 60
done
[ "$export_ok" = "1" ] || die "exporter failed after 3 attempts"

# ── 2. PRE-LOAD count guards on the freshly-written parquet ──────────────────
NEW_ROWS="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM read_parquet('$OUT_DIR/account_batch.parquet')" 2>/dev/null || echo 0)"
NEW_RG1="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FILTER (WHERE rg_tag_1 IS NOT NULL) FROM read_parquet('$OUT_DIR/account_batch.parquet')" 2>/dev/null || echo 0)"
NEW_ROWS="${NEW_ROWS:-0}"; NEW_RG1="${NEW_RG1:-0}"
log "new parquet memberships=$NEW_ROWS rg_tag_1_filled=$NEW_RG1"

# build_infra_batch.sql step 6 reloads core.domain_infra_csv from
# domain_purchase.parquet, which this exporter does NOT own (domain-registry
# cadence). Assert it's present so a missing file is a clear up-front error
# rather than a confusing transaction rollback that aborts the whole refresh.
[ -f "$OUT_DIR/domain_purchase.parquet" ] \
  || die "domain_purchase.parquet missing in $OUT_DIR — build step 6 would abort; ensure the domain-registry export is present before the refresh"

[ "$NEW_ROWS" -ge "$MIN_ROWS" ] 2>/dev/null \
  || die "parquet memberships ($NEW_ROWS) below floor ($MIN_ROWS) — refusing to load"
[ "$NEW_RG1" -gt 0 ] 2>/dev/null \
  || die "parquet has 0 rg_tag_1 — RG mapping broke, refusing to load (would wipe attribution)"

# Drop guard vs the prior run (skip on first run / unknown prior).
if [ "$PRIOR_ROWS" -gt 0 ] 2>/dev/null; then
  DROP_PCT=$(( (PRIOR_ROWS - NEW_ROWS) * 100 / PRIOR_ROWS ))
  log "membership delta: prior=$PRIOR_ROWS new=$NEW_ROWS drop_pct=$DROP_PCT (max $MAX_DROP_PCT)"
  if [ "$DROP_PCT" -gt "$MAX_DROP_PCT" ] 2>/dev/null; then
    die "membership drop ${DROP_PCT}% exceeds ${MAX_DROP_PCT}% (prior $PRIOR_ROWS -> new $NEW_ROWS) — likely a partial/failed source read; refusing to load"
  fi
fi

# ── 3. BUILD under the single-writer flock (DELETE+INSERT mirror reload) ──────
log "running build_infra_batch.sql under writer flock ..."
if ! WAREHOUSE_LOCK_WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-3600}" \
     "$LOCK_SH" duckdb "$DB" ".read scripts/build_infra_batch.sql"; then
  die "build_infra_batch.sql failed under the writer flock"
fi

# ── 4. POST-LOAD verification (read the live table back) ─────────────────────
POST_ROWS="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM core.sending_account_batch" 2>/dev/null || echo 0)"
POST_RG1="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FILTER (WHERE rg_tag_1 IS NOT NULL) FROM core.sending_account_batch" 2>/dev/null || echo 0)"
POST_ROWS="${POST_ROWS:-0}"; POST_RG1="${POST_RG1:-0}"
POST_RGPCT=0
[ "$POST_ROWS" -gt 0 ] 2>/dev/null && POST_RGPCT=$(( POST_RG1 * 100 / POST_ROWS ))
log "post-load: memberships=$POST_ROWS rg_tag_1_filled=$POST_RG1 (${POST_RGPCT}%)"
[ "$POST_ROWS" -ge "$MIN_ROWS" ] 2>/dev/null \
  || die "post-load memberships ($POST_ROWS) below floor — build did not load correctly"

# ── 5. PROMOTE the serving snapshot ──────────────────────────────────────────
# publish_serving.sh has its own #cc-sam fail-loud alert + channel; just run it.
log "promoting serving snapshot ..."
"$REPO/scripts/publish_serving.sh" \
  || die "publish_serving.sh failed (table loaded OK but serving snapshot not refreshed)"

# ── 6. Healthy completion: ONE line to #cc-sam ───────────────────────────────
log "=== infra-batch weekly refresh OK ==="
alert ":white_check_mark: infra-batch weekly refresh OK — ${POST_ROWS} memberships, RG attribution ${POST_RGPCT}% (rg_tag_1=${POST_RG1}). Serving snapshot promoted."
exit 0
