#!/usr/bin/env bash
# refresh_infra_batch.sh — WEEKLY re-runnable refresh of the infra-batch layer
# v2 (core.rg_tag_dim / core.sending_account_batch incl. is_cancelled+partner /
# core.infra_batch), off the 3 SHARED sheets only (TKT-1 §3/§4-B, 2026-07-03 —
# the 33 unshared "<Workspace> - Email Accounts" sheets are gone for good, and
# with them the permanent --require-all 33/38 Monday failure).
#
# Pipeline:
#   1. export_infra_batch_v2.py → fresh rg_dim.parquet + batch_sheet_v2.parquet
#      (TRUE MIRROR of Inbox Hub Funding/Cancelled + Cancelling-Accounts partner
#      tabs + Batches registry; rebuilt from scratch each run).
#   2. build_infra_batch_v2.sql under the single-writer flock → one-transaction
#      full reconcile: rg_tag_dim full replace; sending_account_batch = LIVE
#      (core.account_tags ⋈ rg_dim) ∪ LEGACY (06-12 sheet-snapshot rows for
#      emails not live-derived); infra_batch/infra_batch_key/domain_infra_csv.
#   3. publish_serving.sh → promote the read-side serving snapshot.
#
# Failure-aware watchdog (v1 pattern preserved): every gate (export, count
# guards, build, promote) that fails posts a #cc-sam alert and aborts WITHOUT
# promoting — a stale-but-correct table beats a fresh-but-broken one. A healthy
# run posts ONE completion line. The build SQL additionally carries in-txn
# error() guards, so a broken parquet rolls the whole reconcile back.
#
# Scheduled WEEKLY via cron (same Mon 06:30 UTC slot as v1), OUTSIDE the
# 03:30-05:45 UTC nightly/backup window. The flock makes it self-sequence
# behind the nightly if they ever overlap.
#
# Manual run:  /root/renaissance-warehouse/scripts/refresh_infra_batch.sh
# Env knobs (v2 thresholds; measured 2026-07-03 on warehouse_20260703_043558_874:
# rg_dim 5,650 tags / 2,358 cancelled; live-derived 894,775 memberships;
# expected post-rebuild total ≈ 2.85M):
#   INFRA_BATCH_OUTPUT_DIR   (default /root/core/build/infra-batch)
#   CORE_DB_PATH             (default /root/core/warehouse.duckdb)
#   INFRA_BATCH_MIN_ROWS     (floor on TOTAL memberships; default 2000000)
#   INFRA_BATCH_MIN_LIVE     (floor on live-derived memberships; default 300000)
#   RG_DIM_MIN_ROWS          (floor on rg_dim tags; default 2000)
#   INFRA_BATCH_MAX_DROP_PCT (max allowed membership drop vs prior; default 15.
#                             AUTHORITATIVE enforcement is the in-txn step-6c
#                             guard in build_infra_batch_v2.sql — hardcoded 85%
#                             of prior, rolls the reconcile back; this shell
#                             knob only tunes the redundant post-load backstop)
#   INFRA_BATCH_REQUIRE_ALL  (1 = abort if any of the 3 sheets/partner tabs fails; default 1)
#   GOOGLE_SHEETS_TOKEN      (default ~/.config/mcp-google-sheets/token.json)
#   INFRA_BATCH_V2_ID_MAP    (optional logical-name->id override JSON)
set -uo pipefail

REPO="${INFRA_BATCH_REPO:-/root/renaissance-warehouse}"
PY="${INFRA_BATCH_PY:-$REPO/.venv/bin/python}"
OUT_DIR="${INFRA_BATCH_OUTPUT_DIR:-/root/core/build/infra-batch}"
DB="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
MIN_ROWS="${INFRA_BATCH_MIN_ROWS:-2000000}"
MIN_LIVE="${INFRA_BATCH_MIN_LIVE:-300000}"
RG_DIM_MIN="${RG_DIM_MIN_ROWS:-2000}"
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
die() { log "FATAL: $1"; alert ":rotating_light: infra-batch v2 weekly refresh FAILED — $1 (serving NOT promoted by this run; a build-guard failure rolled the reconcile back in-txn — see the specific message for post-commit cases)."; exit 1; }

cd "$REPO" || die "repo not found at $REPO"
log "=== infra-batch v2 weekly refresh start ==="

# Keep the box checkout current so we run the merged exporter + build SQL.
git pull --ff-only origin main >/dev/null 2>&1 || log "WARN git pull --ff-only failed (continuing with local checkout)"

# Prior-run baseline (for the post-load drop-guard BACKSTOP; the authoritative
# drop guard runs INSIDE the build transaction — build_infra_batch_v2.sql step
# 6c — and needs no read here). Empty on first run. Distinguish "DB busy"
# (nightly writer still holds warehouse.duckdb) from a genuinely empty table.
if PRIOR_ROWS="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM core.sending_account_batch" 2>/dev/null)"; then
  PRIOR_ROWS="${PRIOR_ROWS:-0}"
  log "prior memberships=$PRIOR_ROWS"
else
  PRIOR_ROWS=0
  log "WARN: prior membership count unreadable (DB busy — nightly writer likely holds it), not an empty table; shell drop-guard backstop disabled this run (in-txn step-6c guard still enforces)"
fi

# ── 1. EXPORT (3 attempts with backoff on transient Sheets failure) ──────────
# Default REQUIRE_ALL=1: all 3 sheets + every known partner tab must read (no
# silent partial mirror). Set INFRA_BATCH_REQUIRE_ALL=0 only for a deliberate
# partial run.
REQUIRE_ALL_FLAG="--require-all"
[ "${INFRA_BATCH_REQUIRE_ALL:-1}" = "0" ] && REQUIRE_ALL_FLAG=""
ID_MAP_FLAG=""
[ -n "${INFRA_BATCH_V2_ID_MAP:-}" ] && ID_MAP_FLAG="--id-map ${INFRA_BATCH_V2_ID_MAP}"

export_ok=0
for attempt in 1 2 3; do
  log "export attempt $attempt ..."
  if $PY scripts/export_infra_batch_v2.py --output-dir "$OUT_DIR" \
        $REQUIRE_ALL_FLAG $ID_MAP_FLAG; then
    export_ok=1; break
  fi
  log "WARN export attempt $attempt failed; retry in 60s"
  sleep 60
done
[ "$export_ok" = "1" ] || die "exporter v2 failed after 3 attempts"

# ── 2. PRE-LOAD count guards on the freshly-written parquets ─────────────────
# In-memory duckdb ONLY — never open the writer DB just to read parquets (a
# busy DB would map to 0 and fire a FALSE "parse broke" alert). The explicit
# :memory: argument is REQUIRED: without a DB argument duckdb treats the SQL
# string as a database FILENAME and silently creates a junk DB file.
RG_ROWS="$(duckdb -noheader -list :memory: \
  "SELECT count(*) FROM read_parquet('$OUT_DIR/rg_dim.parquet')" 2>/dev/null || echo 0)"
RG_CANCELLED="$(duckdb -noheader -list :memory: \
  "SELECT count(*) FILTER (WHERE is_cancelled) FROM read_parquet('$OUT_DIR/rg_dim.parquet')" 2>/dev/null || echo 0)"
BATCH_LABELS="$(duckdb -noheader -list :memory: \
  "SELECT count(*) FROM read_parquet('$OUT_DIR/batch_sheet_v2.parquet')" 2>/dev/null || echo 0)"
RG_ROWS="${RG_ROWS:-0}"; RG_CANCELLED="${RG_CANCELLED:-0}"; BATCH_LABELS="${BATCH_LABELS:-0}"
log "new parquets: rg_dim=$RG_ROWS (cancelled=$RG_CANCELLED) batch_labels=$BATCH_LABELS"

[ "$RG_ROWS" -ge "$RG_DIM_MIN" ] 2>/dev/null \
  || die "rg_dim.parquet has $RG_ROWS tags, below floor ($RG_DIM_MIN) — refusing to load"
[ "$RG_CANCELLED" -gt 0 ] 2>/dev/null \
  || die "rg_dim.parquet has 0 cancelled tags — Cancelled-tab parse broke, refusing to load (would silently un-cancel all capacity)"
[ "$BATCH_LABELS" -ge 100 ] 2>/dev/null \
  || die "batch_sheet_v2.parquet has $BATCH_LABELS labels (< 100) — registry parse broke, refusing to load"

# build_infra_batch_v2.sql step 9 reloads core.domain_infra_csv from
# domain_purchase.parquet, which the exporter does NOT own (domain-registry
# cadence). Assert it's present so a missing file is a clear up-front error
# rather than a confusing transaction rollback that aborts the whole refresh.
[ -f "$OUT_DIR/domain_purchase.parquet" ] \
  || die "domain_purchase.parquet missing in $OUT_DIR — build step 9 would abort; ensure the domain-registry export is present before the refresh"

# ── 3. BUILD under the single-writer flock (one-txn full reconcile) ───────────
# The live-derived membership floor (>= $MIN_LIVE) is enforced INSIDE the
# transaction by the build SQL's error() guards, alongside its own rg_dim /
# batch-label floors and the step-6c >15%-drop guard — a guard failure rolls
# the whole reconcile back. Lock wait defaults to 8h: since 2026-06-30 the
# nightly tail (account_tags_late) can hold the writer DB well past the Mon
# 06:30Z cron slot, so the build must queue behind it rather than time out.
log "running build_infra_batch_v2.sql under writer flock ..."
BUILD_OK=1
WAREHOUSE_LOCK_WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-28800}" \
  "$LOCK_SH" duckdb "$DB" ".read scripts/build_infra_batch_v2.sql" || BUILD_OK=0

# The duckdb CLI '.read' bails at the first in-file error (verified v1.5.2), so
# the in-file post-COMMIT recreate (build SQL step 10) never runs on a failed
# build and the index dropped in step 0 would be silently lost. Recreate it
# here UNCONDITIONALLY: on a successful run this is a no-op (IF NOT EXISTS),
# on a rolled-back run it rebuilds on the untouched (unique) prior data.
WAREHOUSE_LOCK_WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-28800}" \
  "$LOCK_SH" duckdb "$DB" "CREATE UNIQUE INDEX IF NOT EXISTS ux_sab_email_key ON core.sending_account_batch (account_email, batch_key);" \
  || log "WARN: failed to recreate ux_sab_email_key (will be recreated by the next successful build)"

[ "$BUILD_OK" = "1" ] || die "build_infra_batch_v2.sql failed under the writer flock (transaction rolled back; unique index restored above)"

# ── 4. POST-LOAD verification (read the live tables back) ─────────────────────
POST_ROWS="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM core.sending_account_batch" 2>/dev/null || echo 0)"
POST_LIVE="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FILTER (WHERE attribution_source = 'account_tags_live') FROM core.sending_account_batch" 2>/dev/null || echo 0)"
POST_CANCELLED="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FILTER (WHERE is_cancelled) FROM core.sending_account_batch" 2>/dev/null || echo 0)"
POST_DIM="$(duckdb "$DB" -readonly -noheader -list \
  "SELECT count(*) FROM core.rg_tag_dim" 2>/dev/null || echo 0)"
POST_ROWS="${POST_ROWS:-0}"; POST_LIVE="${POST_LIVE:-0}"
POST_CANCELLED="${POST_CANCELLED:-0}"; POST_DIM="${POST_DIM:-0}"
log "post-load: memberships=$POST_ROWS live=$POST_LIVE cancelled=$POST_CANCELLED rg_dim=$POST_DIM"

[ "$POST_ROWS" -ge "$MIN_ROWS" ] 2>/dev/null \
  || die "post-load memberships ($POST_ROWS) below floor ($MIN_ROWS) — build did not load correctly"
[ "$POST_LIVE" -ge "$MIN_LIVE" ] 2>/dev/null \
  || die "post-load live-derived memberships ($POST_LIVE) below floor ($MIN_LIVE)"
[ "$POST_CANCELLED" -gt 0 ] 2>/dev/null \
  || die "post-load has 0 cancelled memberships — cancellation truth did not land"

# Drop guard vs the prior run (skip on first run / unknown prior). REDUNDANT
# BACKSTOP only: the authoritative >15% guard runs INSIDE the build txn
# (build_infra_batch_v2.sql step 6c) and rolls the reconcile back before
# anything commits — if THIS check fires, the in-txn guard was somehow bypassed
# and the shrunken table IS committed, so escalate before the next nightly
# serving promote ships it.
if [ "$PRIOR_ROWS" -gt 0 ] 2>/dev/null; then
  DROP_PCT=$(( (PRIOR_ROWS - POST_ROWS) * 100 / PRIOR_ROWS ))
  log "membership delta: prior=$PRIOR_ROWS post=$POST_ROWS drop_pct=$DROP_PCT (max $MAX_DROP_PCT)"
  if [ "$DROP_PCT" -gt "$MAX_DROP_PCT" ] 2>/dev/null; then
    die "membership drop ${DROP_PCT}% exceeds ${MAX_DROP_PCT}% (prior $PRIOR_ROWS -> post $POST_ROWS) — backstop tripped though the in-txn step-6c guard should have rolled this back: the shrunken table IS committed; investigate + restore before the next nightly serving promote"
  fi
fi

# ── 5. PROMOTE the serving snapshot ──────────────────────────────────────────
# publish_serving.sh has its own #cc-sam fail-loud alert + channel; just run it.
log "promoting serving snapshot ..."
"$REPO/scripts/publish_serving.sh" \
  || die "publish_serving.sh failed (tables loaded OK but serving snapshot not refreshed)"

# ── 6. Healthy completion: ONE line to #cc-sam ───────────────────────────────
log "=== infra-batch v2 weekly refresh OK ==="
alert ":white_check_mark: infra-batch v2 weekly refresh OK — ${POST_ROWS} memberships (${POST_LIVE} live-derived, ${POST_CANCELLED} cancelled), rg_dim ${POST_DIM} tags. Serving snapshot promoted."
exit 0
