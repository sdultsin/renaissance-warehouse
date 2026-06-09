#!/usr/bin/env bash
# Nightly sync entry. Cron target.
#
# Cron line (UTC, droplet):
#   30 3 * * * /root/renaissance-warehouse/scripts/nightly.sh >> /root/renaissance-warehouse/logs/nightly.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

mkdir -p logs

LOG_FILE="logs/$(date -u +%Y-%m-%d).log"

echo "=== nightly @ $(date -u +%FT%TZ) ===" | tee -a "$LOG_FILE"

# Activate venv if present
if [[ -f .venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

PYTHON="${PYTHON:-python3}"
ORCHESTRATOR_ARGS="${ORCHESTRATOR_ARGS:-}"
export WAREHOUSE_PULL_IAM_SENT="${WAREHOUSE_PULL_IAM_SENT:-1}"  # incremental IAM sent email ingest (iam_response_time)

# Apply any new versioned DDL before the run so new tables/views (sync_registry,
# infra-capacity views, campaign_daily, ...) always materialize. Idempotent
# (version-tracked); runs before the orchestrator opens its connection.
echo "applying versioned DDL (setup_db)" | tee -a "$LOG_FILE"
"$PYTHON" scripts/setup_db.py 2>&1 | tee -a "$LOG_FILE" \
    || echo "WARN setup_db_failed (continuing)" | tee -a "$LOG_FILE"

# The orchestrator returns 1 on a PARTIAL run (some peripheral ingest failed —
# e.g. pipeline-supabase intermittently refusing connections during retirement).
# Under `set -e` a non-zero pipeline aborts the script BEFORE EXIT_CODE is captured,
# which silently skipped compaction + all dashboard/serving publishes (root cause of
# the 06-03 serving freeze). Disable -e just around the orchestrator so the partial-
# handling logic below actually runs. (2026-06-08 F2 fix.)
set +e
# shellcheck disable=SC2086
"$PYTHON" -m core.orchestrator $ORCHESTRATOR_ARGS 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

# Publish dashboards on success (0) OR partial (1). A partial run means every
# phase executed and the warehouse tables were rebuilt; only peripheral ingests
# failed (dead workspaces 401/402, archive-DB lock, etc.) which do NOT feed the
# dashboards. Gating publish on a perfectly-clean exit froze the feeds for days
# (2026-06-04 fix). Only a hard abort (exit 2 / crash) keeps the old copies.
# Each publish step is non-fatal so one failure can't block the others, and the
# nightly's final exit code still reflects the orchestrator status for monitoring.
if [[ "$EXIT_CODE" -eq 0 || "$EXIT_CODE" -eq 1 ]]; then
    if [[ "$EXIT_CODE" -eq 1 ]]; then
        echo "orchestrator partial (some ingests failed); publishing dashboards anyway" | tee -a "$LOG_FILE"
    fi

    echo "compacting warehouse (skips unless bloated)" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/compact_warehouse.sh" 2>&1 | tee -a "$LOG_FILE" || echo "compaction non-fatal failure/skip" | tee -a "$LOG_FILE"

    echo "refreshing campaign-performance dashboard data" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/refresh_campaign_performance.sh" 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN campaign_performance_refresh_failed (continuing)" | tee -a "$LOG_FILE"

    echo "refreshing sms-campaign-performance dashboard data" | tee -a "$LOG_FILE"
    "$PYTHON" -m scripts.sms_campaign_dashboard_data --out /root/lens/sms-campaign-performance/data/latest.json 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN sms_feed_failed (continuing)" | tee -a "$LOG_FILE"

    echo "refreshing overview data.json" | tee -a "$LOG_FILE"
    if "$PYTHON" scripts/dashboard_data.py > /root/lens/overview/data.json.tmp 2>>"$LOG_FILE" && [[ -s /root/lens/overview/data.json.tmp ]]; then
        mv -f /root/lens/overview/data.json.tmp /root/lens/overview/data.json
        echo "overview data.json refreshed" | tee -a "$LOG_FILE"
    else
        rm -f /root/lens/overview/data.json.tmp
        echo "WARN overview_data_refresh_failed (continuing)" | tee -a "$LOG_FILE"
    fi

    # Lens serving copy disabled 2026-06-09 (warehouse_serving.duckdb deleted)
    # "$SCRIPT_DIR/publish_serving.sh" 2>&1 | tee -a "$LOG_FILE" \
    #     || echo "WARN publish_serving_failed (continuing)" | tee -a "$LOG_FILE"

    # Publish the campaign_data read-model snapshot to Cloudflare D1 so Campaign
    # Control can read it from D1 instead of Pipeline Supabase (retirement Lane C).
    # .env is NOT shell-sourceable (a comment on line ~131 contains a ')'), so
    # export the two D1 vars via grep rather than `source .env`.
    echo "publishing campaign_data read-model to D1" | tee -a "$LOG_FILE"
    CC_D1_API_TOKEN="$(grep '^CC_D1_API_TOKEN=' .env | cut -d= -f2- | tr -d '"')" \
    CLOUDFLARE_RG_ACCOUNT_ID="$(grep '^CLOUDFLARE_RG_ACCOUNT_ID=' .env | cut -d= -f2- | tr -d '"')" \
        "$PYTHON" scripts/publish_campaign_data_d1.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN campaign_data_d1_publish_failed (continuing)" | tee -a "$LOG_FILE"

    # Mirror cc_* operational tables from D1 into the warehouse (raw_cc_*) for
    # consolidation/BI (retirement Step 4). Non-fatal; writes after the
    # orchestrator has released the DuckDB writer lock.
    echo "mirroring cc_* from D1 to warehouse" | tee -a "$LOG_FILE"
    CC_D1_API_TOKEN="$(grep '^CC_D1_API_TOKEN=' .env | cut -d= -f2- | tr -d '"')" \
    CLOUDFLARE_RG_ACCOUNT_ID="$(grep '^CLOUDFLARE_RG_ACCOUNT_ID=' .env | cut -d= -f2- | tr -d '"')" \
        "$SCRIPT_DIR/mirror_cc_to_warehouse.sh" 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN cc_mirror_failed (continuing)" | tee -a "$LOG_FILE"

    # Track H — per-campaign day-by-day metrics from the Instantly analytics API
    # (lock-free fetch + brief write; runs after the orchestrator released the lock).
    echo "building core.campaign_daily (Track H)" | tee -a "$LOG_FILE"
    ( set +u; source /root/codex-ops/instantly-api-keys.env 2>/dev/null || true
      INSTANTLY_KEYS_ENV=/root/codex-ops/instantly-api-keys.env \
        "$PYTHON" scripts/build_campaign_daily.py 2>&1 | tee -a "$LOG_FILE" ) \
        || echo "WARN campaign_daily_build_failed (continuing)" | tee -a "$LOG_FILE"

    # Track I — refresh the NS sweep weekly (cheap-ish; NS rarely changes) and backfill
    # core.domain_registry.nameserver_host from it each night.
    if [[ ! -f /root/core/ns_sweep.parquet || $(find /root/core/ns_sweep.parquet -mtime +6 2>/dev/null) ]]; then
        echo "refreshing NS sweep (Track I)" | tee -a "$LOG_FILE"
        "$PYTHON" scripts/ns_sweep.py --out /root/core/ns_sweep.parquet 2>&1 | tee -a "$LOG_FILE" \
            || echo "WARN ns_sweep_failed (continuing)" | tee -a "$LOG_FILE"
    fi
    echo "backfilling domain_registry NS (Track I)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/backfill_domain_registry.py --ns /root/core/ns_sweep.parquet 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN domain_registry_backfill_failed (continuing)" | tee -a "$LOG_FILE"

    # Track I — backfill purchased_at (+ exact expires_at) from ALL THREE registrar APIs
    # (Porkbun, Spaceship, Dynadot), every configured account. Refresh the per-registrar
    # date caches weekly; load from cache nightly. Upgrades derived->API-exact where a
    # registrar covers the domain. OTD vendor-provisioned domains have no registration we
    # own and aren't in any account -> stay sheet-derived/null (filled below). Residual
    # derived rows = domains not in any of our registrar accounts.
    if [[ ! -f /root/core/porkbun_dates.parquet || $(find /root/core/porkbun_dates.parquet -mtime +6 2>/dev/null) ]]; then
        "$PYTHON" scripts/backfill_purchased_at_registrars.py --refresh-cache 2>&1 | tee -a "$LOG_FILE" \
            || echo "WARN registrars_purchased_at_failed (continuing)" | tee -a "$LOG_FILE"
    else
        "$PYTHON" scripts/backfill_purchased_at_registrars.py --from-cache 2>&1 | tee -a "$LOG_FILE" \
            || echo "WARN registrars_purchased_at_failed (continuing)" | tee -a "$LOG_FILE"
    fi

    # Track I — fill remaining purchased_at (+ expires_at) from the Domain Tech Sheet
    # mirror (expiration − 1y, per Sam). Runs AFTER the exact-registrar fills so exact
    # dates win; the sheet fills the rest as purchased_at_is_derived=TRUE.
    echo "backfilling purchased_at from Domain Tech Sheet (Track I)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/backfill_purchased_at_from_sheet.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN purchased_at_sheet_backfill_failed (continuing)" | tee -a "$LOG_FILE"

    # Track E — freshness backbone. Refresh core.sync_registry (writer; runs after
    # the orchestrator + all mirrors released the lock) then fail-loud QA. The QA
    # job posts a #cc-sam alert on any SLA breach so silent staleness is impossible.
    echo "refreshing sync_registry (freshness backbone)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/refresh_sync_registry.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN sync_registry_refresh_failed (continuing)" | tee -a "$LOG_FILE"

    echo "running warehouse QA (fail-loud freshness/invariant alert)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/warehouse_qa.py 2>&1 | tee -a "$LOG_FILE"
    QA_RC=${PIPESTATUS[0]}
    if [[ "$QA_RC" -ne 0 ]]; then
        echo "WARN warehouse_qa reported breaches (alert posted to #cc-sam)" | tee -a "$LOG_FILE"
    fi

    # Hardening DoD status to the log (no Slack — warehouse_qa already alerts).
    echo "hardening DoD check (log-only)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/verify_hardening_dod.py --no-post 2>&1 | tee -a "$LOG_FILE" || true
else
    echo "orchestrator hard-failed (exit=$EXIT_CODE); keeping existing dashboards + serving copy" | tee -a "$LOG_FILE"
fi

echo "exit=$EXIT_CODE" | tee -a "$LOG_FILE"
exit $EXIT_CODE
