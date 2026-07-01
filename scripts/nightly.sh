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

# Direct-Instantly reply ingest (raw_instantly_email → core.reply). The entity reads
# this gate from the PROCESS environment only (not the merged .env files), so it must
# be exported here — without it the nightly logs "skipping" and reports ok rows_in=0
# (silent success; feed went stale after the June-8 step-2a backfill finished).
# (2026-06-11 hygiene fix, Task 2.)
export WAREHOUSE_PULL_REPLIES=1

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

# Set to 1 by any fail-loud post-orchestrator step (e.g. a failed campaign_data
# D1 publish or a parity divergence) so the nightly's FINAL exit reflects the
# degradation even when the orchestrator itself ran clean. Without this a failed
# publish would exit 0 (silent success) — the exact failure mode the 06-17
# read-model staleness incident exposed.
NIGHTLY_DEGRADED=0

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

    # Signature->phone self-enrichment (sig-phone Phase 2): extract US phones from
    # tonight's new inbound reply signatures -> public.leads enriched_phone sidecar.
    # Watermarked + restartable; fail-loud to Slack inside the script itself. Runs
    # AFTER compaction (warehouse lock free; read-only, coexists with publishes).
    # Spec: Renaissance handoffs/2026-06-12-signature-phone-warehouse-native.md
    echo "signature-phone sync (reply signatures -> leads.enriched_phone)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/signature_phone_sync.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN signature_phone_sync_failed (continuing)" | tee -a "$LOG_FILE"

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
    # export the D1 + Slack vars via grep rather than `source .env`.
    #
    # FAIL-LOUD (2026-06-17): this publish was previously best-effort-swallowed
    # (`|| echo WARN ... continuing`). When the 06-17 nightly's publish failed on
    # a stale DuckDB lock, the D1 snapshot went ~28h stale SILENTLY and CC was
    # blind to ~17 active campaigns for a cycle. The publish is now fail-loud: a
    # non-zero exit posts a :rotating_light: alert to the warehouse alert channel
    # (same SLACK_TOKEN / SLACK_ALERT_CHANNEL path as warehouse_qa.py) and is
    # recorded as a nightly failure so the nightly-success watchdog sees it.
    # Cross-checked by the CC-side self-audit freshness guard (independent path).
    echo "publishing campaign_data read-model to D1" | tee -a "$LOG_FILE"
    # GUARD (2026-06-22): a no-match `grep` returns non-zero, which under the
    # top-level `set -euo pipefail` makes the `$(...)` assignment fail and SILENTLY
    # aborts the entire nightly right here — before the `[[ -z ]]` fallback can run.
    # That is exactly what killed the nightly for a day when CC_D1_DATABASE_ID was
    # absent from .env. Disable -e just around this env-extraction block (mirroring
    # the orchestrator + timeout-publish guards elsewhere in this script) so a
    # missing var can NEVER abort the nightly: the fallback then fires for
    # CC_D1_DATABASE_ID, and empty values for the others are handled downstream
    # (the publish step is fail-loud but non-fatal).
    set +e
    CC_D1_API_TOKEN="$(grep '^CC_D1_API_TOKEN=' .env | cut -d= -f2- | tr -d '"')"
    CLOUDFLARE_RG_ACCOUNT_ID="$(grep '^CLOUDFLARE_RG_ACCOUNT_ID=' .env | cut -d= -f2- | tr -d '"')"
    # CC_D1_DATABASE_ID is often absent from .env (the publisher hardcodes a
    # default); fall back to the canonical id (also in campaign-control/wrangler.toml,
    # not a secret) so the parity check below has a database to read.
    CC_D1_DATABASE_ID="$(grep '^CC_D1_DATABASE_ID=' .env | cut -d= -f2- | tr -d '"')"
    [[ -z "$CC_D1_DATABASE_ID" ]] && CC_D1_DATABASE_ID="25a32aa3-9d95-42a3-9e9e-8cd3a9e3f3eb"
    SLACK_ALERT_CHANNEL="$(grep '^SLACK_ALERT_CHANNEL=' .env | cut -d= -f2- | tr -d '"')"
    set -e
    export CC_D1_API_TOKEN CLOUDFLARE_RG_ACCOUNT_ID CC_D1_DATABASE_ID SLACK_ALERT_CHANNEL

    # HARD TIMEOUT (2026-06-20): wrap the publish so a hung/slow publish can NEVER
    # wedge the rest of the nightly. ROOT CAUSE of the 06-17→06-20 staleness: this
    # step hung indefinitely (lock-wait and/or an unbounded D1 HTTP request), and
    # because it had no outer bound the WHOLE TAIL after it — Track H
    # (build_campaign_daily.py → core.campaign_daily) and the cc_* mirror
    # (raw_cc_*) and every freshness/QA step — NEVER RAN. The read-model going
    # stale is recoverable; the tail not running is the real damage. A normal
    # publish is ~37s; we allow 600s then SIGTERM, and SIGKILL 60s later if it
    # ignores TERM (--kill-after), so the publish is always bounded and the tail
    # ALWAYS proceeds. A timed-out publish is treated exactly like a failed
    # publish: loud log + :rotating_light: alert + NIGHTLY_DEGRADED=1, but it does
    # NOT block Track H / the mirror / the rest of the tail below.
    # `timeout` exits 124 on TERM-expiry, 137 (128+SIGKILL) if --kill-after fired.
    # Tunable via PUBLISH_TIMEOUT_S / PUBLISH_KILL_AFTER_S. Reversible: drop the
    # `timeout ...` prefix (back to a bare "$PYTHON scripts/publish_campaign_data_d1.py").
    PUBLISH_TIMEOUT_S="${PUBLISH_TIMEOUT_S:-600}"
    PUBLISH_KILL_AFTER_S="${PUBLISH_KILL_AFTER_S:-60}"
    set +e
    timeout --signal=TERM --kill-after="$PUBLISH_KILL_AFTER_S" "$PUBLISH_TIMEOUT_S" \
        "$PYTHON" scripts/publish_campaign_data_d1.py 2>&1 | tee -a "$LOG_FILE"
    PUBLISH_RC=${PIPESTATUS[0]}
    set -e
    if [[ "$PUBLISH_RC" -eq 124 || "$PUBLISH_RC" -eq 137 ]]; then
        echo "ERROR campaign_data_d1_publish_TIMED_OUT (rc=$PUBLISH_RC after ${PUBLISH_TIMEOUT_S}s) — killed so the rest of the nightly (Track H, cc_* mirror, QA) can proceed; CC read-model stays at its last good snapshot" | tee -a "$LOG_FILE"
        "$PYTHON" scripts/alert_slack.py \
            ":rotating_light: *campaign_data D1 publish TIMED OUT* (rc=$PUBLISH_RC after ${PUBLISH_TIMEOUT_S}s) — the publish hung and was KILLED so the rest of the nightly could finish. CC's read-model stays at its last good D1 snapshot until the next successful publish. CC self-audit freshness guard will also flag this. Investigate publish_campaign_data_d1.py (D1 HTTP timeout / DuckDB writer-lock wait)." \
            2>&1 | tee -a "$LOG_FILE" || true
        NIGHTLY_DEGRADED=1
    elif [[ "$PUBLISH_RC" -ne 0 ]]; then
        echo "ERROR campaign_data_d1_publish_failed (rc=$PUBLISH_RC) — CC read-model will go stale" | tee -a "$LOG_FILE"
        "$PYTHON" scripts/alert_slack.py \
            ":rotating_light: *campaign_data D1 publish FAILED* (rc=$PUBLISH_RC) — Campaign Control's read-model will go STALE until the next successful nightly. CC self-audit freshness guard will also flag this. Investigate publish_campaign_data_d1.py / DuckDB lock." \
            2>&1 | tee -a "$LOG_FILE" || true
        # Record into the nightly's exit status so the nightly-success watchdog
        # (warehouse_qa.py reads sync_registry; nightly final exit is monitored)
        # treats a failed publish as a failed nightly rather than silent success.
        NIGHTLY_DEGRADED=1
    else
        # Publish succeeded — assert the D1 snapshot matches LIVE Pipeline Supabase
        # on active-campaign set + Σemails_sent + Σopportunities (point-in-time
        # parity). Fail-loud on divergence (posts its own :rotating_light: alert).
        echo "verifying campaign_data D1<->Pipeline-Supabase parity" | tee -a "$LOG_FILE"
        set +e
        "$PYTHON" scripts/verify_campaign_data_parity.py 2>&1 | tee -a "$LOG_FILE"
        PARITY_RC=${PIPESTATUS[0]}
        set -e
        if [[ "$PARITY_RC" -eq 1 ]]; then
            echo "ERROR campaign_data_parity_divergence (alert posted)" | tee -a "$LOG_FILE"
            NIGHTLY_DEGRADED=1
        elif [[ "$PARITY_RC" -eq 2 ]]; then
            echo "WARN campaign_data_parity_could_not_run (continuing)" | tee -a "$LOG_FILE"
        fi
    fi

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

    # Portal gap dimensions (DDL 67-70, 2026-06-14) — keep them fresh going forward.
    # advisor / inbox_manager (core.meeting) refresh automatically via the canonical
    # 'meeting' phase above + meetings_refresh.sh; only SLA + Instantly credits need
    # an explicit nightly step. Both run AFTER the orchestrator released the writer
    # lock; core/db.py's in-process warehouse-writer lock serializes these (acquire-or-wait).
    LOCK_NIGHTLY=/root/core/warehouse.write.lock

    # SLA reply-time: rebuild the response-level fact + re-snapshot the trailing 14d
    # (covers late-arriving IM responses). Reads core.iam_response_time (built in the
    # canonical phase) + main.raw_pipeline_conversation_messages.
    echo "building core.sla_reply_time (portal gap dim)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/build_sla_reply_time.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN sla_reply_time_build_failed (continuing)" | tee -a "$LOG_FILE"

    # Deliverability reply-lag monitor (D2): rebuild core.deliv_reply_lag (send->first-reply
    # latency, the deliverability leading indicator — distinct from the SLA handling metric
    # above) + re-snapshot the trailing 14d. Reads main.raw_pipeline_conversation_messages.
    # Pairs with the human-vs-auto tile (pure views over raw_pipeline_campaign_daily_metrics,
    # no build). Both surfaced to #cc-sam by the 07:00Z deliv-monitors cron. (deliv-monitors)
    echo "building core.deliv_reply_lag (deliverability reply-lag monitor)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/build_deliv_reply_lag.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN deliv_reply_lag_build_failed (continuing)" | tee -a "$LOG_FILE"

    # Email-thread sync (DoD-4) — keep core.email_thread / core.email_message current with new replies
    # + their IM responses (ue_type=3 capture is owned by THIS entity, not pipeline_mirror which
    # DO-NOTHINGs). Runs AFTER the orchestrator released the writer lock: `fetch` is lock-free (it opens
    # only short-lived read conns, closed before each network pull — see entities/email_thread_sync.py),
    # then `apply` upserts under core/db.py's in-process writer flock (acquire-or-wait, same as the steps
    # above). Flag-gated (WAREHOUSE_PULL_THREADS=1) + pinned to the 9 canonical workspaces via
    # WAREHOUSE_THREADS_ORG_ALLOWLIST (both in /root/core/.env.threads, which also carries the per-ws keys).
    # --local-only: incremental discovery from the synced inbound atom only (NO all_emails live walk),
    # which would otherwise cross the 1000-page ceiling on the high-volume funding workspaces (~200k
    # sends/day) and HARD-FAIL them every night. A new replier's FULL thread is re-pulled, so their IM
    # responses ARE captured; the only uncovered edge is an IM message on a thread with no NEW prospect
    # reply (rare cold-thread chase) — acceptable vs a nightly that ceilings.
    THREADS_NIGHTLY_STAGE=/root/core/threads_nightly_stage.jsonl
    echo "email-thread sync (native reply threads — nightly incremental, local-only)" | tee -a "$LOG_FILE"
    ( set -a; [ -f /root/core/.env.threads ] && . /root/core/.env.threads; set +a
      set -o pipefail
      export WAREHOUSE_PULL_THREADS=1
      rm -f "$THREADS_NIGHTLY_STAGE" "$THREADS_NIGHTLY_STAGE".* 2>/dev/null
      # [2026-07-01] FAIL-SAFE cap on the per-lead /emails fetch. When the committed watermark falls
      # behind (429-throttled backlog), the fetch can exceed a nightly window and HANG the singleton
      # nightly lock for 10-15h (build-fan-out incident 2026-07-01 — same class as the #124/#126 tag
      # runaway). Time-box the fetch; APPLY only on a CLEAN (exit-0) fetch. A timed-out/partial fetch
      # is DISCARDED — the 2-day watermark OVERLAP re-pulls the same window next run, so NOTHING is
      # skipped; email_message just holds last-good for the day. Durable fix = resumable ordered drain.
      if timeout -k 30s "${THREADS_FETCH_TIMEOUT:-60m}" "$PYTHON" -m entities.email_thread_sync fetch --local-only --stage "$THREADS_NIGHTLY_STAGE" 2>&1 | tee -a "$LOG_FILE"; then
          "$PYTHON" -m entities.email_thread_sync apply --stage "$THREADS_NIGHTLY_STAGE" 2>&1 | tee -a "$LOG_FILE"
      else
          echo "WARN email_thread fetch incomplete/timed-out (>${THREADS_FETCH_TIMEOUT:-60m} backlog) — skipping apply, keeping last-good (no watermark advance; 2d overlap re-pulls next run)" | tee -a "$LOG_FILE"
      fi
      rm -f "$THREADS_NIGHTLY_STAGE" "$THREADS_NIGHTLY_STAGE".* 2>/dev/null ) \
        || echo "WARN email_thread_sync_nightly_failed (continuing)" | tee -a "$LOG_FILE"
    # keep apply manifests out of the repo working tree (gitignored too, belt + suspenders)
    mkdir -p /root/core/threads_bf/manifests 2>/dev/null
    mv /root/renaissance-warehouse/core/email_thread_manifest_*.txt /root/core/threads_bf/manifests/ 2>/dev/null || true

    # Instantly credits: pull per-workspace lead-list quota from the Instantly billing
    # API (read-only) and UPSERT a daily snapshot into core.instantly_credit (drops the
    # "The Eagles" free-trial junk row). Self-contained (runs portal_credits.py internally).
    echo "loading core.instantly_credit (portal gap dim)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/load_instantly_credit.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN instantly_credit_load_failed (continuing)" | tee -a "$LOG_FILE"

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

# Final exit reflects BOTH the orchestrator status AND any fail-loud
# post-orchestrator degradation (failed campaign_data publish / parity
# divergence). A clean orchestrator (0) with a degraded step exits 1 (partial),
# so the nightly-success watchdog never reads a failed publish as green. A
# hard-failed orchestrator (2) keeps its code. Slack alerts already fired inline.
FINAL_EXIT=$EXIT_CODE
if [[ "$NIGHTLY_DEGRADED" -eq 1 && "$EXIT_CODE" -eq 0 ]]; then
    FINAL_EXIT=1
    echo "nightly degraded by a fail-loud post-orchestrator step (publish/parity); exit 1" | tee -a "$LOG_FILE"
fi
# ─── Promote the serving snapshot AT NIGHTLY COMPLETION (06-20 stale-serving fix) ───
# Root cause of the 06-18 stale-serving incident: the snapshot publisher ran ONLY on a
# FIXED snapshot-publisher.timer at 06:30Z, decoupled from when this nightly actually
# finishes. The nightly routinely runs until ~06:55-07:30Z (well past 06:30), so the
# 06:30 timer kept promoting a PRE-COMPLETION build -> serving froze on a half-built DB.
# Fix: trigger the publisher HERE, as the nightly's final step, gated on a clean
# build (EXIT_CODE 0=clean or 1=partial -> the warehouse tables WERE rebuilt, matching
# the dashboard/D1-publish policy above; only exit 2 hard-abort skips). This
# couples the promote to actual build completion. The 06:30 timer is KEPT as a fallback
# (covers a nightly that died before reaching this line). The publisher's own flock
# (publish.lock, LOCK_NB) makes a double-promote impossible — if the 06:30 timer is mid-
# promote, this one aborts cleanly ("another publish holds the lock"), and vice-versa.
# The publisher ALSO re-validates server-side (size/verdict gate) before swapping, so it
# can never promote a bad snapshot regardless of trigger. Non-fatal: a failed promote
# logs + alerts (the 06:30 fallback + the 07:15/09:00 success-watchdog still cover it)
# and does NOT change FINAL_EXIT, so it can't mask the orchestrator's own status.
# Reversible: delete this block to revert to timer-only promotion.
if [[ "$EXIT_CODE" -eq 0 || "$EXIT_CODE" -eq 1 ]]; then
    echo "promoting serving snapshot (nightly complete; coupling promote to build completion)" | tee -a "$LOG_FILE"
    PUBLISHER_BIN="${PUBLISHER_BIN:-/opt/duckdb/bin/publisher.py}"
    PUBLISHER_PY="${PUBLISHER_PY:-/opt/duckdb/venv/bin/python}"
    PROMOTE_TIMEOUT_S="${PROMOTE_TIMEOUT_S:-900}"   # serving copy is ~50GiB; allow 15m, then bound
    if [[ -x "$PUBLISHER_PY" && -f "$PUBLISHER_BIN" ]]; then
        set +e
        SERVING_PROFILE=prod SERVING_CONFIG=/opt/duckdb/bin/config.yaml \
            timeout --signal=TERM --kill-after=60 "$PROMOTE_TIMEOUT_S" \
            "$PUBLISHER_PY" "$PUBLISHER_BIN" --reason nightly-complete 2>&1 | tee -a "$LOG_FILE"
        PROMOTE_RC=${PIPESTATUS[0]}
        set -e
        if [[ "$PROMOTE_RC" -eq 0 ]]; then
            echo "serving snapshot promoted at nightly completion (rc=0)" | tee -a "$LOG_FILE"
        else
            # rc=1 can be a benign no-op (the 06:30 timer already promoted this same build,
            # or we landed inside the 03:30-05:45 guard on an unusually fast night) — the
            # publisher logs the precise reason. Alert anyway so a REAL promote failure is
            # never silent; the 06:30 fallback + success-watchdog remain the safety net.
            echo "WARN serving snapshot promote at completion returned rc=$PROMOTE_RC (06:30 timer fallback still armed)" | tee -a "$LOG_FILE"
            "$PYTHON" scripts/alert_slack.py \
                ":warning: *serving snapshot promote-at-completion rc=$PROMOTE_RC* — the nightly tried to promote the freshly-built serving snapshot but the publisher returned non-zero (could be a benign no-op if the 06:30 timer already promoted this build; check /opt/duckdb/logs). The 06:30 timer + 07:15/09:00 success-watchdog still cover serving freshness. Investigate /opt/duckdb/bin/publisher.py if serving is stale." \
                2>&1 | tee -a "$LOG_FILE" || true
        fi
    else
        echo "WARN publisher not found ($PUBLISHER_PY / $PUBLISHER_BIN) — skipping promote-at-completion; 06:30 timer fallback still promotes" | tee -a "$LOG_FILE"
    fi
fi

echo "exit=$FINAL_EXIT (orchestrator=$EXIT_CODE degraded=$NIGHTLY_DEGRADED)" | tee -a "$LOG_FILE"
exit $FINAL_EXIT
