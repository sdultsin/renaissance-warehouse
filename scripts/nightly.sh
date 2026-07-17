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

# ─── TWO-PASS NIGHTLY (2026-07-14) ────────────────────────────────────────────────────────────
# The serving snapshot can only be published while the DuckDB WRITER LOCK IS FREE — i.e. only once
# the orchestrator process has exited (the publisher aborts with "warehouse_writer_lock_held"
# otherwise; see /opt/duckdb/logs/publish.jsonl). With a single ~7h pass, that meant the snapshot
# landed ~09:40 ET, so the Renaissance Data Hub's morning rebuild ALWAYS served yesterday's
# snapshot — fleet health was a full day stale every morning, every day.
#
# The fleet-health tables (account_census, sending_account, account_tags, account_first_cold_send)
# are FAST. What sat in front of them was slow and unrelated: instantly_replies (~90m), dns_sweep
# (~73m), CRM/comms. So the night is now split:
#   PASS A  fleet-health only (~1.5h) -> PROMOTE       => Hub has same-day data by ~03:30 ET
#   PASS B  everything else -> compaction -> PROMOTE   => all other consumers land as before
# Every ingest still runs EXACTLY ONCE (no phase is re-run: account_status_history is append-only
# and would double-insert), and no ingest moved ahead of an upstream it reads. PHASE_ORDER in
# core/config.py is the source of truth for ordering; the orchestrator re-sorts into PHASE_ORDER,
# so a typo here cannot run an ingest before its upstream.
PASS_A_PHASES="${PASS_A_PHASES:-pipeline_mirror,inbox_loader,instantly,account_census,portal_core,account_tags_late}"
PASS_B_PHASES="${PASS_B_PHASES:-comms_mirror,sendivo,iskra,outreachify,replies_late,close,sheets,otd_billing,im_bookings,account_truth,dns_sweep,canonical,iam_response_time,derived}"

# The tag pull now runs inside PASS A, so it must not be able to stall the morning promote.
# 8 workers = one per live workspace key (paging stays serial per key, so the aggregate IP rate
# stays gentle); the deadline makes a slow Instantly night degrade gracefully — finished
# workspaces are written, the rest keep their last-good rows. Both are env-overridable.
export WAREHOUSE_ACCOUNT_TAGS_WORKERS="${WAREHOUSE_ACCOUNT_TAGS_WORKERS:-8}"
export WAREHOUSE_ACCOUNT_TAGS_DEADLINE_MIN="${WAREHOUSE_ACCOUNT_TAGS_DEADLINE_MIN:-75}"

# Tariffs rides the Instantly analytics/dim pull WITHOUT joining the shared report
# roster (config/daily_report_sources.json drives the daily report render — adding
# tariffs there would change the report). The analytics entity reads this gate from
# the PROCESS environment only, so it must be exported here (R32 sending-infra
# attribution needs tariffs' campaign tag_labels in raw_instantly_campaign_dim;
# key = INSTANTLY_KEY_TARIFFS, present in the box .env since 2026-07-14).
export WAREHOUSE_INSTANTLY_ANALYTICS_EXTRA_SLUGS="${WAREHOUSE_INSTANTLY_ANALYTICS_EXTRA_SLUGS:-tariffs}"

# ─── promote_serving <reason> ─────────────────────────────────────────────────────────────────
# Publishes the serving snapshot. Extracted into a function (2026-07-14) so it can be called TWICE:
# once after PASS A (reason=portal-am) and once at nightly completion (reason=nightly-complete).
# Unchanged semantics: bounded by a hard timeout, non-fatal, alerts on a non-zero publisher rc.
# The publisher's own flock (publish.lock, LOCK_NB) makes a double-promote impossible, and it
# re-validates server-side (size/verdict gate) before swapping, so it can never promote a bad
# snapshot regardless of trigger. Sets PROMOTE_RC.
promote_serving() {
    local reason="$1"
    PROMOTE_RC=0
    # A just-finished orchestrator/phase leaves a STALE writer-lock marker (its pid is now dead).
    # The publisher aborts on ANY marker ("warehouse_writer_lock_held") — unlike core.db it does not
    # validate the pid — so the mid-run PASS-A "portal-am" promote never lands and the Hub serves
    # YESTERDAY's fleet health until PASS B finishes ~9am ET. Clear a dead-pid marker first:
    # core.db._clear_stale_lock_marker ONLY ever clears a DEAD pid (never a live writer's lock) and
    # never raises, so this is safe by construction; on any error the promote proceeds unchanged
    # (degrades to the old behaviour, never worse). [2026-07-15 — fixes the missed 8am morning promote]
    "$PYTHON" -c "from pathlib import Path; from core import db; db._clear_stale_lock_marker(Path(db._WRITE_LOCK_PATH))" 2>&1 | tee -a "$LOG_FILE" || true
    local PUBLISHER_BIN="${PUBLISHER_BIN:-/opt/duckdb/bin/publisher.py}"
    local PUBLISHER_PY="${PUBLISHER_PY:-/opt/duckdb/venv/bin/python}"
    local PROMOTE_TIMEOUT_S="${PROMOTE_TIMEOUT_S:-4500}"  # serving copy ~118GiB (~16min measured 2026-07-07);
                                                          # [2026-07-16] 2400->4500: publisher now
                                                          # waits (<=1800s) for + HOLDS the writer
                                                          # flock across copy+validate+swap.
                                                          # 900s got the copy KILLED at 110/126GB -> serving
                                                          # stale ~17h. 40m = ~2.5x headroom, then bound.
    if [[ ! -x "$PUBLISHER_PY" || ! -f "$PUBLISHER_BIN" ]]; then
        echo "WARN publisher not found ($PUBLISHER_PY / $PUBLISHER_BIN) — skipping promote ($reason); 06:30 timer fallback still promotes" | tee -a "$LOG_FILE"
        PROMOTE_RC=127
        return 0
    fi
    echo "promoting serving snapshot (reason=$reason)" | tee -a "$LOG_FILE"
    set +e
    SERVING_PROFILE=prod SERVING_CONFIG=/opt/duckdb/bin/config.yaml \
        timeout --signal=TERM --kill-after=60 "$PROMOTE_TIMEOUT_S" \
        "$PUBLISHER_PY" "$PUBLISHER_BIN" --reason "$reason" 2>&1 | tee -a "$LOG_FILE"
    PROMOTE_RC=${PIPESTATUS[0]}
    set -e
    if [[ "$PROMOTE_RC" -eq 0 ]]; then
        echo "serving snapshot promoted (reason=$reason, rc=0)" | tee -a "$LOG_FILE"
    else
        echo "WARN serving snapshot promote (reason=$reason) returned rc=$PROMOTE_RC" | tee -a "$LOG_FILE"
    fi
    return 0
}

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
if [[ -n "$ORCHESTRATOR_ARGS" ]]; then
    # MANUAL/AD-HOC run (e.g. ORCHESTRATOR_ARGS="--phase dns_sweep"): behave exactly as before —
    # one orchestrator invocation, no two-pass split, no mid-run promote. Only the unattended
    # cron path (no args) takes the two-pass path below.
    set +e
    # shellcheck disable=SC2086
    "$PYTHON" -m core.orchestrator $ORCHESTRATOR_ARGS 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
    set -e
else
    # ── PASS A — fleet-health critical. Its whole purpose is to free the writer lock EARLY so the
    # serving snapshot can be published while it is still the middle of the night ET. ──
    echo "=== PASS A (fleet-health): $PASS_A_PHASES ===" | tee -a "$LOG_FILE"
    set +e
    "$PYTHON" -m core.orchestrator --phases "$PASS_A_PHASES" 2>&1 | tee -a "$LOG_FILE"
    EXIT_A=${PIPESTATUS[0]}
    set -e

    # Promote on clean (0) or partial (1) — same policy as the dashboard publishes below: a partial
    # means the tables WERE rebuilt and only peripheral ingests failed. Only a hard abort (2) keeps
    # the last-good snapshot. The orchestrator has exited, so the writer lock is FREE and the
    # publisher can actually copy (this is the whole point of the split).
    if [[ "$EXIT_A" -eq 0 || "$EXIT_A" -eq 1 ]]; then
        promote_serving "portal-am"
        if [[ "$PROMOTE_RC" -ne 0 && "$PROMOTE_RC" -ne 127 ]]; then
            # Non-fatal: PASS B still runs and its promote-at-completion is the backstop (that is
            # exactly today's behaviour), so a failed morning promote degrades to the OLD timing —
            # it can never make things worse than before this change.
            "$PYTHON" scripts/alert_slack.py \
                ":warning: *morning serving promote failed (rc=$PROMOTE_RC)* — PASS A (fleet health) rebuilt fine but the snapshot did not publish, so the Renaissance Data Hub will serve YESTERDAY's fleet health this morning. The nightly continues; the promote-at-completion still runs later. Check /opt/duckdb/logs/publish.jsonl." \
                2>&1 | tee -a "$LOG_FILE" || true
        fi
    else
        echo "PASS A hard-aborted (exit=$EXIT_A) — skipping morning promote (last-good snapshot kept)" | tee -a "$LOG_FILE"
    fi

    # ── PASS B — everything else. Slow, and nothing the fleet-health pages read depends on it. ──
    echo "=== PASS B (everything else): $PASS_B_PHASES ===" | tee -a "$LOG_FILE"
    set +e
    "$PYTHON" -m core.orchestrator --phases "$PASS_B_PHASES" 2>&1 | tee -a "$LOG_FILE"
    EXIT_B=${PIPESTATUS[0]}
    set -e

    # Roll the two passes into the single EXIT_CODE the rest of this script (and every downstream
    # guard/watchdog) already understands: 2 (hard abort) dominates, then 1 (partial), else 0.
    if [[ "$EXIT_A" -ge 2 || "$EXIT_B" -ge 2 ]]; then
        EXIT_CODE=2
    elif [[ "$EXIT_A" -eq 1 || "$EXIT_B" -eq 1 ]]; then
        EXIT_CODE=1
    else
        EXIT_CODE=0
    fi
    echo "two-pass complete (PASS A=$EXIT_A, PASS B=$EXIT_B -> EXIT_CODE=$EXIT_CODE)" | tee -a "$LOG_FILE"
fi

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

    # Compaction failure must ALERT, not just log: it silently no-op'ing for 2 weeks is how
    # the writer DB ballooned to 171GB (2026-06-16..30). rc=0 covers success AND the healthy
    # below-threshold skip; every abort path exits nonzero -> Slack (non-fatal to the nightly).
    echo "compacting warehouse (skips unless bloated)" | tee -a "$LOG_FILE"
    if ! "$SCRIPT_DIR/compact_warehouse.sh" 2>&1 | tee -a "$LOG_FILE"; then
        echo "compaction non-fatal failure (alerting)" | tee -a "$LOG_FILE"
        "$PYTHON" "$SCRIPT_DIR/alert_slack.py" ":warning: warehouse compaction FAILED tonight (nightly continues; writer-DB bloat keeps growing until fixed) — grep '[compact' /root/renaissance-warehouse/logs/nightly.log for the ABORT reason" \
            2>&1 | tee -a "$LOG_FILE" || true
    fi

    # Signature->phone self-enrichment (sig-phone Phase 2): extract US phones from
    # tonight's new inbound reply signatures -> LEAD MIRROR enriched_phone sidecar
    # (mirror.leads_current; repointed from retired Supabase public.leads 2026-07-01,
    # [[project_phone_truth_lead_mirror_20260701]] — the mirror is THE phone master).
    # Watermarked + restartable; fail-loud to Slack inside the script itself. Runs
    # AFTER compaction (warehouse lock free; the mirror write serializes on the
    # mirror's own .writer.lock inside apply_phone_mirror_updates.sh).
    # Spec: Renaissance handoffs/2026-06-12-signature-phone-warehouse-native.md
    echo "signature-phone sync (reply signatures -> lead-mirror enriched_phone)" | tee -a "$LOG_FILE"
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

    # NOTE: core.sla_reply_time (the canonical §6 business-minute fact) is built LATER — AFTER the
    # email-thread drain/apply below feeds core.email_message with tonight's new prospect replies +
    # our ue_type=3 responses. Building it here (before that apply) would source a stale
    # core.email_message and understate answered pairs. See the build block after the email-thread sync.

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
    # [2026-07-10 flag #26] The inline SERIAL thread fetch is REMOVED from the nightly critical path.
    # Root cause (warehouse-flags #26): the nightly's other phases run ~05:30->14:00 UTC, so this
    # phase started ~14:00 and its 90m serial /emails drain advanced only ONE workspace (renaissance-4)
    # per night — a 99% coverage collapse across the other 7 while it also (a) added ~90m to the nightly,
    # (b) DOUBLE-PULLED the same Instantly /emails the standalone SYNC-7 drain already pulls (violates the
    # MotherDuck "never double-run source syncs" rule), and (c) held the writer lock to ~15:27, starving
    # 2 of the 3 daily parallel drains that skip when the nightly overruns. The standalone continuous
    # per-workspace parallel drain (threads_daytime_drain.sh, supervisor cron in the post-nightly window)
    # OWNS thread freshness now — same --local-only discovery, but ~8x parallel + ~14h/day instead of ~1
    # ws/90m. Watermark honesty + convergence are watched by threads_drain_watchdog.py.
    # REVERSIBLE: set THREADS_IN_NIGHTLY=1 (env or .env.threads) to restore the inline phase below.
    if [ "${THREADS_IN_NIGHTLY:-0}" = "1" ]; then
    THREADS_NIGHTLY_STAGE=/root/core/threads_nightly_stage.jsonl
    echo "email-thread sync (native reply threads — nightly incremental, local-only)" | tee -a "$LOG_FILE"
    ( set -a; [ -f /root/core/.env.threads ] && . /root/core/.env.threads; set +a
      set -o pipefail
      export WAREHOUSE_PULL_THREADS=1
      # Clean run artifacts but PRESERVE .failed: the old `stage.*` glob deleted failed.jsonl
      # before every nightly, silently killing the §4.F one-retry contract on this path — with
      # the 429-hardened partial commits below, failed leads' ONLY re-pull path is failed.jsonl,
      # so it must survive across runs. [2026-07-02]
      rm -f "$THREADS_NIGHTLY_STAGE" "$THREADS_NIGHTLY_STAGE".ceiling \
            "$THREADS_NIGHTLY_STAGE".progress "$THREADS_NIGHTLY_STAGE".progress.tmp \
            "$THREADS_NIGHTLY_STAGE".sanitized 2>/dev/null
      # [2026-07-01] FAIL-SAFE cap on the per-lead /emails fetch (60m so a 429-throttled backlog
      # can never hang the singleton nightly lock for 10-15h — the build-fan-out incident class).
      # [2026-07-02] the fetch is now a RESUMABLE ORDERED DRAIN (the durable fix the old comment
      # named): leads pull ascending by last-reply time with adaptive-exponential 429 backoff;
      # a timed-out/partial fetch is NO LONGER discarded — the apply commits whatever landed and
      # advances the explicit per-ws drain watermark (/root/core/threads_watermark.json) ONLY
      # through the contiguously-drained prefix, so partial progress accrues nightly and NOTHING
      # is skipped (ceiling-hit workspaces stay quarantined via the .ceiling sidecar; grep
      # RUN-SUMMARY-FETCH / RUNLOG-APPLY here for fetched/applied/429 counts).
      if timeout -k 30s "${THREADS_FETCH_TIMEOUT:-90m}" "$PYTHON" -m entities.email_thread_sync fetch --local-only --stage "$THREADS_NIGHTLY_STAGE" 2>&1 | tee -a "$LOG_FILE"; then
          # `|| echo`: apply exits 4 when it QUARANTINED a ceiling-hit ws (rows for the other
          # ws DID commit) — without the guard, `set -e` would abort the subshell here, mislabel
          # the run as email_thread_sync_nightly_failed, and skip the trailing cleanup.
          "$PYTHON" -m entities.email_thread_sync apply --stage "$THREADS_NIGHTLY_STAGE" 2>&1 | tee -a "$LOG_FILE" \
              || echo "WARN email_thread apply rc!=0 (ceiling quarantine or failure — see RUNLOG-APPLY above)" | tee -a "$LOG_FILE"
      else
          echo "WARN email_thread fetch incomplete (timeout >${THREADS_FETCH_TIMEOUT:-90m} / ceiling / crash) — applying PARTIAL progress (drain watermark advances only through the completed prefix; the rest re-pulls next run)" | tee -a "$LOG_FILE"
          "$PYTHON" -m entities.email_thread_sync apply --stage "$THREADS_NIGHTLY_STAGE" 2>&1 | tee -a "$LOG_FILE" \
              || echo "WARN email_thread partial apply failed (stage kept nothing; watermark unchanged — clean re-pull next run)" | tee -a "$LOG_FILE"
      fi
      rm -f "$THREADS_NIGHTLY_STAGE" "$THREADS_NIGHTLY_STAGE".ceiling \
            "$THREADS_NIGHTLY_STAGE".progress "$THREADS_NIGHTLY_STAGE".progress.tmp \
            "$THREADS_NIGHTLY_STAGE".sanitized 2>/dev/null ) \
        || echo "WARN email_thread_sync_nightly_failed (continuing)" | tee -a "$LOG_FILE"
    # keep apply manifests out of the repo working tree (gitignored too, belt + suspenders)
    mkdir -p /root/core/threads_bf/manifests 2>/dev/null
    mv /root/renaissance-warehouse/core/email_thread_manifest_*.txt /root/core/threads_bf/manifests/ 2>/dev/null || true
    else
        echo "email-thread sync SKIPPED in nightly — owned by the standalone SYNC-7 continuous drain (flag #26, 2026-07-10); set THREADS_IN_NIGHTLY=1 to restore" | tee -a "$LOG_FILE"
    fi

    # SLA reply-time (§6 canonical business-minute clock) — MUST run AFTER the email-thread apply
    # above so core.email_message carries tonight's fresh prospect replies (ue_type=2) + our replies
    # (ue_type=3). Rebuilds the first-reply (seq=1, thread-grain) fact and materializes
    # biz_latency_minutes (12:00-20:00 ET Mon-Fri clamp) + clock_open_date — the SAME validated §6
    # clamp render_daily.py §6 now READS, so warehouse + report share ONE definition (DR-7,
    # 2026-07-03). Re-snapshots the trailing 14d (late replies back-fill as the SYNC-7 drain advances).
    echo "building core.sla_reply_time (§6 canonical business-minute clock)" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/build_sla_reply_time.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN sla_reply_time_build_failed (continuing)" | tee -a "$LOG_FILE"

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

    # Track I — turn the fetched registrar domain lists into the full raw_registrar_domains
    # table + rebuild core.registrar_snapshot (the Data Hub's registrar / account view). Reads
    # the enriched date caches the backfill just wrote (no extra API calls); shrink-guarded +
    # atomic-swap so a bad run never empties the live tables. [2026-07-14]
    echo "syncing registrar domains -> raw_registrar_domains + registrar_snapshot" | tee -a "$LOG_FILE"
    "$PYTHON" scripts/sync_registrar_domains.py 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN registrar_domains_sync_failed (continuing)" | tee -a "$LOG_FILE"

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
    # GUARD [2026-07-02]: this was the ONE unguarded pipeline in the success branch. Under the
    # top-level `set -euo pipefail`, warehouse_qa.py exiting non-zero (any SLA breach — true on
    # BOTH Jul-1 and Jul-2) made the pipeline fail and ABORTED the whole script right here,
    # BEFORE QA_RC was even captured: no hardening-DoD check, no promote-at-completion, and no
    # final `exit=` line — which in turn made daily_report_sync.sh's backfill guard read the
    # nightly as "died without an exit line" and skip the D-1 re-render. Mirror the orchestrator/
    # publish steps: disable -e around the pipeline so breaches WARN (the QA already alerted
    # #cc-sam itself) and the tail always runs.
    set +e
    "$PYTHON" scripts/warehouse_qa.py 2>&1 | tee -a "$LOG_FILE"
    QA_RC=${PIPESTATUS[0]}
    set -e
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
    # [2026-07-14] Now the SECOND promote of the night (PASS A already published the fleet-health
    # snapshot hours ago). This one carries everything else — replies, CRM, DNS, canonical, derived
    # — so all other consumers land exactly as they did before the two-pass split.
    promote_serving "nightly-complete"
    if [[ "$PROMOTE_RC" -ne 0 && "$PROMOTE_RC" -ne 127 ]]; then
        # rc=1 can be a benign no-op (the 06:30 timer already promoted this build, or we landed
        # inside the 03:30-05:45 publisher guard on an unusually fast night) — the publisher logs
        # the precise reason. Alert anyway so a REAL promote failure is never silent; the 06:30
        # fallback + the 07:15/09:00 success-watchdog remain the safety net.
        "$PYTHON" scripts/alert_slack.py \
            ":warning: *serving snapshot promote-at-completion rc=$PROMOTE_RC* — the nightly tried to promote the freshly-built serving snapshot but the publisher returned non-zero (could be a benign no-op if an earlier promote already published this build; check /opt/duckdb/logs). The 06:30 timer + 07:15/09:00 success-watchdog still cover serving freshness. Investigate /opt/duckdb/bin/publisher.py if serving is stale." \
            2>&1 | tee -a "$LOG_FILE" || true
    fi
    # [2026-07-14] MotherDuck write-path kick — the MD publish fires ON PROMOTE, regardless of
    # hour. WHY: the wrapper's */30 cron ticks race the promote (today's 8.7h nightly promoted
    # at 14:15Z and warehouse_current flipped at 14:31 — 30 SECONDS after the 14:30 tick read
    # it, so the day's MD publish only happened because a human ran it manually). This removes
    # the race and the window-edge entirely: a successful promote-at-completion immediately
    # launches the wrapper DETACHED (nohup; survives this script exiting). Safe by construction:
    # flock -n no-ops if a cron tick is already publishing, the wrapper's snapshot-identity
    # stamp no-ops if this snapshot is already published, and the runner's GATE +
    # last_success_snapshot dedup make a double-publish impossible. The (extended, 6-17Z)
    # cron remains the retry path if this kicked run fails (rc>=2 alerts #cc-sam itself).
    if [[ "$PROMOTE_RC" -eq 0 && -x /root/md-migration/md_write_path.sh ]]; then
        nohup /usr/bin/flock -n /tmp/md_write_path.lock /root/md-migration/md_write_path.sh \
            >> /root/md-migration/md_write_path.log 2>&1 &
        echo "kicked MotherDuck write-path publish (detached pid $!)" | tee -a "$LOG_FILE"
    fi
fi

echo "exit=$FINAL_EXIT (orchestrator=$EXIT_CODE degraded=$NIGHTLY_DEGRADED)" | tee -a "$LOG_FILE"
exit $FINAL_EXIT
