#!/usr/bin/env bash
export WAREHOUSE_WRITE_LOCK_HELD=1  # outer flock holds the warehouse-writer lock; tell core/db.py not to re-lock (deadlock guard) [warehouse-ops 2026-06-17]
# =============================================================================
# Funding-Form -> core.meeting DAILY SYNC  (conductor job body)
# STAGED 2026-06-14 — deliverables/2026-06-14-funding-form-sync/. NOT yet deployed.
# Deploy target on the droplet: /root/renaissance-warehouse/scripts/sync_funding_form_meetings.sh
# Invoked by the conductor job spec funding-form-sync.json (cmd = this script).
# =============================================================================
#
# WHAT THIS IS
# ------------
# The idempotent re-projection of the Funding-Form 'Data' tab into core.meeting.
# It is a thin, single-purpose wrapper around the SAME three orchestrator steps the
# live `meetings_refresh.sh` already runs (sheets load -> core.meeting rebuild ->
# campaign_daily meetings column), MINUS the Slack-scrape mirror step.
#
# This is the conductor-managed replacement for the cron line:
#   0 7 * * * /root/renaissance-warehouse/scripts/meetings_refresh.sh
#
# WHY drop the Slack mirror step?  meetings_refresh.sh step 1 mirrors
# raw_pipeline_meetings_booked_raw (the Slack scrape) only to keep the <2026-06-01
# legacy tail current. That tail is FROZEN history (the sheet is canonical from
# 2026-06-01 on, Slack rows before it are never edited). Once the Slack scraper is
# retired (see SLACK-RETIREMENT-PLAN.md) there is nothing new to mirror, so this
# job omits it. It is harmless to add back as an optional step if ever needed.
#
# IDEMPOTENCE
# -----------
# core.meeting is a PURE PROJECTION of its two raw sources (entities/meeting.py does
# DELETE FROM core.meeting then full rebuild). Re-running this job any number of times
# produces the identical table. The sheet load reads ONLY the latest snapshot
# (entities/meeting.py filters _run_id = latest), so a re-stage with corrected cells
# wins and the projection stays a true rebuild. Safe to re-run / retry freely.
#
# PRECONDITION (the Mac producer)
# -------------------------------
# The droplet has NO Google credentials. The fresh CSV must already be at
# /root/core/sheets_staging/funding_form__Data.csv, scp'd by the Mac launchd producer
# com.renaissance.funding-form-producer (stage_funding_form.py, 22:00 + 12:00 EST).
# A missing/stale CSV is NON-FATAL: sheets_mirror skips it (last-known-good stays) and
# meeting.py rebuilds from the latest snapshot it has. This job FAILS LOUD (rc!=0, the
# conductor escalates to #cc-sam) only on a real orchestrator/DB error, and WARNS (rc=0)
# if the staged CSV is older than STALE_HOURS so a silently-dead producer is visible
# without blocking the rebuild.
#
# SINGLE-WRITER DISCIPLINE
# ------------------------
# Every write is wrapped in flock on the warehouse write lock with a bounded wait. If a
# long writer (nightly/compaction) holds it past the wait, we exit 0 (skip) rather than
# queue. The conductor ALSO serializes via the spec's  "lock": "warehouse-write"  so it
# will not even start this while another conductor job holds that resource. Belt + braces.
# NEVER run inside the nightly window 03:30-05:45 UTC (the conductor schedule + the daily
# reset hook keep this job's natural slot at 07:00 UTC, same as today's cron).
# =============================================================================

set -euo pipefail

WH=/root/renaissance-warehouse
PY="$WH/.venv/bin/python"
LOCK=/root/core/warehouse.write.lock
STAGED_CSV=/root/core/sheets_staging/funding_form__Data.csv
STALE_HOURS=18          # producer runs 2x/day; >18h stale = a likely missed/dead producer
FLOCK_WAIT=300          # seconds to wait for the write lock before skipping

cd "$WH"
echo "=== sync_funding_form_meetings @ $(date -u +%FT%TZ) ==="

# -- 0. Producer-freshness check (WARN only — never blocks the rebuild). ------------
if [[ -f "$STAGED_CSV" ]]; then
  csv_age_h=$(( ( $(date -u +%s) - $(stat -c %Y "$STAGED_CSV") ) / 3600 ))
  echo "staged CSV age: ${csv_age_h}h ($(stat -c %y "$STAGED_CSV"))"
  if (( csv_age_h > STALE_HOURS )); then
    echo "WARN: staged Funding-Form CSV is ${csv_age_h}h old (> ${STALE_HOURS}h) — Mac producer may be down; rebuilding from last-known-good."
  fi
else
  echo "WARN: no staged Funding-Form CSV at $STAGED_CSV — Mac producer never delivered; rebuilding from whatever snapshot the warehouse already holds."
fi

# -- 1. Load the Funding-Form snapshot into raw_sheets_funding_form_data ('sheets'
#       phase). Channel-aware projection happens downstream in meeting.py via the
#       explicit Channel column. A stale/missing CSV is skipped by sheets_mirror. ----
flock -w "$FLOCK_WAIT" "$LOCK" -c "$PY -m core.orchestrator --phase sheets" \
  || { echo "SKIP: writer lock busy (sheets)"; exit 0; }

# -- 2. Rebuild core.meeting (idempotent full rebuild: Slack < 2026-06-01 +
#       sheet >= 2026-06-01, channel-aware on the sheet's Channel column). ----------
flock -w "$FLOCK_WAIT" "$LOCK" -c "$PY -m core.orchestrator --phase canonical --ingest meeting" \
  || { echo "SKIP: writer lock busy (meeting)"; exit 0; }

# -- 3. Re-apply the meetings column of core.campaign_daily in place (email-channel
#       meetings only, matching the live meetings_refresh.sh logic). ----------------
flock -w "$FLOCK_WAIT" "$LOCK" -c "duckdb /root/core/warehouse.duckdb \"
UPDATE core.campaign_daily d SET meetings_booked = COALESCE(m.n, 0)
FROM (
  SELECT campaign_id, CAST(posted_at AS DATE) AS date, count(*) n
  FROM core.meeting
  WHERE campaign_id IS NOT NULL
    AND ((source = 'sheet' AND channel = 'Email')
      OR (source <> 'sheet'
          AND NOT regexp_matches(lower(COALESCE(campaign_name_raw,'')||' '||COALESCE(raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra')))
  GROUP BY 1, 2
) m
WHERE m.campaign_id = d.campaign_id AND m.date = d.date;
\"" || { echo "SKIP: writer lock busy (campaign_daily)"; exit 0; }

# -- 4. Health line (visible in the conductor job log + escalation context). --------
flock -w "$FLOCK_WAIT" "$LOCK" -c "duckdb -readonly /root/core/warehouse.duckdb \"
SELECT 'core.meeting by source: ' || string_agg(source || '=' || n, ', ' ORDER BY source) FROM (
  SELECT source, count(*) n FROM core.meeting GROUP BY source
);
SELECT 'sheet email-meetings unmatched (campaign_id NULL): ' ||
  count(*) FILTER (WHERE source='sheet' AND channel='Email' AND campaign_id IS NULL)
  FROM core.meeting;
\"" || true

echo "=== sync_funding_form_meetings done @ $(date -u +%FT%TZ) ==="
