#!/usr/bin/env bash
# [2026-06-17 C3] Daily refresh of warehouse-native dims sourced from box-side state:
# core.account_campaign (account<->active-campaign) + warmup_started_at/first_cold_send_at.
# Both scripts self-flock the warehouse single-writer lock and skip if a writer is active.
# Runs at 05:30Z (after the nightly, before the 06:30 publish). NOT an autonomous infra action.
cd /root/renaissance-warehouse
PY=/root/renaissance-warehouse/.venv/bin/python
LOG=logs/refresh_c3_dims.log
{ echo "[$(date -u +%FT%TZ)] start"
  "$PY" scripts/sync_account_campaign.py
  "$PY" scripts/backfill_warmup_coldstart.py
  echo "[$(date -u +%FT%TZ)] done"
} >> "$LOG" 2>&1
