#!/usr/bin/env bash
# One-off heal: wait for the live warehouse writer to free (Sendivo heal / nightly / #4),
# THEN transcribe the Jun-12/15 recording backlog. Idempotent + incremental; retries transient locks.
cd /root/renaissance-warehouse || exit 1
LOG=logs/transcribe_heal.log
export CALL_TRANSCRIBE_FLUSH_EVERY=25
echo "=== wait-then-transcribe start $(date -u +%FT%TZ) ===" >> "$LOG"
for w in $(seq 1 150); do
  if .venv/bin/python -c "from core import db; from core.config import DB_PATH; db.connect(DB_PATH, read_only=True).close()" 2>/dev/null; then
    echo "writer FREE after ${w}m wait $(date -u +%FT%TZ)" >> "$LOG"; break
  fi
  sleep 60
done
for i in $(seq 1 8); do
  echo "=== transcribe attempt $i $(date -u +%FT%TZ) ===" >> "$LOG"
  .venv/bin/python scripts/transcribe_calls.py >> "$LOG" 2>&1
  if tail -6 "$LOG" | grep -qE "nothing pending|transcription complete:"; then break; fi
  sleep 90
done
echo "=== transcribe_heal finished $(date -u +%FT%TZ) ===" >> "$LOG"
