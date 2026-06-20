#!/usr/bin/env bash
# Recovery finisher — run ONCE after warehouse_recovery.duckdb copy from serving replica.
# Applies BI DDLs, pulls precious data from the pre-recovery backup, swaps the files,
# rebuilds canonical/derived, then resumes transcription.
#
# Usage: bash scripts/finish_recovery.sh
# Expected: warehouse_recovery.duckdb already copied from warehouse_serving.duckdb (full 51GB)
#           warehouse_pre_recovery.duckdb is the backup of the corrupted warehouse

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"
source .venv/bin/activate 2>/dev/null
RECOVERY=/root/core/warehouse_recovery.duckdb
BACKUP=/root/core/warehouse_pre_recovery.duckdb
MAIN=/root/core/warehouse.duckdb
LOG=logs/finish_recovery.log

echo "=== finish_recovery.sh $(date -u +%FT%TZ) ===" | tee -a "$LOG"

# Step 1: verify recovery file is the full 51GB
SZ=$(stat -c%s "$RECOVERY" 2>/dev/null || echo 0)
REF=$(stat -c%s /root/core/warehouse_serving.duckdb 2>/dev/null)
if [ "$SZ" != "$REF" ]; then
    echo "ERROR: recovery file incomplete ($SZ vs $REF) — re-run cp first" | tee -a "$LOG"
    exit 1
fi
echo "Step 1: recovery file size OK ($((SZ/1024/1024/1024))GB)" | tee -a "$LOG"

# Step 2: apply BI DDLs 41-48 to the recovery file
echo "Step 2: applying BI DDLs to recovery file..." | tee -a "$LOG"
CORE_DB_PATH=$RECOVERY python scripts/setup_db.py 2>&1 | grep -iE "applied|error" | tee -a "$LOG" || true

# Step 3: copy precious data from the backup (pre-recovery corrupted warehouse)
# Reads work fine on the corrupted DB even though writes are broken.
echo "Step 3: copying precious BI data from backup..." | tee -a "$LOG"
python3 - <<PY 2>&1 | tee -a "$LOG"
import duckdb, os
c = duckdb.connect("$RECOVERY")
c.execute("ATTACH '$BACKUP' AS bk (READ_ONLY)")

precious = [
    # (source_table, dest_table, description)
    ("bk.main.raw_close_call",               "raw_close_call",             "Close call raw"),
    ("bk.main.raw_partner_lead_feedback",    "raw_partner_lead_feedback",  "partner feedback raw"),
    ("bk.main.raw_comms_close_sync",         "raw_comms_close_sync",       "comms close_sync"),
    ("bk.main.raw_comms_gbc_application",    "raw_comms_gbc_application",  "comms gbc_app"),
    ("bk.main.raw_comms_app_link_check",     "raw_comms_app_link_check",   "comms app_link"),
    ("bk.core.call",                         "core.call",                  "core.call"),
    ("bk.core.call_outcome",                 "core.call_outcome",          "core.call_outcome"),
    ("bk.core.call_transcript",              "core.call_transcript",       "call_transcript (irreplaceable)"),
    ("bk.core.warm_caller",                  "core.warm_caller",           "warm_caller"),
    ("bk.core.reply_intent",                 "core.reply_intent",          "reply_intent (API cost)"),
    ("bk.core.lead_disposition",             "core.lead_disposition",      "lead_disposition"),
]
for src, dst, label in precious:
    try:
        n = c.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
        c.execute(f"DELETE FROM {dst}")
        c.execute(f"INSERT INTO {dst} SELECT * FROM {src}")
        print(f"  OK {label}: {n} rows")
    except Exception as e:
        print(f"  SKIP {label}: {str(e)[:100]}")

# raw_instantly_email from backup if newer
try:
    bk_n = c.execute("SELECT count(*) FROM bk.main.raw_instantly_email").fetchone()[0]
    rc_n = c.execute("SELECT count(*) FROM raw_instantly_email").fetchone()[0]
    if bk_n > rc_n:
        c.execute("DELETE FROM raw_instantly_email")
        c.execute("INSERT INTO raw_instantly_email SELECT * FROM bk.main.raw_instantly_email")
        print(f"  OK raw_instantly_email (backup {bk_n} > serving {rc_n}): {bk_n} rows")
    else:
        print(f"  OK raw_instantly_email: kept serving ({rc_n} rows, backup had {bk_n})")
except Exception as e:
    print(f"  SKIP raw_instantly_email: {e}")

c.execute("DETACH bk")
c.close()
print("Step 3 complete")
PY

# Step 4: swap recovery → main
echo "Step 4: swapping recovery -> main..." | tee -a "$LOG"
mv "$MAIN" /root/core/warehouse_pre_swap.duckdb
mv "$RECOVERY" "$MAIN"
echo "  warehouse.duckdb is now the recovery build" | tee -a "$LOG"

# Step 5: verify write path works
echo "Step 5: write-path verify..." | tee -a "$LOG"
python3 -c "
import duckdb
c = duckdb.connect('$MAIN')
c.execute(\"DELETE FROM core.call WHERE chr(49)=chr(48)\")
n = c.execute(\"SELECT count(*) FROM core.call\").fetchone()[0]
print(f'  core.call write OK, rows={n}')
c.close()
" 2>&1 | tee -a "$LOG"

# Step 6: rebuild canonical + derived
echo "Step 6: rebuilding canonical + derived..." | tee -a "$LOG"
python -m core.orchestrator --phase canonical 2>&1 | grep -iE "rebuilt|FAILED|ended" | tee -a "$LOG"
python -m core.orchestrator --phase derived   2>&1 | grep -iE "lead_intel|FAILED|ended"  | tee -a "$LOG"

# Step 7: resume transcription
echo "Step 7: resuming transcription backfill..." | tee -a "$LOG"
nohup python scripts/transcribe_calls.py >> logs/transcribe_resume.log 2>&1 &
echo "  transcription PID $!" | tee -a "$LOG"

echo "=== RECOVERY COMPLETE $(date -u +%FT%TZ) ===" | tee -a "$LOG"
