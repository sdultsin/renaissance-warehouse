#!/usr/bin/env bash
# Durability sweep: re-lowercase any industry values reintroduced Title-Case by ANY mirror writer
# (catchup ingest, future Arseny backfill, etc.). Reuses the exact acronym-safe transform module.
# Cheap in steady state: post-swap only newly-ingested rows are still Title-Case, so it touches only those.
set -uo pipefail
VOL=/mnt/volume_nyc1_1781398428838/lead-mirror
DB=$VOL/lead_mirror.duckdb
LOCKFILE=$VOL/.writer.lock
DUCK=/usr/local/bin/duckdb
TMP=$VOL/duck_tmp
MAP=$TMP/industry_lc_sweep.csv
LOG=/root/renaissance-warehouse/logs/industry_lowercase_sweep.log
ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }
log(){ echo "[$(ts)] $*" >> "$LOG"; }
ddw(){ ( exec -a duckdb_cli_writer "$DUCK" "$@" ); }
alert(){ local py=/root/renaissance-warehouse/.venv/bin/python; [ -x "$py" ]||py=python3; "$py" /root/renaissance-warehouse/scripts/alert_slack.py "$1" >/dev/null 2>&1||true; }
mkdir -p "$TMP"

[ -f "$VOL/.industry_lc.done" ] || { log "skip: initial swap not applied yet"; exit 0; }
if fuser "$DB" >/dev/null 2>&1; then log "skip: primary busy"; exit 0; fi
AVAIL=$(free -m|awk '/^Mem:/{print $7}'); [ "${AVAIL:-0}" -ge 12000 ] || { log "skip: low RAM ${AVAIL}MB"; exit 0; }

exec 9>"$LOCKFILE"; flock -w 300 9 || { log "skip: could not get lock"; exit 0; }

# 1. derive current stragglers (values still != their transform) from primary, readonly.
#    argv[0]-tagged duckdb_cli_writer (exec -a, the same convention every sanctioned mirror
#    writer uses, e.g. renaissance-worker:jobs/lead-mirror) so the */5 primary-lock guard
#    sees this holder as domestic — it runs under the writer flock held above. Untagged,
#    this read step was the only sweep invocation the guard would flag as FOREIGN.
( exec -a duckdb_cli_writer python3 - "$DB" "$MAP" ) <<'PY'
import sys, csv, duckdb
sys.path.insert(0,'/root'); import industry_lc as M
db, out = sys.argv[1], sys.argv[2]
con=duckdb.connect(':memory:'); con.execute(f"ATTACH '{db}' AS p (READ_ONLY)")
rows=[]
for col in ('general_industry','specific_industry'):
    for v,n in con.execute(f"SELECT {col},count(*) FROM p.mirror.leads_current WHERE {col} IS NOT NULL AND {col}<>'' GROUP BY 1").fetchall():
        nv=M.transform(v)
        if nv!=v: rows.append((col,v,nv))
con.close()
with open(out,'w',newline='') as f:
    w=csv.writer(f); w.writerow(['col','old_value','new_value']); w.writerows(rows)
print(len(rows))
PY
CNT=$(( $(wc -l < "$MAP") - 1 ))
if [ "$CNT" -le 0 ]; then log "clean: 0 stragglers"; flock -u 9; exit 0; fi
log "sweeping $CNT straggler value(s)"

# memory_limit 20GB + threads=4 = the exact config that carried the full-table CTAS swap
# (apply_industry_lowercase.sh) on this 32GB box. At 8GB/8-threads the post-consolidation
# backlog sweep (62k values / ~700k rows) crashed duckdb v1.5.2 with malloc heap corruption
# [2026-07-12]; the txn rolled back cleanly (rows/PK verified). Gate above requires 12GB free.
ddw "$DB" <<SQL >> "$LOG" 2>&1
PRAGMA temp_directory='$TMP'; SET memory_limit='20GB'; SET threads=4;
CREATE OR REPLACE TABLE mirror._sweepmap AS
  SELECT * FROM read_csv('$MAP', header=true, quote='"', escape='"', columns={'col':'VARCHAR','old_value':'VARCHAR','new_value':'VARCHAR'});
UPDATE mirror.leads_current AS l SET general_industry=m.new_value
  FROM (SELECT old_value,new_value FROM mirror._sweepmap WHERE col='general_industry') m WHERE l.general_industry=m.old_value;
UPDATE mirror.leads_current AS l SET specific_industry=m.new_value
  FROM (SELECT old_value,new_value FROM mirror._sweepmap WHERE col='specific_industry') m WHERE l.specific_industry=m.old_value;
DROP TABLE mirror._sweepmap;
SQL
rc=$?; flock -u 9
if [ $rc -eq 0 ]; then log "sweep done ($CNT values relowercased)"; else log "sweep FAILED rc=$rc"; alert ":rotating_light: industry-lowercase SWEEP failed rc=$rc (see industry_lowercase_sweep.log)"; fi
