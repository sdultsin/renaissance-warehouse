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

# 2. apply in bounded, auto-committed chunks via the warehouse venv duckdb (1.5.3).
#    WHY NOT the CLI: /usr/local/bin/duckdb v1.5.2 reproducibly crashed (glibc heap
#    corruption, SIGABRT) on the single-shot 62k-value post-consolidation map UPDATE
#    [2026-07-12, 2 crashes at 8GB/8t and 20GB/4t; clean rollbacks, rows/PK verified].
#    Chunks commit independently, so progress PERSISTS across any failure and re-runs
#    converge (the straggler set only shrinks). Steady state (a handful of values) = 1
#    tiny chunk. argv[0]-tagged duckdb_cli_writer (runs under the writer flock above).
PYBIN=/root/renaissance-warehouse/.venv/bin/python; [ -x "$PYBIN" ] || PYBIN=python3
( exec -a duckdb_cli_writer "$PYBIN" - "$DB" "$MAP" "$TMP" "${SWEEP_CHUNK:-8000}" ) <<'PY' >> "$LOG" 2>&1
import sys, duckdb
db, mapcsv, tmpdir, chunk = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
con = duckdb.connect(db)
con.execute(f"PRAGMA temp_directory='{tmpdir}'")
con.execute("SET memory_limit='16GB'"); con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=false")
con.execute(f"""CREATE TEMP TABLE allmap AS
  SELECT *, row_number() OVER () AS rn
  FROM read_csv('{mapcsv}', header=true,
                columns={{'col':'VARCHAR','old_value':'VARCHAR','new_value':'VARCHAR'}})""")
n = con.execute("SELECT count(*) FROM allmap").fetchone()[0]
for lo in range(1, n + 1, chunk):
    hi = min(lo + chunk - 1, n)
    for col in ("general_industry", "specific_industry"):
        con.execute(f"""UPDATE mirror.leads_current AS l SET {col}=m.new_value
          FROM (SELECT old_value, new_value FROM allmap
                WHERE col='{col}' AND rn BETWEEN {lo} AND {hi}) m
          WHERE l.{col}=m.old_value""")
    print(f"sweep chunk {lo}-{hi}/{n} committed", flush=True)
con.execute("DROP TABLE IF EXISTS mirror._sweepmap")  # legacy CLI-era staging leftover
con.close()
PY
rc=$?; flock -u 9
if [ $rc -eq 0 ]; then log "sweep done ($CNT values relowercased)"; else log "sweep FAILED rc=$rc"; alert ":rotating_light: industry-lowercase SWEEP failed rc=$rc (see industry_lowercase_sweep.log)"; fi
