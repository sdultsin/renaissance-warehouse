#!/usr/bin/env bash
# Industry lowercase CTAS-swap on the lead-mirror PRIMARY. Reversible, single-writer-safe.
# Self-gating: off-box backup landed today + primary not held by nightly/delta. Idempotent.
set -uo pipefail
VOL=/mnt/volume_nyc1_1781398428838/lead-mirror
DB=$VOL/lead_mirror.duckdb
LOCKFILE=$VOL/.writer.lock
MAP=$VOL/industry_lc_20260710/industry_lc_mapping.csv
DUCK=/usr/local/bin/duckdb
TMP=$VOL/duck_tmp
STAMP=20260710
LOG=/root/renaissance-warehouse/logs/industry_lowercase_apply.log
OKFILE=/root/renaissance-warehouse/logs/backup_lead_mirror.last_ok
ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }
log(){ echo "[$(ts)] $*" | tee -a "$LOG"; }
ddw(){ ( exec -a duckdb_cli_writer "$DUCK" "$@" ); }
alert(){ local py="/root/renaissance-warehouse/.venv/bin/python"; [ -x "$py" ] || py=python3; "$py" /root/renaissance-warehouse/scripts/alert_slack.py "$1" >/dev/null 2>&1 || true; }
mkdir -p "$TMP"

[ -f "$MAP" ] || { log "FATAL map missing $MAP"; exit 2; }
# GATE 1: off-box backup landed today
[ -f "$OKFILE" ] || { log "GATE-WAIT: no backup last_ok"; exit 10; }
[ "$(date -u -d @"$(stat -c %Y "$OKFILE")" +%Y%m%d)" = "$STAMP" ] || { log "GATE-WAIT: backup last_ok not from $STAMP"; exit 10; }
# GATE 2: primary free (nightly/delta not attached)
if fuser "$DB" >/dev/null 2>&1; then log "GATE-WAIT: primary busy (nightly/delta)"; exit 11; fi
# GATE 3: RAM headroom (need ~16G free for a safe 12G-limit CTAS)
AVAIL=$(free -m | awk '/^Mem:/{print $7}')
[ "${AVAIL:-0}" -ge 16000 ] || { log "GATE-WAIT: only ${AVAIL}MB available (<16G) — box busy"; exit 12; }
# IDEMPOTENCE: already applied?
pre=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM mirror.leads_current WHERE general_industry='Construction'" 2>/dev/null || echo ERR)
[ "$pre" = "ERR" ] && { log "FATAL cannot read primary"; exit 2; }
if [ "$pre" = "0" ]; then log "IDEMPOTENT: 0 Title-Case 'Construction' left — already applied, exiting clean"; touch "$VOL/.industry_lc.done"; exit 0; fi

exec 9>"$LOCKFILE"
flock -w 600 9 || { log "FATAL could not acquire writer.lock in 600s"; exit 1; }
log "lock acquired (pre-change Title-Case Construction rows=$pre); PHASE A build"

ddw "$DB" <<SQL 2>&1 | tee -a "$LOG"
PRAGMA temp_directory='$TMP';
SET memory_limit='12GB';
SET threads=4;
DROP TABLE IF EXISTS mirror.leads_current_lcnew;
CREATE TABLE IF NOT EXISTS mirror.industry_lc_backup_${STAMP} AS
  SELECT id, general_industry, specific_industry FROM mirror.leads_current;
CREATE OR REPLACE TABLE mirror._industry_lc_map AS
  SELECT * FROM read_csv('$MAP', header=true,
    columns={'col':'VARCHAR','old_value':'VARCHAR','new_value':'VARCHAR','n_leads':'BIGINT'});
CREATE TABLE mirror.leads_current_lcnew AS
  SELECT * REPLACE(
    COALESCE(mg.new_value, l.general_industry) AS general_industry,
    COALESCE(ms.new_value, l.specific_industry) AS specific_industry)
  FROM mirror.leads_current l
  LEFT JOIN (SELECT old_value,new_value FROM mirror._industry_lc_map WHERE col='general_industry') mg
    ON mg.old_value=l.general_industry
  LEFT JOIN (SELECT old_value,new_value FROM mirror._industry_lc_map WHERE col='specific_industry') ms
    ON ms.old_value=l.specific_industry;
SQL
[ "${PIPESTATUS[0]}" -eq 0 ] || { log "FATAL phase A rc!=0"; flock -u 9; exit 5; }

OLD=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM mirror.leads_current" 2>/dev/null)
NEW=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM mirror.leads_current_lcnew" 2>/dev/null)
log "rowcount old=$OLD new=$NEW"
if [ "$OLD" != "$NEW" ] || [ -z "$NEW" ]; then
  log "FATAL rowcount mismatch — aborting, dropping lcnew (no swap done)"; alert ":rotating_light: industry-lowercase ABORTED: rowcount mismatch (no swap). See industry_lowercase_apply.log"
  ddw "$DB" "DROP TABLE IF EXISTS mirror.leads_current_lcnew;" >>"$LOG" 2>&1
  flock -u 9; exit 6
fi

log "PHASE B swap + reindex"
ddw "$DB" <<SQL 2>&1 | tee -a "$LOG"
ALTER TABLE mirror.leads_current_lcnew ADD PRIMARY KEY (id);
DROP TABLE mirror.leads_current;
ALTER TABLE mirror.leads_current_lcnew RENAME TO leads_current;
CREATE INDEX idx_leads_current_bounce ON mirror.leads_current(bounce_suppressed);
CREATE INDEX idx_leads_current_company_size ON mirror.leads_current(company_size);
CREATE INDEX idx_leads_current_created_at ON mirror.leads_current(created_at);
CREATE INDEX idx_leads_current_email ON mirror.leads_current(email);
CREATE INDEX idx_leads_current_esg_lookup ON mirror.leads_current(esg_lookup_status);
CREATE INDEX idx_leads_current_industry ON mirror.leads_current(general_industry);
CREATE INDEX idx_leads_current_phone10 ON mirror.leads_current(phone10);
CREATE INDEX idx_leads_current_seniority ON mirror.leads_current(seniority);
CREATE INDEX idx_leads_current_source ON mirror.leads_current("source");
CREATE INDEX idx_leads_current_source_list ON mirror.leads_current(source_list_name);
CREATE INDEX idx_leads_current_specific_ind ON mirror.leads_current(specific_industry);
CREATE INDEX idx_leads_current_state ON mirror.leads_current(state);
CREATE INDEX idx_leads_current_updated_at ON mirror.leads_current(updated_at);
CREATE INDEX idx_leads_current_verification ON mirror.leads_current(verification_status);
DROP TABLE mirror._industry_lc_map;
SQL
rc=${PIPESTATUS[0]}
flock -u 9
[ "$rc" -eq 0 ] || { log "FATAL phase B rc=$rc — data intact; indexes/PK may be partial, re-run to repair"; alert ":rotating_light: industry-lowercase phase-B FAILED rc=$rc — data intact, indexes may be partial, re-run apply script"; exit 7; }

# verify + counts
IDX=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM duckdb_indexes() WHERE table_name='leads_current'" 2>/dev/null)
POST=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM mirror.leads_current WHERE general_industry='Construction'" 2>/dev/null)
LOWC=$(ddw "$DB" -readonly -noheader -list "SELECT count(*) FROM mirror.leads_current WHERE general_industry='construction'" 2>/dev/null)
log "post-swap: indexes=$IDX (want 14), Title-Case Construction=$POST (want 0), lowercase construction=$LOWC"
log "refreshing serving copy"
/opt/duckdb/bin/refresh_lead_serving.sh >>"$LOG" 2>&1 && log "serving refreshed" || log "WARN serving refresh failed — will propagate at 03:50"
touch "$VOL/.industry_lc.done"
log "DONE industry lowercase swap complete"; alert ":white_check_mark: industry-lowercase swap DONE — Title-Case Construction=$POST (want 0), lowercase=$LOWC, indexes=$IDX/14. Mirror general_industry + specific_industry now lowercase (acronym-safe)."
