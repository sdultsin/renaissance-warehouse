#!/usr/bin/env bash
# apply_phone_mirror_updates.sh — the ONE writer path for enrichment phones -> lead mirror.
#
# Phone-truth architecture [2026-07-01, project_phone_truth_lead_mirror_20260701]:
# the lead-mirror DuckDB is THE master store for phones. Every standing sync that
# lands an enrichment/signature phone on a lead goes through THIS script so the
# write discipline lives in exactly one place:
#
#   - INPUT: a TSV (no header) of  email \t phone \t source_tag \t event_ts(ISO)
#     event_ts = when the phone was OBSERVED (reply timestamp / vendor attempted_at),
#     NOT the write time — freshness comparisons across sources stay write-order
#     independent.
#   - UPDATES ONLY the enriched_phone / enriched_phone_source / enriched_phone_at
#     sidecar columns on mirror.leads_current (keyed lower(email); mirror emails are
#     verified all-lowercase). NEVER touches the bought `phone` / `phone10` columns.
#   - NEVER clobbers a fresher value: a row is written only when the existing
#     enriched_phone_at is NULL or OLDER than the incoming event_ts.
#   - Holds the OS .writer.lock (same mutex the mirror writers use) for the whole
#     burst; argv[0]=duckdb_cli_writer so the */5 primary-lock guard allow-lists it.
#     Single transaction -> atomic, no torn state on interrupt.
#
# Pattern proven by /root/renaissance-worker/jobs/lead-mirror/apply_sms_phone_enrich.sh
# (the 2026-06-28 SMS rescue). Callers: scripts/comms_phone_mirror_sync.py (nightly
# comms paid-enrichment sync) and scripts/signature_phone_sync.py (nightly sig-phone
# Phase 2, repointed from the retired Supabase public.leads 2026-07-01).
#
# Usage: apply_phone_mirror_updates.sh <staged.tsv>
# Output (stdout): metric lines `staged= matched= will_update= committed=` parsed by callers.
set -euo pipefail

TSV="${1:?usage: apply_phone_mirror_updates.sh <staged.tsv>}"
DB="${MIRROR_DB:-/mnt/volume_nyc1_1781398428838/lead-mirror/lead_mirror.duckdb}"
LOCK="${MIRROR_WRITER_LOCK:-/mnt/volume_nyc1_1781398428838/lead-mirror/.writer.lock}"
LOCK_WAIT_S="${MIRROR_LOCK_WAIT_S:-600}"
DUCKDB_BIN="${DUCKDB_BIN:-/usr/local/bin/duckdb}"

ts(){ date -u +%FT%TZ; }
ddw(){ ( exec -a duckdb_cli_writer "$DUCKDB_BIN" "$@" ); }

if [[ ! -s "$TSV" ]]; then
  echo "[$(ts)] staged=0 matched=0 will_update=0 committed=0 (empty TSV, nothing to apply)"
  exit 0
fi

echo "[$(ts)] acquiring mirror writer.lock (wait up to ${LOCK_WAIT_S}s)"
exec 9>"$LOCK"
flock -w "$LOCK_WAIT_S" 9 || { echo "[$(ts)] ERR: could not acquire writer.lock in ${LOCK_WAIT_S}s"; exit 75; }
echo "[$(ts)] lock held"

ddw "$DB" <<SQL
BEGIN TRANSACTION;
CREATE TEMP TABLE src_raw AS
  SELECT lower(trim(column0)) AS email,
         trim(column1)        AS phone,
         trim(column2)        AS source,
         TRY_CAST(column3 AS TIMESTAMPTZ) AS event_ts
  FROM read_csv('$TSV', delim='\t', header=false, quote='',
                columns={'column0':'VARCHAR','column1':'VARCHAR',
                         'column2':'VARCHAR','column3':'VARCHAR'})
  WHERE column0 IS NOT NULL AND trim(column1) <> '' AND trim(column2) <> '';

-- one winner per email: latest observation wins (defensive even when the caller
-- already dedupes; UPDATE..FROM with dup keys would be non-deterministic)
CREATE TEMP TABLE src AS
  SELECT email, phone, source, coalesce(event_ts, now()) AS event_ts
  FROM (SELECT *, row_number() OVER (PARTITION BY email ORDER BY event_ts DESC NULLS LAST) AS rn
        FROM src_raw)
  WHERE rn = 1;

SELECT 'staged=' || count(*) FROM src;
SELECT 'matched=' || count(*)
  FROM mirror.leads_current l JOIN src s ON l.email = s.email;
SELECT 'will_update=' || count(*)
  FROM mirror.leads_current l JOIN src s ON l.email = s.email
  WHERE (l.enriched_phone_at IS NULL OR s.event_ts > l.enriched_phone_at)
    AND (l.enriched_phone IS DISTINCT FROM s.phone
         OR l.enriched_phone_source IS DISTINCT FROM s.source);

UPDATE mirror.leads_current AS l
SET enriched_phone        = s.phone,
    enriched_phone_source = s.source,
    enriched_phone_at     = s.event_ts,
    updated_at            = now()
FROM src s
WHERE l.email = s.email
  AND (l.enriched_phone_at IS NULL OR s.event_ts > l.enriched_phone_at)   -- never clobber fresher
  AND (l.enriched_phone IS DISTINCT FROM s.phone
       OR l.enriched_phone_source IS DISTINCT FROM s.source);             -- unchanged rows cost nothing

COMMIT;
SQL

echo "[$(ts)] === post-commit verify ==="
ddw -readonly "$DB" <<SQL
CREATE TEMP TABLE src_raw AS
  SELECT lower(trim(column0)) AS email,
         trim(column1)        AS phone,
         trim(column2)        AS source,
         TRY_CAST(column3 AS TIMESTAMPTZ) AS event_ts
  FROM read_csv('$TSV', delim='\t', header=false, quote='',
                columns={'column0':'VARCHAR','column1':'VARCHAR',
                         'column2':'VARCHAR','column3':'VARCHAR'})
  WHERE column0 IS NOT NULL AND trim(column1) <> '' AND trim(column2) <> '';
CREATE TEMP TABLE src AS
  SELECT email, phone, source, coalesce(event_ts, now()) AS event_ts
  FROM (SELECT *, row_number() OVER (PARTITION BY email ORDER BY event_ts DESC NULLS LAST) AS rn
        FROM src_raw)
  WHERE rn = 1;
SELECT 'committed=' || count(*)
  FROM mirror.leads_current l JOIN src s ON l.email = s.email
  WHERE l.enriched_phone = s.phone AND l.enriched_phone_source = s.source;
SQL

flock -u 9
echo "[$(ts)] lock released -- apply complete"
