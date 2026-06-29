-- One-time cleanup: collapse raw_sendivo_inbound to its latest run. Version 1041.
-- @gate: data-backfill
-- Depends on 34
--
-- WHY: entities/sendivo_inbound.py re-pulls the COMPLETE inbound history every run but only
-- deleted the CURRENT run_id before inserting (empty at insert time), so each of the 24 runs
-- to date appended a full copy -> 8,905,071 rows for a true ~661,811 inbound set (measured
-- 2026-06-29). Any un-windowed count/group-by was inflated ~13x (e.g. is_opt_out read as
-- "83% of inbound"). The entity is fixed in the same change to delete older runs after each
-- insert; this statement does the one-time backfill cleanup of the rows already accumulated.
--
-- LOSSLESS (verified 2026-06-29): every run is a strict superset of all earlier runs because
-- the pull has no date filter -- 0 message_ids present in older runs are absent from the latest
-- (`in_older_not_latest = 0`). Keeping only max(_run_id) drops 0 distinct inbound messages.
-- IDEMPOTENT: after this runs only the max run remains, so a re-run deletes nothing.
DELETE FROM raw_sendivo_inbound
WHERE _run_id <> (SELECT max(_run_id) FROM raw_sendivo_inbound);
