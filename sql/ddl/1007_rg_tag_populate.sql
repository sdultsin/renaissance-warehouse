-- @gate: add
-- Depends on 1006
-- ============================================================================
-- 1007_rg_tag_populate.sql — ADD-ONLY backfill of the empty RG-tag columns.
-- ----------------------------------------------------------------------------
-- PURELY ADDITIVE: core.sending_account_batch.rg_tag_1 / rg_tag_2 are 100% NULL
-- today; this fills them and NEVER overwrites or deletes anything (the UPDATE is
-- guarded `WHERE rg_tag_1 IS NULL`). One-time backfill; version-guarded so it
-- never re-runs.
--
-- WHY external read: the values are NOT on the box (probe 1006 confirmed the
-- account_batch.parquet was built without them) — they exist only in the
-- final-data CSV. They are transported as a HASHED parquet (sha256 of the
-- lower/trimmed email + the two RG tags — NO plaintext emails) and joined back
-- on the email hash.
--
-- TAMPER/INTEGRITY GUARD: the fetched asset is verified before any write — exact
-- row count AND a pinned known (hash -> RG3326) sample — and the migration ABORTS
-- (division-by-zero) if either fails, so a swapped/truncated asset can never
-- write into production. A maliciously-crafted asset is moot anyway: rows only
-- update where the hash matches a real account_email already in the table.
-- ============================================================================
INSTALL httpfs;
LOAD httpfs;

CREATE TEMP TABLE _rg_src AS
SELECT email_sha256, rg_tag_1, rg_tag_2
FROM read_parquet('https://github.com/sdultsin/renaissance-warehouse/releases/download/rg-tags-load-1007/rg_tags_hashed.parquet');

-- Integrity gate: abort (1/0) unless the asset is exactly the expected content.
SELECT 1 / (
  CASE WHEN (SELECT COUNT(*) FROM _rg_src) = 2541824
        AND (SELECT rg_tag_1 FROM _rg_src
             WHERE email_sha256 = '77452c6a4458a6317e7aa9c24accb8b7be44604212c1eb18ef87116d888f3cc7') = 'RG3326'
       THEN 1 ELSE 0 END
) AS _integrity_ok;

-- Add-only fill (never overwrites: only rows where rg_tag_1 is still empty).
UPDATE core.sending_account_batch AS b
SET rg_tag_1 = p.rg_tag_1,
    rg_tag_2 = p.rg_tag_2
FROM _rg_src AS p
WHERE sha256(lower(trim(b.account_email))) = p.email_sha256
  AND b.rg_tag_1 IS NULL;
-- (_rg_src is a TEMP table — it auto-drops when the apply connection closes.)
