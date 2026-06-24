-- @gate: add
-- Depends on 1006
-- ============================================================================
-- 1007_rg_tag_populate.sql — fill core.sending_account_batch.rg_tag_1 / rg_tag_2
-- ----------------------------------------------------------------------------
-- The on-box account_batch.parquet does NOT carry the RG-tag columns (confirmed
-- by probe 1006 — they were dropped at parquet build). The values live only in
-- the final-data CSV. They are transported here as a HASHED parquet (sha256 of
-- the lower/trimmed email + the two RG tags — NO plaintext emails), hosted as a
-- public GitHub release asset, and joined back on the email hash. This fills the
-- currently-empty rg_tag_1/rg_tag_2 columns (additive — nothing overwritten;
-- both columns are 100% NULL today). Idempotent: re-running sets the same values.
--
-- (The throwaway probe table from 1006 is cleaned up separately to keep this
--  change purely additive / non-destructive so it auto-merges.)
-- ============================================================================
INSTALL httpfs;
LOAD httpfs;

UPDATE core.sending_account_batch AS b
SET rg_tag_1 = p.rg_tag_1,
    rg_tag_2 = p.rg_tag_2
FROM read_parquet('https://github.com/sdultsin/renaissance-warehouse/releases/download/rg-tags-load-1007/rg_tags_hashed.parquet') AS p
WHERE sha256(lower(trim(b.account_email))) = p.email_sha256;
