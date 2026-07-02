-- @gate: add
-- Depends on 1005
-- ============================================================================
-- 1006_rg_src_probe.sql — TEMP diagnostic: does the on-box batch parquet still
--   carry the RG-tag columns, and under what names?
-- ----------------------------------------------------------------------------
-- scripts/build_infra_batch.sql loads core.sending_account_batch from
--   /root/core/build/infra-batch/account_batch.parquet  with an explicit column
-- list that dropped the two RG-tag columns ("RG# Tag (Tag1)" / "RG#-# (Tag2)").
-- This materialises a 3-row sample so we can read the parquet's full column list
-- via the query API and then write the real populate (1007). Throwaway; dropped
-- by 1007. Additive, touches no existing surface. If the parquet path is gone or
-- lacks the columns, this still tells us (apply error / column list).
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS core;
CREATE OR REPLACE TABLE core._rg_src_probe AS
SELECT * FROM read_parquet('/root/core/build/infra-batch/account_batch.parquet') LIMIT 3;
