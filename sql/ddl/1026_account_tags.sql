-- 1026_account_tags.sql  [2026-06-26]
-- core.account_tags — ONE row per inbox, with a single column holding ALL of that
-- inbox's Instantly tags (verbatim, no curation). This is the per-inbox tag column the
-- Inbox Hub overview reads — NOT an edge table. ~one row per live inbox (~433k), not
-- millions. Populated nightly by entities/account_tags.py from /custom-tag-mappings.
--
-- `tags`     = all tag labels for the inbox, sorted-distinct, ' | '-joined (human read)
-- `tags_arr` = the same as an array, for list_contains('MilkBox …') style filtering
--
-- ADDITIVE new table. The entity full-replaces per workspace_uuid (delete+insert after
-- a clean pull) so a single failed workspace never wipes its rows.
--
-- @gate: add
-- Depends on 97
CREATE TABLE IF NOT EXISTS core.account_tags (
    email          VARCHAR NOT NULL,   -- lower(trim(email)) — one row per inbox
    workspace_slug VARCHAR,            -- warehouse-canonical slug (latest census)
    workspace_uuid VARCHAR NOT NULL,   -- Instantly org id (the full-replace key)
    tags           VARCHAR,            -- ALL tags for this inbox, ' | '-joined, sorted distinct
    tags_arr       VARCHAR[],          -- same, as an array
    n_tags         INTEGER,            -- count of distinct tags on this inbox
    _loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        VARCHAR,
    PRIMARY KEY (workspace_uuid, email)
);
CREATE INDEX IF NOT EXISTS ix_actags_email ON core.account_tags (email);
