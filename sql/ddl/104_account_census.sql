-- 104_account_census.sql  [2026-06-20 portal-data-rebuild / WS2]
-- @gate: add
-- DDL version 104 (verified live MAX(core.schema_version)=103 on warehouse_20260621_011836_436;
--   WS1 landed at 103 (103_workspace_name_normalization), so WS2 = MAX+1 = 104 per CONTRACT C1.
--   re-check SELECT max(version) FROM core.schema_version immediately before apply).
-- Depends on 00 (core schema). SOFT-depends on WS1's core.workspace for the authoritative name, but is
--   self-sufficient via the baked seed below (does NOT hard-require WS1 to have added warm-leads).
--
-- core.account_census: canonical, IMMUTABLE, per-DATE census of LIVE Instantly accounts.
-- One row per (census_date, workspace_uuid, lower(email)). Sourced from the hourly live /accounts poll
-- (/root/core/live_accounts/accounts_live_<ts>.parquet) — the only trustworthy CURRENT inventory.
-- REPLACES the account_truth-derived inflated inventory (core.sending_account = 1,359,514 distinct
-- emails = 4.33x the 314,069 live truth as of 2026-06-20).
--
-- Identity: Instantly exposes NO account id (the /accounts `id` field is null); the stable per-account
-- key is lower(email). workspace_uuid = the account payload's `organization` field (verified ==
-- core.workspace.workspace_id, and == CONTRACT C4 warm-leads 58ae9dc4). CURRENT workspace name resolves
-- COALESCE(core.workspace.name [authoritative], seed [CONTRACT C4 literals], slug [last resort]) — never
-- frozen per-snapshot. Fully additive, reversible (DROP the table + seed + 2 views).

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.account_census (
    census_date             DATE      NOT NULL,            -- CAST(snapshot_at AS DATE), partition key
    workspace_uuid          VARCHAR   NOT NULL,            -- = account payload `organization` (== core.workspace.workspace_id)
    email                   VARCHAR   NOT NULL,            -- account identity, stored lower(email) (no Instantly id)
    domain                  VARCHAR,                       -- split_part(lower(email),'@',2)
    workspace_slug          VARCHAR,                       -- the env-key slug the poll used (provenance; NOT the name source)
    provider_code           INTEGER,                       -- 1=custom SMTP/OTD, 2=Google, 3=Microsoft/Outlook
    daily_limit             DOUBLE,                        -- cold-send/day cap
    warmup_status           INTEGER,                       -- 1=active, 0=paused, -1=banned
    warmup_status_label     VARCHAR,
    warmup_limit            INTEGER,                       -- warmup.limit (warmup network sends/day)
    stat_warmup_score       INTEGER,                       -- 0..100
    status                  INTEGER,                       -- 1=active, 2=paused, -1=connection_error, -3=sending_error
    status_label            VARCHAR,
    setup_pending           BOOLEAN,
    timestamp_created       TIMESTAMP WITH TIME ZONE,
    timestamp_warmup_start  TIMESTAMP WITH TIME ZONE,      -- NULL => never warmed
    timestamp_updated       TIMESTAMP WITH TIME ZONE,
    snapshot_at             TIMESTAMP WITH TIME ZONE,      -- exact UTC instant of the promoted poll
    source                  VARCHAR DEFAULT 'instantly_api',
    _snapshot_file          VARCHAR,                       -- provenance: the parquet promoted for this date
    _loaded_at              TIMESTAMP WITH TIME ZONE,
    _run_id                 VARCHAR,
    PRIMARY KEY (census_date, workspace_uuid, email)       -- email lower-cased on ingest; PK == dedup key
);

-- CONTRACT-C4 UUID -> CURRENT-name seed. Baked here so census name resolution does NOT depend on WS1 having
-- already added a UUID to core.workspace. core.workspace wins the COALESCE when present; this seed is the
-- fallback; the slug is the last resort. All 8 live workspace UUIDs seeded (names per workspace-rename-history
-- + CONTRACT C3/C4).
CREATE TABLE IF NOT EXISTS core.account_census_workspace_seed (
    workspace_uuid  VARCHAR PRIMARY KEY,
    current_name    VARCHAR NOT NULL
);
INSERT INTO core.account_census_workspace_seed (workspace_uuid, current_name) VALUES
    ('cdae94c6-5a88-4614-92e2-09e28a073a2e', 'Funding 1 (Samuel)'),
    ('88de6a7c-55db-4594-8851-ed7d56342a45', 'Funding 2 (Ido)'),
    ('d5ebf2bd-d7c8-4feb-8310-e57e6140e12a', 'Funding 3 (Leo)'),
    ('6ab744f5-be81-4c5b-8333-c0c119a19b80', 'Funding 4 (Sam)'),
    ('f02d3d50-0e9f-4687-981d-6134e789baa4', 'Funding 5 (Eyver)'),
    ('9e822ccc-549d-4d91-ac13-a9c313af8fd3', 'Max''s workspace'),          -- CONTRACT C3 (NOT "Pre-IPO")
    ('587765d7-e9ed-4057-85d1-eca48bcc9384', 'Renaissance 1 (Instantly)'),
    ('58ae9dc4-9bc0-46d6-beb2-a1dc3e99cbf5', 'Warm leads')                 -- CONTRACT C4 (absent from core.workspace)
ON CONFLICT (workspace_uuid) DO UPDATE SET current_name = excluded.current_name;

-- Current-name resolution: census x core.workspace (authoritative) x seed (C4 fallback) x slug (last resort).
-- Read THIS view for any name/display; read the base table for raw census facts.
CREATE OR REPLACE VIEW core.v_account_census_current AS
SELECT
    c.census_date,
    c.workspace_uuid,
    COALESCE(w.name, s.current_name, c.workspace_slug)    AS workspace_current_name,
    COALESCE(w.is_active, TRUE)                           AS workspace_is_active,  -- seeded ws are live; default true
    (w.workspace_id IS NULL AND s.workspace_uuid IS NULL) AS name_unresolved,      -- TRUE only if degraded to slug
    c.email, c.domain, c.workspace_slug, c.provider_code, c.daily_limit,
    c.warmup_status, c.warmup_status_label, c.warmup_limit, c.stat_warmup_score,
    c.status, c.status_label, c.setup_pending,
    c.timestamp_created, c.timestamp_warmup_start, c.timestamp_updated,
    c.snapshot_at, c.source
FROM core.account_census c
LEFT JOIN core.workspace w ON w.workspace_id = c.workspace_uuid
LEFT JOIN core.account_census_workspace_seed s ON s.workspace_uuid = c.workspace_uuid;

-- Latest-date census (the "live inventory" the portal reads — replaces dead-account-poisoned
-- core.sending_account for the CURRENT roster).
CREATE OR REPLACE VIEW core.v_account_census_latest AS
SELECT *
FROM core.v_account_census_current
WHERE census_date = (SELECT max(census_date) FROM core.account_census);
