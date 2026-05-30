-- Workspace entity (raw + canonical). Version 2.
-- Source: Instantly per-workspace API keys, GET /workspaces/current.

-- Raw snapshot, append-only. One row per (workspace, run).
CREATE TABLE IF NOT EXISTS raw_instantly_workspace (
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR     NOT NULL,
  workspace_id      VARCHAR     NOT NULL,     -- Instantly's internal UUID (`id` in the API response)
  slug              VARCHAR     NOT NULL,     -- derived from the env-key (e.g. renaissance-4); Instantly does not return one
  name              VARCHAR,                  -- Instantly display name
  plan              VARCHAR,                  -- plan_id if exposed
  trial_active      BOOLEAN,                  -- not currently exposed by the API; nullable
  organization_id   VARCHAR,                  -- owner UUID
  api_response_raw  VARCHAR,                  -- full JSON for audit/recovery
  PRIMARY KEY (workspace_id, _loaded_at)
);

-- Canonical workspace. One row per workspace_id.
CREATE TABLE IF NOT EXISTS core.workspace (
  workspace_id   VARCHAR PRIMARY KEY,
  slug           VARCHAR NOT NULL,
  name           VARCHAR,
  plan           VARCHAR,
  is_active      BOOLEAN NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  resolved_at    TIMESTAMPTZ NOT NULL
);
