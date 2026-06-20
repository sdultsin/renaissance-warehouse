-- Campaign + tag entities (raw + canonical). Version 3.
-- Source: Instantly per-workspace API keys, GET /campaigns and GET /custom-tags.

-- =====================================================================
-- RAW
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_instantly_campaign (
  _loaded_at         TIMESTAMPTZ NOT NULL,
  _run_id            VARCHAR     NOT NULL,
  workspace_id       VARCHAR     NOT NULL,
  campaign_id        VARCHAR     NOT NULL,
  name               VARCHAR,
  status             INTEGER,                -- Instantly status code (1=active draft, etc.)
  status_label       VARCHAR,                -- our human label
  created_at         TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ,
  email_gap          INTEGER,
  random_wait_max    INTEGER,
  daily_limit        INTEGER,
  schedule_raw       VARCHAR,                -- JSON: campaign_schedule
  sequence_raw       VARCHAR,                -- JSON: sequences (steps, variants, spintax)
  api_response_raw   VARCHAR,
  PRIMARY KEY (campaign_id, _loaded_at)
);

CREATE TABLE IF NOT EXISTS raw_instantly_campaign_marker_tag (
  _loaded_at    TIMESTAMPTZ NOT NULL,
  _run_id       VARCHAR     NOT NULL,
  workspace_id  VARCHAR     NOT NULL,
  campaign_id   VARCHAR     NOT NULL,
  tag_id        VARCHAR     NOT NULL,
  tag_label     VARCHAR     NOT NULL,
  PRIMARY KEY (campaign_id, tag_id, _loaded_at)
);

CREATE TABLE IF NOT EXISTS raw_instantly_campaign_sending_tag (
  _loaded_at     TIMESTAMPTZ NOT NULL,
  _run_id        VARCHAR     NOT NULL,
  workspace_id   VARCHAR     NOT NULL,
  campaign_id    VARCHAR     NOT NULL,
  tag_id         VARCHAR     NOT NULL,
  tag_label      VARCHAR     NOT NULL,
  account_count  INTEGER,                    -- not currently exposed; nullable
  PRIMARY KEY (campaign_id, tag_id, _loaded_at)
);

-- =====================================================================
-- CANONICAL
-- =====================================================================

CREATE TABLE IF NOT EXISTS core.campaign (
  campaign_id      VARCHAR PRIMARY KEY,
  workspace_id     VARCHAR NOT NULL,
  name             VARCHAR,
  status           INTEGER,
  status_label     VARCHAR,
  -- regex-derived attributes
  cm               VARCHAR,                  -- SAM | SAMUEL | LEO | IDO | EYVER | TOUKIR | TOMER | LUCAS | MAX | NULL
  offer            VARCHAR,                  -- HELOC | Tariffs | s125 | R&D | Funding | NULL
  is_mca           BOOLEAN NOT NULL,
  email_gap        INTEGER,
  random_wait_max  INTEGER,
  daily_limit      INTEGER,
  created_at       TIMESTAMPTZ,
  is_active        BOOLEAN NOT NULL,
  first_seen_at    TIMESTAMPTZ NOT NULL,
  last_seen_at     TIMESTAMPTZ NOT NULL,
  resolved_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS core.campaign_marker_tag (
  workspace_id   VARCHAR NOT NULL,
  campaign_id    VARCHAR NOT NULL,
  tag_name       VARCHAR NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (campaign_id, tag_name)
);

CREATE TABLE IF NOT EXISTS core.campaign_sending_tag (
  workspace_id   VARCHAR NOT NULL,
  campaign_id    VARCHAR NOT NULL,
  tag_name       VARCHAR NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (campaign_id, tag_name)
);
