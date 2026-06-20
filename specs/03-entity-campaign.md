# 03 — Entity: campaign + tags

**Phase:** 2 (parallel after foundation, runs after workspace)
**Status:** spec'd 2026-05-30
**Owner:** Track B agent

## Goal

Three canonical entities in one pass: `core.campaign`, `core.campaign_marker_tag`, `core.campaign_sending_tag`. Each backed by a raw snapshot table. All registered in the `instantly` phase, run after workspace.

This is the entity that touches everything downstream — every send, every reply, every meeting is attributed to a campaign. Get the resolution rules right and the rest compose cleanly.

## Inputs

**Primary source:** Instantly API per workspace key.

Endpoints:
- `GET /api/v2/campaigns` — list all campaigns in the workspace (paginated)
- `GET /api/v2/campaigns/{id}` — full campaign detail (we may need this for `email_gap`, `daily_limit`, etc. — check what the list endpoint returns first)
- Tags: Instantly exposes two distinct tag concepts.
  1. **Marker tags** — freeform custom tags attached to campaigns. Endpoint: `GET /api/v2/tags` lists all tags in the workspace; `tag.id → tag.label` mapping. Then `GET /api/v2/campaigns/{id}` returns `tag_ids` per campaign. Cross.
  2. **Sending account-set tags** — the tags used in campaign settings to attach a set of accounts to the campaign. These appear in the campaign config under sending_account_tags (exact field name TBD; check the API response). They are a distinct concept from marker tags.

Confirm exact endpoint and field names by probing one workspace before writing the full ingest.

## Outputs

### `raw_instantly_campaign` (append-only)

```sql
CREATE TABLE IF NOT EXISTS raw_instantly_campaign (
  _loaded_at         TIMESTAMPTZ NOT NULL,
  _run_id            VARCHAR NOT NULL,
  workspace_id       VARCHAR NOT NULL,
  campaign_id        VARCHAR NOT NULL,
  name               VARCHAR,
  status             INTEGER,           -- Instantly's campaign status code (1=draft, 2=active, etc. — verify)
  status_label       VARCHAR,           -- our human label
  created_at         TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ,
  email_gap          INTEGER,
  random_wait_max    INTEGER,
  daily_limit        INTEGER,
  schedule_raw       VARCHAR,           -- JSON
  sequence_raw       VARCHAR,           -- JSON
  api_response_raw   VARCHAR
);
```

PK: `(campaign_id, _loaded_at)`.

### `raw_instantly_campaign_marker_tag` (append-only)

```sql
CREATE TABLE IF NOT EXISTS raw_instantly_campaign_marker_tag (
  _loaded_at      TIMESTAMPTZ NOT NULL,
  _run_id         VARCHAR NOT NULL,
  workspace_id    VARCHAR NOT NULL,
  campaign_id     VARCHAR NOT NULL,
  tag_id          VARCHAR NOT NULL,
  tag_label       VARCHAR NOT NULL
);
```

PK: `(campaign_id, tag_id, _loaded_at)`.

### `raw_instantly_campaign_sending_tag` (append-only)

```sql
CREATE TABLE IF NOT EXISTS raw_instantly_campaign_sending_tag (
  _loaded_at         TIMESTAMPTZ NOT NULL,
  _run_id            VARCHAR NOT NULL,
  workspace_id       VARCHAR NOT NULL,
  campaign_id        VARCHAR NOT NULL,
  tag_id             VARCHAR NOT NULL,
  tag_label          VARCHAR NOT NULL,
  account_count      INTEGER            -- if exposed; helpful for diagnostics
);
```

PK: `(campaign_id, tag_id, _loaded_at)`.

### `core.campaign` (canonical)

```sql
CREATE TABLE IF NOT EXISTS core.campaign (
  campaign_id        VARCHAR PRIMARY KEY,
  workspace_id       VARCHAR NOT NULL,
  name               VARCHAR,
  status             INTEGER,
  status_label       VARCHAR,
  -- regex-derived attributes (resolution rules below)
  cm                 VARCHAR,           -- 'SAM' | 'SAMUEL' | 'LEO' | 'IDO' | 'EYVER' | 'TOUKIR' | NULL
  offer              VARCHAR,           -- 'Funding' | 's125' | 'R&D' | 'Tariffs' | 'HELOC' | NULL
  is_mca             BOOLEAN NOT NULL,  -- regex(?i)\b(isaac|mca|cheap leads)\b
  email_gap          INTEGER,
  random_wait_max    INTEGER,
  daily_limit        INTEGER,
  created_at         TIMESTAMPTZ,
  is_active          BOOLEAN NOT NULL,
  first_seen_at      TIMESTAMPTZ NOT NULL,
  last_seen_at       TIMESTAMPTZ NOT NULL,
  resolved_at        TIMESTAMPTZ NOT NULL
);
```

### `core.campaign_marker_tag`

```sql
CREATE TABLE IF NOT EXISTS core.campaign_marker_tag (
  workspace_id   VARCHAR NOT NULL,
  campaign_id    VARCHAR NOT NULL,
  tag_name       VARCHAR NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (campaign_id, tag_name)
);
```

`tag_name` is the label, not the ID. Sam asked for `(workspace_id, tag_name)` to be the natural lookup so duplicate tag names across workspaces don't collide.

### `core.campaign_sending_tag`

```sql
CREATE TABLE IF NOT EXISTS core.campaign_sending_tag (
  workspace_id   VARCHAR NOT NULL,
  campaign_id    VARCHAR NOT NULL,
  tag_name       VARCHAR NOT NULL,
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (campaign_id, tag_name)
);
```

## Resolution rules

**Source of truth:** Instantly for all fields. Sheets contribute nothing here.

**`is_mca`:** `regexp_matches(lower(name), '\b(isaac|mca|cheap leads)\b')`. Confirmed by Sam — Isaac, MCA, "cheap leads" covers the full set today.

**`cm`:** parse the campaign name. Per Sam: "if a campaign has someone's name (SAM), you can attribute that to SAM. But some workspaces are shared so no point. If there is no mapping available, leave blank for now."

Implementation: regex against name for known CMs in this priority order, returning the first match: `\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\b`. Case-insensitive, but uppercase the captured value before storing. Multiple matches in one name → null (ambiguous). No match → null.

**`offer`:** regex against name. Priority order matters (some patterns are substrings of others):
- `(?i)\bHELOC\b` → 'HELOC'
- `(?i)\bTariff(s)?\b` → 'Tariffs'
- `(?i)\b(s125|section\s*125|125)\b` → 's125'
- `(?i)\bR&?D\b|\bRD\b` → 'R&D'
- `(?i)\bFunding\b|\bMCA\b|\bIsaac\b` → 'Funding'  (MCA + Isaac are Funding-offer flavors)
- No match → null

**`is_active`:** True if this campaign appeared in the most recent run for its workspace, else False (do not delete from canonical, just flip the flag).

## Implementation

**`sources/instantly.py`** — extend with `list_campaigns(workspace_id) -> Iterator[dict]`, `get_campaign(campaign_id)`, `list_tags(workspace_id)`.

**`entities/campaign.py`** — register three phase functions or one composite that does all three. Composite is simpler — one Instantly call traversal per workspace, write all three raws, then resolve all three canonicals.

```python
def register(registry: Registry) -> None:
    registry.add_phase("instantly", "campaign", run_campaign_ingest)
```

Iterate workspaces serially. For each workspace: list campaigns, list tags, fetch per-campaign sending tags. Write raws. After all workspaces are done, run the resolution pass over the latest raw rows.

**`sql/ddl/03_campaign.sql`** — all three raw + all three canonical tables.

## Dependency

This entity reads `core.workspace` to know which workspace IDs to scope to. Workspace ingest must succeed first in the same run. If workspace failed in this run, the campaign ingest can still proceed using `.env.instantly` keys directly — but it should warn that workspace canonical is stale.

## Definition of done

1. `python scripts/setup_db.py` applies `03_campaign.sql` (version=3)
2. After running the orchestrator's `instantly` phase, `core.campaign` has rows for the ~22-100 active campaigns across all workspaces
3. `is_mca`, `cm`, `offer` populated where regex matches; NULL elsewhere — never silently wrong
4. Marker tags + sending tags populated per campaign
5. Re-running the orchestrator doesn't duplicate canonical rows
6. Smoke queries:
   - `SELECT count(*), is_active FROM core.campaign GROUP BY is_active` — sensible split
   - `SELECT cm, count(*) FROM core.campaign WHERE is_active GROUP BY cm` — most CMs hit, some NULL
   - `SELECT offer, count(*) FROM core.campaign WHERE is_active GROUP BY offer` — Funding dominates
   - `SELECT name, is_mca FROM core.campaign WHERE is_mca LIMIT 5` — only Isaac/MCA/cheap-leads names

## Things to NOT do

- Don't write a fancy spintax / variant parser. That's a separate entity later.
- Don't try to extract step delays into a richer schema — just store the raw `sequence_raw` JSON. Step-level entity comes later.
- Don't pull campaign analytics (sends/replies). That belongs in pipeline-supabase slim mirror (Track C).
- Don't parallelize across workspaces. Per `feedback_instantly_list_accounts_serial_only.md`, the Instantly API has fragile parallel behavior — serial per workspace.

## Open questions to surface

- If the `email_gap` / `daily_limit` fields aren't exposed in the list endpoint (only the detail endpoint), surface this. Cost is 1 extra API call per campaign.
- If there are tag types beyond "marker" and "sending account-set" (e.g., contact-list tags, lead-source tags), surface them — Sam wanted these two, but if Instantly has a third we should add it now.
- If a campaign appears with no workspace (orphan), surface — should be impossible but log defensively.
