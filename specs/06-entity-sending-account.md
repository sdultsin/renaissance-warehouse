# 06 — Entity: sending_account

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

`core.sending_account` — one row per Instantly inbox ever ingested. Source-of-truth for "what inboxes do we have, what state are they in, who owns them."

## Inputs

**Primary:** the existing `account_truth_*.duckdb` snapshots on the droplet (`/root/archive/mac-offload/account_truth_2026-05-27.duckdb` + future snapshots from the cron that produces them). This is what the Sending Truth Vercel app reads. Don't re-derive from scratch — absorb what's already there.

**Supplement:** Instantly REST `GET /accounts` per workspace key for fields the account_truth snapshot doesn't have (current daily limit, warmup state changes since snapshot).

## Outputs

### `raw_account_truth_accounts`
Snapshot-style ingest of every row from the account_truth duckdb's accounts table, copy-through. Same `_loaded_at` / `_run_id` convention. Daily.

### `raw_instantly_account_supplement`
Append-only rows from Instantly REST `GET /accounts` per workspace. Captures fields that drift between account_truth snapshots (typically warmup phase, paused state, daily_limit).

### `core.sending_account` (canonical)
```sql
CREATE TABLE core.sending_account (
  account_id          VARCHAR PRIMARY KEY,    -- Instantly account UUID
  email               VARCHAR NOT NULL,
  domain              VARCHAR NOT NULL,       -- FK to core.domain
  workspace_id        VARCHAR NOT NULL,       -- FK to core.workspace

  -- Classification (inherited from core.domain when possible — see spec 07)
  esp                 VARCHAR,                -- 'google' | 'outlook' | 'otd' | 'other'
  infra_provider      VARCHAR,                -- 'OTD' | 'MailIn' | 'Reseller' | 'Folderly' | 'Maxify' | 'Tucows' | 'Warmly'

  -- Lifecycle state machine (per Sam 2026-05-30: 'lay the lattice work now')
  lifecycle_state     VARCHAR NOT NULL,       -- 'created' | 'warming' | 'warmed' | 'ramping' | 'active' | 'paused' | 'retired'
  rotation_state      VARCHAR,                -- 'on' | 'off' | NULL (only meaningful when lifecycle_state='active')

  -- Lifecycle transition timestamps (nullable; recorded on first observation of transition)
  created_at          TIMESTAMPTZ,            -- when account first appeared in Instantly
  warmup_started_at   TIMESTAMPTZ,            -- warmup toggle turned ON
  warmup_completed_at TIMESTAMPTZ,            -- warmup toggle turned OFF (moved to ramp or active)
  rampup_started_at   TIMESTAMPTZ,            -- first cold-send activity below target daily_limit
  rampup_completed_at TIMESTAMPTZ,            -- daily_limit reached target (full-blast send)
  paused_at           TIMESTAMPTZ,            -- explicitly paused (rotation off, blocklist hit, etc.)
  retired_at          TIMESTAMPTZ,            -- deleted or marked permanently retired

  -- Operational state from Instantly API (refreshed nightly)
  status              VARCHAR,                -- 'active' | 'paused' | 'connection_error' | etc.
  warmup_phase        VARCHAR,                -- raw value from Instantly warmup field
  daily_limit         INTEGER,                -- configured target daily limit
  daily_limit_used    INTEGER,                -- today's send count

  -- Cost projection columns (per spec 13). NULL until v3 derivation populates.
  cost_per_day_usd_estimated  DOUBLE,         -- monthly vendor cost ÷ 30 ÷ inboxes-in-workspace
  vendor_billing_cycle        VARCHAR,        -- 'monthly' | 'annual' | 'pay_as_you_go'

  is_active           BOOLEAN NOT NULL,       -- seen in most-recent run
  first_seen_at, last_seen_at, resolved_at  TIMESTAMPTZ
);
```

### `core.sending_account_state_event` (lifecycle transition log)

Append-only event log so we can reconstruct history + answer "how long was X in warmup" questions. The inline state on `sending_account` is the latest snapshot; this table is the full timeline.

```sql
CREATE TABLE core.sending_account_state_event (
  account_id      VARCHAR NOT NULL,
  event_type      VARCHAR NOT NULL,       -- 'created' | 'warmup_started' | 'warmup_completed' | 'rampup_started' | 'rampup_completed' | 'paused' | 'resumed' | 'rotation_on' | 'rotation_off' | 'retired'
  event_at        TIMESTAMPTZ NOT NULL,
  previous_state  VARCHAR,
  new_state       VARCHAR,
  notes           VARCHAR,
  _detected_at    TIMESTAMPTZ NOT NULL,   -- when warehouse noticed (may lag actual event)
  PRIMARY KEY (account_id, event_type, event_at)
);
```

## Lifecycle state derivation rules

The transitions aren't always cleanly visible from Instantly's API alone. Hybrid resolution:

| Transition | Detection signal |
|---|---|
| `created` | Account first appears in `raw_instantly_workspace` accounts list |
| `warming` | `warmup_phase` field in Instantly API == warming AND `daily_limit_used` < `daily_limit / 4` |
| `warmed` | `warmup_phase` flips OFF after period of being ON |
| `ramping` | First non-warmup outbound send detected via `raw_pipeline_campaign_daily_metrics` for an account-attributed campaign, while `daily_limit_used` < `daily_limit * 0.9` |
| `active` | `daily_limit_used / daily_limit >= 0.9` for ≥3 consecutive days |
| `paused` | `status` field transitions to `paused` or `connection_error` for >24h |
| `retired` | Account no longer returned by Instantly API for ≥7 days |

Edge cases (worth flagging to Sam when each is hit, not auto-resolved):
- Account goes `active` → `paused` → `active` repeatedly: rotation cycling. Use `rotation_state` on/off, don't churn `lifecycle_state`.
- Account marked `warmup_phase=off` but never moves to ramping (e.g. paused indefinitely): leave at `warmed` until evidence of next phase.

## Resolution rules

- **`account_id`, `email`, `domain`, `workspace_id`:** from Instantly verbatim (account_truth snapshot is downstream of Instantly).
- **`infra`:** OTD = Instantly reports "Other" (per Sam's call — we use no custom SMTP except OTD).
- **`status` + `warmup_phase`:** prefer Instantly supplement (fresher) over account_truth snapshot (24h stale).
- **`daily_limit_used`:** only available from Instantly supplement — supplement-only field.

## Definition of done

1. DDL applied at version 6
2. End-to-end ingest populates `core.sending_account` with all inboxes across active workspaces (expect ~5,000-10,000 rows)
3. Spot-check a known inbox (e.g. one in `core.campaign_sending_tag.tag_name='RG4843'` → the inboxes in that tag group are reachable here)
4. `is_active = FALSE` rows accumulate cleanly for retired inboxes
5. SCHEMA.md updated with `core.sending_account` section

## Things to NOT do

- Don't iterate Instantly `list_accounts` in parallel across workspaces — per `feedback_instantly_list_accounts_serial_only.md`, hangs 12+ hours
- Don't build a custom account_truth replacement — absorb the existing snapshot
- Don't try to derive inbox→tag mapping here (sending tags already resolved at campaign-level via `core.campaign_sending_tag`)

## Open questions

- What's the cadence on account_truth snapshot generation? Confirm before deciding sync window for this entity
- The Sending Truth Vercel app — does it stay pointing at the standalone account_truth duckdb, or do we point it at core.sending_account after this lands? Sam's call.
