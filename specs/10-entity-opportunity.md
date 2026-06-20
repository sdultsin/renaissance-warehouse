# 10 — Entity: opportunity (reconciled across Instantly / Sendivo / Close)

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

`core.opportunity` — one row per qualified-interest event, dedup'd across the three systems that emit them:
- Instantly (`lead_interested` event, `interest_status=4`)
- Sendivo (state transitions in `comms.conversation` → `comms.call_opportunity`)
- Close CRM (Lead opens in pipeline)

## Inputs

**v1:**
- `raw_pipeline_lead_events` (Instantly webhook deliveries, currently NOT mirrored — see spec 04 deferred tables; bring back when COPY-rewrite ships)
- Optional: pull Instantly opportunities directly via `GET /campaigns/{id}/analytics` (opps count) for cross-check

**v1.5:**
- comms-orchestration `comms.call_opportunity` (20,599 rows)
- Close API for opportunity stage + custom fields

## Outputs

### `core.opportunity`
```sql
CREATE TABLE core.opportunity (
  opportunity_id        VARCHAR PRIMARY KEY,     -- derived: f"{source}:{source_event_id}"
  source                VARCHAR NOT NULL,        -- 'instantly' | 'sendivo' | 'close'
  source_event_id       VARCHAR NOT NULL,
  lead_email            VARCHAR,
  campaign_id           VARCHAR,                 -- FK to core.campaign where attributable
  workspace_id          VARCHAR,
  opened_at             TIMESTAMPTZ NOT NULL,
  state                 VARCHAR,                 -- 'interested' | 'qualified' | 'meeting_booked' | etc.
  state_updated_at      TIMESTAMPTZ,
  is_duplicate_of       VARCHAR,                 -- cross-source dedup
  raw                   VARCHAR                  -- JSON of original event
);
```

## Resolution rules

- **PK:** `(source, source_event_id)` composite via concat. Avoids cross-source collisions.
- **Cross-source dedup (v1.5):** match by `(lead_email, opened_at within 30 min)` across Instantly + Sendivo + Close. First-seen wins as canonical; others get `is_duplicate_of`.
- **`state`:** normalize across sources. Instantly emits `interest_status` integer codes; Sendivo emits string states; Close uses pipeline-stage names. Build a mapping table.

## Definition of done

1. DDL at version 10
2. v1: `core.opportunity` populated from Instantly lead_events (requires pipeline_mirror COPY rewrite to land first)
3. Per-campaign opp count matches campaign_daily_metrics within tolerance
4. v1.5: Sendivo + Close ingested + cross-source dedup

## Things to NOT do

- Don't try to attribute Sendivo opps to Instantly campaigns directly — they're a different channel (SMS, not email); cross-source dedup is about deduplicating the same prospect across channels, not attributing to one
- Don't add Close ingest until comms_mirror lands

## Open questions

- Sam's call: do warm-call opps (the 20k in `comms.call_opportunity`) get rolled into `core.opportunity` (they're qualified-interest events) or kept separate as a different entity? Default: roll in with source='sendivo' for one unified opp surface.
