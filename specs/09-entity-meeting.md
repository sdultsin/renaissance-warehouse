# 09 — Entity: meeting (reconciled across Slack / Calendly / Close)

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

`core.meeting` — one row per booked meeting, with deduplication + attribution across sources.

Per Sam's call: **Slack is the source of truth** (the success-channel posts). Calendly direct is bookmarked but deferred (we don't have access to funding partners' Calendly APIs). Close is bookmarked but not ingested in v1.

## Inputs

**Primary:** `raw_pipeline_meetings_booked_raw` (already mirrored, 29,903 rows, sourced from Slack via data-pipeline-v2 cron).

**Future:** Calendly API direct (per spec 09 of original handoff), Close opportunities (spec 10).

## Outputs

### `core.meeting`
```sql
CREATE TABLE core.meeting (
  meeting_id            VARCHAR PRIMARY KEY,    -- derived: f"{channel_id}:{message_ts}:{line_index}"
  source                VARCHAR NOT NULL,       -- 'slack' (v1) | 'calendly' | 'close' (v1.5+)
  source_event_id       VARCHAR,                -- original ID from source
  posted_at             TIMESTAMPTZ NOT NULL,
  partner               VARCHAR,                -- which funding partner (Slack channel-derived)
  campaign_id           VARCHAR,                -- nullable — match_method/match_confidence in raw
  campaign_name_raw     VARCHAR,                -- the literal name as it appeared
  cm                    VARCHAR,                -- derived from campaign or directly from post
  match_method          VARCHAR,                -- exact | alias | manual | unmatched
  match_confidence      DOUBLE,
  is_duplicate_of       VARCHAR,                -- if this is a deduplicated copy, points to canonical row
  -- Cost projection column (per spec 13). NULL until v3 derivation logic populates.
  cost_per_meeting_usd_estimated   DOUBLE,      -- derived: campaign cost / meetings in attribution window
  raw_text              VARCHAR
);
```

## Resolution rules

- **`source = 'slack'`** in v1; Calendly + Close added in v1.5 with deduplication logic.
- **Deduplication for v1:** simple PK on `(channel_id, message_ts, line_index)` derived. No cross-source dedup needed yet.
- **Future cross-source dedup (v1.5):** for each Slack meeting, look for a matching Calendly event (within 24h of posted_at, same prospect email if available) and a matching Close opportunity (same lead, status=meeting_booked). Mark Calendly/Close as `is_duplicate_of` the Slack canonical row; preserve everything for audit.

## Definition of done

1. DDL at version 9
2. `core.meeting` populated from `raw_pipeline_meetings_booked_raw` — ~29,000 rows v1
3. Per-CM monthly meeting query passes:
   ```sql
   SELECT cm, COUNT(*) FROM core.meeting WHERE posted_at >= '2026-05-01' GROUP BY cm;
   ```
4. SCHEMA.md updated

## Things to NOT do

- Don't try to ingest Calendly direct in v1 — Sam said Slack is canonical
- Don't ingest Close opps in v1 — defer
- Don't build cross-source dedup yet — v1.5 work

## Open questions

- Sam's call when Phase 3 starts: should `cm` derive from campaign join, or directly from the Slack post (some posts have a CM mentioned even when campaign match is fuzzy)? Default: campaign join first, post-parse as fallback.
