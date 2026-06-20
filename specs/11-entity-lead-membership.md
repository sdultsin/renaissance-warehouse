# 11 — Entity: lead-membership change-event log

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

Track per-day NEW and REMOVED leads per campaign without snapshotting full membership daily. This is the change-event log Sam approved in the Phase 2 planning discussion.

Two analytical surfaces depend on this:
1. **Recipient ESP distribution per campaign** (join lead_email → core.recipient_domain → aggregate)
2. **Audience overlap detection** (same lead hit across multiple campaigns over time)

## Inputs

Daily Instantly API call per active campaign:
- `GET /api/v2/leads/list?campaign_id=X` (paginated)
- Pull only the `lead_email` field (plus `lead_id` if present)
- For each campaign, get the set `{lead_emails today}`
- Diff against the set `{lead_emails yesterday}` (queried from this table's prior-run snapshot)
- Emit NEW rows for additions, REMOVED rows for deletions

## Outputs

### `raw_instantly_campaign_membership` (per-run snapshot of just IDs)
```sql
CREATE TABLE raw_instantly_campaign_membership (
  _loaded_at         TIMESTAMPTZ NOT NULL,
  _run_id            VARCHAR NOT NULL,
  workspace_id       VARCHAR NOT NULL,
  campaign_id        VARCHAR NOT NULL,
  lead_email         VARCHAR NOT NULL,
  PRIMARY KEY (campaign_id, lead_email, _run_id)
);
```

Used internally for the next-day diff. Pruned after N days (default 14) to keep storage bounded — the change-event log below preserves the history.

### `core.campaign_membership_event` (canonical, append-only)
```sql
CREATE TABLE core.campaign_membership_event (
  workspace_id     VARCHAR NOT NULL,
  campaign_id      VARCHAR NOT NULL,
  lead_email       VARCHAR NOT NULL,
  event_type       VARCHAR NOT NULL,        -- 'added' | 'removed'
  event_at         TIMESTAMPTZ NOT NULL,    -- when we detected the change (== _loaded_at of detecting run)
  PRIMARY KEY (campaign_id, lead_email, event_at)
);
```

### Derived: `v_campaign_current_membership` (view, point-in-time reconstruction)
```sql
CREATE VIEW v_campaign_current_membership AS
SELECT campaign_id, lead_email,
       MAX(event_at) AS as_of_event
FROM core.campaign_membership_event
GROUP BY campaign_id, lead_email
HAVING last(event_type ORDER BY event_at) = 'added';
```

## Resolution rules

- **First-run for a campaign:** treat all leads as 'added' events at the current `_loaded_at`. No diff possible since no prior snapshot.
- **Subsequent runs:** diff today's set vs yesterday's. Emit added/removed events. Idempotent: re-running the same day produces zero new events (same set).
- **Backfill:** if a campaign exists in Instantly but has no prior snapshot, on first ingest emit a single 'added' event per lead at `_loaded_at` — we won't recover when those leads were originally added.

## Size estimate

- ~22-100 active campaigns × avg 100k leads = ~2-10M total membership
- Daily churn: probably 5-15% (new uploads + completed deletions). 100k-1.5M change events/day.
- DuckDB compressed: ~50-300 MB/day. ~20-100 GB/year. Tractable.

## Definition of done

1. DDL at version 11
2. First-run populates `core.campaign_membership_event` with 'added' events for every active campaign
3. Second-run (next day) shows realistic delta: small number of added/removed events, no full re-add
4. `v_campaign_current_membership` returns the current membership for any `(campaign_id)` at any point in time
5. Join to `core.recipient_domain` produces a per-campaign ESP distribution

## Things to NOT do

- Don't snapshot full lead records — just lead_email + campaign_id
- Don't try to parallelize across workspaces — per the standing rule, Instantly API hangs on parallel reads
- Don't backfill from `conversation_messages` — that's only 1.2% of sends (verified)
- Don't depend on Instantly webhook for membership changes — webhooks fire on events (sends, replies, opps), not on lead-list membership changes

## Open questions

- Should we sync `removed` events even when the campaign itself was deleted? Default: yes — the removal is still a real event; campaign deletion is captured separately via `core.campaign.is_active`.
- Pagination size: Instantly defaults to 100; for 100k-lead campaigns that's 1000 paginated calls. Need rate limit handling. Per `feedback_instantly_list_accounts_serial_only`, serial only.
