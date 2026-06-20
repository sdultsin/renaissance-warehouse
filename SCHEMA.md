# SCHEMA — renaissance-warehouse

> **You are reading this to write SQL against this warehouse.** You're either Sam asking a question or an LLM agent acting for Sam. Use DuckDB syntax (1.5+). Tables live in three schemas: `raw_*` (untouched source snapshots), `core.*` (canonical entities, resolved), and the default `main` schema (derived views, when present). Resolution rules tell you which source wins when two systems disagree. **Instantly is the source of truth whenever possible.**

---

## Architecture in 30 seconds

Three layers, strict dependency direction (derived → core → raw, never reverse).

- **`raw_*`** — source mirrors. Every row has `_loaded_at` + `_run_id`. ⚠ The **`raw_pipeline_*`** tables use **sync modes (spec 15, since 2026-06-02): one row per natural `_key`** (insert-only events, content-hashed copy, upserted dimensions/daily), with **freeze-on-delete** (a campaign deleted from Instantly keeps its last-known rows). **Do NOT filter `WHERE _run_id = latest` on raw_pipeline_* — they're already deduped, and that filter drops frozen/older rows.** (Other raw families — `raw_comms_*`, `raw_account_truth_*`, etc. — are still append-per-run snapshots; filter those to the latest `_run_id`.)
- **`core.*`** — one row per real-world entity (workspace, campaign, etc.), with explicit per-field resolution rules below. `is_active` flips to FALSE when an entity is no longer seen in the most recent sync run; rows are never deleted from canonical.
- **derived views (`v_*` / `mv_*`)** — analytical surfaces (ESP×ESP matrices, portfolio rollups, factor visibility). Pure functions of `core.*`. v1 ships none — Phase 3 adds them.

Sync window: nightly 03:30-05:45 UTC, single observable run, sequenced phases. `core.sync_run` and `core.sync_run_phase` log every run.

---

## Conventions

| Concern | Convention |
|---|---|
| Table naming | `raw_<source>_<table>` (`raw_pipeline_campaigns`, `raw_instantly_campaign`). Canonical: `core.<entity>` (`core.workspace`, `core.campaign`). Derived: `v_<view>` or `mv_<materialized>`. |
| Time | All timestamps `TIMESTAMPTZ`. `_loaded_at` on every raw row. Most facts also have a source-side timestamp. |
| IDs | Source-system IDs verbatim (no surrogate keys). UUIDs from Instantly, slugs from pipeline. |
| Nulls | Missing values are NULL. Never `-1`, never `"unknown"`. Use `WHERE x IS NULL` to find them. |
| `is_active` | TRUE if entity appeared in the most recent run; FALSE if not. Canonical rows are never deleted. |
| Tag mappings | Two surfaces: `core.campaign_marker_tag` (badge on campaign — e.g. "AIM Active") and `core.campaign_sending_tag` (account-set tag in campaign config — e.g. "RG4843"). Both keyed by `(workspace_id, campaign_id, tag_name)`. Same workspace_id × tag_name can repeat across campaigns. |

---

## Workspace renaming context (load-bearing)

Sam renamed his Instantly workspaces in the UI at some point. The mapping:

| Old name (still appears in pipeline) | New name (Instantly API + UI) |
|---|---|
| Renaissance 4 | Funding 1 |
| Renaissance 5 | Funding 2 |
| Prospect Power | Funding 3 |
| Koi and Destroy | Funding 4 |
| Renaissance 2 | Funding 5 |
| The Gatekeepers | Funding 6 |
| Automated Application | Funding UK |
| Outlook 1 | Funding Canada |
| Equinox | RE Wholesale |

The workspace UUID is stable across renames. **`raw_pipeline_campaigns.workspace_id`** uses the OLD slugs (`koi-and-destroy`). **`raw_instantly_workspace.slug`** uses the OLD env-key-derived slug (also `koi-and-destroy` because we name env keys by historical slug). To join pipeline data to Instantly data, use `workspace_id` (old slug) consistently across both raws. The current display name is in `raw_instantly_workspace.name`. Some env keys are duplicate keys for the same workspace (e.g. `INSTANTLY_KEY_FUNDING_4` and `INSTANTLY_KEY_KOI_AND_DESTROY` both point at workspace UUID `6ab744f5-...`).

---

## Entities

### `core.workspace`

One row per Instantly workspace ever ingested.

| Column | Type | Description |
|---|---|---|
| `workspace_id` | VARCHAR (PK) | Instantly's internal UUID. Stable across renames. |
| `slug` | VARCHAR | Slug derived from env-key name (e.g. `koi-and-destroy`). Used for joins to `raw_pipeline_*`. |
| `name` | VARCHAR | Current display name from Instantly API (e.g. "Funding 4"). |
| `plan` | VARCHAR | Plan ID. |
| `is_active` | BOOLEAN | TRUE if workspace appeared in the most recent run, FALSE if it returned 401/402 (deleted) or wasn't probed. |
| `first_seen_at` | TIMESTAMPTZ | When we first ingested this workspace. |
| `last_seen_at` | TIMESTAMPTZ | Most recent successful ingest. |
| `resolved_at` | TIMESTAMPTZ | When canonical was last refreshed. |

**Source:** Instantly REST API `GET /workspaces/current`, called once per `INSTANTLY_KEY_*` env var.

**Resolution rules:**
- `workspace_id`, `name`, `plan`: from Instantly API verbatim.
- `slug`: from env-key name lowercased + underscores → hyphens. When two env keys point at the same `workspace_id` (e.g. FUNDING_4 + KOI_AND_DESTROY), we pick alphabetically first. Treat slug as a join key, not as the canonical identifier.
- `is_active = FALSE`: workspace previously seen but missing from latest run.
- **Workspace keys that error (401/402) on FIRST sighting are not added to canonical at all.** They never enter `core.workspace`. If a workspace was previously active and then keys go bad, it stays in `core.workspace` with `is_active = FALSE` and `last_seen_at` frozen at last successful sync.

**Common queries:**

```sql
-- All active workspaces with current display name
SELECT slug, name, plan
FROM core.workspace
WHERE is_active
ORDER BY name;

-- Detect deleted/retired workspaces
SELECT slug, name, last_seen_at
FROM core.workspace
WHERE NOT is_active
ORDER BY last_seen_at DESC;
```

---

### `core.campaign`

One row per Instantly campaign ever ingested.

| Column | Type | Description |
|---|---|---|
| `campaign_id` | VARCHAR (PK) | Instantly's campaign UUID. |
| `workspace_id` | VARCHAR | FK to `core.workspace.workspace_id`. |
| `name` | VARCHAR | Campaign name as set in Instantly UI. |
| `status` | INTEGER | Instantly status code (0=draft, 1=active, 2=paused, 3=completed, 4=running_subsequences, -1=archived, -2=deleted). |
| `status_label` | VARCHAR | Human label of `status`. |
| `cm` | VARCHAR | Campaign manager derived from name regex: one of SAM / SAMUEL / LEO / IDO / EYVER / TOUKIR / TOMER / LUCAS / MAX. NULL if no match or ambiguous (multiple matches). |
| `offer` | VARCHAR | Offer derived from name regex, priority order: HELOC, Tariffs, s125, R&D, Funding. NULL if no match. |
| `is_mca` | BOOLEAN | TRUE if name matches `\b(isaac\|mca\|cheap leads)\b` case-insensitive. |
| `email_gap` | INTEGER | Send pacing — minutes between sends per inbox. |
| `random_wait_max` | INTEGER | Max random jitter on email_gap, minutes. |
| `daily_limit` | INTEGER | Per-campaign daily send ceiling. |
| `created_at` | TIMESTAMPTZ | When campaign was created in Instantly. |
| `is_active` | BOOLEAN | TRUE if campaign was seen in most recent run. |
| `first_seen_at`, `last_seen_at`, `resolved_at` | TIMESTAMPTZ | Audit timestamps. |

**Source:** Instantly REST API `GET /campaigns` per workspace key.

**Resolution rules:**
- All Instantly-native fields (id, workspace_id, name, status, email_gap, etc.): verbatim from API.
- `cm`, `offer`, `is_mca`: regex against `name`. Documented above. When in doubt, NULL is correct. **Never silently wrong.**
- `is_active = FALSE`: campaign no longer returned by `/campaigns` (deleted, archived, or workspace key revoked).

**Common queries:**

```sql
-- Active campaign count by CM
SELECT cm, COUNT(*) AS n
FROM core.campaign
WHERE is_active
GROUP BY cm
ORDER BY n DESC;

-- Active campaigns per offer
SELECT offer, COUNT(*) AS n
FROM core.campaign
WHERE is_active
GROUP BY offer
ORDER BY n DESC;

-- MCA campaigns (Sam's regex)
SELECT campaign_id, name, workspace_id
FROM core.campaign
WHERE is_active AND is_mca;

-- All campaigns in Funding 4 with their CM + status
SELECT c.name, c.cm, c.status_label, c.daily_limit
FROM core.campaign c
JOIN core.workspace w USING (workspace_id)
WHERE w.name = 'Funding 4' AND c.is_active
ORDER BY c.name;

-- Campaigns that disappeared in the latest run (status flipped)
SELECT campaign_id, name, last_seen_at
FROM core.campaign
WHERE NOT is_active
ORDER BY last_seen_at DESC
LIMIT 50;
```

---

### `core.campaign_marker_tag` ⚠ EMPTY IN V1

The "badge" tags visible next to campaign names in the Instantly UI (e.g. "AIM Active"). One row per (campaign, tag_name).

**Status:** DDL exists but table is unpopulated. The public Instantly REST API does not expose marker-tag mappings (`/api/v2/tag-mappings?resource_type=2` returns 404). The MCP tool `mcp__instantly__list_tag_mappings` works via a private/admin endpoint we have not reverse-engineered. **Until that endpoint is identified, this table will be empty.** Queries against it will return no rows; that is a known gap, not a bug. Filed as Phase 3 follow-up.

| Column | Type | Description |
|---|---|---|
| `workspace_id` | VARCHAR | FK to `core.workspace`. |
| `campaign_id` | VARCHAR | FK to `core.campaign`. |
| `tag_name` | VARCHAR | Label of the tag. |
| `first_seen_at`, `last_seen_at` | TIMESTAMPTZ | Audit timestamps. |

PK: `(campaign_id, tag_name)`.

**Source:** Instantly REST API `GET /tag-mappings?resource_type=2`, joined to `GET /custom-tags` for labels.

**Resolution rules:**
- `tag_name`: from `/custom-tags` label.
- A campaign can have multiple marker tags. Same tag_name can appear across campaigns; primary key prevents duplicates within one campaign.
- Rows are NOT removed when a marker tag is removed from a campaign — `last_seen_at` stops advancing instead. Use `last_seen_at >= (most-recent-run started_at)` to filter for currently-applied tags.

**Common queries:**

```sql
-- All campaigns with "AIM Active" marker (will return 0 rows in v1 — see Status above)
SELECT c.name, w.name AS workspace
FROM core.campaign_marker_tag t
JOIN core.campaign c ON c.campaign_id = t.campaign_id
JOIN core.workspace w ON w.workspace_id = c.workspace_id
WHERE t.tag_name = 'AIM Active'
ORDER BY w.name, c.name;

-- Marker tag inventory per workspace (also 0 rows in v1)
SELECT w.name AS workspace, t.tag_name, COUNT(*) AS campaigns_tagged
FROM core.campaign_marker_tag t
JOIN core.workspace w ON w.workspace_id = t.workspace_id
GROUP BY w.name, t.tag_name
ORDER BY w.name, campaigns_tagged DESC;
```

---

### `core.campaign_sending_tag`

The tags applied to sending accounts that a campaign uses (e.g. `RG4843`, `RG4844`). Sourced from the campaign config `email_tag_list` field, resolved to labels. One row per (campaign, tag_name).

| Column | Type | Description |
|---|---|---|
| `workspace_id` | VARCHAR | FK to `core.workspace`. |
| `campaign_id` | VARCHAR | FK to `core.campaign`. |
| `tag_name` | VARCHAR | Label of the tag. RG-prefixed for Renaissance's standard batches. |
| `first_seen_at`, `last_seen_at` | TIMESTAMPTZ | Audit timestamps. |

PK: `(campaign_id, tag_name)`.

**Source:** Instantly REST API `GET /campaigns` (the `email_tag_list` array of UUIDs), joined to `GET /custom-tags` for labels.

**Resolution rules:**
- `tag_name`: from `/custom-tags` label.
- Same tag CAN appear in `campaign_marker_tag` AND `campaign_sending_tag` for the same campaign — they're different surfaces of the same tag entity.
- The same tag_name across workspaces is fine — workspace_id scopes the lookup.

**Common queries:**

```sql
-- Campaigns that use sending tag RG4843
-- (Cannot chain USING() on workspace_id here — three tables share it. Explicit ON.)
SELECT c.name, w.name AS workspace
FROM core.campaign_sending_tag t
JOIN core.campaign c ON c.campaign_id = t.campaign_id
JOIN core.workspace w ON w.workspace_id = c.workspace_id
WHERE t.tag_name = 'RG4843';

-- Tag count per campaign (how many sending account groups a campaign uses)
SELECT c.name, COUNT(*) AS sending_tags_n
FROM core.campaign_sending_tag t
JOIN core.campaign c USING (campaign_id)
WHERE c.is_active
GROUP BY c.name
ORDER BY sending_tags_n DESC
LIMIT 20;
```

---

### `core.sending_account`

One canonical row per Instantly inbox we have ever seen, sourced from the
`account_truth_<date>.duckdb` snapshot (the same file the Sending Truth Vercel app
reads). **~1.55M rows** (the full historical inbox universe — far larger than a "current
active inboxes" count). Filter `WHERE is_active` for inboxes still present in Instantly
(~1.39M); the rest are retired (dropped from Instantly's live inventory).

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR (PK) | The inbox email (account_truth carries no Instantly account UUID; email is unique). |
| `email` | VARCHAR | Inbox address. |
| `domain` | VARCHAR | Sending domain. FK to `core.domain` (future). |
| `workspace_slug` | VARCHAR | **The reliable join key** — matches `core.workspace.slug` and `raw_pipeline_campaigns.workspace_id`. |
| `workspace_id` | VARCHAR | Instantly UUID, resolved via `core.workspace`. **NULL** for `warm-leads` + `the-dyad` (not in core.workspace). |
| `esp` | VARCHAR | `google` / `outlook` / `otd` / NULL. NULL for retired accounts (esp unknown once gone). |
| `infra_provider` | VARCHAR | Vendor brand. Only `OTD` is resolvable from account_truth; for Outlook/Google inboxes the brand (MailIn/Reseller/Folderly/Tucows/Maxify) needs domain/tag resolution → **NULL for now** (known gap). |
| `lifecycle_state` | VARCHAR | `created`/`warming`/`warmed`/`ramping`/`active`/`paused`/`retired`. See derivation below. |
| `rotation_state` | VARCHAR | `on`/`off`/NULL. Not derivable from account_truth → **always NULL in v1**. |
| `created_at` | TIMESTAMPTZ | When the inbox first appeared in Instantly. NULL for retired (detail gone). |
| `warmup_started_at` … `paused_at` | TIMESTAMPTZ | Transition timestamps. **NULL in v1** — a single snapshot can't observe them; they fill in as nightly snapshot-diffs detect transitions. |
| `retired_at` | TIMESTAMPTZ | Set from snapshot `updated_at` for retired inboxes. |
| `status` | VARCHAR | `active`/`paused`/`connection_error`/`missing`. |
| `warmup_phase` | VARCHAR | `warmed`/`not_warmed`/`degraded` (label of account_truth `warmup_status`). |
| `warmup_score` | DOUBLE | 0-100 warmup/deliverability score from account_truth. |
| `daily_limit` | INTEGER | Configured target daily send limit. |
| `daily_limit_used` | INTEGER | Today's send count. **NULL in v1** (Instantly `/accounts` supplement only). |
| `cost_per_day_usd_estimated`, `vendor_billing_cycle` | DOUBLE, VARCHAR | Cost projection (spec 13). NULL until v3 derivation. |
| `is_active` | BOOLEAN | FALSE when the inbox is gone from Instantly's live inventory (`status='missing'`). |
| `first_seen_at`, `last_seen_at`, `resolved_at` | TIMESTAMPTZ | Audit timestamps. |

**Lifecycle derivation (v1, single-snapshot — coarse by necessity):**
`Missing Current Inventory → retired`; `Paused`/`Connection Error → paused`;
`Active` + warmed (warmup_status=1) `→ active`; `Active` + degraded (warmup_status=-1) `→ warmed`;
`Active` + not-warmed (warmup_status=0) `→ warming`. The active-vs-ramping distinction needs
`daily_limit_used` (supplement-only) — not available in v1, so warmed+active inboxes all read `active`.

**Source:** `raw_account_truth_accounts` (latest `_run_id`), deduped to one row per email
(prefers the live row when an email appears in two workspaces). Companion timeline table
`core.sending_account_state_event` seeds a `created` event per inbox; richer transition
events accumulate as nightly runs diff successive snapshots.

**Common queries:**

```sql
-- Active inbox inventory by ESP
SELECT esp, COUNT(*) AS inboxes
FROM core.sending_account
WHERE is_active
GROUP BY esp ORDER BY inboxes DESC;

-- Inboxes per workspace (current display name), active only
SELECT w.name AS workspace, COUNT(*) AS inboxes
FROM core.sending_account sa
JOIN core.workspace w ON w.slug = sa.workspace_slug
WHERE sa.is_active
GROUP BY w.name ORDER BY inboxes DESC;

-- Warmup pipeline: how many inboxes are warming vs in service
SELECT lifecycle_state, COUNT(*) AS n
FROM core.sending_account
WHERE is_active
GROUP BY lifecycle_state ORDER BY n DESC;

-- All inboxes on a given domain
SELECT email, esp, lifecycle_state, warmup_score, daily_limit
FROM core.sending_account
WHERE domain = 'armenta-syndicate.info';
```

---

### `raw_pipeline_*` tables (slim mirror of pipeline-supabase)

We mirror 4 tables from `pipeline-supabase` Postgres into local DuckDB for fast analytical queries. Excluded from v1 (too big or not yet needed): `reply_data`, `lead_events`, `conversation_messages`, `contact_frequency_*`, `infra_*`, `sender_inboxes`, `bounce_suppression`, `variant_copy`. They live in Supabase; query via psycopg2 if you need them.

#### `raw_pipeline_campaigns`

| Column | Type | Description |
|---|---|---|
| `campaign_id` | VARCHAR | |
| `workspace_id` | VARCHAR | OLD slug (e.g. `koi-and-destroy`). |
| `workspace_name` | VARCHAR | OLD display name (e.g. "Koi and Destroy") — pipeline cache, stale post-rename. |
| `name` | VARCHAR | |
| `status` | VARCHAR | |
| `cm_name` | VARCHAR | **Pipeline-side derived CM** (`"SAM"`, `"SAMUEL"`, etc). Often more reliable than `core.campaign.cm` (pipeline maintains a name-parsing lookup table; canonical only regexes). |
| `industry` | VARCHAR | |
| `product` | VARCHAR | **Pipeline-side derived offer** (`"FUNDING"`, etc). Cross-reference with `core.campaign.offer`. |
| `infra_type` | VARCHAR | **Pipeline-side derived sending infra** (`"google"`, `"outlook"`, `"otd"`). |
| `tags` | VARCHAR | JSON array of sending tag names (e.g. `["RG4843", "RG4844"]`). Pre-resolved by pipeline. |
| `bounced_count`, `contacted_count`, `leads_count`, `completed_count`, `unsubscribed_count` | INTEGER | Lifetime aggregates. |
| `daily_limit` | INTEGER | |
| `instantly_created_at`, `timestamp_updated`, `synced_at` | TIMESTAMPTZ | |
| `excluded_from_analysis`, `exclusion_reason` | BOOLEAN, VARCHAR | Pipeline-side flagging. |
| `lead_source`, `rg_batch_ids`, `segment` | VARCHAR | Pipeline-side metadata. |
| `_loaded_at`, `_run_id` | | Warehouse audit. |

**Why mirror it when we have `core.campaign`?** Because pipeline-supabase already does heavy lifting on `cm_name`, `product`, `infra_type`, and `tags` (sending tags). These are HIGHER quality than the regex-derived equivalents in `core.campaign` because pipeline maintains explicit lookup tables. **When the question is "what infra does this campaign send from," prefer `raw_pipeline_campaigns.infra_type` over deriving from `core.campaign.name`.**

#### `raw_pipeline_campaign_data`

Per `(campaign, step, variant)` rows with per-variant performance. Use for variant-level analytics. Has `subject`, `body` (resolved spintax), `emails_sent`, `replies`, `opportunities`, `meetings_booked`, `reply_rate`, `close_rate`, `campaign_score`.

#### `raw_pipeline_campaign_daily_metrics`

Per `(campaign, date)` rows, **full pipeline history (back to 2026-01-26)**, with `sent`, `unique_opened`, `replies`, `unique_replies`, `replies_automatic`, `opportunities`, `unique_opportunities`, `clicks`. Use for **time-series / per-day trend** of the ADDITIVE fields (`sent`, opens, clicks).

> ⚠ **`unique_replies` / `unique_opportunities` are per-day-DISTINCT counts — do NOT SUM them across days for a campaign total.** A lead unique on two days counts twice ("Instantly - Short": summed = 98 opps / 500 replies vs the UI's 49 / 457). The old "unique_opportunities is canonical" rule was wrong (it only held for tiny single-day campaigns).
>
> ✅ **For any campaign-total reply / opportunity number, read `v_campaign_metrics`** (one row per campaign; `sent`, `unique_replies`, `opportunities`, `opp_rate`, `positive_reply_rate`, `email_per_opp`, `metric_source`). It sources opps/replies from the Instantly campaigns/analytics endpoint (`raw_instantly_campaign_analytics`) = exact UI numbers. `v_campaign_opportunities` (weekly) is for TREND SHAPE only — its weekly opp sum overcounts.

#### `raw_pipeline_meetings_booked_raw`

Per-meeting rows, sourced from Slack success channels (canonical per Sam's call). Has `campaign_name_raw`, `campaign_id`, `match_method`, `match_confidence`, `partner`, `posted_at`. Use for meeting attribution.

**Common pipeline queries:**

```sql
-- Campaign-total performance (UI-true opps/replies) — PREFER THIS for totals.
SELECT cm_name, campaign_name, sent, unique_replies, opportunities,
       email_per_opp, positive_reply_rate, metric_source
FROM v_campaign_metrics
WHERE workspace_id = 'koi-and-destroy'        -- Funding 4
ORDER BY opportunities DESC NULLS LAST;

-- Per-CM weekly TREND (sends/replies are additive → safe to SUM per week).
-- For opportunity TOTALS use v_campaign_metrics (above), NOT a daily SUM.
SELECT c.cm_name,
       SUM(m.sent) AS sends,
       SUM(m.unique_replies) AS replies,
       ROUND(SUM(m.unique_replies) * 1000.0 / NULLIF(SUM(m.sent), 0), 2) AS rr_per_1k
FROM raw_pipeline_campaign_daily_metrics m
JOIN raw_pipeline_campaigns c USING (campaign_id)
WHERE m.date >= current_date - INTERVAL '7 days'
GROUP BY c.cm_name
ORDER BY sends DESC;

-- Meetings per campaign (Slack-sourced)
SELECT campaign_name_raw, COUNT(*) AS meetings
FROM raw_pipeline_meetings_booked_raw
WHERE posted_at >= current_date - INTERVAL '7 days'
GROUP BY campaign_name_raw
ORDER BY meetings DESC;
```

---

### `core.domain`

One row per **active sending domain** (~52k). Spine is the nightly DNS+blacklist sweep
(`raw_dns_sweep_domain`), enriched with the ESP/vendor/lifecycle of the inboxes hosted on
it (`core.sending_account`) and — for the 2,002 Lucas/Tomer `.co` domains — NS provider,
registrar slot, acquisition date, and the $1.80 batch cost.

Key columns: `esp` (google/outlook/otd, inherited from inboxes), `infra_provider` (OTD only
for now — GAP), `ns_provider` (Lucas/Tomer for the 2,002 NS-handoff domains, else NULL),
`mx_provider`, `a_record_ip`/`a_record_24` (IP clustering, factor 3), `dkim_tenant_prefix`
(`*.onmicrosoft.com` tenant — factor-3 homogeneous-provisioning signal), `dns_signature`
(SHA1 fingerprint — domains sharing one are provisioned identically), `brand_prefix` (domain
minus TLD — factor-4 brand clustering), `blacklist_count`/`any_blacklist_active`/`listed_on`
(JSON), `inbox_count`, `lifecycle_state` (in_use/dns_configured/paused), `acquisition_batch`,
`cost_acquisition_usd_estimated`. `redirect_chain`/`terminal_redirect` are NULL in v1 (factor-5
probe deferred — GAP F1). Blocklists checked: surbl, spamrl, spamhaus_dbl (GAP F2).

```sql
-- Blacklist exposure by ESP (the SURBL carpet-bomb on cheap TLDs)
SELECT esp, COUNT(*) domains, COUNT(*) FILTER (WHERE any_blacklist_active) listed,
       ROUND(100.0*COUNT(*) FILTER (WHERE any_blacklist_active)/COUNT(*),1) pct_listed
FROM core.domain GROUP BY esp ORDER BY domains DESC;

-- Factor 3: homogeneous provisioning — domains sharing a DNS signature
SELECT dns_signature, COUNT(*) n, MIN(domain) example
FROM core.domain WHERE dns_signature IS NOT NULL
GROUP BY dns_signature HAVING COUNT(*) > 50 ORDER BY n DESC LIMIT 20;

-- Factor 3: shared MailIn tenant across many domains
SELECT dkim_tenant_prefix, COUNT(*) domains
FROM core.domain WHERE dkim_tenant_prefix IS NOT NULL
GROUP BY 1 ORDER BY domains DESC LIMIT 20;

-- Factor 4: brand clustering — many domains on one brand string
SELECT brand_prefix, COUNT(*) n FROM core.domain
GROUP BY brand_prefix HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 30;

-- /24 IP clustering
SELECT a_record_24, COUNT(*) domains FROM core.domain
WHERE a_record_24 IS NOT NULL GROUP BY 1 ORDER BY domains DESC LIMIT 20;
```

`raw_dns_sweep_domain` (per-domain fingerprint, one row per run) and `raw_blacklist_check`
(append-only event log, one row per domain×blocklist×run — the history of listings/delistings)
back this entity. Query `raw_blacklist_check` for blacklist *trends* over time.

---

### `core.opportunity`

**Lead-level** opportunity records (who specifically is an opportunity, with contact info +
call disposition). Source = the warm-call/AIM `raw_comms_call_opportunity` table, source-aware:
`source='sendivo'` (SMS opps, ~22k) + `source='instantly'` (email opps routed to calling, ~9).

> **"Opportunities" ≠ "interested" (Sam, 2026-05-31).** We do NOT track Instantly lead-status
> `lead_interested` — that's a different, broader signal. An earlier build used it; it was removed.
>
> **Instantly EMAIL opportunities** (the dominant dashboard KPI: opportunities to meetings, $145/mtg)
> are an aggregate campaign metric, not lead-level (the lead-level feed `opportunity_webhook_log` is empty,
> so we get the *number* of opportunities, not which leads). **The canonical campaign-total source is
> `v_campaign_metrics.opportunities`**, sourced from the Instantly campaign analytics endpoint and reconciled
> to the UI. Daily `raw_pipeline_campaign_daily_metrics.unique_opportunities` is per-day distinct and
> non-additive; do not sum it into campaign totals.

| Column | Notes |
|---|---|
| `opportunity_id` (PK) | `'{source}:{source_event_id}'` |
| `source` | `sendivo` \| `instantly` (from call_opportunity.source) |
| `lead_email`, `workspace_id` | `campaign_id` is NULL (not carried on call_opportunity). |
| `opened_at` | `opportunity_marked_at` (fallback `created_at`). |
| `state`, `state_updated_at` | Sendivo status string. 18,315 of ~22k are `state='duplicate'` — filter `state <> 'duplicate'` for ~4,034 unique warm-call opps. |
| `is_duplicate_of` | Same-source dupe pointer when present (cross-source dedup is v1.5). |

```sql
-- INSTANTLY email-opportunity KPI - campaign totals by CM.
SELECT cm_name, SUM(sent) sends, SUM(unique_replies) replies, SUM(opportunities) opps,
       ROUND(SUM(opportunities)*1000.0/NULLIF(SUM(sent),0),3) opp_per_1k
FROM v_campaign_metrics
GROUP BY cm_name ORDER BY opps DESC NULLS LAST;

-- Lead-level warm-call opportunities (unique), by source
SELECT source, COUNT(*) FROM core.opportunity
WHERE state <> 'duplicate' GROUP BY source;
```

---

### `derived.enrichment_cost*` (phone-enrichment spend — warm-call pipeline)

**Use these for any "phone-enrichment spend / cost-per-lead" question.** Phone-enrichment
spend is the per-lead consumption cost of finding mobile numbers for warm-call opportunities
that arrived without a phone (Instantly cold-email leads; Sendivo opps already carry a phone
so they cost $0). It is **NOT in `core.cost_ledger`** by design — that ledger is direct
infrastructure only (leadmagic/findymail/prospeo were deliberately dropped from it). These
derived views are the canonical surface instead. Source: `raw_comms_phone_enrichment` ×
`raw_comms_enrichment_vendor_pricing` (editable $/credit rates) × `raw_comms_call_opportunity`.

| View | Grain | Key columns |
|---|---|---|
| `derived.enrichment_cost` | one row per enrichment attempt | `provider` (prospeo/leadmagic/findymail), `opportunity_source` (instantly/sendivo), `hit` (mobile found), `credits_spent`, `usd_per_credit`, `cost_usd`, `attempt_date` |
| `derived.enrichment_cost_daily` | provider × day × source | `attempts`, `hits`, `credits`, `cost_usd` |
| `derived.enrichment_cost_per_lead` | opportunity source | `opps`, `in_close`, `total_cost_usd`, `cost_per_lead_in_close` |

Cost model (rates live in the editable `comms.enrichment_vendor_pricing`): prospeo $0 (free
60k/mo plan), leadmagic $0.02/credit (≈$0.10/phone), findymail ≈$0.02/credit; all pay on
success only. Update a vendor rate in that comms table and the next nightly run flows it through.

```sql
-- Total phone-enrichment spend and cost per callable lead, by source
SELECT * FROM derived.enrichment_cost_per_lead;

-- Spend by vendor
SELECT provider, SUM(attempts) attempts, SUM(hits) hits, ROUND(SUM(cost_usd),2) cost_usd
FROM derived.enrichment_cost_daily GROUP BY provider ORDER BY cost_usd DESC;

-- Spend trend
SELECT attempt_date, ROUND(SUM(cost_usd),2) cost_usd
FROM derived.enrichment_cost_daily GROUP BY attempt_date ORDER BY attempt_date DESC;
```

---

### `v_campaign_metrics` (derived view - canonical campaign totals)

One row per campaign with UI-true `sent`, `unique_replies`, and `opportunities` from
`raw_instantly_campaign_analytics`. Use this for any campaign-total, CM-total, offer-total,
or workspace-total opp/reply KPI. It intentionally avoids summing daily distinct counts.

| Column | Notes |
|---|---|
| `campaign_id`, `campaign_name`, `workspace_id`, `workspace_name`, `cm_name` | Campaign grain and attributes. |
| `sent`, `unique_replies`, `opportunities` | Canonical Instantly UI metrics. |
| `reply_rate`, `opp_rate`, `positive_reply_rate`, `email_per_opp` | Campaign-level rates from canonical totals. |
| `metric_source` | Source marker, currently Instantly campaign analytics. |

```sql
-- Spot-check one campaign against the Instantly UI.
SELECT campaign_name, sent, unique_replies, opportunities, email_per_opp
FROM v_campaign_metrics
WHERE campaign_name = 'Instantly - Short';
```

### `v_campaign_opportunities` (derived view - weekly trend shape only)

The first Layer-4 view. One row per **campaign × ISO week**, with `cm` / `offer` / `infra_type`
attributes (pipeline-derived). This view is useful for weekly send/reply/opportunity trend shape,
but its `opportunities` field is `SUM(unique_opportunities)` across days and is therefore
non-additive for campaign totals. For campaign/CM/offer/workspace totals, use `v_campaign_metrics`.
`opportunities_raw` (= `SUM(opportunities)`) is kept for reference and also should not be used as a
campaign-total KPI.

| Column | Notes |
|---|---|
| `week_start` | ISO week (Monday). |
| `campaign_id`, `campaign_name`, `cm`, `offer`, `infra_type`, `workspace_id/name` | grain + attributes |
| `sends`, `unique_replies` | weekly sums |
| `opportunities` | `SUM(unique_opportunities)` - weekly trend only, not a campaign-total KPI |
| `opportunities_raw` | `SUM(opportunities)` - cross-day double-count; reference only |
| `opp_per_1k`, `reply_per_1k` | per-1k-send efficiency |

> **Coverage:** full pipeline history — `campaign_daily_metrics` is now mirrored **un-windowed**
> (the 90-day filter was dropped 2026-05-31), so this view reaches back to **2026-01-26** (the
> pipeline's earliest data) across **2,003 campaigns** — including ~1,500 since deleted from Instantly
> (the pipeline preserves what Instantly bills-away). Older-than-Jan-26 history simply doesn't exist in any source.
> Use this view only for time-series shape; use `v_campaign_metrics` for UI-true campaign totals.

```sql
-- Weekly opportunity trend by offer, last 8 weeks. Do not roll this into campaign totals.
SELECT offer, SUM(opportunities) opps, SUM(sends) sends,
       ROUND(SUM(opportunities)*1000.0/NULLIF(SUM(sends),0),3) opp_per_1k
FROM v_campaign_opportunities
WHERE week_start >= current_date - INTERVAL '56 days'
GROUP BY offer ORDER BY opps DESC NULLS LAST;

-- Weekly opportunity trend for one CM
SELECT week_start, SUM(opportunities) opps
FROM v_campaign_opportunities WHERE cm = 'EYVER'
GROUP BY week_start ORDER BY week_start DESC LIMIT 12;
```

---

### `core.meeting`

One row per booked meeting (**29,903**), Slack success-channel sourced. PK
`'{channel_id}:{message_ts}:{line_index}'`. Has `posted_at`, `partner`, `campaign_id`, `cm`
(campaign-join first, raw-text regex fallback — ~60% NULL, GAP B4), `match_method`,
`match_confidence`. Revenue is derivable as `meetings × $145` (not stored). Calendly/Close
reconciliation is v1.5.

```sql
-- Meetings per CM per month
SELECT date_trunc('month', posted_at) mth, cm, COUNT(*) meetings
FROM core.meeting GROUP BY 1,2 ORDER BY 1 DESC, meetings DESC;
```

---

### `core.campaign_daily` (Track H — per-campaign daily metrics from the Instantly analytics API)

One row per `(campaign_id, date)` for campaigns in the **CURRENT Instantly build set only**.
Built nightly by `scripts/build_campaign_daily.py` from the per-day analytics endpoint, with
the 07:00 UTC meetings re-join and the `core.instantly_bounce_daily` bounce join. Columns
include `sent`, `unique_replies`, `bounces`, `meetings_booked` (0 — never NULL — on observed
no-meeting days).

> ⚠ **Coverage: current campaigns only.** Campaigns deleted from Instantly (or in dead
> workspaces returning 401/402) have NO rows here, and known campaigns have no rows for
> unobserved days. Meetings on deleted/dead campaigns live ONLY in `core.meeting` — as of
> 2026-06-11, **65% of the last-45-day matched meetings (2,050 of 3,137) sit on
> campaign-days with no `campaign_daily` row** (1,947 of them on campaigns absent from the
> current build set entirely). **For all-time or per-CM meeting counts, query
> `core.meeting`**; use `campaign_daily` only for questions about currently-live campaigns.
> On every cell BOTH tables cover, `meetings_booked` reconciles exactly with `core.meeting`
> (verified 2026-06-11 QA).

`core.campaign_variant` (same builder) follows the same current-build-set coverage rule.

---

### `core.cost_ledger` (financial fact table — seeded with reference rates, real ingest pending)

The authoritative source for every cost Renaissance pays. Mix of actuals (`source IN ('stripe','invoice_csv','manual')`) and estimates / reference rates (`source IN ('estimated','reference_rate')`). Same shape for both — the `source` column tells you which.

| Column | Type | Description |
|---|---|---|
| `cost_id` | VARCHAR (PK) | Synthetic: `vendor:sku:period_start[:attribution_id]`. Stable across re-runs so actuals replace estimates cleanly. |
| `vendor` | VARCHAR | `instantly`, `dynadot`, `maxify`, `tomer`, `lucas`, `tucows`, `folderly`, `warmly`, `anthropic`, `blitzapi`, `leadmagic`, `findymail`, `prospeo`, `supabase`, `cloudflare`, `do_droplet`, `otd`, `google`, etc. |
| `sku` | VARCHAR | Vendor-specific item id (e.g. `.co_renewal`, `inbox_monthly`, `workspace_subscription`). |
| `cost_unit` | VARCHAR | `workspace` / `domain` / `inbox` / `send` / `enrichment_lookup` / `platform` / `service` / `one_time`. |
| `unit_count` | INTEGER | Units this row covers. **For reference_rate rows, this is 1** (rate per unit). For actual invoices, this is the count covered. |
| `total_usd` | DOUBLE | **For reference_rate rows, this is per-unit rate**. For actuals, total spend for the period. |
| `period_start`, `period_end` | DATE | Coverage window. One-time costs have equal dates. |
| `amortize_method` | VARCHAR | `monthly` / `daily` / `one_time` / `annual_spread`. |
| `attribution_dim` | VARCHAR | `global` / `workspace` / `offer` / `infra` / `channel` / `domain` / `inbox`. |
| `attribution_id` | VARCHAR | The specific id (workspace_id, channel slug, etc.). NULL for global. |
| `source` | VARCHAR | `stripe` / `invoice_csv` / `manual` / `estimated` / `reference_rate`. |
| `source_ref` | VARCHAR | Pointer back: Stripe invoice id, CSV path, doc path. |
| `notes` | VARCHAR | Free text. |

**Important for queries:** in v1 the table holds REFERENCE RATES, not current spend. To compute current monthly burn you need to multiply per-unit rates × current entity counts. Examples:

```sql
-- Cost per inbox by vendor (works directly on reference rates)
SELECT vendor, total_usd AS cost_per_inbox_monthly
FROM core.cost_ledger
WHERE cost_unit = 'inbox'
  AND source = 'reference_rate'
  AND period_end >= current_date
ORDER BY cost_per_inbox_monthly ASC;

-- Domain renewal rates
SELECT vendor, sku, total_usd AS annual_renewal_usd
FROM core.cost_ledger
WHERE cost_unit = 'domain'
  AND amortize_method = 'annual_spread'
ORDER BY annual_renewal_usd ASC;

-- Channel-level monthly costs (mix of per-channel platforms + per-inbox rates)
SELECT attribution_id AS channel,
       SUM(total_usd) AS monthly_cost_unit_basis
FROM core.cost_ledger
WHERE attribution_dim = 'channel'
  AND amortize_method = 'monthly'
  AND period_end >= current_date
GROUP BY 1
ORDER BY 2 DESC;
-- ⚠ NOTE: this gives per-channel sum of unit rates + flat fees. For "current monthly burn at scale,"
-- multiply inbox rates by COUNT(*) FROM core.sending_account (when Phase 3 lands) per channel.

-- Burn rate forecast pattern (post Phase 3):
--   SELECT vendor, total_usd × <current inbox count from core.sending_account>
--   FROM core.cost_ledger
--   WHERE cost_unit = 'inbox' AND source = 'reference_rate'
-- This is the chatbot's "what would it cost to scale to N inboxes" calculation.
```

**Phase 2 will fold in Stripe actuals.** The same `cost_id` formula means Stripe rows replace reference_rate rows for the same `(vendor, sku, period)` — no duplicates, no migration.

See `specs/13-financial-data-architecture.md` for the full design.

> **Sendivo is the first `source` with actuals.** `core.cost_ledger` now holds real Sendivo
> SMS billing (`source='invoice_api'`, `vendor='sendivo'`, ~$25k/mo, itemized carrier/SMS/setup/
> brand/phone fees) alongside the reference-rate estimates. Filter on `source` to separate them.

---

### Sendivo (SMS send-side) — `raw_sendivo_*` + `v_sms_performance`

The SMS **send** layer, pulled from the Sendivo API (`app.sendivo.io/api/v1`, Bearer
`SENDIVO_API_KEY`) by the nightly `sendivo` phase (spec 14). Complements the comms-orchestration
**reply** layer (`raw_comms_*`, `core.opportunity` source='sendivo') — two halves of the SMS funnel.

| Table / view | What |
|---|---|
| `raw_sendivo_delivery_metrics` | One row per (scope, `metric_date`, run). `sms_sent`, `segments_sent`, `inbound_sms_received`, `delivery_rate`, `opt_out_rate`, `response_rate`. scope='agency' (key is agency-wide). Backfilled per-day. |
| **`v_sms_performance`** | The dashboard surface — latest snapshot per (scope, date). Query this, not the raw. |
| `raw_sendivo_campaigns` | 30 campaigns + `status` (Carriers Pending/Approved/…). Ties to the Sendivo UI exactly (correctness check). |
| `raw_sendivo_brands` | Brand registration/verification status. |
| `raw_sendivo_billing` | Per (sub_account, month) actual spend, itemized. **Feeds `core.cost_ledger`** (source='invoice_api'). |

> **⚠ Rates must be volume-weighted.** `delivery_rate`/`opt_out_rate`/`response_rate` are
> per-day. A naïve `AVG()` across days is wrong (e.g. 30% vs the true 65%). Use
> `SUM(sms_sent*rate)/SUM(sms_sent)` — that recovers the exact period rate.
>
> **⚠ Scope.** The API key returns one **agency-wide** aggregate (~3M sent/mo). The Sendivo UI
> screenshots are *filtered* sub-account views, so they'll be smaller. Billing is per-sub-account.

```sql
-- Real SMS funnel, last 30 days (volume-weighted rates) — the SMS dashboard headline
SELECT SUM(sms_sent) sent, SUM(inbound_sms_received) inbound, SUM(segments_sent) segments,
       ROUND(SUM(sms_sent*delivery_rate)/NULLIF(SUM(sms_sent),0),1) delivery_rate,
       ROUND(SUM(sms_sent*response_rate)/NULLIF(SUM(sms_sent),0),2) response_rate,
       ROUND(SUM(sms_sent*opt_out_rate)/NULLIF(SUM(sms_sent),0),2)  opt_out_rate
FROM v_sms_performance WHERE metric_date >= current_date - 30;

-- Unit economics: cost per 1k SMS (actual billing ÷ actual sends, same month)
SELECT ROUND( (SELECT SUM(total_usd) FROM core.cost_ledger WHERE vendor='sendivo')
              / NULLIF((SELECT SUM(sms_sent) FROM v_sms_performance
                        WHERE metric_date >= date_trunc('month', current_date))/1000.0, 0), 2) AS usd_per_1k_sms;
```

**Context — SMS is brand new (launched 2026-05-18).** `v_sms_performance` is zero before
2026-05-18, then ramps hard (0 → 2.3M/wk). So all SMS cost/rate/meeting metrics cover a ~2-week
**ramp** and are early/inflated — don't read them as steady-state. Cost-per-SMS-meeting ≈ $64
($25.3k ÷ 398 booked, same window); treat **398 as a likely floor** — some Slack booking posts
lack an explicit SMS tag, so `core.meeting` channel attribution may modestly undercount. Cost-per-1k-SMS
($8.41) is the cleaner unit metric (both sides from Sendivo).

---

## Operational tables

### `core.sync_run`, `core.sync_run_phase`

For debugging the nightly sync only. Not for analytics. Each `run_id` is a timestamp-prefixed unique ID. `core.sync_run.status` is `running` / `success` / `partial` / `failed`. `core.sync_run_phase` has per-phase rows with `rows_in`, `rows_out`, `error`.

```sql
-- Most recent run summary
SELECT * FROM core.sync_run ORDER BY started_at DESC LIMIT 5;

-- Phase failures in latest run
SELECT * FROM core.sync_run_phase
WHERE run_id = (SELECT run_id FROM core.sync_run ORDER BY started_at DESC LIMIT 1)
  AND status = 'failed';
```

### `core.schema_version`

DDL application audit. One row per applied `sql/ddl/NN_*.sql` file.

---

## What is NOT here (yet)

Coming in Phase 3:

- **`core.recipient_domain`** — MX-resolved recipient ESP classification (gates the ESP×ESP matrix)
- **`core.send_event` / lead-membership change-event log** — per-day change events from Instantly campaign memberships
- **Derived views** for the 5 dip-factor visibility surfaces (copy fingerprint, scaling, homog. provisioning, brand clustering, redirects)
- **Cost / P&L** — schema is shaped for it but only reference-rate cost data ingested yet

Already shipped (documented above / below): `core.workspace`, `core.campaign`, `core.campaign_sending_tag`, `core.sending_account`, `core.domain`, `core.recipient_domain`, `core.meeting`, `core.opportunity`, `core.cost_ledger`; views `v_campaign_opportunities`, `v_sms_performance`, `mv_esp_send_matrix`, `derived.enrichment_cost*` (phone-enrichment spend); and the `raw_*` mirrors (pipeline, comms, account_truth, dns_sweep, sendivo).

If a question requires any of the above, flag it as a Phase 3 ask.

---

## Working with the warehouse

```bash
# Local query (read-only)
ssh renaissance-worker duckdb -readonly /root/core/warehouse.duckdb "<sql>"

# Or interactive session
ssh renaissance-worker
duckdb /root/core/warehouse.duckdb

# Nightly sync runs at 03:30 UTC via cron — don't manually run during the window
# Manual sync:
ssh renaissance-worker 'cd /root/renaissance-warehouse && source .venv/bin/activate && python -m core.orchestrator'

# Single phase:
ssh renaissance-worker 'cd /root/renaissance-warehouse && source .venv/bin/activate && python -m core.orchestrator --phase instantly'
```

DuckDB single-writer lock: if you get `Conflicting lock is held`, the nightly sync is running. Wait or query with `-readonly`.

---

**End of SCHEMA.md.** When a question can't be answered cleanly, that's signal — surface the gap to Sam rather than making up a column.
