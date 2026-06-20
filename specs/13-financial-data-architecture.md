# 13 — Financial Data Architecture

**Status:** v1 spec'd 2026-05-30. SHAPE lands now; real-data ingest = Phase 2.
**Reverses** the original v1 non-goal "Cost / P&L integration."

## Why now (the reframe)

The original architecture spec listed cost integration as v1 non-goal. Reversing because:

1. **Retrofitting cost into a frozen canonical layer is painful.** Adding `cost_*` columns to entities after agents are already querying them means breaking queries / migrating data.
2. **The acquirer story needs cost-aware queries inside 12 months.** "We're not a cold email agency, we're a data company with streamlined distribution" requires showing cost-per-meeting + margin-by-cohort. Schema has to exist.
3. **The structural cost of nullable cost columns now is zero.** A `cost_per_day_usd_estimated DOUBLE` column on `core.sending_account` is one line of DDL. Skipping it now and adding it later is a migration. Adding it now and stubbing it is a noop.
4. **Sam's actual use case (per 2026-05-30 Ido call):** Friday review, cash discipline, "what's working / what's it costing." The chatbot needs to answer these on day 1 even if numbers are stubs.

## The vibe to honor (per the meeting transcript + Sam's note)

Ido's framing kept landing on: **stop forecasting every scenario; enforce rules + cadence + cash discipline.** Translation for the schema:

- **Do not** build a forecast engine. No projected-burn tables, no scenario-modeling materializations in v1.
- **Do** make actuals queryable. The chatbot can do the forecasting; we just need to give it the right inputs.
- **Do** make "cost per X" trivially answerable so a Friday review doesn't require manual reconciliation.
- **Do** support "what's working" vs "what's not" through cost ÷ result rollups. That's the load-bearing decision support.

## Scope of "financial data" for v1

Per Sam (transcript + 2026-05-30 follow-up):

- **In scope:** DIRECT infrastructure costs only. Domains, inboxes, vendor pilots ($5-15k/mo are not peanuts), platform fees that scale with our volume (ActiveCampaign, Folderly).
- **Out of scope:** software subscriptions Sam called out specifically — droplet, Supabase, BlitzAPI, Cloudflare workers, Anthropic API, enrichment vendors. Rationale per Sam: "if we're going to mention those, we might as well mention the other 50 platforms that we pay for." Hard line; revisit later if a specific software cost becomes large enough to matter.
- **Out of scope:** labor, debt, investor capital, taxes, equity-related cash. Renaissance is bootstrapped, no debt/investors.
- **Out of scope (v1):** revenue ingest. Revenue is derivable (meetings × $145 per Sam's accounting identity).

## Batch-cost tracking pattern (the load-bearing pattern for domains)

Sam called this out explicitly: "we have the 15,000 .co domains that we bought, and each one was, I believe, $1.80... whether it be on a row level or on a tag level. We tagged this batch of domains as this thing, and we know that this thing cost this much per domain."

**The pattern:**

1. **One ledger row per batch purchase.** When we buy 14,978 .co domains for $26,960 in a single sale event, that's ONE row in `core.cost_ledger`:
   - `vendor='dynadot'`, `sku='.co_acquisition_bulk_sale'`, `cost_unit='domain'`
   - `unit_count=14978`, `total_usd=26960.40`, `period_start='2026-05-19'`, `amortize_method='one_time'`
   - `attribution_dim='batch'`, `attribution_id='dynadot_2026-05-19_co_sale_14978'`
   - `source='manual'` (we keyed it in based on Sam's recollection)

2. **Each affected entity gets the batch tag.** In Phase 3 when `core.domain` ships, add an `acquisition_batch` column. Every .co domain bought in this sale gets `acquisition_batch = 'dynadot_2026-05-19_co_sale_14978'`.

3. **Per-domain cost is derived, not stored.** When the chatbot asks "what did this domain cost," the query joins:
   ```sql
   SELECT d.domain,
          (SELECT total_usd / unit_count FROM core.cost_ledger 
           WHERE attribution_id = d.acquisition_batch LIMIT 1) AS cost_per_domain
   FROM core.domain d
   ```

4. **Renewal is separate from acquisition.** Acquisition is one-time per batch. Renewal is annual_spread, per-unit ($9/yr for .co). Both rows coexist in the ledger; they answer different questions.

**Why this is right:** scales to any number of batches without schema changes. When we buy another 5k .co next month at $2.20/domain, that's one new row + a new batch tag. The per-domain cost shifts naturally. No estimating, no blanket averages — actual purchase price preserved with the batch.

**Where to source the batch tag:** for the 14,978 May 19-20 batch, the Domain Tech Sheet `Domains(Table)` tab shows the inventory split across Dynadot #9-15 (2,142 / 2,141 / 2,140 / 2,139 / 2,141 / 2,139 / 2,136). When the domain entity ingest runs, every .co domain in Dynadot accounts #9-15 gets stamped with the batch tag.

**Same pattern applies to inboxes when we get bulk vendor pricing.** "Tomer agreed to $X for the first 1k MailIn inboxes in cohort A" = 1 ledger row + tag on each inbox in `core.sending_account`.

## Architecture: one ledger, inline projections

**Two-layer pattern.** Same shape as the rest of core: a fact table (ledger) + projected inline columns (denormalized for query convenience).

### Layer 1 — `core.cost_ledger` (authoritative fact table)

One row per (vendor, sku, period) cost event. Holds actuals when we have them and estimates when we don't.

```sql
CREATE TABLE core.cost_ledger (
  cost_id              VARCHAR PRIMARY KEY,    -- synthetic: vendor:sku:period_start[:attribution_id]
  vendor               VARCHAR NOT NULL,       -- 'instantly' | 'dynadot' | 'porkbun' | 'maxify' | 'tomer' | 'lucas' | 'tucows' | 'folderly' | 'warmly' | 'aleads' | 'leadmagic' | 'findymail' | 'prospeo' | 'anthropic' | 'openai' | 'mv' | 'do_droplet' | 'vercel' | 'supabase' | 'cloudflare' | etc.
  sku                  VARCHAR,                -- vendor item identifier: 'workspace_subscription', '.co_domain_renewal', 'inbox_monthly', etc.
  cost_unit            VARCHAR NOT NULL,       -- 'workspace' | 'domain' | 'inbox' | 'send' | 'enrichment_lookup' | 'platform' | 'service' | 'one_time'
  unit_count           INTEGER,                -- units this row covers (e.g. 1000 inboxes, 50 domains); NULL for fixed-fee
  total_usd            DOUBLE PRECISION NOT NULL,
  period_start         DATE NOT NULL,
  period_end           DATE NOT NULL,          -- same as period_start for one-time, or extended for monthly/annual
  amortize_method      VARCHAR,                -- 'monthly' | 'daily' | 'one_time' | 'annual_spread'
  attribution_dim      VARCHAR NOT NULL,       -- 'global' | 'workspace' | 'offer' | 'infra' | 'domain' | 'inbox' | 'channel'
  attribution_id       VARCHAR,                -- workspace_id / offer name / 'OTD' / domain string / inbox email; NULL for global
  source               VARCHAR NOT NULL,       -- 'stripe' | 'invoice_csv' | 'manual' | 'estimated' | 'reference_rate'
  source_ref           VARCHAR,                -- Stripe invoice ID, CSV filename, Slack message link, doc path
  notes                VARCHAR,
  _loaded_at           TIMESTAMPTZ NOT NULL,
  _run_id              VARCHAR
);
```

**Examples:**

| vendor | sku | cost_unit | unit_count | total_usd | period | attribution_dim | source |
|---|---|---|---|---|---|---|---|
| dynadot | .co_acquisition | domain | 1000 | 9000 | 2026-05-01..2026-05-31 (one_time) | global | invoice_csv |
| dynadot | .co_renewal | domain | 1000 | 9000 | 2026-05-01..2027-04-30 (annual_spread) | global | reference_rate |
| instantly | workspace_subscription | workspace | 1 | 600 | 2026-05-01..2026-05-31 (monthly) | workspace, id=cdae94c6-... | stripe |
| maxify | warmup_service | platform | 1 | 4500 | 2026-05-01..2026-05-31 (monthly) | infra, id='ac' | manual |
| anthropic | api_credits | service | NULL | 300 | 2026-05-01..2026-05-31 (monthly) | global | stripe |
| lucas | inbox_monthly | inbox | 5000 | 3000 | 2026-06-01..2026-06-30 (monthly) | infra, id='outlook_mailin_lucas' | estimated |

**Key design points:**

- **`source`** is the load-bearing field. `source IN ('stripe', 'invoice_csv', 'manual')` = actual. `source IN ('estimated', 'reference_rate')` = derived/stub. Every query can filter to "actuals only" when needed.
- **Estimates and actuals coexist.** If we have Stripe data for Instantly in May but no actuals for Lucas yet, the ledger has Stripe rows + estimated rows. Phase 2 ingest replaces estimates with actuals as invoices land. No schema change.
- **`amortize_method`** lets us spread a one-time cost across its earning period. A $9k domain registration covering 12 months gets amortized at $750/mo for the "burn rate this month" question.
- **`attribution_dim` + `attribution_id`** lets us roll up by any dimension: per-workspace, per-domain, per-offer (when offer is the attribution), or global.

### Layer 2 — Inline cost columns on canonical entities

Per Sam's explicit instructions, denormalized projections of `cost_ledger` onto the entity rows. Refreshed during the `derived` phase of the sync window.

| Entity | Column | Type | Notes |
|---|---|---|---|
| `core.campaign` | `cost_lead_acquisition_usd_estimated` | DOUBLE | per-lead acquisition cost: sum of upstream costs attributed to this campaign / leads_count |
| `core.sending_account` (Phase 3) | `cost_per_day_usd_estimated` | DOUBLE | monthly vendor cost ÷ 30 ÷ inboxes-in-workspace |
| `core.sending_account` (Phase 3) | `vendor_billing_cycle` | VARCHAR | 'monthly' / 'annual' / 'pay_as_you_go' — billing posture for forecasting |
| `core.domain` (Phase 3) | `cost_acquisition_usd_estimated` | DOUBLE | one-time at registration |
| `core.domain` (Phase 3) | `cost_renewal_annual_usd_estimated` | DOUBLE | annual recurring |
| `core.meeting` (Phase 3) | `cost_per_meeting_usd_estimated` | DOUBLE | derived: campaign cost / meetings booked in attribution window |
| derived view `v_campaign_daily_metrics_cost` | `cost_per_send_usd_estimated` | DOUBLE | per-send marginal cost |

**`_estimated` suffix is intentional.** It signals to agents reading the schema that the value is non-authoritative — possibly derived from reference rates or a stub. Future v1.5 columns without the suffix (`cost_per_meeting_usd_actual`) can sit alongside when real invoice-driven values land.

### Phase split

| Phase | What lands |
|---|---|
| **v1 (this spec)** | DDL for `core.cost_ledger`. Inline `_estimated` columns on existing canonical entities (`core.campaign` for now; sending_account/domain/meeting columns spec'd into 06/07/09 ahead of those builds). Seed `core.cost_ledger` with the known reference rates from the infra strategy suite (HTML §3 of `2026-05-30-infra-strategy-html-data-handoff.md`). |
| **v2** | Stripe API ingest writing actual rows to `core.cost_ledger` (replaces matching estimated rows by source+vendor+period). Manual invoice CSV ingest pipeline. |
| **v3** | Derivation logic that auto-refreshes inline cost columns from ledger on every nightly run. Until then, the inline columns hold the stub values seeded in v1. |
| **v4 (later)** | Scenario modeling views. Forecast tables. Revenue ingest. P&L rollups. |

The point: v1 ships the shape + enough seed data that a chatbot can already answer "what's our cost per inbox at OTD" (`SELECT total_usd / unit_count FROM core.cost_ledger WHERE vendor='otd' AND cost_unit='inbox' AND period_end >= current_date`) on day 1, even though no Stripe data has been ingested yet.

## Seed data for v1

Pulled from `handoffs/2026-05-30-infra-strategy-html-data-handoff.md` §3 + the 2026-05-30 Ido transcript:

**Domain costs:**
- `.co` renewal: $9/year/domain (Ido confirmed 2026-05-30)
- `.info` renewal: $9/year/domain (Ido confirmed 2026-05-30)
- `.com` renewal: ~$12/year/domain (typical; needs confirmation)

**Inbox costs (per the infra plan v11 channel table):**
- OTD: $548k/mo at 396,800 inboxes → ~$1.38/inbox/mo
- Google: $305k/mo at 113,600 inboxes → ~$2.69/inbox/mo (likely includes platform overhead)
- Folderly: $14k M1 → $25-52k Target; $5.25 per 1k sends pricing model
- Lucas (Milkbox): $3k M1 / $5k ongoing for 1k domains × 99 acct/domain → ~$0.05/inbox/mo (cheap)
- Tomer (MailIn): $5k M1 / $6.4k ongoing for 1k domains × 99 acct/domain
- Tucows: $0.75/mailbox/mo
- Maxify (AC): $4.5k/mo recurring for 25 domains × 4 acct/domain
- Warmly: $300 pilot / $13k full fleet

**Platform/service costs (rough; replace with actuals when Stripe ingest lands):**
- Instantly: per-workspace subscription (varies by plan)
- Supabase: pipeline-supabase, comms-orchestration, leads project (~$25-100/mo each)
- DigitalOcean droplet: ~$48/mo Premium-AMD tier
- Cloudflare workers: ~$5/mo
- Anthropic API: variable, currently ~$300-500/mo (AIM disabled has reduced spend)
- BlitzAPI: $599/mo Enterprise
- Enrichment vendors (LeadMagic/Findymail/Prospeo/Aleads): pay-as-you-go, ~$200-2000/mo total

These get loaded as `source='reference_rate'` rows in `core.cost_ledger` at v1 ship time. Each has a wide `period_end` (e.g., 2027-12-31) so they remain queryable until actuals replace them.

## Query patterns this enables

```sql
-- 1. Total infrastructure burn this month
SELECT SUM(total_usd) AS burn_mtd
FROM core.cost_ledger
WHERE period_start <= current_date
  AND period_end >= current_date
  AND attribution_dim != 'one_time';

-- 2. Cost per inbox by vendor (compare hedges)
SELECT vendor, total_usd / NULLIF(unit_count, 0) AS cost_per_inbox_monthly
FROM core.cost_ledger
WHERE cost_unit = 'inbox'
  AND period_end >= current_date
ORDER BY 2 ASC;

-- 3. Cost per meeting by offer (the load-bearing chatbot question)
SELECT c.offer,
       SUM(<attributed cost>) AS spend,
       COUNT(m.meeting_id) AS meetings,
       SUM(<attributed cost>) / NULLIF(COUNT(m.meeting_id), 0) AS cost_per_meeting
FROM core.campaign c
LEFT JOIN core.meeting m ON m.campaign_id = c.campaign_id
WHERE m.posted_at >= current_date - INTERVAL '30 days'
GROUP BY c.offer;
-- (cost attribution logic = derived view; v3 build)

-- 4. Burn rate trajectory (next 90 days at current commitments)
SELECT period_start, SUM(total_usd) AS monthly_burn
FROM core.cost_ledger
WHERE period_start >= current_date AND period_start <= current_date + 90
GROUP BY 1
ORDER BY 1;

-- 5. "What's the cheapest path to 50 more meetings/month?"
-- The chatbot reasons: rate per channel + KPI per channel from mission-control
-- + cost_per_meeting estimated for each → recommend the cheapest hedge
-- (no schema query needed; this is chatbot logic over the ledger + KPI table)
```

## Non-goals (v1)

- **No forecast / projection engine.** Numbers in `core.cost_ledger` are point-in-time facts. The chatbot can extrapolate; the warehouse doesn't.
- **No scenario branching tables.** "If Tucows scales to 5k inboxes" → chatbot SQL, not stored table.
- **No KPI threshold table** (yet). The mission-control HTML §03/§04 KPI tables stay as reference docs for v1. When chatbot queries depend on them, we add `core.kpi_threshold` (one-line table: channel × margin_pct → max_sends_per_meeting).
- **No revenue ingest.** Revenue is currently derivable: `meetings × $145`. Land actual revenue when Close opps surface deal sizes.
- **No P&L statement materialization.** Derived view at the end of v3 / v4.

## Things to NOT do (anti-patterns)

- Don't add cost columns to entities Sam didn't ask for (e.g. `core.workspace.cost_per_month` — workspace cost is in ledger with `attribution_dim='workspace'`).
- Don't build a Stripe ingest in v1. SHAPE only.
- Don't try to handle currency. USD only (all Renaissance vendors bill USD).
- Don't try to handle FX / hedging / accruals. This is bootstrapped OpEx, not corporate accounting.
- Don't try to dedupe cost rows across sources. If Stripe shows $600 for Instantly May and the estimated row also exists, the latest `_run_id` for the same `cost_id` (deterministic) wins via UPSERT. Both source types use the same `cost_id` formula so they collide cleanly.

## Open questions

- **Sam's actual current $/mo for each vendor.** The infra strategy HTML §3 has the at-scale numbers; the current-month actuals require either Stripe ingest or Sam pasting a list. For v1 reference_rate seeding, use the at-scale rates; flag as "rates pending actuals."
- **AC platform costs.** Maxify $4.5k/mo is the labor/SOP fee. Are there platform fees (ActiveCampaign subscription itself, per-account fees) separate from Maxify's retainer? Sam to confirm before seeding.
- **Folderly fee structure.** The $5.25/1k sends is what they bill, but does that include or exclude the $25/yr per-domain registration? Confirm before seeding.
- **Per-workspace Instantly subscription**, vs flat company-wide tier. Per memory and the Instantly UI, workspaces appear to be on different `plan` codes (we see `pid_hg_v1`, `pid_ls_v1`, `pid_free`). Each plan has a different $/mo. Need the mapping.
