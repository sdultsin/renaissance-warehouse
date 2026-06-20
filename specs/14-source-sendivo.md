# 14 — Source: Sendivo (SMS send-side + cost)

**Phase:** 3 (post-MVP source addition)
**Status:** spec'd 2026-05-31. API validated live against our key (see §3). Not built.

## Goal

Bring Sendivo's **send-side** SMS data into the warehouse: outbound/inbound volume,
delivery rate, opt-out rate, response rate, campaign/brand status, and — the bonus —
**actual SMS spend**. This is the half of SMS we don't have today.

**Why it matters / what it fixes.** The warehouse currently mirrors *comms-orchestration*
(the AIM/reply layer: inbound replies → conversations → opportunities). It has **zero** of
Sendivo's sending metrics — the `SMS Performance` dashboard tile shows a conversation proxy,
not the real funnel. Concretely, comms.message has **52 outbound** rows (AIM was disabled),
while Sendivo actually sent **~2.6M SMS in the last 7 days**. The two systems are
complementary halves:

| Layer | System | In warehouse? |
|---|---|---|
| Send / delivery / cost | **Sendivo API** (this spec) | ❌ → this fixes it |
| Reply / conversation / opportunity / AIM | comms-orchestration (`raw_comms_*`, `core.opportunity`) | ✅ already |

After this lands, the SMS tile shows real delivery/opt-out/response rates, and `core.cost_ledger`
gets its **first non-estimated cost** (Sendivo invoices, `source='invoice'`).

## Architecture fit (how it stays clean)

Standard 3-layer flow, same conventions as every other source:
- **`raw_sendivo_*`** — append-only API snapshots. Every row carries `_loaded_at TIMESTAMPTZ`
  + `_run_id VARCHAR`. Never deleted; re-run appends a new snapshot; canonical/derived filter
  to the latest `_run_id` (or dedup by logical key + date). Identical pattern to `raw_pipeline_*`.
- **`core.*`** — Sendivo billing folds into the existing **`core.cost_ledger`** (no new canonical
  entity needed for cost). Optionally a thin `core.sms_number` later (phone numbers as sending
  assets, mirroring `core.sending_account`) — **v2, not now.**
- **derived** — a `v_sms_performance` view (daily/weekly funnel) that the dashboard reads.

New orchestrator phase **`sendivo`**, inserted in `PHASE_ORDER` (core/config.py) between
`comms_mirror` and `instantly`:
```
pipeline_mirror, comms_mirror, sendivo, instantly, sheets, account_truth, dns_sweep, canonical, derived
```
Files: `sources/sendivo.py` (HTTP client — mirrors `sources/instantly.py` shape: Bearer auth,
paginate, gentle pace) + `entities/sendivo.py` (the registered ingest, mirrors
`entities/pipeline_mirror.py`). Key from `SENDIVO_API_KEY` in `.env` (already present), pulled
via `ctx.credentials.require("SENDIVO_API_KEY")`.

## 3. API surface (validated 2026-05-31 against our key)

- **Base:** `https://app.sendivo.io/api/v1` · **Auth:** `Authorization: Bearer sdk_…`
- Key is **multi-sub-account** (agency-scoped). `/campaigns` returned **30 / Pending 16 /
  Approved 14** — exact match to the UI. `/delivery-metrics` (7d) returned 2.59M sent. Both 200.

| Endpoint | Use | Notes |
|---|---|---|
| `GET /delivery-metrics?start_date&end_date` | **headline funnel** | `sms_sent, segments_sent, inbound_sms_received, delivery_rate, opt_out_rate, response_rate`. **Max range 30 days** (422 if exceeded). Aggregate across the key's scope. |
| `GET /campaigns` | campaign roster + status | `id, name, status, brand{}, phone_numbers[], created_at`. Status = Carriers Pending/Approved/Rejected, Sendivo Review/Approved/Rejected. |
| `GET /brands` | brand registration | `verification_status, registration_state, campaigns_count, sub_account_id`. |
| `GET /phone-numbers` | sending assets | `number_status, messaging_status, campaign{}`. |
| `GET /sms/logs?start_date&end_date&per_page` | per-message detail | **HUGE** — 585,877 rows / 2 days, 5,859 pages @100. Carries `status_group_name` (DELIVERED/UNDELIVERABLE/REJECTED/PENDING/EXPIRED), `segments`, `price_per_message`, `campaign{}`. **Do NOT mirror wholesale.** |
| `GET /billing/report?start_date&end_date` | **actual cost** | per sub-account: `total_spend` + itemized `{campaign_setup, campaign_renewal, brand, phone_number_setup, phone_number_renewal, sms, carrier}_fees{quantity,total_spend}`. May = **$19,271**. |
| Webhooks (push): inbound_message, delivery_status, phone_number_ready | **v2** (real-time) | HMAC-SHA256 signed. The no-middleman path: point a CF worker → warehouse. Not v1. |

## 4. Raw tables (`sql/ddl/25_sendivo.sql`)

```sql
-- one row per (scope, date, _run_id). 'scope' = 'agency' in v1 (or sub_account_id when we
-- iterate sub-accounts). Daily grain so we never re-pull > the 30-day cap.
raw_sendivo_delivery_metrics(
  scope VARCHAR, metric_date DATE, sms_sent BIGINT, segments_sent BIGINT,
  inbound_sms_received BIGINT, delivery_rate DOUBLE, opt_out_rate DOUBLE, response_rate DOUBLE,
  _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)

raw_sendivo_campaigns(
  campaign_id BIGINT, name VARCHAR, status VARCHAR, brand_id BIGINT, brand_name VARCHAR,
  phone_numbers VARCHAR /*JSON*/, sub_account_id BIGINT, created_at TIMESTAMPTZ,
  _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)

raw_sendivo_brands(
  brand_id BIGINT, name VARCHAR, legal_company_name VARCHAR, verification_status VARCHAR,
  registration_state VARCHAR, campaigns_count INTEGER, sub_account_id BIGINT, created_at TIMESTAMPTZ,
  _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)

raw_sendivo_billing(
  sub_account_id BIGINT, location_id VARCHAR, period_start DATE, period_end DATE,
  total_spend DOUBLE, sms_fee_qty BIGINT, sms_fee_usd DOUBLE, carrier_fee_qty BIGINT, carrier_fee_usd DOUBLE,
  campaign_setup_usd DOUBLE, campaign_renewal_usd DOUBLE, brand_fee_usd DOUBLE,
  phone_setup_usd DOUBLE, phone_renewal_usd DOUBLE, raw_json VARCHAR,
  _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)

-- OPTIONAL / windowed only (recent N days) — NOT a full mirror. For the status-breakdown donut.
raw_sendivo_sms_logs(
  log_id BIGINT, sub_account_id BIGINT, from_number VARCHAR, to_number VARCHAR, segments INTEGER,
  status VARCHAR, status_group_name VARCHAR, status_name VARCHAR, error_description VARCHAR,
  price_per_message DOUBLE, campaign_id BIGINT, phone_number_id BIGINT, infobip_message_id VARCHAR,
  created_at TIMESTAMPTZ, _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR)
```

## 5. Canonical + derived

- **Cost → `core.cost_ledger`** (no new table). One row per (sub_account, fee_type, period):
  `vendor='sendivo'`, `cost_unit='send'` (sms_fees/carrier_fees) or `'service'` (setup/renewal/brand),
  `total_usd=<fee>.total_spend`, `unit_count=<fee>.quantity`, `period_start/end`, `attribution_dim='channel'`,
  `attribution_id='sms'`, **`source='invoice'`**, `source_ref='sendivo billing/report'`. This is the
  first real cost — it coexists with the reference-rate rows (the `source` column distinguishes; same
  `cost_id` formula means it never duplicates).
- **`v_sms_performance`** (derived view): per-day/week funnel from `raw_sendivo_delivery_metrics`
  (latest `_run_id` per date), plus a status breakdown rolled up from `raw_sendivo_sms_logs` when present.
  The dashboard `SMS Performance` tile reads this instead of the AIM conversation proxy.
- **Relationship to existing SMS data (document, don't merge):** Sendivo send metrics (this) and the
  comms-orchestration reply/opportunity layer (`core.opportunity` source='sendivo') describe the same
  funnel from two ends. v1 keeps them side by side; a unified `core.sms_campaign` reconciling
  Sendivo `campaign_id` ↔ comms `brand`/`sendivo_campaign_id` is a v2 nicety (the IDs exist to join later).

## 6. Sync mechanics

- **delivery-metrics:** daily incremental — each run pulls *yesterday* (1-day window) → one row/day,
  appended. Backfill: loop **30-day windows** back to Sendivo's retention start (probe; unknown).
  Never request > 30 days (422).
- **campaigns / brands / phone-numbers:** full snapshot each run (small — 30 campaigns, handful of brands).
- **billing:** monthly window (per calendar month, per sub-account — the response is already an array
  keyed by sub_account). Re-pull the current + prior month each run (figures settle).
- **sms/logs:** v1 = **windowed, recent only** (e.g. last 2–3 days) purely to compute the
  delivered/failed/blocked breakdown, OR skip entirely and derive delivered ≈ `sms_sent × delivery_rate`.
  **Never** paginate the full history (2.6M/week × 5,859 pages is a non-starter). Decide at build time
  whether the donut is worth even the windowed pull.
- **Pace/limits:** unknown rate limit — reuse the `sources/instantly.py` gentle-serial pattern
  (curl-style UA not needed; standard Bearer). Backoff on 429/5xx.

## 7. Definition of done

1. DDL `25_sendivo.sql` applied; `sources/sendivo.py` + `entities/sendivo.py` registered under phase `sendivo`.
2. `raw_sendivo_delivery_metrics` populated daily; backfilled to retention start.
3. `raw_sendivo_campaigns` matches the UI (30 / 16 / 14) — a built-in correctness check.
4. `raw_sendivo_billing` populated; **`core.cost_ledger` shows real Sendivo `source='invoice'` rows.**
5. `v_sms_performance` view returns delivery/opt-out/response by day & week.
6. `scripts/dashboard_data.py` SMS section switched from the AIM proxy to `v_sms_performance`;
   the Decision-Matrix/Overview `SMS Performance` tile shows real send metrics.
7. `SCHEMA.md` + `GAPS.md` updated; nightly cron runs the `sendivo` phase clean.

## 8. Things NOT to do

- **Don't mirror `/sms/logs` in full.** Aggregate-or-window only. It's the trap.
- **Don't build the webhooks** (real-time delivery/inbound) in v1 — pull is enough; webhooks are the v2 no-middleman path.
- **Don't merge** Sendivo send data with the comms reply layer yet — keep both raw, reconcile in v2.
- **Don't re-pull > 30 days** from delivery-metrics in one call.
- **Don't drop the reference-rate cost rows** when invoices land — `source` distinguishes them; the chatbot needs both.

## 9. Open questions (resolve at build)

1. **Sub-account scope.** Does the key see *all* sub-accounts, or the 3 (Renaissance 1/2/3) the memory notes? `/delivery-metrics` returned a single agency aggregate (2.59M/7d) while the UI screenshot showed 60,772 — so the screenshot was a *filtered* view. Do we want per-sub-account delivery-metrics (is there a `sub_account_id` param?) or is the agency aggregate fine? Billing is already per-sub-account.
2. **Retention depth.** How far back does Sendivo serve delivery-metrics / logs? (probe — sets backfill bound).
3. **Donut worth it?** Is the delivered/failed/blocked breakdown worth the windowed `/sms/logs` pull, or is `delivery_rate` enough?
4. **Cost attribution grain.** SMS cost as a single `channel='sms'` line, or split by sub-account / brand for per-CM attribution?

## Cross-references
- API doc (Sam's UI export): Google Doc `1Z4O-FRs8Mh9c_2Cn-1usfAiu54SnWjr-6RErB_cyG-8`.
- Comms reply layer: `entities/comms_mirror.py`, `core.opportunity` (source='sendivo'), spec 10.
- Cost model: spec 13 (`13-financial-data-architecture.md`), `core.cost_ledger`.
- Client patterns to copy: `sources/instantly.py` (auth+paginate), `entities/pipeline_mirror.py` (snapshot+append).
- Memory: `reference_comms_platforms_and_meeting_source` (SMS=Sendivo, key in .env).
