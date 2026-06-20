# GAPS — renaissance-warehouse

> Honest register of what we could NOT sync, could not sync *accurately*, or that lives only in a human's head at the company (not in any platform). Per Sam's mandate: surface gaps rather than paper over them. He'll answer or route each.
>
> Status legend: **OPEN** (needs Sam / someone's answer) · **KNOWN-LIMIT** (accepted tradeoff, documented) · **DEFERRED** (real, scheduled for later) · **FIXABLE** (we can close it ourselves, just not done yet).

Last updated: 2026-05-31.

---

## A. Data we could not sync at all

### A1. Marker tags (the campaign badges, e.g. "AIM Active") — KNOWN-LIMIT
`core.campaign_marker_tag` is empty. The public Instantly REST API does not expose tag→campaign mappings (`/api/v2/tag-mappings` → 404). The Instantly MCP reaches a private/admin endpoint we haven't reverse-engineered. These badges are how Sam marks which campaigns are AIM-enabled (and other states), so this is real intel we're missing.
**To close:** Instantly support ticket asking for the public-API equivalent of tag-mappings, OR reverse-engineer the MCP's endpoint, OR a headless-browser pull (brittle).
**Owner:** Sam (support ticket).

### A2. Outreachify deliverability data — DEFERRED (deliberately out of MVP)
The 61-object Outreachify star schema (reply-time MVs, domain health, etc.) is not mirrored. Decision: rebuild our own deliverability layer via the DNS/blacklist sweep instead of depending on theirs. Anything genuinely unique they compute, we sync ourselves later, directly from source (no-middleman principle).
**Owner:** us, v2.

### A3. Close CRM + GBC — DEFERRED (deliberately out)
Close: no data in it yet ("in a week it will"). GBC: no access. Both excluded until they have data / we have access.
**Owner:** Sam (tell us when Close has data / GBC access exists).

---

## B. Data we synced but NOT accurately / incompletely

### B1. Domain Tech Sheet + blacklist sheet — only summary tabs captured — FIXABLE
We captured the small summary tabs (~98 rows) but NOT the per-domain inventory (~125k rows) or the full blacklist sheet — they were too large to page through the Google Sheets MCP, and the orchestrator has no MCP access at runtime anyway (it's a plain Python cron). The sheet loader currently loads 0 rows (format/path mismatch — see B2).
**Mitigation:** per Sam, the sheet is a *rough guide, never source of truth* — per-domain truth comes from Instantly + the DNS sweep. So this is low-stakes for accuracy, but the inventory counts (used/not-used per registrar account) are useful context we don't have in-warehouse yet.
**To close:** Google API service-account creds so a sync job can pull the full sheets directly (not via MCP). Then ingest the full Domains(Table) inventory.
**Owner:** Sam (Google API creds) + us (loader).

### B2. sheets loader broken — FIXABLE (low priority)
`entities/sheets_mirror.py` expects `row_index,row_json` CSVs in `/root/core/sheets_staging`; the seed CSVs are raw exports in `seed_data/sheets/`. Loads 0 rows. Reference-only data, so deferred, but should be fixed for completeness.
**Owner:** us.

### B3. `ai_decision_log.cost_usd` ~3× overstated historically — KNOWN-LIMIT
The AIM decision log's `cost_usd` was computed with Sonnet pricing when the calls were actually Haiku (~3× overstatement) for historical rows. We mirror the value verbatim. **Any cost/spend view reading AIM cost must recompute from `input_tokens`/`output_tokens`** (token counts are trustworthy), not trust `cost_usd`.
**Owner:** us (handle in the derived layer).

### B4. `core.meeting` cm ~60% NULL — KNOWN-LIMIT (raw data quality)
~60% of meetings have no `cm` because the Slack success-channel post had no campaign match and no recognizable CM name token. Not a bug — the signal isn't in the source. Campaign-join + raw-text-regex fallback already applied.
**To close (partial):** richer campaign-name aliasing, or ask CMs to include a tag in success posts going forward (process change, not code).
**Owner:** Sam (process) — optional.

### B5. `meetings_booked_raw` ~2 rows per logical meeting in some snapshots — KNOWN-LIMIT
Source has duplicate snapshots; `core.meeting` dedups by `(channel_id, message_ts, line_index)` → 29,903 distinct. Worth a one-time check that there are no *semantic* duplicates under *different* keys (same meeting posted twice with different line_index). Low risk.
**Owner:** us (QA pass).

### B6. `infra_provider` vendor-brand unresolved for non-OTD inboxes/domains — FIXABLE
`core.sending_account.infra_provider` is populated only for **OTD** (provider_code=1). For Outlook/Google inboxes the *vendor brand* (MailIn / Reseller / Folderly / Tucows / Maxify / Warmly) is NOT in the account_truth snapshot — it needs resolution from the sending-tag (RG####) → vendor mapping or the domain tech sheet. ~1.21M Outlook + 32k Google inboxes currently have `infra_provider = NULL`. `esp` (google/outlook/otd) IS resolved.
**To close:** build the RG-tag → vendor-brand lookup (sheet B1 has it) and/or NS/registrar → vendor inference; backfill on `core.domain` then inherit to `sending_account`.
**Owner:** us (after sheet ingest / tag-vendor map).

### B7. `core.sending_account` lifecycle transitions are NULL in v1 — KNOWN-LIMIT
Only `created_at` (and `retired_at` for gone inboxes) is observed. `warmup_started_at`, `warmup_completed_at`, `rampup_*`, `paused_at` are NULL — a single account_truth snapshot can't see *when* an inbox changed phase. `rotation_state` is always NULL (not in account_truth). The active-vs-ramping distinction needs `daily_limit_used` (Instantly `/accounts` supplement, not ingested in v1 — hang-prone per `feedback_instantly_list_accounts_serial_only`). So warmed+active inboxes all read `lifecycle_state='active'`.
**To close:** nightly snapshot-diffing — compare successive `raw_account_truth_accounts` runs, emit transition events into `core.sending_account_state_event` (the seed currently holds only `created` events). Fills in over time once 2+ snapshots accumulate.
**Owner:** us (v1.1 snapshot-diff job).

### B8. `core.opportunity` — "opportunities" ≠ "interested"; no lead-level Instantly opp source — RESOLVED (design) / KNOWN-LIMIT
**Corrected 2026-05-31 (Sam):** "opportunities" is NOT Instantly lead-status `lead_interested`. The first build sourced the Instantly side from `lead_events WHERE event_type='lead_interested'` (20,958 events) — **wrong signal, removed.** core.opportunity now sources lead-level records only from the warm-call/AIM `raw_comms_call_opportunity` table (source-aware: ~22k sendivo + 9 instantly).
**The limit:** Instantly EMAIL opportunities (the dominant dashboard KPI: opportunities→meetings) exist ONLY as the aggregate `raw_pipeline_campaign_daily_metrics.opportunities` (per campaign×day). There is **no populated lead-level Instantly opportunity table** in the mirror — `opportunity_webhook_log` is empty (0 rows), and `lead_events` only carries lead-*status* events. So the Instantly opp KPI is served by the aggregate metric (query `campaign_daily_metrics`, prefer `unique_opportunities`), not core.opportunity.
**To close (optional):** if lead-level Instantly opportunity records are ever wanted, either populate `opportunity_webhook_log` (the Instantly opportunity webhook → pipeline) or pull Instantly's leads API by interest status. Until then, Instantly opps are aggregate-only by design.
Cross-source dedup (same prospect via email AND SMS) remains v1.5. Sendivo's own dupes are surfaced via `state='duplicate'` (18,315 of ~22k; filter `state<>'duplicate'` for ~4,034 unique).
**Owner:** us (aggregate opp view) + Sam (if lead-level Instantly opps are ever needed → fix the webhook log).

### B9. `workspace_id` NULL for `warm-leads` + `the-dyad` inboxes — KNOWN-LIMIT
2,193 inboxes in `core.sending_account` carry `workspace_slug` but no `workspace_id`, because those two workspaces aren't in `core.workspace` (warm-leads was never ingested; the-dyad is a dead 401/402 workspace). `workspace_slug` is always present and is the reliable join key; only the UUID is missing.
**Owner:** us (add the two workspaces to core.workspace if they should be tracked) — likely a no-op (the-dyad is dead).

---

## C. Cost / financial unknowns (numbers, not structure)

The cost SHAPE is built; these are the actual numbers we're missing. All current cost rows are `source='reference_rate'` (estimates) — flagged NEEDS-ACTUAL. Track forward, don't backfill.

### C1. Current per-vendor monthly actuals — OPEN
We have at-scale reference rates, not what's being paid *this month*. A one-line list from Sam ("paying vendor X $Y this month") → add as `source='manual'` rows → "current burn" becomes accurate instead of derived-from-target.
**Owner:** Sam.

### C2. ActiveCampaign platform fees vs Maxify retainer split — OPEN
Seeded AC at $1k/mo @ 1M emails (pilot) → $15k/mo @ 10M (Sam estimate). Maxify (Daniyal) retainer is separately $4.5k/mo. Confirm both numbers + that they're truly separate line items.
**Owner:** Sam / AC invoice.

### C3. Folderly final tier pricing — OPEN
Seeded $14k M1 → $52k target from infra-plan-v11. Closing call (Mon/Tue) will give real numbers. Folderly's unit is IPs + domains + subdomains (not accounts) — different metric; may need a cost_unit adjustment.
**Owner:** Sam (closing call).

### C4. Per-workspace Instantly plan pricing — OPEN
Workspaces show different plan codes (`pid_hg_v1`, `pid_ls_v1`, `pid_free`) — each a different $/mo. Need the plan→price mapping to attribute Instantly subscription cost per workspace.
**Owner:** Sam / Instantly billing.

### C5. `.co` bulk price confirmation — OPEN
Seeded the May 19-20 batch at $1.80/domain (14,978 domains = $26,960.40) from Sam's recollection; not found in any platform/memory. Confirm the actual price.
**Owner:** Sam.

### C6. Lucas / Tomer / Tucows real quotes — OPEN
Inbox rates ($0.05 / $0.065 / $0.75) are derived from total-÷-count math or "OTD parity" assumptions, not vendor quotes. Confirm against actual contracts.
**Owner:** Sam / vendors.

---

## D. Human-knowledge gaps (only a person at the company knows)

### D1. comms jsonb internal structure — OPEN
`conversation.metadata`, `conversation.last_proposed_slots`, `phone_enrichment.raw_response`, `instantly_message.raw_payload` are jsonb stored as VARCHAR text. The internal key structure is undocumented — needs sampling or someone who knows the AIM worker code to define extraction.
**Owner:** whoever owns the comms-orchestration worker.

### D2. `conversation_state` enum ordering/meaning — OPEN
The state machine (cold_blast_sent → engaged → qualifying → app_link_sent → … → booked / escalated / opted_out / declined / stalled) — canonical ordering and transition meaning live in worker code, not in the data. Needed if we build a funnel view over SMS conversations.
**Owner:** comms worker owner.

### D3. `instantly_message` empty — OPEN
Local cache of Instantly email threads, populated by the thread-sync worker; currently 0 rows. Whether/when it backfills is an operational question.
**Owner:** Sam.

### D4. Account/domain lifecycle transition truth — PARTIAL
The lifecycle state machines (created→warming→…→active→retired) derive transitions from Instantly signals (warmup field, send-volume ratio, status). But the *real* moment something moved phases (esp. manual rotation on/off decisions) lives in operators' heads + the manual Inbox Hub. Our derived transitions are best-effort inferences until we have a cleaner signal.
**Owner:** Sam / Darcy / Toukir (the rotation operators).

---

## E. Structural / process gaps

### E1. Orchestrator has no MCP access — KNOWN-LIMIT
The nightly cron is plain Python; it can't use MCP tools (Google Sheets, etc.). Anything currently MCP-pulled needs a direct-API path (creds) to be automatable. Affects sheets (B1/B2).
**Owner:** us (build direct-API sync) + Sam (creds).

### E2. Raw tables bloat across runs — FIXABLE
Raw tables are append-only; re-running a mirror adds a full new snapshot. Over many nightly runs this grows unbounded. Need a retention/compaction policy (keep last N snapshots per raw table, or keep daily for 14d then weekly).
**Owner:** us (add a compaction step to the nightly run).

### E3. Data QA pass not yet done — DEFERRED (by design)
Per Sam: assume sources accurate during the build; QA accuracy as a separate pass *after* the MVP. **This is the gate before any dashboard number drives a real money decision.** Spot-check warehouse vs Instantly UI, pipeline, sheets across ~20 metrics.
**Owner:** us, immediately post-MVP.

### E4. No-middleman direct syncs not built — DEFERRED (v2)
MVP mirrors from pipeline-supabase / comms-orchestration (intermediaries). The v2 goal is direct syncs (our own GH Actions, CF workers pointed at us). The raw→canonical split makes this swap painless when we do it.
**Owner:** us, v2 (~2-4 weeks).

---

## F. DNS / domain sweep scope (v1 deliberate narrowing)

### F1. Redirect-chain (dip factor 5) deferred from the bulk sweep — DEFERRED
The full DNS+blacklist sweep runs with `include_redirect=False`. Reason: the HTTP HEAD redirect probe was **87% of per-domain wall time** (172s → 21s for 400 domains with it off) — our `.info`/`.co` sending domains mostly don't serve HTTP, so every probe waits out a connect timeout. With it off the full 52k sweep is ~47 min instead of ~6 h. **Factor-5 (redirecting-domains) visibility is therefore NOT yet populated** (`redirect_chain` / `terminal_redirect` columns are NULL).
**To close:** a separate, targeted redirect-only pass over the subset of domains that actually resolve an A record / serve HTTP (much smaller set), on its own cadence.
**Owner:** us (v1.1 redirect pass).

### F2. Blocklist set = the production "existing 3" only — KNOWN-LIMIT (by design)
The sweep checks `surbl`, `spamrl`, `spamhaus_dbl` (domain zones) — NOT the 5 new ones in spec 07 (Barracuda, SORBS, UCEPROTECT, SpamCop, URIBL). Per prep-notes 07a rec #1: SURBL is already 99.9% of listings and carpet-bombs cheap TLDs; add new zones one at a time *with N-day corroboration* before treating any as actionable, rather than importing 5 unvalidated noise sources. `spamrl` is likely moribund (0 listings in 51k prod queries); kept for continuity. Spamhaus via the plain DNS zone (not the DQS REST endpoint) — may under-report (guarded by FALSE_POSITIVE_IPS).
**To close:** add IP-based zones (need reversed A-record IP, which we now resolve) + Spamhaus DQS REST (`BLS_SPAMHAUS_DQS_KEY` is live in the blocklist-surveillance `.env`) one at a time, measuring baseline noise first.
**Owner:** us (v1.1, incremental).

### F4. Factor-3 DKIM-tenant + factor-4 brand-clustering signals incomplete — KNOWN-LIMIT
Two of the five dip-factor surfaces are only partially populated in `core.domain`:
- **`dkim_tenant_prefix` = NULL for all 52k domains.** The sweep chases `selector1._domainkey` for a `*.onmicrosoft.com` CNAME, but our Outlook/MailIn domains publish DKIM as direct **TXT** records (selector1/selector2), not an onmicrosoft CNAME — so no tenant prefix surfaces. The homogeneity *is* visible via `dns_signature` (4 signatures cover >100 domains each) + identical SPF Microsoft ranges; the tenant-prefix angle just doesn't apply to this provisioning style. (factor-3 is still covered, via dns_signature + a_record_24 — 99 /24 blocks host >50 domains each.)
- **`brand_prefix` = the full second-level label** (domain minus TLD), so each random-word `.co` (checklevelgroup, labmezzgo, …) is its own "brand" → 0 clusters >5. For *these* domains that's accurate (deliberate brand diversity = factor-4 mitigation working). Real factor-4 detection of historical brand families (numbered/stemmed variants like `brandname1/2/3`) needs a stem-based extractor, not TLD-strip.
**To close:** factor-4 stem clustering + (optional) a DKIM-TXT-content fingerprint to complement dns_signature.
**Owner:** us (derived-view refinement).

### F3. `.co` Max-OTD batch has no per-domain list — OPEN
The May 19-20 14,978-domain `.co` batch splits Lucas 1,001 / Tomer 1,001 / Max-OTD ~12,976. We have explicit per-domain CSVs for the 2,002 Lucas+Tomer domains (→ `ns_provider` + `acquisition_batch` + $1.80 cost attribution on those). The **12,976 Max-OTD remainder has no per-domain file**, so those `.co` domains can't be individually stamped with the batch tag / ns_provider=Max-OTD. The single `cost_ledger` batch row (14,978 @ $1.80) still exists at the aggregate.
**To close:** export the full Dynadot #9-15 `.co` registration list (minus the 2,002 named) → stamp the remainder as the Max-OTD cohort.
**Owner:** Sam (Dynadot export) + us (stamp).

---

## Top items needing Sam specifically

1. **C1** — rough current per-vendor monthly $ (makes burn accurate).
2. **C5** — confirm $1.80/.co.
3. **A1** — decide on Instantly support ticket for marker tags.
4. **B1/E1** — Google API creds (unlocks full sheet ingest + automation).
5. **C2/C3/C4/C6** — vendor cost actuals as they come in (forward-tracked, no rush).
