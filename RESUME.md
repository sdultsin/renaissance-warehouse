# RESUME — renaissance-warehouse MVP build

> For a fresh chat picking up the MVP build. Read this + `SCHEMA.md` + `specs/00-architecture.md`. Then continue from "Next steps" below. **Go one command at a time** — ssh+duckdb output buffers a turn behind in some sessions; do not batch parallel Bash calls or you'll read stale results.

Last updated: 2026-05-31 (after Phase 3: sending_account + opportunity + DNS sweep + domain).

---

## What this is

Single-operator DuckDB warehouse on droplet `renaissance-worker` at `/root/core/warehouse.duckdb`. Repo: `sdultsin/renaissance-warehouse` (public). 3 layers: `raw_*` (append-only source snapshots) → `core.*` (canonical) → derived views. Nightly cron 03:30 UTC + backup 05:45 UTC. North star: consolidate every Renaissance data source into one queryable layer + dashboards. See `SCHEMA.md` for the LLM query interface.

## Verified state (as of 2026-05-31)

| Table | Rows | Status |
|---|---|---|
| core.workspace | 16 | ✅ solid |
| core.campaign | 337 | ✅ cm/offer/is_mca regex |
| core.campaign_sending_tag | 2,349 | ✅ |
| core.campaign_marker_tag | 0 | ⚠️ deferred — no public Instantly API ([[reference-instantly-tag-mappings-endpoint-missing]]) |
| core.meeting | 29,903 | ✅ source-confirmed (meetings since 2024-04-12) |
| **core.sending_account** | **1,546,032** (1,393,501 active) | ✅ NEW — from account_truth snapshot; esp/lifecycle/warmup; + state_event log |
| **core.opportunity** | **22,349** (4,034 non-dup) | ✅ NEW — lead-level warm-call (call_opportunity). **NOT lead_interested** (see below) |
| **core.domain** | **54,223** (52,221 swept + 2,002 acquired) | ✅ NEW — DNS fingerprint + blacklist + ns/cost |
| core.cost_ledger | 16 | ✅ reference rates (direct-infra only) |
| raw_pipeline_* (8 tables) | ~2M | ✅ via postgres_scanner |
| raw_comms_* (9 tables) | 66,929 | ✅ call_opp 22,349 / conversation 17,474 / ... |
| **raw_account_truth_accounts** | **1,555,427** | ✅ NEW — account_inventory copy-through (raw_json dropped) |
| **raw_dns_sweep_domain** | **52,221** | ✅ NEW — full DNS fingerprint, surbl/spamrl/spamhaus_dbl |
| **raw_blacklist_check** | **156,663** | ✅ NEW — append-only (domain×blocklist) event log |
| raw_sheets_* | 0 | ⚠️ loads 0 — seed-CSV format/path mismatch (GAP B2) |

### Phase-3 headlines (this build)
- **OTD domains 94.5% SURBL-listed**, Outlook 46%, Google 2.7% — the cheap-TLD carpet-bomb, now queryable per ESP.
- **Factor-3 visible:** 4 `dns_signature`s each cover >100 domains; 99 `/24` blocks host >50 domains each (homogeneous provisioning).
- **"opportunities" ≠ "interested"** (Sam): Instantly email opps are aggregate-only (`campaign_daily_metrics.unique_opportunities`), NOT lead-level. `core.opportunity` = lead-level warm-call/AIM only. See GAPS B8.
- Handoff overstated readiness: account_truth/DNS/domain entity files **did not exist** — built this session from specs 06/07/10.

## Next steps (in order)

1. **recipient_domain (spec 08) + ESP×ESP dashboard** — the remaining long pole. Needs an **MX sweep** (`sources/dns.py resolve_mx`) over recipient domains pulled from the lead-mirror duckdb, → `core.recipient_domain` (MX→ESP classification). Then the ESP×ESP matrix = `contact_frequency_campaign_daily` (pipeline-supabase, 25M rows — NOT mirrored; query via psycopg2/postgres_scanner) × `core.recipient_domain` MX × `core.domain`/`sending_account` sender ESP. Another ~hour-class sweep — run in background tmux like the sender sweep.
2. **Layer 4 dashboards / derived views**: ESP×ESP, the 5 dip-factor surfaces (factor-3 via dns_signature/a24 + factor-1 copy via Funding-4 cluster + blacklist-by-ESP all ready in `core.domain`), cost-per-X rollups, warmup/lifecycle board. + the Instantly opp KPI view over campaign_daily_metrics. + chatbot-over-SCHEMA.md.
3. **sending_account lifecycle transitions (GAP B7)** — nightly snapshot-diff of `raw_account_truth_accounts` to emit warmup/pause/retire events into `core.sending_account_state_event` (only `created` seeded today).
4. **infra_provider vendor brand (GAP B6)** — RG-tag→vendor map to fill MailIn/Reseller/Folderly/etc. on `core.domain` + `core.sending_account` (only OTD resolved now).
5. **factor-5 redirect pass (GAP F1)** — targeted redirect-chain sweep over web-serving domains only (dropped from the bulk sweep for speed).
6. **sheets loader fix** (low priority, GAP B2) — reference-only.
7. **Raw-table compaction (GAP E2)** + the post-MVP **data QA pass (GAP E3)**.

> Note: the `dns_sweep` phase runs `include_redirect=False`, `qps=64`, blocklists surbl/spamrl/spamhaus_dbl. ~38 min for 52k domains. It holds the DuckDB write lock — run it (and any future MX sweep) in a background tmux BEFORE the 03:30 UTC cron, never during. Test knobs: `DNS_SWEEP_LIMIT`, `DNS_SWEEP_QPS`, `DNS_SWEEP_REDIRECT`.

## Known issues / gotchas (learned the hard way)

- **DuckDB single-writer lock.** Only one writer to warehouse.duckdb. Agents test on `/tmp` temp DBs; integrate serially.
- **DuckDB cannot parameterize ATTACH.** Use f-string: `ATTACH '{url}' AS pg (TYPE postgres, READ_ONLY)`. (Bit comms_mirror.)
- **PhaseResult signature** = `PhaseResult(rows_in, rows_out, notes)`. Not name/rows_written/ok. (Bit comms_mirror.)
- **DuckDB file name ≠ schema name.** File is `warehouse.duckdb`, schema is `core`. Same name → BinderException.
- **Raw tables are append-only across runs** (DELETE only by same _run_id). Re-running a mirror appends a new snapshot. Canonical layers must filter to latest _run_id OR dedup by logical key. (comms had to be de-duped after debug re-runs.)
- **Orchestrator has NO MCP access** — it's a plain Python cron job. Google Sheets can't be MCP-pulled at runtime; needs Google API creds or seed CSVs. (sheets gap.)
- **ssh+duckdb output buffers a turn behind in some sessions.** One command at a time; wait for each result.
- **account_truth duckdb is locked** while Sam's sending-volume-truth job runs (~1hr periodically). Don't read it then.
- **postgres_scanner is the fast mirror pattern**: `INSTALL postgres; LOAD postgres; ATTACH ...; INSERT INTO raw_X (...) SELECT ..., now(), ? FROM pg.public.X`. 2M rows in 60s. CAST jsonb/enum/array cols to VARCHAR.

## Locked decisions

- **Excluded from MVP:** Close CRM (no data yet), GBC (no access), Outreachify (rebuild own deliverability via DNS sweep), software subscriptions in cost_ledger (droplet/supabase/blitz/cloudflare/anthropic/enrichment — "else we'd track 50 platforms"). Direct infra only: domains, inboxes, vendor pilots, AC platform.
- **ESP×ESP via `contact_frequency_campaign_daily`** (25M rows, actual contacts) × `recipient_domain` MX — better than Instantly membership snapshots (which include uploaded-but-unsent). Membership = v2 only.
- **No-middleman is the v2 end state.** MVP mirrors from pipeline-supabase etc.; raw→canonical split means swapping to direct syncs later won't touch canonical.
- **Workspace renames:** env keys + pipeline use OLD slugs (koi-and-destroy); Instantly API returns NEW (funding-4). workspace_id UUID stable. core.workspace.slug picks the pipeline-compatible slug for clean joins.
- **Cost: SHAPE now, numbers later.** cost_ledger seeded with reference rates (`source='reference_rate'`); Stripe/invoice actuals = Phase 2, replace by same cost_id. Batch-cost pattern: 1 ledger row per bulk purchase (e.g. 14,978 .co @ $1.80 = $26,960) + acquisition_batch tag on each domain.

## How to run

```bash
# Manual sync (all phases):
ssh renaissance-worker 'cd /root/renaissance-warehouse && source .venv/bin/activate && python -m core.orchestrator'
# Single phase:
ssh renaissance-worker 'cd /root/renaissance-warehouse && source .venv/bin/activate && python -m core.orchestrator --phase <name>'
# Read-only query:
ssh renaissance-worker 'duckdb -readonly /root/core/warehouse.duckdb "<sql>"'
# Sync local repo -> droplet:
cd "/Users/sam/Documents/Claude Code/Renaissance" && rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='*.duckdb' --exclude='.venv' renaissance-warehouse/ renaissance-worker:/root/renaissance-warehouse/
```

PHASE_ORDER (core/config.py): pipeline_mirror, comms_mirror, outreachify(unused), instantly, sheets, account_truth, dns_sweep, canonical, derived.

## GAPS accumulating (fold into GAPS.md)

- core.campaign_marker_tag empty (no public Instantly tag-mappings API; MCP-only).
- sheets per-domain inventory (125k rows) + blacklist sheet not captured (too big for MCP paging); summary tabs only.
- core.meeting cm NULL on ~60% (Slack posts without campaign/CM signal — raw data quality).
- ai_decision_log.cost_usd ~3x overstated historically (Sonnet/Haiku pricing bug); recompute from token counts.
- meetings_booked_raw has ~2 rows per logical meeting in some snapshots (dedup handles it; verify no semantic dupes under different keys).
- Google API creds needed for automated sheet refresh (orchestrator can't use MCP).
- Cost actuals: per-vendor current $/mo, AC platform fees vs Maxify retainer, Folderly tier, per-workspace Instantly plan pricing — all pending Sam/vendor numbers.
