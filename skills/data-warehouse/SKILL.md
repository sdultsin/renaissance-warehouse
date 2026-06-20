---
name: data-warehouse
description: Load how to ACCESS the new DuckDB warehouse (read-only, multi-user, via a read-only HTTP query API) and the MASTER PROMPT for navigating + querying it correctly. Use when the user says "/data-warehouse", "query the warehouse", "warehouse access", "how do I read the DuckDB warehouse", "connect to the data warehouse", "flag it" on a warehouse number, or asks for sends/opps/replies/meetings/ESP/SMS numbers from the Renaissance warehouse. Self-contained and shareable with other people/chats.
---

# /data-warehouse — Renaissance DuckDB warehouse: READ access + master navigation prompt

> ## ⚠️ SUPERSEDED for ACCESS + the read/write model — load `warehouse-access` instead
> The single source of truth for **warehouse access** (both READ and WRITE/edit, the doctor, tokens,
> and the current apply model) is now the **`warehouse-access`** skill. **This file is now ONLY a
> query-navigation / anti-hallucination reference** — how to write a correct read query and not
> fabricate numbers. For "how do I connect / get a token / fix setup / edit the warehouse," load
> `warehouse-access`, not this.
>
> **Two things this file used to imply that are NO LONGER TRUE — read this so you don't repeat them:**
> 1. **The warehouse is NOT read-only-forever.** The query API *you* use is read-only **by design**
>    (that's correct), but the warehouse itself is **writable** through the **Schema Moderator
>    pipeline** (author → `moderator_client.py loop` → record → apply). Writers (David/Darcy/Thomas/
>    Sam) edit DDL/views/columns/syncs. "read_only" in a query response means *this query path* is
>    read-only, **not** that the warehouse can't be changed.
> 2. **Changes are NOT nightly-only.** A recorded change applies on the ~03:30 UTC nightly **by
>    default, OR** a writer can run `moderator_client.py apply-now` to make it **live (applied +
>    promoted to readers) in minutes**. So a served `snapshot_id` can advance off-nightly.
>
> Everything below about **reading correctly** (Sections B/C/D and the table map) is still valid and
> current — use it. Anything about Lens / cutover history is historical context only.

This skill makes anyone (any person, any chat) able to **read the Renaissance DuckDB warehouse correctly**.
Read **Section A** to connect, **Section B** to query the right thing, **Section C** for the anti-hallucination
rules (non-negotiable), **Section D** to flag a problem, **Section E** to read a stable snapshot.

> **What the "new DuckDB warehouse" is:** `serving-mcp` — a **read-only, multi-user snapshot + serving
> layer** in front of the droplet DuckDB file. The writers (the nightly pipeline + the moderator
> apply path) and readers never share a file:
> readers query an **immutable, validated snapshot** opened `read_only`, so many people can read concurrently
> with **no locking** and no kill-9 corruption. **It is NOT MotherDuck / not a managed cloud** — PII stays
> self-hosted on the droplet. The primary read path is a **read-only HTTP query API**; an MCP server is an
> optional secondary client for agents. (Full status: `deliverables/2026-06-14-new-duckdb-warehouse-status.md`.)

---

## ✅ CURRENT REALITY — read this first

The serving layer is **LIVE** and is the primary read path. **Use Section A.1 (the HTTP query API).**
A.3 (interim direct DuckDB) is only a break-glass fallback if the API is ever down. (History: the
serving layer cut over 2026-06-14; the old Lens read layer is decommissioned — ignore any Lens
reference. **For WRITING/editing the warehouse, see the `warehouse-access` skill — it is writable via
the moderator pipeline, with apply-now for live-in-minutes; this file is read-navigation only.**)

**URL status (UPDATED 2026-06-15):** there is now a **permanent, stable URL** —
**`https://renaissance-droplet.tailae5c80.ts.net`** — served via a **Tailscale Funnel** on the droplet
(port 8899, valid Let's Encrypt cert, **no DNS records**). It is **stable across cloudflared/droplet
restarts** and is the URL to put in scripts/configs and share. The old anonymous cloudflared quick-tunnel
still runs alongside as a fallback but **rotates on restart — don't use it**.

To verify it's live (the stable URL is permanent — no lookup needed):
```bash
curl -sS https://renaissance-droplet.tailae5c80.ts.net/healthz   # -> {"ok":true,"read_only":true,"snapshot_id":...}
ssh renaissance-worker 'ls /opt/duckdb/CUTOVER_DONE >/dev/null 2>&1 && echo LIVE || echo "NOT LIVE — use A.3"'
ssh renaissance-worker 'tailscale funnel status'                 # confirms the funnel ingress is on
```

---

## SECTION A — ACCESS (read-only, from any computer)

**Order of preference: A.1 query API (primary) → A.2 MCP (optional, for agents) → A.3 interim direct read (until cutover).**

### A.1 — PRIMARY: the read-only HTTP query API (LIVE)

The warehouse is read via a **read-only HTTP query API**: you POST SQL, you get back JSON with the rows and
the `snapshot_id`. Works from anything that can make an HTTPS request (curl, a script, any agent) — no DB
driver, no DuckDB install, no local file.

1. **Base URL — PERMANENT:** `https://renaissance-droplet.tailae5c80.ts.net` (Tailscale Funnel, valid cert,
   stable across restarts — put this in scripts/configs; no lookup needed).
   - *(Legacy fallback only — an anonymous cloudflared quick-tunnel still runs but its URL **rotates on
     restart**, so don't use it. If ever needed:
     `ssh renaissance-worker "grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' /opt/duckdb/logs/mcp-tunnel.log | tail -1"`.)*
2. **Auth — STANDING SERVICE TOKEN (no minting, no asking Sam).** Send `Authorization: Bearer <TOKEN>`.
   As of **2026-06-18** there is a permanent shared **read-only** reader labeled **`cc-service-reader`**, so
   **no Claude Code chat ever needs Sam to mint a token again.** Resolve the token in this order — the **last
   step ALWAYS works** for any chat that has droplet SSH (the bootstrap capability Sam set up):
   1. **`$WAREHOUSE_API_TOKEN`** if it's already in your environment → use it.
   2. else read it from the Renaissance repo **`.env`** (keys `WAREHOUSE_API_URL` / `WAREHOUSE_API_TOKEN`):
      ```bash
      TOK=$(grep '^WAREHOUSE_API_TOKEN=' "/Users/sam/Documents/Claude Code/Renaissance/.env" | cut -d= -f2-)
      ```
   3. else **self-serve over SSH** (the token file is root-readable and you SSH as root — never fails):
      ```bash
      TOK=$(ssh renaissance-worker "awk -F'\t' '\$2==\"cc-service-reader\"{print \$1}' /opt/duckdb/allowed_tokens.txt")
      ```
   This token is **read-only by construction** (write is physically impossible — see below) and scoped to the
   warehouse only, so it's fine to keep in the **local-only** repo `.env`. Do **NOT** paste it into a
   *shareable* doc, a commit that leaves Sam's machine, or the `sdultsin/warehouse-flags` repo. Revoke = delete
   that line from `/opt/duckdb/allowed_tokens.txt` (instant). Per-person human tokens via
   `add-reader.sh "<name>"` still exist; **chats use `cc-service-reader`.**
3. **Endpoints:**
   - `POST /query` — body `{"sql": "<read-only SQL>"}` → `{columns, rows, row_count, execution_ms, snapshot_id, truncated, note}`.
   - `GET /schema` → live tables/columns of the served snapshot (introspected, never hardcoded).
   - `GET /guide` → the always-current navigation prompt (the served copy of Section B's source).
     **Fetch this first in a fresh session** — it's the live guide; Section B is the cached human copy.
   - `GET /healthz` (unauthenticated) → `{ok, read_only, snapshot_id}` liveness.
4. **Call it** (verified working 2026-06-14):
   ```bash
   BASE="https://renaissance-droplet.tailae5c80.ts.net"; TOK="$WAREHOUSE_API_TOKEN"   # token from env, never inline
   curl -sS -X POST "$BASE/query" \
     -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
     -d '{"sql":"SELECT max(date) AS d FROM raw_pipeline_campaign_daily_metrics"}'
   # → {"columns":["d"],"rows":[["2026-06-13"]],"row_count":1,"execution_ms":11.3,
   #    "snapshot_id":"warehouse_20260614_231024_347.duckdb","truncated":false,"note":""}
   ```
   **Always read `snapshot_id` off the response and record it next to any finding** (Section C requires it).

Read-only is enforced physically on **this query path** (`duckdb.connect(..., read_only=True)`, asserted)
+ a SQL guard (only `SELECT`/`WITH`/`PRAGMA`/`DESCRIBE`/`EXPLAIN`/`SHOW` allowed). You **cannot** write
*through the query API* — by construction. (This is the read path being read-only **by design**; the
warehouse itself **is** writable via the Schema Moderator pipeline — see the `warehouse-access` skill.)

> NOTE on transport: the deployed server is a single Starlette/uvicorn app exposing **both** the plain REST
> routes above (`POST /query`, `GET /schema`, `GET /guide`) **and** the MCP endpoint at `/mcp` (A.2) — same
> bearer auth, same read-only enforcement, same `snapshot_id`. The **REST query API (this section) is the
> primary, lowest-friction way to read** (any HTTP client, a one-line curl). Use A.2 only if you specifically
> want a native MCP tool surface in an agent.

### A.2 — SECONDARY (optional, for agents): the read-only MCP server

For Claude Code agents that prefer a native tool surface, the same warehouse is exposed as a remote MCP
server (streamable-HTTP). Same endpoint URL (append **`/mcp`**), same per-person bearer token (A.1 step 2).

Add it to your Claude Code (e.g. `~/.claude/settings.json`):
```jsonc
{
  "mcpServers": {
    "renaissance-warehouse": {
      "type": "http",
      "url": "https://renaissance-droplet.tailae5c80.ts.net/mcp",
      "headers": { "Authorization": "Bearer <YOUR_TOKEN>" }
    }
  }
}
```
Tools (identical semantics to the API endpoints):
- `query(sql)` → `{columns, rows, row_count, execution_ms, snapshot_id, truncated, note}`.
- `get_schema()` → live tables/columns of the served snapshot.
- `get_query_guide()` → the live navigation prompt. **Call it first in a fresh chat.**

MCP is **optional/secondary** — prefer the API (A.1) unless you specifically want the MCP tool surface.

### A.3 — FALLBACK ONLY (API/MCP are live; use this only if they're down): direct read-only DuckDB on the droplet

**Post-cutover, A.1 is the path — use this only as a break-glass fallback** if the query API is unreachable.
The warehouse data is fully queryable read-only on the droplet:
```bash
ssh renaissance-worker
duckdb -readonly /root/core/warehouse.duckdb
```
This opens the live writer file **read-only** (the same data the first snapshot is taken from).

⚠ **Caveat (the exact reason the new warehouse exists):** this IS the file the writer mutates. During a
nightly/pipeline write you can hit the single-writer lock, and a read during a mid-rebuild is a **non-pinned,
possibly-inconsistent** view. **For trustworthy QA, read during a writer-idle window** — avoid **03:30–05:45
UTC** (nightly) and any active bulk load. The API/MCP (A.1/A.2) remove this caveat entirely (Section E).
**There is no `snapshot_id` in interim mode** — when citing a number (or filing a flag), record
`interim: direct /root/core/warehouse.duckdb @ <UTC time>` in its place.

> Do **not** use Lens for trustworthy numbers — it's the thing serving-mcp replaces, and its KPI views had
> the integrity bugs the 2026-06-13 audit fixed.

---

## SECTION B — MASTER NAVIGATION PROMPT (how to query correctly)

Paste/internalize this when querying the warehouse. Source of truth that auto-updates:
`GET /guide` (API) / `get_query_guide()` (MCP), and `deliverables/warehouse-query-prompt.md` +
`deliverables/2026-06-13-warehouse-audit/METRIC-DICTIONARY.md` (local). **The Instantly API is ground truth;
when the warehouse disagrees, the API wins and the warehouse is the bug** (→ flag it, Section D).

### B.0 You are
You answer NL questions about Renaissance cold-email + SMS performance by writing **read-only SQL** against
the DuckDB warehouse. Return the **canonical number per metric** using the source/grain/filter pinned below,
and refuse the known traps.

### B.1 Schema / table map (layers)
- **`raw_*`** — mirrored upstream facts, the COMPLETE sources. **`raw_pipeline_campaign_daily_metrics`** =
  the canonical sends/opps/replies fact (campaign×date, additive, 45-day rolling window + frozen older rows).
  `raw_pipeline_campaigns` = campaign→workspace→cm_name dimension (use `DISTINCT ON (campaign_id) … ORDER BY _loaded_at DESC`).
- **`core.*`** — modeled tables. `core.meeting` (one row per booking, `campaign_id` = authoritative attribution),
  `core.sending_account_daily` (account×date, the ONLY ESP/OTD-splittable surface), `core.campaign` (names),
  `core.campaign_daily` (⚠ **listable-only, ~284 of 2,496 campaigns** — the delisting bug; avoid for absolutes).
- **`derived.*`** — BI rollups. `derived.v_funnel` (campaign-grain funnel), `derived.lead_intel` (lead-grain wide row),
  `derived.v_funnel_detail`.
- **Registered views:** `v_campaign_metrics` (cumulative per-campaign opps/sends/reply_rate),
  `v_kpi_email` (channel-aware email KPI), `v_sms_campaign_performance` + `v_kpi_sms` (SMS funnel).

### B.2 Scope (hard rule)
**Funding workspace ALLOW-list (warehouse slugs):** `renaissance-4`, `renaissance-5`, `prospects-power`,
`koi-and-destroy`, `renaissance-2`. Scope CM-performance to these. **`renaissance-1` = Instantly DFY — NEVER a CM.**
Instantly API slug ≠ warehouse slug — join by `campaign_id`, scope by warehouse slug:
`funding-1-samuel`→`renaissance-4` (SAMUEL) · `funding-2-ido`→`renaissance-5` (IDO; campaigns may be mis-tagged LAUTARO) ·
`funding-3-leo`→`prospects-power` (LEO) · `funding-4-sam`→`koi-and-destroy` (SAM) · `funding-5-eyver`→`renaissance-2` (EYVER).

### B.3 Canonical source + "use this not that" per metric
| Metric | USE (canonical) | NOT |
|---|---|---|
| **Emails sent** | `raw_pipeline_campaign_daily_metrics.sent` (campaign×date, additive) | `core.campaign_daily.sent` (delisting); `SUM(unique_*)` as additive |
| **Opportunities (email)** | cumulative: `v_campaign_metrics.opportunities`; windowed *(trend only)*: `SUM(raw_pipeline_campaign_daily_metrics.unique_opportunities)` — `unique_opportunities` IS the email-opp fact | `core.opportunity` (=SMS/call opps, campaign_id always NULL); summing windowed as an absolute (overcounts 30–56%) |
| **Human replies (email)** | **Instantly NATIVE ONLY** — `raw_pipeline_campaign_daily_metrics.unique_replies` (complete; per-day-distinct, additive across days for a campaign) + `v_kpi_email` | `_cum` cols; **`core.reply.is_auto_reply` heuristic (BROKEN — ~3.5% auto vs ~63% native truth, NEVER funded)**; `core.reply_intent` (never-funded Haiku classifier) |
| **Auto replies (email)** | **Instantly NATIVE ONLY** — `raw_pipeline_campaign_daily_metrics.unique_replies_automatic` (native, complete) | `intent='auto_reply'` (under-tagged ~1.4%); **`core.reply.is_auto_reply` (BROKEN heuristic)**; `core.reply_intent` |
| **Total replies (email)** | **Instantly NATIVE** — `unique_replies + unique_replies_automatic` (human + auto) | any `core.reply`/`core.reply_intent`-derived total |
| **Positive replies (email)** | from 2026-05-15: **`unique_opportunities ÷ human replies`** (positive-reply rate = email opps over `unique_replies`) — Instantly native only | `intent='positive'`; `core.reply_intent` sentiment; any classifier-derived "positive" count |
| **Meetings booked** | `core.meeting` (`campaign_id` authoritative; `source='slack'` <Jun-1, `'sheet'` ≥Jun-1) | **`core.meeting_campaign_attribution` = STALE ORPHAN, never trust**; Instantly `total_meeting_booked` (always 0) |
| **Infra / ESP split** | `core.sending_account_daily.esp` + `actual_sends` (account grain) | campaign `infra_type` (`'otd'` NEVER appears — OTD lumped into google) |
| **SMS funnel** | `v_sms_campaign_performance` + `v_kpi_sms` meetings; clamp `opps ≤ delivered ≤ sent` | merging SMS with email; trusting `positive_replies` without the `≤ delivered` guard |

### B.4 The NEVER list (gotchas)
- **Never `count(*)` / `SUM` across feeders** for an absolute — `raw_pipeline_campaign_daily_metrics` is the complete fact; `core.campaign_daily` covers only ~284/2,496 campaigns (delisting). Sends/opps/replies → the raw fact.
- **Never `SUM(unique_*)` as an additive absolute** — `unique_opportunities`/`unique_replies` are per-day-distinct; summing double-counts (opps 30–56%). Windowed = a **trend**, not an absolute. Cumulative absolute → `v_campaign_metrics`.
- **Never trust `core.meeting_campaign_attribution`** — stale orphan snapshot, no builder. Authoritative = `core.meeting.campaign_id`.
- **Never use campaign `infra_type` for OTD/Google** — only `core.sending_account_daily.esp` splits OTD (~19% of sends are `esp=NULL`; Tariffs absent; feed lags ~2d). Per-CM ESP isn't cleanly derivable → report ESP at workspace/org grain.
- **Never use `core.reply_intent` or `core.reply.is_auto_reply` for ANY human/auto/total/positive reply count (email).** They are BROKEN: the `is_auto_reply` heuristic reports ~3.5% auto vs the ~63% native truth, and the `core.reply_intent` Haiku LLM classifier was NEVER funded (no Anthropic spend → it never ran; the nightly `intent` phase is removed). Email reply truth comes **ONLY from Instantly native**: `unique_replies` (human), `unique_replies_automatic` (auto), total = their sum, positive = `unique_opportunities ÷ unique_replies` (from 2026-05-15) — all from `raw_pipeline_campaign_daily_metrics` / `v_kpi_email`. (Sam source-of-truth decision 2026-06-14, memory `reference_warehouse_reply_and_tag_truth_20260614`.) `derived.reply_intent` email rows inherit the same broken source — do not surface them; only its SMS leg (conversation-state proxy) is usable, and only for SMS.
- **Never reconcile `core.opportunity` against email opps** — it's the SMS/call-opp surface (campaign_id always NULL).
- **Never merge SMS (Sendivo) with email** — separate funnels; SMS meetings carry NULL `campaign_id`.
- **Never sum `_cum` columns** — lifetime running totals.
- **Never count `renaissance-1` as a CM** — Instantly DFY.
- **Campaign tags are NO LONGER synced (since 2026-06-14).** `core.campaign_sending_tag` / `raw_instantly_campaign_sending_tag` (and `*_marker_tag`) are **frozen** — the nightly stopped pulling Instantly campaign tags (Sam decision; tags aren't a trustworthy attribution surface). Existing rows are kept as **stale fallback hints only** — never treat a tag as current truth or use it for AI/non-AI/offer attribution. Derive CM/offer from the campaign **name** instead.
- **There is NO AI-vs-non-AI (AIM) split in the warehouse** — AIM is not autonomous, so there's no AI-vs-human attribution flag anywhere. Never report, derive, or invent an "AI copy" / "AI-written" / "AIM-written" reply/meeting/send split; the column does not exist and is not deferred-pending — it's intentionally absent. SMS conversations are AIM-assisted end-to-end but that is not an "AI share" metric.
- Sends are a **45-day rolling pull** + frozen older rows — questions >45d back rely on frozen rows, caveat completeness.
- Workspace/CM rollups undercount the API ~0–8% (orphan campaign-days + dim drift) — `COALESCE` the fact's own `workspace_id` where present.

### B.5 Worked query examples
**Emails sent, last 30 days, per CM (the complete fact):**
```sql
WITH dims AS (
  SELECT DISTINCT ON (campaign_id) campaign_id,
         COALESCE(NULLIF(cm_name,''),'(no cm)') AS cm, workspace_id
  FROM raw_pipeline_campaigns ORDER BY campaign_id, _loaded_at DESC)
SELECT COALESCE(d.workspace_id, cd.workspace_id) AS workspace, d.cm,
       SUM(cd.sent) AS emails_sent_30d
FROM raw_pipeline_campaign_daily_metrics cd LEFT JOIN dims d USING (campaign_id)
WHERE cd.date >= current_date - 30 AND cd.date < current_date + 1
GROUP BY 1,2 ORDER BY emails_sent_30d DESC;
```
**Cumulative email opps (UI-faithful) + incompleteness flag:**
```sql
SELECT SUM(opportunities) AS opps_live_campaigns,
       COUNT(*) FILTER (WHERE opportunities IS NULL) AS campaigns_with_no_opp_source
FROM v_campaign_metrics;
```
**Replies per campaign — Instantly NATIVE split (human / auto / total / positive-rate):**
```sql
-- The ONLY correct email reply source. unique_replies=human, unique_replies_automatic=auto.
-- total = human+auto. positive (from 2026-05-15) = unique_opportunities ÷ human replies.
WITH dims AS (
  SELECT DISTINCT ON (campaign_id) campaign_id, name, workspace_id
  FROM raw_pipeline_campaigns ORDER BY campaign_id, _loaded_at DESC)
SELECT d.name,
       SUM(m.unique_replies)                                         AS human_replies,
       SUM(m.unique_replies_automatic)                              AS auto_replies,
       SUM(m.unique_replies) + SUM(m.unique_replies_automatic)      AS total_replies,
       SUM(m.unique_opportunities)                                  AS opps,
       ROUND(100.0*SUM(m.unique_replies)/NULLIF(SUM(m.sent),0),3)   AS human_reply_rate_pct,
       ROUND(100.0*SUM(m.unique_opportunities)/NULLIF(SUM(m.unique_replies),0),1) AS positive_rate_pct
FROM raw_pipeline_campaign_daily_metrics m JOIN dims d USING (campaign_id)
WHERE d.workspace_id IN ('renaissance-4','renaissance-5','prospects-power','koi-and-destroy','renaissance-2')
GROUP BY 1 ORDER BY total_replies DESC;
-- NEVER substitute core.reply.is_auto_reply (~3.5% auto, broken) or core.reply_intent (unfunded).
```
**Email meetings per campaign, 30d (SMS-exclusion regex byte-identical to v_kpi_email):**
```sql
SELECT COALESCE(c.name, m.campaign_name_raw, '(unattributed)') AS campaign, m.campaign_id, count(*) AS meetings
FROM core.meeting m LEFT JOIN core.campaign c USING (campaign_id)
WHERE m.posted_at >= current_date - 30
  AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra')
GROUP BY 1,2 ORDER BY meetings DESC;
```
**Sending volume per ESP, last 7d (account grain — the only OTD-splittable surface):**
```sql
SELECT COALESCE(esp,'unknown') AS esp, SUM(actual_sends) AS sends,
       ROUND(100.0*SUM(actual_sends)/SUM(SUM(actual_sends)) OVER (),1) AS pct
FROM core.sending_account_daily
WHERE date >= (SELECT max(date) FROM core.sending_account_daily) - 6
GROUP BY 1 ORDER BY sends DESC;
```
**SMS funnel, 30d, with the containment guard:**
```sql
SELECT SUM(sent) AS sent, SUM(delivered) AS delivered,
       LEAST(SUM(positive_replies), SUM(delivered)) AS opps_guarded, SUM(positive_replies) AS opps_raw,
       (SUM(positive_replies) > SUM(delivered)) AS fanout_suspected
FROM v_sms_campaign_performance
WHERE metric_date >= current_date - 30 AND sent IS NOT NULL;
```

### B.6 Self-check before answering
- [ ] Sends from `raw_pipeline_campaign_daily_metrics`, never `core.campaign_daily`, never `SUM(unique_*)` as additive?
- [ ] Opps: cumulative from `v_campaign_metrics`; windowed labeled a trend?
- [ ] **Replies (human/auto/total/positive) from Instantly NATIVE only (`unique_replies` / `unique_replies_automatic` / their sum / `unique_opportunities÷unique_replies`) — NEVER `core.reply.is_auto_reply` or `core.reply_intent`?**
- [ ] **No campaign-tag relied on as current (tags frozen since 2026-06-14); no AI-vs-non-AI split reported (doesn't exist)?**
- [ ] Infra split only from `core.sending_account_daily.esp` (never campaign `infra_type`)?
- [ ] SMS kept separate from email; SMS opps clamped `≤ delivered`?
- [ ] Meetings via `core.meeting.campaign_id` (not the orphan attribution table)?
- [ ] Scoped to the 5 Funding slugs; `renaissance-1` excluded?
- [ ] **Cited the source view + `snapshot_id` (Section C) next to the result?**
- [ ] **Any gap / uncertain derivation / off-looking number → said so explicitly and offered to flag it (Section C + D)?**

---

## SECTION C — HARD ANTI-HALLUCINATION RULES (non-negotiable — Sam: this is the most important part)

The warehouse exists to give **trustworthy** numbers. A confidently-wrong answer is worse than "I don't
know" — it corrupts decisions downstream. These rules are mandatory for every answer, every chat, everyone.

1. **NEVER guess. NEVER fabricate.** Do not invent a number, a column, a table, a slug, an attribution, or
   a "roughly". If you did not read it from a query result, you do not state it.
2. **Cite your source on every answer.** Every number you report MUST carry:
   - the **source view/table** it came from (e.g. `raw_pipeline_campaign_daily_metrics`, `v_campaign_metrics`,
     `core.meeting`), and
   - the **`snapshot_id`** from the API/MCP result (or, in interim mode, `interim: direct /root/core/warehouse.duckdb @ <UTC time>`).
   No source + snapshot = not a finished answer. Put it inline, e.g. *"SAMUEL sent 182,304 emails in the
   last 30d (`raw_pipeline_campaign_daily_metrics`, snapshot `warehouse_20260614T0930Z.duckdb`)."*
3. **On a DATA GAP — say so explicitly.** If the data is missing / absent / NULL / out of the 45-day window
   / not yet loaded, state exactly **what is missing and where**, and do not paper over it with an estimate.
   Show the gap (e.g. "0 rows for Tariffs in `core.sending_account_daily` — ESP split unavailable for that
   workspace"). Then offer to **flag it** (Section D).
4. **On an UNCERTAIN or COMPLEX derivation — show your work and flag the uncertainty.** If getting the number
   requires a non-obvious join, a heuristic, a regex, or a judgment call about which source is canonical,
   **say it's uncertain, show the exact SQL and assumptions**, and give the range/caveat rather than a false
   single number. If two plausible sources disagree, present both with their sources — do not silently pick one.
5. **On a number that LOOKS OFF — do not smooth it over.** If a result contradicts the Instantly API
   (ground truth), a prior known-good, or basic sanity (e.g. opps > sends, reply_rate > 1, a CM at 0 that's
   actively sending), **call it out explicitly as suspect**, show the contradiction, and flag it. The
   Instantly API wins; a warehouse number that disagrees is a **bug to flag**, not a number to report.
6. **Prefer "I can't answer that reliably" over a fabricated answer.** It is always acceptable — expected —
   to say "the warehouse can't answer this reliably because X; here's exactly what's missing/uncertain;
   want me to flag it?" That is a *correct* outcome, not a failure.
7. **Reproducibility.** Anyone should be able to re-run your exact SQL against your cited `snapshot_id` and
   get your number. If they can't, you haven't cited enough — add the query.

**The one-line contract:** *every answer cites its source view + snapshot; on any gap / uncertainty /
off-looking number, say so explicitly, show what's missing or wrong, and flag it — never fabricate.*

---

## SECTION D — FLAG IT (file a data-quality flag when something's wrong/missing/uncertain)

When you (or the user saying **"flag it"**) hit a **data gap**, an **inaccurate number**, or a **question**
about which source is canonical, file a flag as a **GitHub Issue** instead of guessing. Flags go to the
**private** repo **`sdultsin/warehouse-flags`** (issues only — no warehouse data / PII / tokens). The `gh`
CLI is authed as `sdultsin`. A background triage agent investigates open flags, fixes/escalates, and closes them.

**When to flag:** any time Section C says "say so / it's suspect" — a confirmed gap, a number that
contradicts the Instantly API or sanity, or a derivation too uncertain to resolve. Flagging is the correct
action; it is NOT optional politeness.

**How to flag (one command):**
```bash
gh issue create --repo sdultsin/warehouse-flags \
  --title "[flag] <one-line, e.g. SAMUEL emails_sent_30d = 0 but Instantly shows active sending>" \
  --label inaccurate \                 # choose ONE: data-gap | inaccurate | question
  --body "$(cat <<'EOF'
## What I was looking at
<the question / number, in plain English>

## Source view + snapshot   (REQUIRED — never omit)
- source view/table: v_campaign_metrics
- snapshot_id: warehouse_20260614T0930Z.duckdb      # or: interim: direct /root/core/warehouse.duckdb @ 14:05 UTC
- access path: API                                   # API | MCP | interim direct-DuckDB

## What I got vs expected
- got: SAMUEL emails_sent_30d = 0
- expected (and why): ~180k — the Instantly API shows active sending; looks like a dim-join/scope bug

## Query
\`\`\`sql
<the exact SQL I ran>
\`\`\`
EOF
)"
```

**Label choice:**
- `data-gap` — something is **missing / absent / incomplete** in the warehouse.
- `inaccurate` — a number is **wrong / doesn't reconcile** vs the source of truth (Instantly API / known-good).
- `question` — **uncertain derivation** or unclear which source/view is canonical.

**Hard requirement (mirrors Section C):** a flag MUST cite the **source view/table** and the **`snapshot_id`**
(or the interim marker). No snapshot = unreproducible = not a usable flag.

After filing, tell the user the issue URL `gh` printed, and that triage will investigate + comment the
finding. (Triage design: `deliverables/2026-06-14-warehouse-flag-pipeline.md`. The background triage agent
itself is built but **not yet scheduled**.)

---

## SECTION E — STABILITY (read a consistent snapshot while the warehouse is changing)

The whole point of the new warehouse: **a reader never pulls mid-change data.**

**Mechanism (on the API/MCP — A.1/A.2):** you query an **immutable, timestamped snapshot**, never the live
writer file. The writer never mutates a published snapshot, so your view is fully self-consistent *as of that
snapshot* — pulling mid-change is structurally impossible. Every snapshot already passed the validation gate
(schema, row-floors, sanity invariants, the F4-Jun-10=508,685 canary), so it's correct, not just frozen.
- **Every result carries a `snapshot_id`.** That is your version stamp — **record it next to any finding**
  (Section C) so the result is reproducible and staleness is visible.
- Within one query you always get one consistent snapshot; the server only picks up a newer snapshot on the
  **next** request (an in-flight query is never swapped underneath you).
- Snapshots are retained (current + prior known-good + `_known_good`), so a QA pass can keep hitting the same
  `snapshot_id`. If you need a **frozen baseline that outlives normal retention** ("QA against snapshot X for
  a week"), ask Sam to bump `keep_snapshots` or copy that snapshot aside — every snapshot is already a
  pinnable, restore-verifiable point-in-time backup.
- A surveillance layer guarantees the served data is always either correct-and-fresh or rolled back to the
  last correct known-good (correct-but-slightly-stale) — **never wrong-but-plausible.** Trust the served snapshot.

**Mechanism (interim, on direct DuckDB — A.3):** there is **no snapshot isolation** — you're on the live
writer file, and there is **no `snapshot_id`**. To get a consistent view: **read during a writer-idle window**
(avoid 03:30–05:45 UTC and any active bulk load), and re-run a quick sanity (e.g.
`SELECT max(date) FROM raw_pipeline_campaign_daily_metrics;`) to confirm freshness. Cite the interim marker
in place of a snapshot_id. This caveat is exactly what the API/MCP remove — push for the cutover so QA moves
onto pinned snapshots.
