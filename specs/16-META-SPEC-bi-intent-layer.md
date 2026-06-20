# META-SPEC — BI / Lead-Intent Layer (Renaissance Warehouse)

> ## ✅ EXECUTED 2026-06-08 — all 10 workstreams shipped to the live warehouse.
> Objects populated: core.lead 247,043 · core.reply 542,668 · core.reply_intent 20,379
> (draining nightly, full 11-value enum) · core.lead_disposition 134 · core.call 634 +
> outcome + 8 transcripts (draining) · core.warm_caller 3 · core.conversion_event 31,155 ·
> derived.lead_intel 247,043 + v_objection_library / v_question_library / v_intent_distribution
> / v_disposition_funnel. Phone backfill: 298 enriched phones → lead DB sidecar. New phases
> `close`+`intent` wired into nightly; feeds registered in sync_registry. 2 integration bugs
> found+fixed (close.py lead-fetch timeout escaping the phase; conversion_event joining
> core.lead on the wrong column). End-to-end demonstration verified (Premium Truck Services:
> 12 replies + DNQ partner disposition both attributed to one lead). Remaining work drains
> automatically via the nightly `intent` (replies) + `close` (transcripts) phases. Deferred:
> lead_intel scraped-attr enrichment from the lead mirror (WS-F `_ENABLE_MIRROR_ENRICH=False`,
> mirror table TBD); core.meeting carries no lead identity so 31k IM meetings don't attribute
> to a lead (v1 gap — warm-call appts do attribute).
>
> [2026-06-08] Build spec for a long autonomous (ralph-loop) session. Goal: build the **BI
> side** of the data-consolidation warehouse — turn our reply threads, funding-partner
> feedback, and warm-call (Close) data into an attributed **lead-intent ontology** (objects +
> links), so that for any lead we can say *what they actually said/did*, not just what we
> scraped. Design reference = `specs/16-bi-intent-layer.md` (read it; this meta-spec is the
> **execution plan + DoD**, not a re-derivation). Everything here is **additive** to a live
> warehouse — break nothing.

---

## HOW TO RUN THIS UNDER A LOOP

Each workstream (WS-0…WS-I) has its own **Definition of Done (DoD)** — a checklist of
*objectively verifiable* conditions, each with a stated check command. Loop a workstream until
**every** DoD box is green, then move on.

Rules for the loop:
- **Verify, don't self-attest.** A DoD box counts as done only when you've actually run the
  stated check (the SQL, the orchestrator phase, the `git check-ignore`, the row-count) and seen
  it pass. Paste the evidence (the query + its output).
- **Respect dependencies (§4).** Don't start a workstream until its prerequisites are DoD-green.
- **Parallel lanes:** WS-A, WS-B, WS-C, WS-D, WS-E have **no cross-dependencies** — run them in
  parallel agents/lanes. WS-F/G/H depend on B (and C/A); WS-I is last. WS-0 is cross-cutting.
- **One workstream per loop pass per lane.** End each pass by printing the WS's DoD checklist
  with ✅/❌ + evidence and what remains.
- **Idempotent or bust.** Every entity applies its own DDL with `CREATE … IF NOT EXISTS` and
  loads via UPSERT / DELETE-by-key+INSERT. Re-running a phase must converge, never duplicate.
- **Additive invariant (§3) is sacred.** No `ALTER`/`DROP`/rename on any table that existed
  before this project. New tables/views/DDL-files only. If you think you need to alter an
  existing table, stop and flag it — don't.
- **PII never enters git.** Lead emails, reply verbatims, call audio, transcripts → git-ignored,
  droplet/local only. Run `git check-ignore` as the proof.
- A workstream is "done" only when its DoD is fully green AND a full local orchestrator dry-run
  imports cleanly AND nothing in §5 (global ship gate) regressed.

---

## 0. NORTH STAR

We sit on thousands of real human responses to our cold outreach (replies), funding-partner
dispositions on the leads we book, and warm-call recordings in Close — and draw **zero**
structured insight from any of it. This builds the **objects + links layer** of a Palantir-style
lead-intent ontology in the DuckDB warehouse:

*this lead → replied with this thread → to this variant → in this campaign → which used this
infra → and the warm-caller/partner said this about them → and here's what they actually meant.*

The **right half** (Campaign→Variant→Infra→performance) is already built. This project lands the
**left half** (Lead, Reply, ReplyIntent, PartnerDisposition, Call, Transcript, ConversionEvent)
and the **join edges**, then a per-lead profile (`derived.lead_intel`) and the first insight
surfaces. MVP = get the data together, organized, and **attributed** — insight analytics ride on
top once the objects exist.

---

## 1. DECISIONS LOCKED (ground truth — do not re-litigate)

**Architecture / placement:**
- Warehouse (DuckDB on `renaissance-worker:/root/core/warehouse.duckdb`) = the single
  **analytics** sink. Comms Supabase (`hlkdpldliczhfjdnfsbq`) stays the **comms OLTP** store
  (it runs 5 pg_cron jobs + is the webhook target — it canNOT move into DuckDB). The warehouse
  already nightly-mirrors it (`raw_comms_*`). We do **not** "fold ops into the warehouse."
- Sync model = **direct-pull hub-and-spoke, nightly** (03:30 UTC, `scripts/nightly.sh`). Each
  source's API → `raw_*` → `core.*` → `derived`. No new middleman. New BI feeds = new nightly
  phases in this same orchestrator.
- **Cadence = nightly only** (no intraday). **Incremental where the source is expensive**
  (replies: incremental + 2-day overlap, dedup on id; calls: incremental by `date_updated`;
  daily perf: trailing window + freeze closed days). **Full-refresh the cheap/small** dimensions
  (variants, campaign settings) — content-hash upsert, don't build skip-logic. **Compute
  all-time rollups in-warehouse** from daily facts; don't re-pull rollups from APIs.

**BOF / conversion semantics:**
- A warm-caller "close" = **an appointment set with the funding partner**, NOT a funded deal.
  The lead then comes back as a **PartnerDisposition** (the xlsx). Closing-rate/quality is
  measured off **partner disposition + call outcome**, not the caller's "yes."
- **Conversion model (extensible, MVP binary):** every conversion carries three dims —
  `source_channel` {cold_email | sms | wa}, `conversion_agent` {**im**, **warm_caller** now;
  `sms_aim_v1` app-link, `sms_aim_v2` autobook = ROADMAP, schema must absorb without migration},
  `conversion_type` {meeting_booked | appointment_set | application_submitted}. MVP only ever
  populates im + warm_caller.
- **Warm-caller = one aggregate entity for MVP** (`core.warm_caller`, `warm_caller_id='ALL'`).
  Per-rep split is soon but later: backfill `warm_caller_id` from Close `user_id` — **no schema
  change**.

**Identity mapping (Sam's, supersedes the old email∪phone "bridge" framing):**
- Opp/lead → lead-DB lead via **fallback chain**: email match first → on miss, phone (E.164) →
  if **neither matches, FLAG** (surface, never silently drop). Sendivo (phone-only) → phone match.
- **Phone matches MULTIPLE lead-DB leads → ALERT Sam first** (#cc-sam), do not auto-pick.

**Transcription:**
- **Local Whisper on the droplet** (`faster-whisper`, not yet installed) — no paid API, no Mac
  dependency, PII stays in-house. Transcribe **everything** recorded; also keep the structured
  call data regardless. Go-forward now + bounded historical backfill in the background.

**Phone backfill:**
- Enriched phones (`comms.phone_enrichment.mobile_e164`) currently die in comms Supabase. Write
  them to the **lead DB (`edpyqbiqzduabtjhwfaa`) as a sidecar column** (`enriched_phone` +
  source/timestamp — NOT overwriting `public.leads` core fields) → nightly lead-mirror picks them
  up. Single source, no double-sync.

**Reply intent:**
- Sam runs **no** other reply-enrichment workstream — this project owns it. The old
  `raw_pipeline_reply_intent_classifications` (804k rows, frozen ~06-06) is a **historical
  reference only**; **supersede it** with our own classifier over `core.reply`. Investigate why
  it stopped (WS-C) only to confirm no live signal is lost — never depend on it going forward.

**Classifier:** Haiku-class batch, cheap, just run it. Include a one-line LLM `profile_summary`
in `lead_intel`.

**Alerts:** all flags/alerts → **#cc-sam** (existing fail-loud channel).

---

## 2. CREDENTIALS & LOCATIONS (loop is self-sufficient — all present)

| Need | Where |
|---|---|
| Warehouse DB | `renaissance-worker:/root/core/warehouse.duckdb` (read-only: `duckdb -readonly`); repo `/root/renaissance-warehouse`; nightly `scripts/nightly.sh` @ 03:30 UTC cron |
| Local dev repo | `/Users/sam/Documents/Claude Code/Renaissance/renaissance-warehouse` |
| Close API | `CLOSE_API_KEY` in repo-root `.env`; basic auth `-u "$KEY:"`, `https://api.close.com/api/v1/…`; recordings `GET {recording_url}` → MP3 |
| Comms Supabase | `COMMS_SUPABASE_DB_URL` (.env) — psycopg2 over pooler:6543 |
| Lead DB (write) | `LEADS_DB_SUPABASE_SERVICE_ROLE_KEY` + `LEADS_DB_URL` (.env, session-mode :5432 for COPY) |
| Instantly | `INSTANTLY_KEY_<WS>` per workspace; Cloudflare blocks default UA → send a browser UA |
| Alert channel | Slack `#cc-sam` |
| Entity/DDL idiom | new `entities/<name>.py` with `register(registry)` → auto-discovered; applies own DDL `sql/ddl/<NN>_*.sql` via `conn.execute(ddl.read_text())`; returns `PhaseResult(rows_in, rows_out, notes)`. Phases ride `PHASE_ORDER` in `core/config.py` (add a new slot only for a genuinely new phase). |

Next free DDL number: **42** (41 = partner_feedback, taken).

---

## 3. ADDITIVE INVARIANT (the thing that protects the live warehouse)

- New tables only: `raw_partner_lead_feedback`, `raw_close_call`, `core.lead_disposition`,
  `core.call`, `core.warm_caller`, `core.call_outcome`, `core.call_transcript`, `core.reply`,
  `core.reply_intent`, `core.lead`, `core.conversion_event`, `derived.lead_intel`, + `v_*` views.
- **No `ALTER`/`DROP`/rename** on any pre-existing table or view. `comms_mirror` gap-close (WS-E)
  adds NEW `raw_comms_*` tables, doesn't modify existing ones.
- Every new phase is **gated** by an env flag (`WAREHOUSE_RUN_BI=1` etc.) until its DoD is green,
  mirroring the `instantly_replies` roll-out — so a half-built phase can't poison the nightly.
- Lead-DB write (WS-D) is a **sidecar column**, reversible, never clobbers canonical lead fields.

---

## 4. DEPENDENCY GRAPH / PARALLEL LANES

```
WS-0 (reliability/runtime)  — cross-cutting, do early, maintain throughout

Lane 1:  WS-B Close calls ───┬─► WS-G ConversionEvents
                             ├─► WS-H Transcription
Lane 2:  WS-C Reply+Intent ──┤
Lane 3:  WS-D Phone backfill │
Lane 4:  WS-E Comms mirror   │
WS-A Partner feedback (≈done)┘
                             └─► WS-F Lead spine (needs A,B,C) ─► WS-I lead_intel + insights
```
Parallel immediately: **A, B, C, D, E**. Then **F** (after A+B+C), **G/H** (after B). **I** last.

---

## WS-0 — Runtime & reliability (cross-cutting)

**Goal:** new phases run in the nightly, are observable, and the run is healthy.

**Tasks:** clean the two hung `core.sync_run` rows stuck in `status='running'` (mark `failed`,
`ended_at=now()`). Confirm `discover_and_register` picks up each new `entities/*.py`. Add each new
feed to `core.sync_registry` (so `v_warehouse_freshness` covers it) with the right cadence policy.
Add any genuinely-new phase name to `core/config.py` `PHASE_ORDER` (after `instantly`, before
`canonical`). Wire `warehouse_qa.py` so a stale BI feed alerts #cc-sam.

**DoD:**
- [ ] `SELECT count(*) FROM core.sync_run WHERE status='running' AND started_at < now()-INTERVAL 6 HOUR` → **0**.
- [ ] Every new entity appears in orchestrator logs as `Registered: entities.<name>`.
- [ ] `SELECT name FROM core.sync_registry WHERE name IN (<new feeds>)` → all present, none `status='retired'`.
- [ ] A forced-stale test on one BI feed produces a #cc-sam alert (then revert).

---

## WS-A — Partner disposition feedback  *(code complete — deploy + register)*

**Status:** `sql/ddl/41_partner_feedback.sql` + `entities/partner_feedback.py` **built & tested
locally** (134 rows, `v_disposition_funnel` reconciles to the sheet). Remaining = deploy + wire.

**Tasks:** ship the seed CSV + DDL + entity to the droplet; run the `sheets` phase; confirm
canonical + funnel; add `raw_partner_lead_feedback`/`core.lead_disposition` to sync_registry
(cadence `periodic`, manual drop). Keep `seed_data/partner-feedback/` git-ignored.

**DoD:**
- [ ] `SELECT count(*) FROM raw_partner_lead_feedback` ≥ 134; `SELECT count(*) FROM core.lead_disposition` = same.
- [ ] `SELECT * FROM v_disposition_funnel` → no_show ≈46%, live ≈1.5%, exactly one `unknown` ("No note").
- [ ] `git check-ignore seed_data/partner-feedback/*.xlsx` and `*__lead_detail.csv` both return the path.
- [ ] Phase logged in `core.sync_run_phase` with `rows_out>0`.

---

## WS-B — Close call ingest (warm-caller, structured)  **[Lane 1, parallel]**

**Goal:** pull warm-call activity from Close into the warehouse, structured (no transcript yet).

**Deliverables:** `sources/close.py` (auth, paginated `GET /api/v1/activity/call/`, incremental by
`date_updated`), `entities/close_calls.py`, `sql/ddl/42_close_calls.sql`:
- `raw_close_call` — one row per Close call id (disposition, status, duration, recording_url,
  has_recording, recording_duration, outcome_id/reason, note, cost, direction, lead_id, phone,
  user_id, user_name, date_created, date_answered, date_updated). UPSERT on id.
- `core.call` — canonical: call_id, close_lead_id, lead_email (from Close lead), phone_e164,
  warm_caller_id (='ALL' MVP, keep user_id/user_name), direction, disposition, duration_seconds,
  has_recording, recording_url, cost, occurred_at, source_campaign (from Close custom field
  `cf_nNHBuW…`), source_channel (Instantly/Sendivo from `cf_aguD41…`).
- `core.warm_caller` — aggregate row `'ALL'` + per-`user_id` rows populated (id kept for the
  later split), with rollups: calls, connect_rate, appt_set_rate (left null until WS-G/outcome).
- `core.call_outcome` — structured outcome from disposition + rep `note` (no LLM yet): one row per
  call_id, `outcome_class` {answered_appt_set | answered_not_interested | answered_other |
  no_answer | voicemail}, derived deterministically from disposition + note keywords; LLM/transcript
  enrichment is WS-H.

**Notes:** Close `lead_id` joins to a Close lead carrying email∪phone + the Source/Campaign custom
fields = the attribution edge. Sendivo calls are phone-only.

**DoD:**
- [ ] `sources/close.py` pulls incrementally (second run with no new calls writes 0 new rows, re-run idempotent).
- [ ] `SELECT count(*) FROM raw_close_call` > 0 and matches a spot `GET …/activity/call/?_limit=1 has_more` sanity check.
- [ ] `SELECT warm_caller_id, count(*) FROM core.call GROUP BY 1` → includes `'ALL'` semantics (every call attributed).
- [ ] `SELECT outcome_class, count(*) FROM core.call_outcome GROUP BY 1` → sane spread; **0 calls with NULL outcome_class**.
- [ ] Every `core.call.close_lead_id` resolves to a Close lead (no orphan calls); attribution `source_campaign` non-null for ≥90% (log the misses).
- [ ] Phase in `core.sync_run_phase` rows_out>0; feed in sync_registry (cadence `daily`).

---

## WS-C — Reply canonicalization + intent  **[Lane 2, parallel]**

**Goal:** one canonical reply fact + a rich intent classification, superseding the dead pipeline intent.

**Deliverables:** `sql/ddl/43_reply_intent.sql`, `entities/reply_canonical.py`,
`entities/reply_intent_llm.py`:
- `core.reply` — consolidate `raw_instantly_email` (primary, current to 06-08) + `raw_pipeline_reply_data`
  (historical fallback for pre-cutover), dedup on (lead_email, thread_id, reply_timestamp), carry
  recovered `variant` from `v_reply_enriched`, `is_auto_reply`, campaign_id, workspace_id, step, subject, reply_text, source.
- `core.reply_intent` — LLM (Haiku-class) over reply_text+subject+step: `primary_intent` (the
  §6 enum from the design spec), `intent_tags[]`, `sentiment`, `is_question`, `is_objection`,
  `objection_type`, `is_unsubscribe`, `is_referral`, `is_wrong_person`, `summary`,
  `classifier_model`, `classifier_version`, `confidence`, `classified_at`. **Incremental:** only
  classify `core.reply` rows lacking a `core.reply_intent` at the current `classifier_version`.
  Classifier failure logs + continues, never aborts the nightly.
- **Investigation task** (report only): how was `raw_pipeline_reply_intent_classifications` filled,
  when/why did it stop (~06-06), is any live signal lost vs. our `raw_instantly_email` path?
  Document in the WS-C closeout. Do NOT revive it; we supersede it.

**DoD:**
- [ ] `SELECT count(*) FROM core.reply` ≥ count(distinct replies in raw_instantly_email); no exact dup (lead_email, thread_id, reply_timestamp).
- [ ] `v_reply_enriched` is consumed (variant non-null where recoverable) — not re-implemented.
- [ ] `SELECT count(*) FROM core.reply r LEFT JOIN core.reply_intent i USING(reply_id) WHERE i.reply_id IS NULL AND r.is_auto_reply=false` → **0** after a full classify pass (everything human is classified).
- [ ] `SELECT primary_intent, count(*) FROM core.reply_intent GROUP BY 1` → all values in the fixed enum, none NULL.
- [ ] Re-running the classifier with the same `classifier_version` classifies **0** new rows (idempotent).
- [ ] WS-C closeout documents the dead-pipeline-intent finding.

---

## WS-D — Phone backfill → lead DB  **[Lane 3, parallel]**

**Goal:** stop losing enriched phones; land them where the nightly lead-mirror reads.

**Deliverables:** a job (`scripts/backfill_enriched_phones.py`) that reads
`comms.phone_enrichment` (mobile_e164, where `mobile_status` = found) + its `call_opportunity`
(email/source) and writes to the **lead DB** sidecar (`public.leads.enriched_phone` +
`enriched_phone_source`, `enriched_phone_at` — additive columns, NOT the core phone). Identity
fallback per §1: email → phone → flag. **Multi-match phone → #cc-sam alert, skip auto-write.**
Backfill the 2,283 existing rows + make it a repeatable nightly step.

**DoD:**
- [ ] New sidecar columns exist on lead DB `public.leads` (additive; verified they did not exist before — `\d` / information_schema check).
- [ ] `SELECT count(*) FROM public.leads WHERE enriched_phone IS NOT NULL` > 0 after backfill; spot-check 5 against `comms.phone_enrichment`.
- [ ] No-match opps are written to a flag log (count reported), not silently dropped.
- [ ] A synthetic multi-match fires a #cc-sam alert and does NOT write (then revert).
- [ ] Re-running the backfill writes 0 changed rows (idempotent upsert by lead key).

---

## WS-E — Comms mirror gap-close  **[Lane 4, parallel, small]**

**Goal:** complete the comms→warehouse analytics mirror.

**Deliverables:** extend `entities/comms_mirror.py` to also mirror `comms.close_sync` (16,113),
`comms.gbc_application`, `comms.app_link_check` → new `raw_comms_close_sync` /
`raw_comms_gbc_application` / `raw_comms_app_link_check`. Leave `webhook_receipt` (5.97M) OUT
(noise, by design — note it). Additive only.

**DoD:**
- [ ] `SELECT count(*) FROM raw_comms_close_sync` ≈ 16k (±sync drift); other two present.
- [ ] No change to any existing `raw_comms_*` table definition (additive).
- [ ] Feeds in sync_registry (cadence `daily`); phase rows_out>0.

---

## WS-F — Lead spine + identity resolution  *(needs A,B,C)*

**Goal:** one `core.lead` per signal-lead, keyed email∪phone, every signal attributed.

**Deliverables:** `sql/ddl/44_lead.sql`, `entities/lead_spine.py`:
- `core.lead` — union of all leads appearing in ANY signal source (`core.reply`, `core.call`,
  `core.lead_disposition`, `core.opportunity`, `core.meeting`, comms conversations) — NOT the 27M.
  Columns: `lead_key` (stable surrogate), `email`, `phone_e164`, `first_name`, `company`,
  `segment`, `industry`, `lead_source`, `resolution_confidence`, `first_seen_at`. Scraped attrs
  pulled from the lead-DB mirror for the signal subset (slim join).
- Identity resolution per §1 fallback chain; unresolved fragments stay separate with
  `resolution_confidence='unmatched'`; multi-match → #cc-sam alert + flag, no silent merge.

**DoD:**
- [ ] `SELECT count(*) FROM core.lead` ≪ 27M and ≥ distinct signal-lead count.
- [ ] Every `core.reply.lead_email`, `core.call` (email or phone), `core.lead_disposition.lead_email` resolves to exactly one `core.lead` OR is flagged `unmatched` (0 silent drops — the union check balances).
- [ ] `SELECT resolution_confidence, count(*) FROM core.lead GROUP BY 1` reported; any multi-match alerted.
- [ ] Idempotent rebuild (re-run → same lead_key set).

---

## WS-G — Conversion events  *(needs B + existing core.meeting)*

**Goal:** unify "how did this lead convert, by which mechanism" into one object.

**Deliverables:** `sql/ddl/45_conversion_event.sql`, `entities/conversion_event.py`:
- `core.conversion_event` — feeders: `core.meeting` (agent=`im`, type=`meeting_booked`) +
  `core.call` where outcome=appt_set (agent=`warm_caller`, type=`appointment_set`). Cols:
  lead_key, source_channel, conversion_agent, conversion_type, occurred_at, campaign_id,
  warm_caller_id. Enum-extensible for sms_aim_v1/v2 (no schema change later).

**DoD:**
- [ ] `SELECT conversion_agent, conversion_type, count(*) FROM core.conversion_event GROUP BY 1,2` → only {im×meeting_booked, warm_caller×appointment_set} present (MVP binary), counts sane.
- [ ] Every event has a non-null `source_channel` and resolves to a `core.lead`.
- [ ] Adding a hypothetical `sms_aim_v1` row requires no DDL change (enum/text dims — verify column types).

---

## WS-H — Transcription (local Whisper)  *(needs B)*

**Goal:** transcribe Close recordings on the droplet, no API, PII-safe.

**Deliverables:** install `faster-whisper` + `ffmpeg` on the droplet; `entities/call_transcription.py`:
incremental list of `core.call` where `has_recording` and no `core.call_transcript`; download
`recording_url` MP3 (authed) → faster-whisper (base/small int8, English) → `core.call_transcript`
(call_id, transcript, model, lang, duration, transcribed_at) **droplet-only, git-ignored**;
delete the MP3 after. `nice`/off-peak; go-forward + bounded historical backfill. Then enrich
`core.call_outcome` (LLM over transcript: appt_set?, objection, quality flags) where a transcript exists.

**DoD:**
- [ ] `faster-whisper` + `ffmpeg` importable/`which` on the droplet; one sample MP3 transcribes end-to-end.
- [ ] `SELECT count(*) FROM core.call_transcript` > 0; transcript text non-empty; PII path git-ignored (`git check-ignore`).
- [ ] No MP3s left on disk after a run (staging dir empty); disk delta ≈ 0.
- [ ] `core.call_outcome` rows with a transcript carry the LLM-enriched fields; the structured-only (WS-B) outcome still present for calls without a transcript.
- [ ] Re-run transcribes only new recordings (idempotent).

---

## WS-I — `derived.lead_intel` + insight surfaces  *(needs A,B,C,F,G; H enriches)*

**Goal:** the intent DB (one row per signal-lead) + the first analytics views.

**Deliverables:** `sql/ddl/46_lead_intel.sql`, `entities/lead_intel.py`:
- `derived.lead_intel` (view first, materialize if heavy) — one row per `core.lead`: identity +
  reply behaviour (n_replies, dominant_intent, all_intent_tags, has_question, has_objection,
  top_objection_type, last_reply_text, last_sentiment) + partner disposition + call/conversion
  state (is_opportunity, is_meeting, conversion_agent, funnel_stage) + `engagement_score` +
  optional LLM `profile_summary`.
- Insight views: `v_intent_distribution` (intent counts/rates by campaign/offer/segment/channel),
  `v_objection_library` (objection_type × verbatim × frequency — feeds the copy engine),
  `v_question_library` (clustered top questions). `v_disposition_funnel` already exists (WS-A).

**DoD:**
- [ ] `SELECT count(*) FROM derived.lead_intel` = `core.lead` count; no row with all-null signal columns.
- [ ] A spot lead with a reply + a disposition shows BOTH attributed in its row.
- [ ] `v_objection_library`, `v_question_library`, `v_intent_distribution` each return >0 rows.
- [ ] Funnel-by-channel works: `SELECT source_channel, disposition_class, count(*) …` returns the quality-by-channel cut Sam asked for.

---

## 5. GLOBAL SHIP GATE (overall DoD)

The project is DONE when ALL of:
- [ ] WS-0…WS-I DoDs each fully green (evidence pasted).
- [ ] A **full nightly orchestrator run completes `status='success'`** with every new phase present
  in `core.sync_run_phase` at `rows_out>0`.
- [ ] `v_warehouse_freshness` shows **no new BI feed stale**; `warehouse_qa.py` green.
- [ ] **Additive invariant holds:** diff of pre-existing table DDL is empty; only new tables/views
  added (`git diff` shows no `ALTER/DROP` on prior objects).
- [ ] **PII fully git-ignored:** `git check-ignore` passes for the partner sheet, any reply/transcript
  exports, and audio staging; `git status` shows none of them tracked.
- [ ] `derived.lead_intel` populated and joinable; the three insight views return data.
- [ ] `specs/16-bi-intent-layer.md` updated with any deltas discovered during the build; each WS has
  a short closeout (what shipped, evidence, surprises).
- [ ] One end-to-end demonstration query run and pasted: pick a real lead, show *reply → intent →
  variant → campaign → infra* on one side and *call/conversion → partner disposition* on the other.

---

## 6. KNOWN GOTCHAS

- **Single-writer DuckDB + flock contention** is the #1 failure class (hung runs, Lens-freeze
  history). New phases must be quick, idempotent, and never hold the writer long. Don't run heavy
  LLM/transcription work *inside* the writer lock — stage results, then load.
- Cloudflare blocks default UA on Instantly keys (looks like 403) → browser UA.
- Sendivo calls/leads are **phone-only** (no email) — every join must handle that.
- `raw_pipeline_reply_intent_classifications` is FROZEN — reference only, never a live dependency.
- Comms Supabase pooler auth: **port 6543 only** (5432 session mode rejects the current password
  for pipeline; lead DB uses :5432 for COPY — different project, different rule).
- Disk on the droplet is **80% full** — transcription must delete audio immediately.
