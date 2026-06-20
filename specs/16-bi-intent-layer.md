# Spec 16 — BI / Lead-Intent Layer

**Status:** DESIGN (draft for Sam's sign-off — no DDL applied yet)
**Date:** 2026-06-08
**Owner:** new "Renaissance BI" workstream
**Depends on:** spec 15 (sync modes), the in-flight reply-enrichment work (`raw_instantly_email`, `v_reply_enriched`)

---

## 1. What this is

The BI side of the warehouse. Today we *store* replies and dispositions but draw
**zero structured insight** from them. We are sitting on thousands of real human
responses to our cold email — the good, the bad, the questions, the objections —
plus a funding-partner feedback sheet that tells us what happened to leads *after*
we booked them. None of it is queryable as intent.

This spec adds a thin, **additive** layer that turns that exhaust into:

1. **A rich intent taxonomy on every reply** — beyond today's `positive/negative`.
2. **A per-lead "intent profile" (the intent DB)** — one row per lead we have *any*
   signal on, combining scraped attributes with *what the lead actually said/did*.
3. **Insight surfaces** — objection library, question library, intent distribution,
   disposition funnel — so the copy engine, sales, and Sam can act on the feedback.

> **Scope discipline:** the universe is **leads with a signal** (a reply OR partner
> feedback), not the 27M scraped leads. That is thousands of rows, not millions. We
> do **not** build a canonical `core.lead` over the whole DB.

---

## 2. Non-negotiable: don't break anything

Everything here is **purely additive**. Hard rules:

- **No `ALTER`/`DROP` on any existing table.** New tables only.
- New DDL files start at **`41_*`** (last used = `40_campaign_daily.sql`).
- New entity modules register under **new phase names** (`partner_feedback`, `intent`,
  `bi`); they do not touch `core/config.py` ordering for existing phases, and they
  reuse the existing `sheets` phase slot for the partner sheet just like
  `entities/sheets_mirror.py` does.
- **Reuse existing reply infra — do not duplicate it.** We already have
  `raw_instantly_email` (direct Instantly inbound), `raw_pipeline_reply_data` (mirror),
  and `raw_pipeline_reply_intent_classifications` (thin intent). The new `core.reply`
  consolidates those *as a view-backed canonical*, it does not re-ingest.
- The intent classifier is **incremental + idempotent** (classify only un-classified
  replies). A classifier failure must never block the nightly build.
- **PII:** lead emails + verbatim reply text are PII and the warehouse repo is public.
  The partner sheet and any verbatim exports are **git-ignored** (`seed_data/partner-feedback/`,
  `*.xlsx`) and live droplet/local only — same rule as the domain list.

---

## 3. What already exists (don't rebuild)

| Surface | What it has | Gap |
|---|---|---|
| `raw_instantly_email` | raw inbound reply: `lead_email`, `reply_text`, `step`, `thread_id`, `reply_timestamp` | no intent, no variant, no lead rollup |
| `raw_pipeline_reply_data` | mirror: `reply_text`, `intent`, `from_name`, `subject`, `step`, `variant` | thin `intent`, dependency being retired |
| `raw_pipeline_reply_intent_classifications` | `intent` (positive/negative/...), `intent_source` | binary-ish, no objection/question structure |
| `core.opportunity` | lead-level opps (sendivo + instantly) | only opps, no general reply lead |
| `core.meeting` | booked meetings | funnel endpoint only |
| sheets mirror (`raw_sheets_*`) | generic Google-Sheet snapshot loader | partner sheet not modeled |

The gap is everything between "we have the raw text" and "we know what the lead meant
and who they are."

---

## 4. The partner feedback source (this sheet)

`seed_data/partner-feedback/partner-disposition-feedback__2026-06-MTD.xlsx`
(normalized → `…__lead_detail.csv`). 134 booked leads the funding partner's sales reps
worked, with their disposition. Distribution this period:

```
No show 62 · DNQ 29 · Bad contact info/Data issue 14 · Not interested 6 ·
Unreachable 6 · Cancelled 4 · Disputed booking 3 · Already in system 3 ·
Reschedule/Rebooked 4 · LIVE OPPORTUNITY 1 · Pipeline (soft) 1
```

Columns: `lead_email, business_name, industry, id_confidence (High/Med/Low), rep,
disposition, rep_notes`. Business name/industry are **inferred** from email handle +
rep notes (82/134 are low-confidence / Gmail handles) → store `id_confidence`, never
treat inferred business as fact.

This sheet is recurring (MTD). The permanent ingest path is the **Google Sheet → sheets
mirror**, not the xlsx; the xlsx is the seed/backfill for this first period.

---

## 5. New schema

Three layers, same `raw_ → core → derived` discipline as the rest of the warehouse.

### 5.1 RAW — landing

**`raw_partner_lead_feedback`** *(new, DDL `41`)* — one row per (lead_email, source_period).
Typed (not opaque `row_json`) because we query disposition/rep directly.

| Column | Type | Notes |
|---|---|---|
| `lead_email` | VARCHAR | lower-cased, the join key |
| `business_name` | VARCHAR | inferred — pair with `id_confidence` |
| `industry` | VARCHAR | inferred |
| `id_confidence` | VARCHAR | High / Medium / Low |
| `rep` | VARCHAR | partner sales rep who worked the lead |
| `disposition` | VARCHAR | raw disposition string |
| `rep_notes` | VARCHAR | free text — gold for intent |
| `source_period` | VARCHAR | e.g. `2026-06-MTD` |
| `_loaded_at`,`_run_id` | TIMESTAMPTZ/VARCHAR | standard |

Loader = new entity `entities/partner_feedback.py`, runs in the existing **`sheets`**
phase (CSV staging like `sheets_mirror.py`). Idempotent per `source_period`.

### 5.2 CORE — canonical facts

**`core.reply`** *(new)* — one canonical row per inbound human reply, consolidating
`raw_instantly_email` (primary, Instantly-wins) + `raw_pipeline_reply_data` (fallback for
pre-cutover history), deduped on `(lead_email, thread_id, reply_timestamp)`. Carries the
recovered `variant` (from `v_reply_enriched`) and `is_auto_reply`.

| Column | Type |
|---|---|
| `reply_id` | VARCHAR (PK, = Instantly `email_id` when present) |
| `lead_email`, `campaign_id`, `workspace_id` | VARCHAR |
| `step`, `variant` | INTEGER / VARCHAR |
| `subject`, `reply_text` | VARCHAR |
| `reply_timestamp` | TIMESTAMPTZ |
| `is_auto_reply` | BOOLEAN |
| `source` | VARCHAR (`instantly` / `pipeline`) |

**`core.reply_intent`** *(new — the heart of it)* — one row per `reply_id`, produced by
an LLM classifier over `reply_text` (+ subject + step context). Multi-signal, not a
single label:

| Column | Type | Notes |
|---|---|---|
| `reply_id` | VARCHAR (PK→`core.reply`) | |
| `primary_intent` | VARCHAR | one of a **fixed enum** (§6) |
| `intent_tags` | VARCHAR[] | secondary labels (multi) |
| `sentiment` | VARCHAR | positive / neutral / negative |
| `is_question` | BOOLEAN | lead asked something |
| `is_objection` | BOOLEAN | lead pushed back |
| `objection_type` | VARCHAR | price / timing / trust / not-the-DM / already-have / no-need … NULL |
| `is_unsubscribe` | BOOLEAN | "remove me" / "stop" |
| `is_referral` | BOOLEAN | "talk to my partner/CFO" |
| `is_wrong_person` | BOOLEAN | not the decision maker |
| `summary` | VARCHAR | one-line what-they-said |
| `classifier_model` | VARCHAR | e.g. `claude-haiku-4-5` |
| `classifier_version` | INTEGER | bump to force re-classify |
| `confidence` | DOUBLE | 0–1 |
| `classified_at` | TIMESTAMPTZ | |

Classifier = new offline batch entity `entities/reply_intent_llm.py`, new **`intent`**
phase (after the reply ingest). Incremental: only rows in `core.reply` with no
`core.reply_intent` row at the current `classifier_version`. Haiku-class model, batched,
cheap. Falls back silently (logs, no abort) on API error.

**`core.lead_disposition`** *(new)* — canonical partner feedback, one row per
(lead_email, source_period); promotes `raw_partner_lead_feedback` with the disposition
string mapped to a tidy `disposition_class` enum (lost / no-show / disqualified /
bad-data / reschedule / live / in-system).

### 5.3 DERIVED — the intent DB + insight surfaces

**`derived.lead_intel`** *(new — "the intent DB" / customer profile)* — **one row per
lead_email that has any signal** (≥1 reply OR partner feedback OR opp/meeting). This is
the thing Sam described: *"know more about the lead than the name/company/segment we
scraped — what they actually replied."*

| Group | Columns |
|---|---|
| identity (scraped) | `lead_email`, `first_name`, `company`, `segment`, `industry`, `lead_source`, `last_campaign_id` |
| reply behaviour | `n_replies`, `first_reply_at`, `last_reply_at`, `dominant_intent`, `all_intent_tags` (array), `has_question`, `has_objection`, `top_objection_type`, `last_reply_text`, `last_sentiment` |
| partner feedback | `partner_disposition`, `disposition_class`, `partner_rep`, `partner_business_name`, `partner_id_confidence`, `partner_notes` |
| funnel state | `is_opportunity`, `is_meeting`, `funnel_stage` (replied→opp→meeting→worked→disposition) |
| derived | `engagement_score`, `profile_summary` (optional LLM one-liner) |

Built as a view first (`v_lead_intel`); materialize to `derived.lead_intel` nightly if
it gets heavy.

**Insight views** (pure functions of the above — the "draw insights" payoff):

- `v_intent_distribution` — counts/rates per `primary_intent` over time, sliced by
  campaign / offer / segment / lead_source. *"What are people actually saying back?"*
- `v_objection_library` — `objection_type` × verbatim examples × frequency × which
  segment/offer. **Direct feed to the copy engine's `floor.yaml` / angle library.**
- `v_question_library` — most-asked questions, clustered. Feeds FAQ + copy + the AIM
  app-link bot.
- `v_disposition_funnel` — partner disposition distribution and its correlation to our
  own reply-intent labels (e.g. do "No show" leads share a reply pattern we can screen
  pre-booking?).

---

## 6. Intent enum (`primary_intent`)

Fixed, versioned. Start small; grow deliberately (like `floor.yaml`).

```
interested            — wants to talk / move forward (the LIVE OPPORTUNITY)
info_request          — "send me details / rates / how does it work"
objection_price       — too expensive / rates / fees
objection_timing      — "not now / call me in Q3"
objection_trust       — "who are you / is this a scam"
objection_no_need     — "we're funded / don't need it"
not_decision_maker    — wrong person / referred elsewhere
unsubscribe           — remove me / stop / not interested (hard no)
auto_reply            — OOO / autoresponder / bounce-ish
hostile               — angry / spam complaint
neutral_other         — acknowledgement, unclear, smalltalk
```

`intent_tags` carries the secondary signals (e.g. an `interested` reply that also asks
a question → `primary=interested`, `tags=[info_request]`, `is_question=true`).

---

## 7. Wiring (nightly orchestrator)

New phases, slotted after the data they depend on, all behind the same single nightly run:

```
… → instantly (campaign_analytics, instantly_replies) →
    intent        [entities/reply_intent_llm.py]   ← classify new core.reply rows
    sheets        [entities/partner_feedback.py]    ← load partner CSV (existing phase)
    bi            [entities/lead_intel.py]          ← rebuild v_lead_intel + insight views
```

Each registers like `pipeline_mirror.py`. `WAREHOUSE_RUN_INTENT=1` / `WAREHOUSE_RUN_BI=1`
config flags gate them until parity is confirmed, mirroring the `instantly_replies`
roll-out pattern.

---

## 8. Build order (proposed)

1. **`raw_partner_lead_feedback` + `entities/partner_feedback.py`** — load *this* sheet
   (the CSV is ready). Smallest, immediately useful, zero dependencies. → `v_disposition_funnel`.
2. **`core.reply`** — consolidate the two raw reply sources into one canonical fact.
   (Coordinate with the reply-enrichment chat so we share `v_reply_enriched`, not fork it.)
3. **`core.reply_intent` + classifier** — the LLM batch job + enum. Backfill all history once.
4. **`derived.lead_intel` + insight views** — the intent DB and the four insight surfaces.
5. **Recurring partner ingest** — point at the live Google Sheet via the sheets mirror so
   each month's MTD lands automatically.

---

## 8b. Close CRM / warm-call (BOF) layer — VERIFIED 2026-06-08

Live-API findings against `orga_…Renaissance Growth` (key `api_7J…`, =`CLOSE_API_KEY`):

- **Close is the warm-call dialer + sink.** We push opps *in* today (Instantly/Sendivo →
  comms Supabase `call_opportunity` → Close lead, 3 custom fields). The warehouse does **not**
  read Close back. That back-pull is the new BOF ingest.
- **`GET /api/v1/activity/call/`** returns rich call objects: `disposition`
  (answered/no-answer), `status`, `duration`, `recording_duration`, `recording_url`,
  `has_recording`, `outcome_id`/`outcome_reason`, `note` (rep's free-text — e.g. "not
  interested"), `cost`, `direction`, `lead_id`, `phone`, `date_answered`. **97/100 recent
  calls are recorded.**
- **Transcripts: not exposed as a field**, BUT `recording_url`
  (`/call/{id}/recording/`) returns a real **downloadable MP3** with the API key. → We
  transcribe ourselves (Whisper, the existing `/transcribe` path), pennies/call. No native
  Close "Call Assistant" transcript in the public API.
- **Attribution link is intact:** the Close lead carries `cf_aguD41…`=Source
  (Instantly/Sendivo), `cf_nNHBuW…`=Campaign, `cf_IQZwTl…`=Workspace. So a call →
  lead → Campaign/Source → our existing `core.campaign` ontology. ⚠ **Sendivo (SMS) leads
  are phone-only (no email)** → the lead spine must key on **email OR phone**.

**New objects/ingest:** `sources/close.py` + `entities/close_calls.py` → `raw_close_call`
→ `core.call` (+ `core.call_transcript`, droplet-only PII) → `core.call_outcome` (LLM
classify the rep note + transcript into result/quality). New `close` phase, incremental by
`date_updated`.

## 8c. Ontology framing (Palantir-style) — MVP = objects + links

MVP per Sam = the **objects layer** + **links layer** only (get it together, organized,
attributed); insight/analytics views come after.

**Objects (nouns).** New = **Lead** (signal-scoped spine, keyed email∪phone),
**Reply**, **ReplyIntent**, **PartnerDisposition**, **Call**, **CallTranscript**,
**CallOutcome**. Already built (reuse) = **Campaign**, **Variant**, **Inbox/Domain/ESP**
(infra), **Opportunity**, **Meeting**, **SMS/WA Conversation** (sendivo/comms).

**Links (typed edges) — the MVP deliverable.** Most join keys already exist:
```
Lead --replied_with-->        Reply --in_response_to--> Variant --of--> Campaign --sent_via--> Inbox/Domain/ESP
Lead --got_feedback-->        PartnerDisposition (email)
Lead --converted_via-->       Channel {cold_email | sms | wa | warm_call}
Lead --was_called_in-->       Call --resulted_in--> CallOutcome ; Call --has--> CallTranscript
Lead --became-->              Opportunity --became--> Meeting
Call/Conversation --attributed_to--> Campaign  (via Close custom fields / Sendivo brand)
```
This closes Sam's BOF loop: *this lead → replied with this thread → to this variant → in
this campaign → which used this infra → and the partner/warm-caller said this about it.*
The right half (Campaign→Variant→Infra→perf) is **already built**; MVP just lands the left
half and the join edges.

## 9. Decisions — RESOLVED (Sam, 2026-06-08)

1. **Lead spine** = attribute *every* signal we have on a lead (reply / Close / xlsx /
   SMS-WA / opp / meeting) to that lead. Signal-scoped (not all 27M) but **exhaustive on
   signal** — "a pretty big data project." Pull scraped attrs from `pipeline.public.leads`
   for the signal subset (slim, on-demand join). Key = **email ∪ phone** (see §11).
2. **Transcription** = **local Whisper, no paid API** (Sam's Mac, M3 Pro — better box than
   the CPU droplet). Decoupled, swappable stage (§12).
3. **Transcribe everything** recorded. Also keep the structured call data (disposition,
   duration, outcome, cost, rep note) regardless of transcription.
4. **Partner sheet** = **manual xlsx drop**, no live Google Sheet. Loader reads the staged
   CSV; each period is a new drop. (Drop step-5 "live sheet" wiring.)
5. **Classifier** = just run the Haiku-class batch (cheap). `profile_summary` LLM one-liner:
   include it (cheap, high-value), but `lead_intel` stays useful purely structured if it lags.

## 10. Conversion model — the muddy part, made extensible (MVP = binary)

A lead is advanced to a funding-partner appointment by an **agent**, and the set of agents
is **evolving** — the schema must absorb the roadmap without migrations. Model two
orthogonal dimensions on every conversion:

- **`source_channel`** — how we first reached the lead: `cold_email` (Instantly) / `sms` /
  `wa` (WhatsApp). (Already derivable: Instantly vs Sendivo-brand vs Iskra.)
- **`conversion_agent`** — who/what produced the appointment/conversion. **Versioned enum,
  MVP populates only the first two:**
  ```
  im              — human inbox manager replies to SMS/WA and books the meeting   [MVP]
  warm_caller     — warm caller works an opp (from SMS/WA/Instantly) → sets partner appt [MVP]
  sms_aim_v1      — AIM sends application link; lead fills → partner calls them    [ROADMAP]
  sms_aim_v2      — AIM autonomously checks availability, negotiates, books        [ROADMAP]
  ```
- **`conversion_type`** — `meeting_booked` (IM / AIM v2) / `appointment_set` (warm caller) /
  `application_submitted` (AIM v1). MVP only ever sees the first two.

> ⚠ **Semantic Sam flagged:** a warm-caller "close" = **an appointment is set with the
> funding partner**, NOT a funded/closed deal. The lead then enters the partner's hands and
> comes back as a **PartnerDisposition** (the xlsx: no-show / DNQ / LIVE OPPORTUNITY). So the
> true BOF funnel is: `conversion_event (appt set) → partner works it → partner_disposition`.
> Closing-rate/quality is measured off **partner disposition**, not the warm-caller's "yes."

**Objects this adds:**
- **`core.conversion_event`** — one row per appointment/booking/application, carrying
  `lead_key, source_channel, conversion_agent, conversion_type, occurred_at, campaign_id,
  warm_caller_id (nullable)`. Feeders: `core.meeting` (IM/Slack-scraped bookings → agent=`im`),
  `core.call` (Close appointment-set calls → agent=`warm_caller`). One unifying object so
  "how did this lead convert, by which mechanism" is a single query, and AIM v1/v2 are just
  new feeders + enum values later.
- **`core.warm_caller`** — the actor. **MVP: a single aggregate row** (`warm_caller_id =
  'ALL'`); all warm-call activity attributes to it. Column exists now so per-rep split later
  = backfill `warm_caller_id` from Close `user_id`/`user_name`, **no schema change**. Tracks
  warm-caller performance (calls, connect rate, appt-set rate, downstream partner outcome).

## 11. Identity resolution (email ∪ phone)

One real lead surfaces as: an Instantly lead (email), a Sendivo SMS contact (phone), a Close
lead (email and/or phone), an xlsx row (email). To attribute *everything* to *one* lead we
resolve identity:

- **Key precedence:** exact `email` match where present; else exact `phone` (E.164-normalized).
- **Close as the bridge:** a Close lead often carries **both** email and phone → it links the
  email-world (Instantly/xlsx) to the phone-world (Sendivo) for the same person.
- **MVP = deterministic only** (exact email / exact normalized phone / Close bridge). No fuzzy
  name/company matching yet. Unresolved phone-only and email-only rows stay separate Lead
  objects with a `resolution_confidence` flag — never silently merged. `core.lead` carries
  both `email` and `phone` plus a stable surrogate `lead_key`.

## 12. Transcription stage (local Whisper)

Decoupled batch, no paid API:
1. List Close calls where `has_recording` and we have no transcript yet (incremental).
2. Download each `recording_url` MP3 (authenticated).
3. Local **Whisper** (Mac M3 Pro — `mlx-whisper` / `whisper.cpp` Metal; or `faster-whisper`).
4. Write `{call_id, transcript, model, lang, transcribed_at}` to a staging dir →
   loaded into **`core.call_transcript`** (⚠ PII — droplet/local only, git-ignored).

Swappable: the stage only needs *somewhere* Whisper runs. Mac-local respects "no API" + uses
the better hardware; caveat = automated nightly needs the Mac on, so MVP runs it as an
on-demand/periodic local batch, transcripts synced to the droplet warehouse. (If we later want
hands-off nightly, move the same stage to droplet `whisper.cpp` — still no API.)

## 13b. Comms "middleman" Supabase — consolidation assessment (2026-06-08)

Sam: "eliminate the middleman Supabase (`hlkdpldliczhfjdnfsbq`), fold into the warehouse."
**Investigated live. Two goals are tangled here — split them:**

**(A) "All Renaissance data in one place" (analytics) — ALREADY TRUE.** The nightly
`comms_mirror.py` already mirrors this project into the warehouse: `raw_comms_call_opportunity`
(28,867), `_conversation` (60k), `_message` (88k), `_phone_enrichment` (2,283), `_suppression`
(17.8k), `_brand`, `_ai_decision_log` (54k), `_instantly_message`, `_escalation`. Minor gaps to
close (trivial, additive): `close_sync` (16k, outbound-to-Close log), `gbc_application`,
`app_link_check`. `webhook_receipt` (5.97M raw receipts) is intentionally NOT mirrored (noise).
→ For querying/BI, the comms data is already consolidated. **Nothing to "transfer."**

**(B) "Eliminate the Supabase operationally" — NOT viable in DuckDB.** This project is not a
passive relay; it is the **operational backbone** of the SMS/AIM/warm-call system:
- **Scheduler:** 5 active `pg_cron` jobs — `comms_scheduler_run` (1m), `comms_call_pipeline_tick`
  (1m, enrich+push-to-Close), `comms_instantly_poll_tick` (30m, pull Instantly opps),
  `comms_app_link_cadence_sweep` (10m), `comms_enrichment_digest_tick` (1h). They call
  `public.run_*()` → pg_net → the Cloudflare worker (`comms-orchestration…workers.dev`).
- **Webhook target + concurrent OLTP** (Sendivo/Instantly inbound → `webhook_receipt`, 6M rows).
- **State store** for dedup / enrichment results / Close idempotency (`close_sync`, `call_opportunity`).

DuckDB is a **single-writer analytical file** — it can't be a webhook endpoint, run cron, or take
concurrent worker writes (the existing flock contention proves this). Folding ops in = re-platforming
the comms app, which adds complexity, not removes it. **Recommendation: keep Supabase as the comms
OLTP store; warehouse stays the single analytical sink (already mirrors it).** The "one place" goal
is met for data/BI. A separate "fewer operational moving parts" effort is possible but DuckDB is not
the target.

## 13c. Phone-enrichment backfill gap — FIX (MVP)

Real gap Sam flagged: enriched phones for Instantly opps live only in
`comms.phone_enrichment.mobile_e164` → pushed to Close → **never written back to the lead DB
(`edpyqbiqzduabtjhwfaa`) nor the DuckDB lead mirror.** Fix: when enrichment succeeds, **also write
the phone to the lead DB Supabase**, so the nightly lead-mirror picks it up — single source, no
double-sync. Small add to the call-pipeline tick (or a nightly backfill job over `phone_enrichment`).

## 13d. Identity mapping — Sam's simplification (adopted, supersedes §11)

CRM lead → lead-DB lead, **fallback chain, never silent gaps:**
1. If the opp has an **email** (Instantly) → match lead-DB by email.
2. Else / on miss → match by **phone (E.164)** (Sendivo leads are phone-only).
3. **Neither matches → FLAG** (surfaced, not dropped).
4. **Phone matches MULTIPLE lead-DB leads → ALERT Sam first** (don't auto-pick; he decides). Not
   expected, but must be on his radar. Same for any ambiguous merge.

## 13e. Transcription — RESOLVED: droplet-local, no Mac dependency

Droplet (`renaissance-worker`) checked: 8 cores, 11 GB free RAM — **can run `faster-whisper`
locally** (not yet installed; needs `faster-whisper` + `ffmpeg`). → **No Mac dependency, no API
cost, PII (lead call audio) stays in-house.** Caveats: droplet load is high (~7/8) + disk 80% →
run transcription off-peak, `nice` it, delete MP3s after transcribing (~1.5 GB/day at 10 h).
*OpenAI fallback for reference:* 10 h/day = 600 min × $0.006/min = **~$3.60/day (~$108/mo)** for
`whisper-1`/`gpt-4o-transcribe`; `gpt-4o-mini-transcribe` = $0.003 (~$54/mo). Cheap but recurring +
ships lead audio to OpenAI. **Going droplet-local.**

## 13. Build order (revised)

1. **Partner feedback** — `raw_partner_lead_feedback` + `entities/partner_feedback.py` (CSV
   ready, zero deps). → `core.lead_disposition`, `v_disposition_funnel`. **First.**
2. **Close call ingest** — `sources/close.py` + `entities/close_calls.py` → `raw_close_call`
   → `core.call` + `core.warm_caller` (aggregate) + `core.call_outcome`. Zero deps, API verified.
3. **`core.lead` + identity resolution** (§11) — the spine email∪phone, Close-bridged.
4. **`core.reply` + `core.reply_intent`** (§5–6) — canonical replies + LLM intent.
5. **`core.conversion_event`** (§10) — unify meeting + warm-call appt; agent/channel dims.
6. **Transcription** (§12) — local Whisper → `core.call_transcript` → `core.call_outcome` enrich.
7. **`derived.lead_intel` + insight views** — the intent DB + objection/question/disposition surfaces.
```
