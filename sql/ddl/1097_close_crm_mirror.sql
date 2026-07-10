-- @gate: add
-- Depends on 42 (raw_close_call — same Close org; no schema dependency)
-- Close CRM → warehouse daily mirror. Version 1097. [2026-07-10]
--
-- Closes the biggest custody gap in the mirror-coverage audit
-- (deliverables/2026-07-08-mof-orchestrator/MIRROR-COVERAGE-AUDIT.md §4): everything
-- downstream of "warm lead created" — lead status (Qualified/Customer/DNC…), the 8
-- custom fields (Campaign/Source/Source Workspace/Application Status…), disposition
-- history, IM email + SMS activity — had EXCLUSIVE custody in Close. Only calls were
-- mirrored (raw_close_call, v42). Per Sam's standing invariant (PLANE-BOUNDARY-DECISION):
-- operational stores keep copies, never exclusive custody.
--
-- Ingest: entities/close_crm_mirror.py, nightly `close` phase (after close_calls).
-- All Close API GETs, read-only. ~150k-row backfill, <5k rows/day steady.
--
-- raw_close_lead          — full nightly snapshot, UPSERT on id (~20k). `_last_seen_at`
--                           advances every run a lead is still present in Close, so a
--                           lead DELETED in Close keeps its last-known state here and is
--                           detectable via a stale _last_seen_at (durability by design).
-- raw_close_contact       — extracted from the lead payloads (contacts[] is inlined on
--                           GET /lead/) — no extra API calls. DELETE+INSERT rebuild.
-- raw_close_status_change — /activity/status_change/lead/ incremental (the disposition
--                           ledger: opp→called→qualified→booked transitions over time).
-- raw_close_email         — /activity/email/ incremental (~84k backfill). body_text kept;
--                           body_html + preview STRIPPED from api_response_raw (size: html
--                           roughly doubles the payload and adds nothing for custody).
--                           NOT fed into core.email_message: Instantly threads remain the
--                           canonical email record; this table's original content is the
--                           Close-side sends (IM-authored) + the sync provenance.
-- raw_close_sms           — /activity/sms/ incremental (~20-23k, about to grow: CRM
--                           texting moves to Close when A2P clears).
-- raw_close_lead_status   — dim, full refresh (13 statuses incl. Qualified/Customer).
-- raw_close_custom_field  — dim, full refresh (8 custom-field defs; id→name map used to
--                           resolve the named columns on raw_close_lead).
-- raw_close_smart_view    — dim, full refresh (3 saved searches incl. the warm-call view).
--
-- Notes + Opportunities + Tasks are NOT ingested: audited at 0 rows / "not used"
-- (MIRROR-COVERAGE-AUDIT §4). Additive when that changes.
--
-- Additive only. No ALTER/DROP/rename of any pre-existing table or view.

-- ── LEADS ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_lead (
    id                 VARCHAR PRIMARY KEY,   -- lead_…
    display_name       VARCHAR,
    name               VARCHAR,               -- company-ish lead name
    status_id          VARCHAR,               -- stat_… (joins raw_close_lead_status)
    status_label       VARCHAR,               -- denormalized for convenience
    description        VARCHAR,
    url                VARCHAR,
    -- the 4 attribution/funnel custom fields, resolved via raw_close_custom_field
    cf_campaign        VARCHAR,
    cf_source          VARCHAR,               -- 'Instantly' | 'Sendivo' | 'Iskra'
    cf_source_workspace VARCHAR,
    cf_application_status VARCHAR,
    custom_json        VARCHAR,               -- ALL custom.* flattened keys, JSON
    contacts_count     INTEGER,
    created_by         VARCHAR,
    updated_by         VARCHAR,
    date_created       TIMESTAMPTZ,
    date_updated       TIMESTAMPTZ,
    organization_id    VARCHAR,
    api_response_raw   VARCHAR,               -- full lead JSON (incl. contacts[])
    _last_seen_at      TIMESTAMPTZ,           -- advances each run the lead still exists in Close
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── CONTACTS (from the lead payloads) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_contact (
    id                 VARCHAR PRIMARY KEY,   -- cont_…
    lead_id            VARCHAR,
    name               VARCHAR,
    title              VARCHAR,
    primary_email      VARCHAR,               -- first email, lowercased
    primary_phone      VARCHAR,               -- first phone (E.164 as Close stores it)
    emails_json        VARCHAR,               -- all emails, JSON
    phones_json        VARCHAR,               -- all phones, JSON
    date_created       TIMESTAMPTZ,
    date_updated       TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── STATUS-CHANGE HISTORY (the disposition ledger) ───────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_status_change (
    id                 VARCHAR PRIMARY KEY,   -- acti_…
    lead_id            VARCHAR,
    old_status_id      VARCHAR,
    old_status_label   VARCHAR,
    new_status_id      VARCHAR,
    new_status_label   VARCHAR,
    user_id            VARCHAR,
    user_name          VARCHAR,
    date_created       TIMESTAMPTZ,
    organization_id    VARCHAR,
    api_response_raw   VARCHAR,
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── EMAIL ACTIVITY ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_email (
    id                 VARCHAR PRIMARY KEY,   -- acti_…
    lead_id            VARCHAR,
    contact_id         VARCHAR,
    user_id            VARCHAR,
    direction          VARCHAR,               -- incoming | outgoing
    status             VARCHAR,               -- sent | inbox | draft | …
    subject            VARCHAR,
    sender             VARCHAR,
    to_json            VARCHAR,               -- recipient list, JSON
    cc_json            VARCHAR,
    body_text          VARCHAR,               -- plain-text body (html stripped, see header)
    template_id        VARCHAR,
    thread_id          VARCHAR,
    date_created       TIMESTAMPTZ,
    date_updated       TIMESTAMPTZ,
    organization_id    VARCHAR,
    api_response_raw   VARCHAR,               -- full JSON MINUS body_html/body_preview
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── SMS ACTIVITY ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_sms (
    id                 VARCHAR PRIMARY KEY,   -- acti_…
    lead_id            VARCHAR,
    contact_id         VARCHAR,
    user_id            VARCHAR,
    direction          VARCHAR,               -- inbound | outbound
    status             VARCHAR,               -- sent | received | delivered | error | …
    text               VARCHAR,
    local_phone        VARCHAR,
    remote_phone       VARCHAR,
    error_message      VARCHAR,
    cost               VARCHAR,
    date_created       TIMESTAMPTZ,
    date_updated       TIMESTAMPTZ,
    organization_id    VARCHAR,
    api_response_raw   VARCHAR,
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── DIMS (tiny, full refresh) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_close_lead_status (
    id                 VARCHAR PRIMARY KEY,   -- stat_…
    label              VARCHAR,
    type               VARCHAR,
    _loaded_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_close_custom_field (
    id                 VARCHAR PRIMARY KEY,   -- cf id (bare, as in custom.cf_<id>)
    name               VARCHAR,               -- 'Campaign', 'Source', …
    type               VARCHAR,
    _loaded_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw_close_smart_view (
    id                 VARCHAR PRIMARY KEY,   -- save_…
    name               VARCHAR,
    type               VARCHAR,
    _loaded_at         TIMESTAMPTZ
);
