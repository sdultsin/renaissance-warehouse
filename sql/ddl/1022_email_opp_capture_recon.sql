-- @gate: add
-- Depends on 02
-- Depends on 04
-- Depends on 16
-- 1022_email_opp_capture_recon.sql  [2026-06-27]  W1c RevOps deep-dive: email opp-capture reconciliation.
-- Applied at schema version 1022 by scripts/setup_db.py / the warehouse DDL applier.
-- Idempotent (CREATE OR REPLACE VIEW) — re-applying is a no-op. Standard SQL.
--
-- WHY (W1c audit, 2026-06-27): the Instantly email reply->opportunity pipeline writes
-- opp rows into comms.call_opportunity (mirrored here as raw_comms_call_opportunity).
-- Reconciling that capture against the warehouse opp surface is currently blocked by a
-- THREE-namespace slug mess:
--   (1) warehouse slug         (core.workspace.slug, e.g. 'koi-and-destroy')
--   (2) comms source_workspace_id = the King/Outreachify client slug, which itself
--       SPLIT ~2026-06-11: old short slugs ('funding-4') -> new full-name slugs
--       ('funding-4-sam') for the SAME physical workspace
--   (3) the hardcoded worker WORKSPACE_KEY_MAP (dead today)
-- Plus 'pre-ipo' is a duplicate slug for Max's workspace (double-poll), and several
-- dead-key workspaces are polled but never capture.
--
-- These two views are the standing reconciliation surface the downstream RevOps lanes
-- (W1f opp-tag reconciliation, W2a meeting attribution, W2b channel funnel) all need:
--   * core.v_instantly_workspace_crosswalk  — resolves all source_workspace_id values
--     (incl. the 06-11 old/new split) -> warehouse slug -> live name -> active-BF flag.
--   * core.v_email_opp_capture_recon        — per live workspace: comms-captured opps
--     (deduped mirror) vs the warehouse opp surface (campaign-day unique_opportunities,
--     poll-era), with the gap.
--
-- CAVEAT (documented, not a bug in this view): comms 'captured' counts leads the poll
-- pulled from Instantly leads/list (interest_status>=1); the warehouse wh_unique_opps
-- counts Instantly's NARROWER campaign-"opportunity" metric. They are related but not
-- equal, so comms_captured >= wh_unique_opps for most workspaces (capture pipe is
-- higher-fidelity than the warehouse opp surface). The FAITHFUL capture % (vs the live
-- leads/list interest set) is computed off-warehouse in W1c-FINDINGS.md = 74.2%.

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- Crosswalk: every comms source_workspace_id (King client slug, both pre- and
-- post-06-11 conventions) -> warehouse slug -> live display name -> active-BF flag.
-- active_bf = is this one of the 8 active Business-Funding capture surfaces.
-- wh_slug NULL = no warehouse workspace row (gone / never mirrored).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_instantly_workspace_crosswalk AS
SELECT * FROM (
    VALUES
    -- king_slug,                wh_slug,                   ws_name,                       active_bf, note
    ('funding-1',                'renaissance-4',           'Funding 1 (Samuel)',          TRUE,  'pre-06-11 slug'),
    ('funding-1-samuel',         'renaissance-4',           'Funding 1 (Samuel)',          TRUE,  'post-06-11 slug'),
    ('funding-2',                'renaissance-5',           'Funding 2 (Ido)',             TRUE,  'pre-06-11 slug'),
    ('funding-2-ido',            'renaissance-5',           'Funding 2 (Ido)',             TRUE,  'post-06-11 slug'),
    ('funding-3',                'prospects-power',         'Funding 3 (Leo)',             TRUE,  'pre-06-11 slug'),
    ('funding-3-leo',            'prospects-power',         'Funding 3 (Leo)',             TRUE,  'post-06-11 slug'),
    ('funding-4',                'koi-and-destroy',         'Funding 4 (Sam)',             TRUE,  'pre-06-11 slug'),
    ('funding-4-sam',            'koi-and-destroy',         'Funding 4 (Sam)',             TRUE,  'post-06-11 slug'),
    ('funding-5',                'renaissance-2',           'Funding 5 (Eyver)',           TRUE,  'pre-06-11 slug'),
    ('funding-5-eyver',          'renaissance-2',           'Funding 5 (Eyver)',           TRUE,  'post-06-11 slug'),
    ('renaissance-1',            'renaissance-1',           'Renaissance 1 (Instantly)',   TRUE,  'pre-06-11 slug'),
    ('renaissance-1-instantly',  'renaissance-1',           'Renaissance 1 (Instantly)',   TRUE,  'post-06-11 slug'),
    ('max-s-workspace',          'the-gatekeepers',         'Max''s workspace',            TRUE,  'post-06-11 slug'),
    ('pre-ipo',                  'the-gatekeepers',         'Max''s workspace',            TRUE,  'DUP slug of Max (double-poll); 14 genuine Pre-IPO securities opps = BF contamination'),
    ('warm-leads',               'warm-leads',              'Warm leads',                  TRUE,  'GreenBridge re-engagement; GBC-skip applies'),
    ('funding-6',                NULL,                      'Funding 6 (gone)',            FALSE, 'workspace removed'),
    ('funding-canada',           'outlook-1',               'Funding Canada',              FALSE, 'dropped; key DEAD 402'),
    ('funding-uk',               'automated-applications',  'Funding UK',                  FALSE, 'dropped; key DEAD 402'),
    ('outlook-2',                'outlook-2',               'Outlook 2',                   FALSE, 'dropped; key DEAD 402'),
    ('outlook-3',                'outlook-3',               'Outlook 3',                   FALSE, 'dropped; key DEAD 402'),
    ('renaissance-3',            'renaissance-3',           'Renaissance 3',               FALSE, 'pending-delete; key DEAD 402'),
    ('renaissance-6',            NULL,                      'Renaissance 6',               FALSE, 'gone; key DEAD 402'),
    ('renaissance-7',            NULL,                      'Renaissance 7',               FALSE, 'gone; key DEAD 401'),
    ('renaissance-8',            NULL,                      'Renaissance 8',               FALSE, 'gone; key DEAD 402'),
    ('the-dyad',                 'the-dyad',                'The Dyad',                    FALSE, 'gone; key DEAD 402'),
    ('the-eagles',               'the-eagles',              'The Eagles',                  FALSE, 'dropped (residual opps only)')
) AS t(king_slug, wh_slug, ws_name, active_bf, note);

-- ---------------------------------------------------------------------------
-- Capture reconciliation: comms-captured email opps (deduped mirror) vs the
-- warehouse opp surface (campaign-day unique_opportunities, poll-era >= 2026-06-01),
-- per resolved live workspace.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_email_opp_capture_recon AS
WITH co AS (  -- dedup the snapshot-append mirror (24.8x) to the latest load per id
    SELECT source_workspace_id, status
    FROM (
        SELECT id, source_workspace_id, status,
               row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC) AS _rn
        FROM raw_comms_call_opportunity
        WHERE source = 'instantly'
    ) d
    WHERE _rn = 1
),
captured AS (
    SELECT x.ws_name,
           bool_or(x.active_bf) AS active_bf,
           count(*) FILTER (WHERE co.status <> 'duplicate')                       AS comms_captured,
           count(*) FILTER (WHERE co.status = 'duplicate')                        AS comms_dup_merged,
           count(*) FILTER (WHERE co.status IN ('queued', 'called', 'closed'))    AS reached_close,
           count(*) FILTER (WHERE co.status = 'enrichment_failed')                AS stuck_no_phone
    FROM co
    JOIN core.v_instantly_workspace_crosswalk x ON x.king_slug = co.source_workspace_id
    GROUP BY x.ws_name
),
native AS (  -- warehouse opp surface (Instantly campaign-"opportunity" metric), poll-era
    SELECT w.name AS ws_name, sum(d.unique_opportunities) AS wh_unique_opps_pollera
    FROM (
        SELECT campaign_id, date, unique_opportunities,
               row_number() OVER (PARTITION BY campaign_id, date ORDER BY _loaded_at DESC) AS rn
        FROM raw_pipeline_campaign_daily_metrics
        WHERE date >= DATE '2026-06-01'
    ) d
    JOIN (
        SELECT campaign_id, workspace_id,
               row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) AS rn
        FROM raw_pipeline_campaigns
    ) c ON c.campaign_id = d.campaign_id AND c.rn = 1
    JOIN core.workspace w ON w.slug = c.workspace_id
    WHERE d.rn = 1
    GROUP BY w.name
)
SELECT coalesce(captured.ws_name, native.ws_name)                                  AS workspace,
       captured.active_bf,
       captured.comms_captured,
       captured.comms_dup_merged,
       captured.reached_close,
       captured.stuck_no_phone,
       native.wh_unique_opps_pollera,
       captured.comms_captured - native.wh_unique_opps_pollera                     AS comms_minus_wh_opps
FROM captured
FULL OUTER JOIN native ON captured.ws_name = native.ws_name
ORDER BY captured.comms_captured DESC NULLS LAST;
