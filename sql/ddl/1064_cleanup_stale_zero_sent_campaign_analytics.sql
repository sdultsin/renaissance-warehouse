-- @gate: data-backfill (one-time surgical row deletion; no schema change; no column add/rename/drop)
-- Depends on 32 (raw_instantly_campaign_analytics), 33/v113 (v_campaign_metrics source preference)
--
-- One-time deletion of 2 frozen creation-window rows in raw_instantly_campaign_analytics
-- (2026-07-02, promote-advisory triage: canary vcm_replies_gt_sent_is_zero got 1 != 0 +
-- sanity HARD FAIL C4_email_cum_replies_le_sent on every promote since 2026-06-30 22:23Z).
--
-- ROOT CAUSE (deleted-campaign churn): entities/campaign_analytics.py mirrors
-- GET /campaigns/analytics per workspace and UPSERTs one row per campaign. A campaign
-- deleted from Instantly stops being returned, so its LAST-SEEN row freezes — intended
-- and normally valuable (final cumulative stats for deleted campaigns). But these 2
-- campaigns were deleted inside their creation window BEFORE any post-send analytics
-- pull, freezing an internally-inconsistent creation-time state: emails_sent_count=0
-- with reply_count_unique>0 (a pre-first-send reply, e.g. a lead moved in carrying an
-- existing thread) while raw_pipeline_campaign_daily_metrics records their real sends.
-- v_campaign_metrics prefers an existing analytics row over the pipeline fallback, so
-- they surface as sent=0 < replies — tripping C4/the canary and masking real volume.
--
--   1. 2e67f0d1-cc05-46c9-a943-ef5b10b81520  "F4 - GEN - OTD - R2 (SAM)" (koi-and-destroy)
--      frozen 2026-06-29T10:12Z at 0 sent / 1 reply / 48k leads; pipeline fact shows
--      38,648 real sends (06-29/06-30) + 52 distinct repliers; deleted in the 06-30/07-01
--      F4 purge before the 07-01T09:26Z analytics pull (workspace peers refreshed, it did
--      not). THE canary/C4 tripper.
--   2. 3d029395-e266-48ec-af91-a7ba15560a2c  "General" (workspace 7d4e8e68, analytics sync
--      stopped 06-17 cancellation-wave) frozen 2026-06-12 at 0 sent / 13 replies; pipeline
--      fact shows 791 real sends. Same artifact class (not in the vcm base today, so not
--      tripping the canary — cleaned for consistency).
--
-- Post-delete, v_campaign_metrics serves both campaigns via the pipeline_fallback path
-- (metric_source='pipeline_fallback'): true additive sent + lead-level distinct repliers
-- CLAMPED by the v113 LEAST guard. Verified expectation for (1): sent=38,648,
-- unique_replies=52 -> canary back to 0 with genuine signal (no allowlist needed).
--
-- Keyed on campaign_id (campaign RENAMES keep campaign_id — e.g. the BOUNCED-rename
-- guard — so name is never a key). Guarded on emails_sent_count=0: if Instantly ever
-- returns these campaigns again the upsert re-adds a fresh consistent row and this
-- migration deletes nothing. Idempotent: second run deletes 0 rows. Runs inside the
-- apply transaction (core/db.py apply_ddl_file wraps BEGIN/COMMIT).

DELETE FROM raw_instantly_campaign_analytics
WHERE emails_sent_count = 0
  AND campaign_id IN (
    '2e67f0d1-cc05-46c9-a943-ef5b10b81520',  -- F4 - GEN - OTD - R2 (SAM), frozen 06-29
    '3d029395-e266-48ec-af91-a7ba15560a2c'   -- General (ws 7d4e8e68), frozen 06-12
  );
