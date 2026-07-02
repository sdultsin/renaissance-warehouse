-- 1027_account_campaign_live.sql  [2026-06-26]
-- core.account_campaign_live — fresh, Instantly-DIRECT campaign membership per inbox.
-- ONE row per inbox that is in >=1 campaign, with how many campaigns / how many ACTIVE
-- (status=1) / the campaign names. Populated nightly by entities/account_campaign_live.py
-- straight from Instantly (list campaigns -> resolve each campaign's email_tag_list via
-- /accounts?tag_ids + explicit email_list -> invert to account->campaigns).
--
-- WHY a NEW table (not core.account_campaign): that one is owned by the box-side
-- sync_account_campaign.py and is read by the PORTAL (scripts/portal_data.py) and
-- core.v_account_campaign_offer. It went stale (Jun 17) because that box job stalled.
-- This table is additive and independent — it does NOT touch account_campaign, so no
-- existing consumer is affected. v_inbox_overview will read campaign counts from here.
--
-- ADDITIVE new table; full-replace per workspace_uuid (delete+insert after a clean pull).
--
-- @gate: add
-- Depends on 97
CREATE TABLE IF NOT EXISTS core.account_campaign_live (
    account_email      VARCHAR NOT NULL,   -- lower(trim(email)) — one row per inbox in a campaign
    workspace_slug     VARCHAR,
    workspace_uuid     VARCHAR NOT NULL,   -- the full-replace key
    n_campaigns        INTEGER,            -- distinct campaigns this inbox is attached to
    n_active_campaigns INTEGER,            -- of those, how many are status=1 (active/running)
    campaigns          VARCHAR,            -- campaign names, sorted-distinct, ' | '-joined
    _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id            VARCHAR,
    PRIMARY KEY (workspace_uuid, account_email)
);
CREATE INDEX IF NOT EXISTS ix_acl_email ON core.account_campaign_live (account_email);
