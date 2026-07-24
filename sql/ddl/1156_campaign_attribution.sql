-- @gate: add
-- Depends on 1074, 1141
-- campaign_attribution.sql  (2026-07-23)
-- Canonical campaign -> (CM, offer) attribution map. ONE source of truth for the CM filter, the
-- Offer filter, WL attribution and Sam's CM-bonus tracking. Additive/reversible (views only, in
-- the `derived` schema). Read-back verified on md:warehouse against the 2026-07-01..21 window.
--
-- ============================ LOCKED RULES (Sam, 2026-07-23) ============================
-- CM assignment, in strict precedence:
--   1) NAME markers win. If the campaign NAME carries a recognized CM's marker -> that CM.
--        '(SAM)' | ' Sam '  -> Sam      (word-boundary 'sam' also catches -SAM-, SAM, etc.)
--        'Samuel'           -> Samuel   (checked FIRST so 'Samuel' never falls to the 'sam' rule)
--        'Ido'              -> Ido       (word-boundary)
--        'Leo'              -> Leo       (word-boundary)
--        'Eyver'            -> Eyver
--      ONLY these 5 are recognized CMs. Names like 'Tim'/'Shaan'/'Tomi' are NOT CMs -> ignored.
--   2) Else WORKSPACE OWNER (authoritative core.workspace.name via core.campaign.workspace_id):
--        Funding 1 (Samuel)->Samuel · Funding 2 (Ido)->Ido · Funding 3 (Leo)->Leo
--        Funding 4 (Sam)->Sam · Funding 5 (Eyver)->Eyver · Max's workspace->Max
--   3) Renaissance 1 (Instantly, house): NAME cm if present; ELSE 'unknown' (NOT 'house', NOT R1).
--   4) Growth 1 (Pre-IPO/house): NAME cm if present; ELSE 'unknown'.
--   5) Any other workspace (Warm Leads, Tariffs, Section 125, ERC, R&D, Funding UK/Canada, The
--      Eagles/Dyad, RE Wholesale, Outlook*, Renaissance 2/3/6/7, ...): NAME cm if present; ELSE
--      'unknown'. 'unknown' is the ONLY allowed non-CM bucket (folds house / no-name / no-owner).
--
--   The cm_name TAG (v_campaign_scoreboard.cm_name / raw_pipeline_campaigns.cm_name) and
--   core.campaign.cm are NOT authoritative — stale/wrong in places (e.g. 'BRENDAN' stamped on
--   Max's 'BF# · v3/v4 · exploit/explore/scale' campaigns, which are MAX's). This map consults
--   NAME + authoritative WORKSPACE ONLY, so those stale tags are stripped automatically:
--     - the two required corrections land for free:
--         (a) R1 'Ido'-named campaign -> Ido (name marker).
--         (b) Max's BRENDAN-tagged campaigns -> Max (no name marker -> Max's-workspace owner).
--
-- OFFER assignment (name + authoritative workspace), first match wins:
--   Pre-IPO       : workspace='Growth 1' OR name ~ 'Pre-IPO'|'pre-ipo'|'accredited'
--   Section 125   : name ~ 'Section 125'|'S125'| bare '125' (digit-bounded)
--   Tariffs       : name ~ 'Tariff'
--   ERC           : name ~ 'ERC'|'R&D'
--   Business Funding : else (default)
--
--   FIX (2026-07-23, bug 2): bare 'FINANCIAL' was DROPPED from the Pre-IPO markers. It collided with
--   the *financial-industry* vertical in Leo's business-funding campaigns ('... - ARC 18 - Financial
--   - WH (LEO)'), mislabeling ~20 Leo July funding meetings as Pre-IPO. Pre-IPO is now ONLY
--   Growth-1-workspace OR name ~ 'Pre-IPO'/'pre ipo'/'accredited'. Real Pre-IPO campaigns are all
--   caught by those anyway. Re-confirmed: true July (07-01..21) Pre-IPO = 0 (Growth 1 / Pre-IPO
--   launched 2026-07-22, outside the window).
--   NOTE (meeting view): the Growth-1 workspace signal for MEETINGS is taken from the booking's own
--   workspace field (raw_im_bookings.workspace = 'Growth 1'), NOT the name->workspace mode, because
--   mode() can spuriously tie a generic reused name (e.g. '... - No show', which also exists as a
--   Growth 1 draft) into Growth 1 and manufacture a phantom Pre-IPO. booking-workspace is
--   deterministic and reflects the actual sending workspace.
--
-- Campaign universe = core.campaign FULL OUTER JOIN main.v_campaign_scoreboard on campaign_id
--   (covers 100% of campaigns with July sends OR July meetings; neither table alone is complete).
--   Authoritative workspace = core.workspace.name (via core.campaign.workspace_id); for the handful
--   of scoreboard-only campaigns, fall back to scoreboard.workspace. NEVER the stale
--   raw_pipeline_campaigns.workspace_name.
-- =======================================================================================

CREATE SCHEMA IF NOT EXISTS derived;

-- =======================================================================================
-- derived.v_campaign_attribution — THE canonical per-campaign map (one row per campaign_id).
-- =======================================================================================
CREATE OR REPLACE VIEW derived.v_campaign_attribution AS
WITH universe AS (
  SELECT COALESCE(cc.campaign_id, sb.campaign_id) AS campaign_id,
         COALESCE(cc.name,        sb.name)        AS campaign_name,
         COALESCE(w.name,         sb.workspace)   AS workspace
  FROM core.campaign cc
  FULL OUTER JOIN main.v_campaign_scoreboard sb ON sb.campaign_id = cc.campaign_id
  LEFT JOIN core.workspace w ON w.workspace_id = cc.workspace_id
),
u AS (SELECT DISTINCT campaign_id, campaign_name, workspace FROM universe),
sig AS (
  SELECT campaign_id, campaign_name, workspace,
    -- precedence 1: NAME marker (Samuel FIRST; the rest word-boundary so 'ido'/'leo'/'sam' never
    -- match inside another word)
    CASE
      WHEN campaign_name ILIKE '%samuel%'                                    THEN 'Samuel'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])sam([^a-z]|$)')      THEN 'Sam'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])ido([^a-z]|$)')      THEN 'Ido'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])leo([^a-z]|$)')      THEN 'Leo'
      WHEN campaign_name ILIKE '%eyver%'                                     THEN 'Eyver'
      ELSE NULL END AS name_cm,
    -- precedence 2: authoritative workspace owner
    CASE workspace
      WHEN 'Funding 1 (Samuel)' THEN 'Samuel'
      WHEN 'Funding 2 (Ido)'    THEN 'Ido'
      WHEN 'Funding 3 (Leo)'    THEN 'Leo'
      WHEN 'Funding 4 (Sam)'    THEN 'Sam'
      WHEN 'Funding 5 (Eyver)'  THEN 'Eyver'
      WHEN 'Max''s workspace'   THEN 'Max'
      ELSE NULL END AS owner_cm
  FROM u
)
SELECT
  campaign_id,
  campaign_name,
  workspace,
  COALESCE(name_cm, owner_cm, 'unknown')                        AS cm,
  CASE WHEN name_cm  IS NOT NULL THEN 'name'
       WHEN owner_cm IS NOT NULL THEN 'workspace_owner'
       ELSE 'unknown' END                                        AS cm_source,
  CASE
    WHEN workspace = 'Growth 1'
      OR campaign_name ILIKE '%pre-ipo%' OR campaign_name ILIKE '%pre ipo%'
      OR campaign_name ILIKE '%accredited%'                                              THEN 'Pre-IPO'
    WHEN campaign_name ILIKE '%section 125%' OR campaign_name ILIKE '%s125%'
      OR regexp_matches(campaign_name, '(^|[^0-9])125([^0-9]|$)')                         THEN 'Section 125'
    WHEN campaign_name ILIKE '%tariff%'                                                   THEN 'Tariffs'
    WHEN campaign_name ILIKE '%erc%' OR campaign_name ILIKE '%r&d%'                       THEN 'ERC'
    ELSE 'Business Funding' END                                  AS offer,
  CASE
    WHEN workspace = 'Growth 1'                                                           THEN 'workspace'
    WHEN campaign_name ILIKE '%pre-ipo%' OR campaign_name ILIKE '%pre ipo%'
      OR campaign_name ILIKE '%accredited%'
      OR campaign_name ILIKE '%section 125%' OR campaign_name ILIKE '%s125%'
      OR regexp_matches(campaign_name, '(^|[^0-9])125([^0-9]|$)')
      OR campaign_name ILIKE '%tariff%'
      OR campaign_name ILIKE '%erc%' OR campaign_name ILIKE '%r&d%'                       THEN 'name'
    ELSE 'default' END                                          AS offer_source
FROM sig;

-- =======================================================================================
-- derived.v_email_meeting_attribution — operationalizes the map for MEETINGS (CM-bonus tracking).
-- One row per LIVE, latest-snapshot Email booking (raw_im_bookings). Bookings carry a campaign NAME
-- (no campaign_id) AND their own originating `workspace` field. CM/offer are resolved by the SAME
-- LOCKED RULES, in precedence:
--   1) NAME marker (Samuel/Sam/Ido/Leo/Eyver) on the booking's campaign name.
--   2) authoritative WORKSPACE OWNER resolved by NAME (mode workspace across the full core.campaign
--      universe) -> F1-5 / Max owner.
--   3) FIX (2026-07-23, bug 1) booking's OWN workspace field (raw_im_bookings.workspace) -> owner.
--      Rationale: a chunk of genuine F-workspace / Max meetings sit on bookings whose campaign NAME
--      is truncated/renamed (e.g. 'F4 - CONSTRUCTION - GMAPS - OTD-MS - 7-09') so it matches no live
--      campaign row, OR whose reused name modes to a non-owner workspace (e.g. an Ido booking whose
--      name also exists in Warm Leads 1). Their name never resolves to the owner, so they wrongly
--      fell to 'unknown'. The booking's own workspace field carries the true owner and recovers them.
--      This step fires ONLY when 1 and 2 both yield no owner, so it is purely additive: it can move a
--      booking OUT of 'unknown' to its owner, never re-shuffle an already-attributed CM. It moved ~43
--      July meetings from unknown to owners (Ido +18, Samuel +9, Eyver +8, Sam +5, Max +2, Leo +1).
--   4) else -> 'unknown'.
--   Dedup is left to the consumer: `dedup_ident` = email > phone > booking_id. Meeting totals for a
--   window = COUNT(DISTINCT dedup_ident) within that date filter (this is the "dedup email>phone>row,
--   latest _snapshot_date" rule).
-- =======================================================================================
CREATE OR REPLACE VIEW derived.v_email_meeting_attribution AS
WITH universe AS (
  SELECT COALESCE(cc.campaign_id, sb.campaign_id) AS campaign_id,
         COALESCE(cc.name,        sb.name)        AS campaign_name,
         COALESCE(w.name,         sb.workspace)   AS workspace
  FROM core.campaign cc
  FULL OUTER JOIN main.v_campaign_scoreboard sb ON sb.campaign_id = cc.campaign_id
  LEFT JOIN core.workspace w ON w.workspace_id = cc.workspace_id
),
name_ws AS (  -- resolve one authoritative workspace per campaign NAME (markers dominate; owner is a fallback)
  SELECT campaign_name, mode(workspace) AS workspace
  FROM (SELECT DISTINCT campaign_id, campaign_name, workspace FROM universe)
  WHERE workspace IS NOT NULL
  GROUP BY 1
),
bk AS (
  SELECT b.id AS booking_id,
         TRY_CAST(b."date" AS DATE)                                                       AS meeting_date,
         b._snapshot_date,
         lower(trim(b.email))  AS email,
         b.phone, b.company, b.campaign AS campaign_name,
         COALESCE(NULLIF(lower(trim(b.email)),''), NULLIF(lower(trim(b.phone)),''),
                  CAST(b.id AS VARCHAR))                                                   AS dedup_ident,
         nw.workspace AS resolved_workspace,
         b.workspace  AS booking_ws   -- booking's OWN originating workspace (precedence-3 owner fallback)
  FROM main.raw_im_bookings b
  LEFT JOIN name_ws nw ON nw.campaign_name = b.campaign
  WHERE b.channel = 'Email'
    AND b.deleted_at IS NULL
    AND b._snapshot_date = (SELECT max(_snapshot_date) FROM main.raw_im_bookings)
)
SELECT
  booking_id, meeting_date, _snapshot_date AS snapshot_date, email, phone, company,
  dedup_ident, campaign_name, resolved_workspace, booking_ws,
  COALESCE(
    -- precedence 1: NAME marker
    CASE
      WHEN campaign_name ILIKE '%samuel%'                                THEN 'Samuel'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])sam([^a-z]|$)')  THEN 'Sam'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])ido([^a-z]|$)')  THEN 'Ido'
      WHEN regexp_matches(campaign_name, '(?i)(^|[^a-z])leo([^a-z]|$)')  THEN 'Leo'
      WHEN campaign_name ILIKE '%eyver%'                                 THEN 'Eyver' END,
    -- precedence 2: authoritative workspace owner resolved by NAME (mode across core.campaign)
    CASE resolved_workspace
      WHEN 'Funding 1 (Samuel)' THEN 'Samuel' WHEN 'Funding 2 (Ido)' THEN 'Ido'
      WHEN 'Funding 3 (Leo)' THEN 'Leo' WHEN 'Funding 4 (Sam)' THEN 'Sam'
      WHEN 'Funding 5 (Eyver)' THEN 'Eyver' WHEN 'Max''s workspace' THEN 'Max' END,
    -- precedence 3 (bug-1 fix): booking's OWN originating workspace field -> owner. Fires only when
    -- 1 & 2 yield no owner, so purely additive (only recovers currently-'unknown' F/Max meetings
    -- whose campaign name is truncated/renamed or modes to a non-owner workspace).
    CASE
      WHEN booking_ws IN ('Funding 1 (Samuel)')             THEN 'Samuel'
      WHEN booking_ws IN ('Funding 2 (Ido)','Funding 2')    THEN 'Ido'
      WHEN booking_ws IN ('Funding 3 (Leo)')                THEN 'Leo'
      WHEN booking_ws IN ('Funding 4 (Sam)')                THEN 'Sam'
      WHEN booking_ws IN ('Funding 5 (Eyver)')              THEN 'Eyver'
      WHEN booking_ws IN ('Max WS','Max''s workspace')      THEN 'Max' END,
    'unknown')                                                          AS cm,
  CASE
    WHEN campaign_name ILIKE '%samuel%'
      OR regexp_matches(campaign_name, '(?i)(^|[^a-z])sam([^a-z]|$)')
      OR regexp_matches(campaign_name, '(?i)(^|[^a-z])ido([^a-z]|$)')
      OR regexp_matches(campaign_name, '(?i)(^|[^a-z])leo([^a-z]|$)')
      OR campaign_name ILIKE '%eyver%'                                  THEN 'name'
    WHEN resolved_workspace IN ('Funding 1 (Samuel)','Funding 2 (Ido)','Funding 3 (Leo)',
                                'Funding 4 (Sam)','Funding 5 (Eyver)','Max''s workspace') THEN 'workspace_owner'
    WHEN booking_ws IN ('Funding 1 (Samuel)','Funding 2 (Ido)','Funding 2','Funding 3 (Leo)',
                        'Funding 4 (Sam)','Funding 5 (Eyver)','Max WS','Max''s workspace')  THEN 'booking_workspace'
    ELSE 'unknown' END                                                 AS cm_source,
  CASE
    WHEN booking_ws = 'Growth 1'
      OR campaign_name ILIKE '%pre-ipo%' OR campaign_name ILIKE '%pre ipo%'
      OR campaign_name ILIKE '%accredited%'                            THEN 'Pre-IPO'
    WHEN campaign_name ILIKE '%section 125%' OR campaign_name ILIKE '%s125%'
      OR regexp_matches(campaign_name, '(^|[^0-9])125([^0-9]|$)')      THEN 'Section 125'
    WHEN campaign_name ILIKE '%tariff%'                                THEN 'Tariffs'
    WHEN campaign_name ILIKE '%erc%' OR campaign_name ILIKE '%r&d%'    THEN 'ERC'
    ELSE 'Business Funding' END                                        AS offer,
  CASE
    WHEN booking_ws = 'Growth 1'                                       THEN 'workspace'
    WHEN campaign_name ILIKE '%pre-ipo%' OR campaign_name ILIKE '%pre ipo%'
      OR campaign_name ILIKE '%accredited%'
      OR campaign_name ILIKE '%section 125%' OR campaign_name ILIKE '%s125%'
      OR regexp_matches(campaign_name, '(^|[^0-9])125([^0-9]|$)')
      OR campaign_name ILIKE '%tariff%'
      OR campaign_name ILIKE '%erc%' OR campaign_name ILIKE '%r&d%'    THEN 'name'
    ELSE 'default' END                                                AS offer_source
FROM bk;
