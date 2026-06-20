-- 75_campaign_lead_type.sql  [2026-06-16 infra-data-truth / C3]
-- lead_type dimension on the campaign grain: cheap/MCA-bought leads vs normal scraped-cold leads.
-- Derived from campaign NAME (tag sync stopped 2026-06-14 — name is the source of truth), same
-- mechanism as cm/offer/is_mca. DATA-DRIVEN: edit core.lead_type_rule to change the keyword set;
-- the view re-derives automatically. Exclude-precedence: a matching EXCLUDE pattern overrides any
-- INCLUDE match (Ben/GBC are partners/people, not cheap-lead sources). Default = normal_cold.
--
-- This is the MCA-split the deliverability cluster requires ("never pool MCA with non-MCA") as a real
-- warehouse dimension, consumed by the C4 analyst + reporting. NOTE: distinct from core.campaign.is_mca,
-- which flags the MCA *offer*; lead_type flags the lead *source/cost*. STARTER keyword set — final set +
-- the 3 ambiguous campaigns (GBC+MCA, 2× "BEN CHEAP") to be locked with Sam (see QA report).

CREATE TABLE IF NOT EXISTS core.lead_type_rule (
  pattern    VARCHAR NOT NULL,   -- DuckDB regexp, matched against lower(campaign_name)
  rule_type  VARCHAR NOT NULL,   -- 'include' (-> cheap_mca) | 'exclude' (-> force normal_cold)
  note       VARCHAR,
  PRIMARY KEY (pattern, rule_type)
);

-- Keyword set = Sam's ruling [2026-06-17]: "Ben Cheap, cheap, Isaac, cheap leads, MCA, Ben ... these
-- are all cheap-leads campaigns; anything not like that = normal leads." So Ben is an INCLUDE (cheap),
-- not an exclude, and "cheap" (alone) matches — there is NO exclude list. Whole-word boundaries keep
-- "ben" from matching "Benefits"/"Brendan" and "mca" from matching inside another token.
DELETE FROM core.lead_type_rule;
INSERT INTO core.lead_type_rule (pattern, rule_type, note) VALUES
  ('\bcheap\b',          'include', 'cheap / cheap leads / "Ben Cheap"'),
  ('\b(isaac|issac)\b',  'include', 'Isaac sourcer (both spellings)'),
  ('\bmca\b',            'include', 'MCA-intent leads'),
  ('\bben\b',            'include', 'Ben = a cheap-lead source (Sam ruling); whole-word excludes "Benefits"');

CREATE OR REPLACE VIEW core.v_campaign_lead_type AS
WITH excl AS (SELECT pattern FROM core.lead_type_rule WHERE rule_type = 'exclude'),
     incl AS (SELECT pattern FROM core.lead_type_rule WHERE rule_type = 'include')
SELECT
  c.campaign_id,
  c.workspace_id,
  c.name AS campaign_name,
  CASE
    WHEN EXISTS (SELECT 1 FROM excl WHERE regexp_matches(lower(c.name), excl.pattern)) THEN 'normal_cold'
    WHEN EXISTS (SELECT 1 FROM incl WHERE regexp_matches(lower(c.name), incl.pattern)) THEN 'cheap_mca'
    ELSE 'normal_cold'
  END AS lead_type,
  -- audit flag: campaign matched BOTH an include and an exclude (the ambiguous set for human review)
  (EXISTS (SELECT 1 FROM excl WHERE regexp_matches(lower(c.name), excl.pattern))
   AND EXISTS (SELECT 1 FROM incl WHERE regexp_matches(lower(c.name), incl.pattern))) AS lead_type_ambiguous
FROM core.campaign c;

-- Meeting attribution by lead_type (Sam: cheap-leads campaign meetings "can be attributed as such").
-- Splits the meeting fact into cheap_mca vs normal_cold via the booking campaign — the input to
-- meeting-QUALITY analysis (cheap/MCA meetings are the low-quality kind).
CREATE OR REPLACE VIEW core.v_meeting_lead_type AS
SELECT m.*, COALESCE(lt.lead_type, 'normal_cold') AS lead_type
FROM core.meeting m
LEFT JOIN core.v_campaign_lead_type lt ON lt.campaign_id = m.campaign_id;
