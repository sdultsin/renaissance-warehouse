-- 87_norm_campaign_name_harden.sql  [2026-06-19]  meeting-attribution chat.
-- handoffs/2026-06-19-meeting-attribution-100pct-handoff.md  (DoD #2: recover the
-- code-recoverable `unmatched` tail in the sheet->campaign norm-name join).
-- Standard SQL, idempotent (CREATE OR REPLACE MACRO). Append-only DDL; supersedes the
-- macro body first defined in sql/ddl/64_funding_form.sql. Apply via apply_ddl_file(version=87).
--
-- WHY
-- ---
-- The Funding-Form 'Campaign Name' is sometimes TRUNCATED mid-suffix by Grace's data entry, e.g.
--   "ON - GOOGLE - BEN CHEAP 1/2 - (EYVER"     (the real campaign is "... - (Eyver)")
-- The 64-version norm only strips a COMPLETE trailing " - (Name)" group (closing paren required:
-- `\s*-\s*\([^)]*\)\s*$`). A truncated tail with an OPEN paren and no close survives the strip,
-- so the normalized key never equals the campaign's key and the meeting lands campaign_id NULL,
-- match_method='unmatched'. This adds ONE surgical strip for that truncated-tail case.
--
-- SAFETY (verified read-only against serving snapshot warehouse_20260619_063042_366, 2026-06-19)
-- ---------------------------------------------------------------------------------------------
--   * The new strip `\s*-\s*\([^)]*$` matches ONLY a trailing " - (" whose paren group is never
--     closed before end-of-string ([^)]* forbids a ')' in the tail). A complete " - (Name)" group
--     is left for the existing 64-strip to handle, so it can NOT over-strip a legitimate name.
--   * Over ALL sheet email rows: old norm-name match = 1916, hardened = 1921 (+5 recovered),
--     rows whose EXISTING match changes = 0 (zero regressions). 0 campaign norms change.
--   * The +5 gain materializes in core.meeting at the next idempotent meeting.py rebuild (no
--     backfill needed); it is pure upside on an already-99.4% sheet-email attribution.
--
-- NOTE: this does NOT address SMS/WhatsApp/Call (no joinable campaign dimension exists for them),
-- nor the source-side channel-mislabels / "No Campaign" free-text entries — those need the
-- Funding-Form campaign_id dropdown (spec'd in deliverables/2026-06-19-meeting-attribution/).

CREATE OR REPLACE MACRO main.norm_campaign_name(x) AS (
  trim(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(lower(trim(x)), '\s+', ' ', 'g'),  -- collapse whitespace
                '^(on|off|old)\b[\s\-:_|]*', ''),                 -- strip ON/OFF/OLD prefix
              '\s*\(copy\)\s*$', ''),                             -- trailing (copy)
            '\s*-\s*\([^)]*\)\s*$', ''),                          -- trailing " - (Name)" (complete)
          '\s*-\s*\([^)]*$', ''),                                 -- trailing " - (Name" (TRUNCATED, no close) [DDL 87]
        '\s*\brv\b\s*$', ''),                                     -- trailing RV
      '\s*\(copy\)\s*$', '')                                      -- second (copy) after RV
  )
);
