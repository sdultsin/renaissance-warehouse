-- @gate: data-backfill (one-time infra label fill; no schema change; no column add/rename/drop)
-- Depends on 1071 (core.campaign_infra table + name_heuristic precedence)
--
-- Restricted name-heuristic infra backfill for core.campaign_infra. Version 1079. [2026-07-03]
--
-- WHY THIS EXISTS (campaign-truth build follow-up; TKT-2 open-question #3, Sam-approved 2026-07-03):
--   The full name-heuristic backfill (scripts/backfill_campaign_infra_names.py, DESIGN §6) is gated
--   at >=99% OVERALL agreement against tag-derived truth AND refused itself when run --validate on
--   the live registry: 318/332 = 95.8% overall (< the 99% bar) -> correctly NOT shipped. But that
--   overall was dragged down by two token classes that DO NOT hold registry-wide; the OTD and
--   MilkBox tokens each validated at exactly 100%. Sam approved shipping the RESTRICTED version:
--   apply ONLY the two validated token classes, leave everything else honestly 'unknown'.
--
--   MEASURED per-token precision (name classifier vs tag-derived truth, live serving snapshot,
--   truth = campaign_infra rows with derivation_source IN dim_tag|census_majority|frozen_tag_table):
--       OTD      279/279 = 100.00%   <- APPLIED
--       MilkBox    7/7   = 100.00%   <- APPLIED
--       Reseller  28/41  =  68.29%   <- EXCLUDED (name 'GOOGLE'/'RESELLER-GOOG' overlaps the
--                                        recipient-vs-arm ambiguity; stays 'unknown')
--       MailIn     4/5   =  80.00%   <- EXCLUDED (too few, sub-bar; stays 'unknown')
--   Ambiguous name tokens are NEVER applied and stay 'unknown': bare OUTLOOK (recipient-vs-infra
--   unclear without OTD context) and GOOG/GOOGLE (google can be the recipient OR the Reseller arm).
--
-- WHAT THIS FILLS (scope reconciliation — READ THIS):
--   The campaign-truth CLOSEOUT quoted "156 campaigns carrying OTD/MilkBox tokens" — that was an
--   acceptance stat measured on the JUNE-ACTIVE (809) slice of the 16:22Z closeout snapshot only.
--   The restricted backfill's actual, principled scope is REGISTRY-WIDE: every currently-'unknown'
--   campaign whose name carries a validated token. On the live snapshot that is:
--       OTD-token unknowns:     248   (197 June-active, 51 May-active, 0 never-sent)
--       MilkBox-token unknowns:   0
--       -> 248 rows filled today.
--   Registry-wide is correct because the OTD->OTD label is 100% precise registry-wide (279/279);
--   excluding the ~51 May-active OTD campaigns would be arbitrary and violate 100%-or-wipe
--   (label what is provable). Reseller/MailIn/ambiguous/unclassified all stay honestly 'unknown'.
--
-- HOW (write semantics — mirrors scripts/backfill_campaign_infra_names.py apply()):
--   derivation_source := 'name_heuristic' (the canonical, LOWEST-precedence source, rank 1), so any
--   real tag/census/frozen derivation that later lands overwrites it automatically via the nightly
--   entities/campaign_infra.py _merge (strictly-higher precedence wins; never-downgrade). infra_esp
--   per the Sam-specified map: OTD->'OTD', MilkBox->'outlook'. matched_tag left NULL (no tag matched
--   — a name heuristic). derivation_note appends the token + name (append-only audit trail).
--
-- GUARDS / IDEMPOTENCY (three, all must hold):
--   1. ci.infra_vendor = 'unknown' AND derivation_source = 'unknown'  -> never clobbers a row that
--      already has (or later gained) a real derivation. Second run = 0 rows.
--   2. classified token IN ('OTD','MilkBox')                          -> only the validated classes.
--   3. ci.first_seen_at < TIMESTAMP '2026-07-04 00:00:00+00'          -> one-shot: bounds the fill to
--      campaigns that existed at ship time (the whole registry was first seeded 2026-07-03 16:07:18Z,
--      so every current row qualifies and any campaign inserted by a future nightly is excluded).
--   The classifier CASE is the EXACT CLASSIFY_SQL from the validated script (OTD > Reseller > MilkBox
--   ordering preserved) so a name the script would call Reseller/MailIn/ambiguous is never mislabeled
--   as OTD/MilkBox here. Runs inside the apply transaction (core/db.py apply_ddl_file wraps BEGIN/COMMIT).
--
-- VERIFIED read-only on the live serving snapshot before authoring: 248 OTD-token + 0 MilkBox-token
-- rows match all three guards; all 248 are (infra_vendor='unknown', derivation_source='unknown').

UPDATE core.campaign_infra AS ci
SET infra_vendor      = s.fam,
    infra_esp         = s.esp,
    derivation_source = 'name_heuristic',
    derivation_note   = trim(coalesce(ci.derivation_note || ' | ', '') ||
                        'name_heuristic [backfill_name_heuristic_restricted_20260703]: token=' ||
                        s.fam || ' name=' || coalesce(s.nm, '')),
    _loaded_at        = now(),
    _run_id           = 'backfill_name_heuristic_restricted_20260703'
FROM (
    SELECT campaign_id, nm, fam,
           CASE fam WHEN 'OTD' THEN 'OTD' WHEN 'MilkBox' THEN 'outlook' END AS esp
    FROM (
        SELECT campaign_id,
               coalesce(last_seen_name, first_seen_name) AS nm,
               CASE
                 WHEN regexp_matches(upper(coalesce(last_seen_name, first_seen_name)),
                        '(^|[^A-Z0-9])(OTD|O2D)([^A-Z0-9]|$)')
                   OR upper(coalesce(last_seen_name, first_seen_name)) LIKE '%OUTREACH TODAY%'
                        THEN 'OTD'
                 WHEN regexp_matches(upper(coalesce(last_seen_name, first_seen_name)),
                        '(^|[^A-Z0-9])RESELLER')
                        THEN 'Reseller'
                 WHEN upper(coalesce(last_seen_name, first_seen_name)) LIKE '%MILKBOX%'
                   OR regexp_matches(upper(coalesce(last_seen_name, first_seen_name)),
                        '(^|[^A-Z0-9])MB ')
                        THEN 'MilkBox'
                 WHEN upper(coalesce(last_seen_name, first_seen_name)) LIKE '%MAILIN%'
                        THEN 'MailIn'
                 WHEN regexp_matches(upper(coalesce(last_seen_name, first_seen_name)),
                        '(^|[^A-Z0-9])OUTLOOK([^A-Z0-9]|$)')
                        THEN 'AMBIG-outlook'
                 WHEN regexp_matches(upper(coalesce(last_seen_name, first_seen_name)), 'GOOG')
                        THEN 'AMBIG-google'
                 ELSE 'unclassified'
               END AS fam
        FROM core.campaign_infra
    )
) s
WHERE s.campaign_id = ci.campaign_id
  AND s.fam IN ('OTD', 'MilkBox')
  AND ci.infra_vendor = 'unknown'
  AND coalesce(ci.derivation_source, 'unknown') = 'unknown'
  AND ci.first_seen_at < TIMESTAMP '2026-07-04 00:00:00+00';
