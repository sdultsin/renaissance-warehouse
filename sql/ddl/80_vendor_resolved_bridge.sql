-- 80_vendor_resolved_bridge.sql  [2026-06-17 infra-data-truth / C3]
-- Bridge for the MilkBox-vs-MailIn test (cm-management): the OPERATIONAL vendor view
-- (core.v_sending_account_vendor — joins to ops via account_id -> campaign/meeting/daily) has no
-- MilkBox; the authoritative registry (core.account_registry, via email) has MilkBox but no outcome
-- linkage. This overlays the authoritative MilkBox/MailIn onto the ops view via account_id=email, so
-- ONE view both joins to outcomes AND carries the MilkBox split. NOTE: outcome linkage only populates
-- as the cohorts go COLD (MilkBox cold-ready ~2026-06-30; MailIn currently a domain-connection buffer)
-- — today ~2% of registry accounts exist in the ops layer (they're warming/buffer), so the resolved
-- MilkBox label is sparse until then. The view is the ready wiring; volume comes with the cohorts.
CREATE OR REPLACE VIEW core.v_sending_account_vendor_resolved AS
SELECT
  v.account_id,
  v.workspace_slug,
  v.esp,
  v.is_active,
  v.status,
  COALESCE(ar.vendor, v.vendor_category) AS vendor_resolved,      -- authoritative MilkBox/MailIn wins
  v.vendor_category                      AS vendor_derived,
  ar.vendor                              AS vendor_authoritative,  -- non-null only where the registry knows
  ar.cohort,
  v.resolved_source,
  v.confidence,
  v.matched_tag
FROM core.v_sending_account_vendor v
LEFT JOIN core.account_registry ar ON lower(ar.email) = lower(v.account_id);
