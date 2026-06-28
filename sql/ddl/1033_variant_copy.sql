-- Per-variant copy: persisted, accumulate-only. 4 copy columns per variant. Version 1033.
-- @gate: add
-- Depends on 03
--
-- WHAT: an accumulate-only history of every distinct piece of campaign copy, one row per
-- (campaign_id, step, content_hash). Exposes EXACTLY four copy columns --
--   subject_raw   : subject as authored (with spintax)
--   subject_clean : subject with every spin block resolved to its FIRST option (recursive)
--   body_raw      : body as authored (with spintax; full HTML; INCLUDES the signature)
--   body_clean    : body with every spin block resolved to its FIRST option (recursive)
-- NO further decomposition: this DELIBERATELY supersedes / retires the 2026-06-21
-- subject/opener/CTA/PS variant-matrix breakdown -- only these 4 copy columns.
--
-- POPULATED BY: the nightly entity entities/variant_copy.py (registered in the 'derived'
-- phase). The first nightly run is the initial backfill; every run thereafter is incremental.
--
-- KEY / DEDUP: content_hash = md5(subject_raw || 0x1F || body_raw), unique per (campaign_id, step)
-- -- NOT keyed by variant slot/label. The entity INSERTs with ON CONFLICT DO NOTHING, so:
--   * identical copy already present -> no-op (the same text is never re-stored)
--   * new or changed copy           -> a new row is INSERTed (versions accumulate)
-- STRICTLY NON-DESTRUCTIVE / ACCUMULATE-ONLY: a variant or campaign that disappears from
-- Instantly is NEVER deleted or blanked -- its row(s) persist. The entity only ever INSERTs.
-- (This is exactly why it is a TABLE, not a view: a view would recompute from the current
-- sequence_raw and silently lose copy for variants/campaigns Instantly later deletes.)
--
-- UNSPINTAX (clean) = first option of each spin block, recursive. Handles BOTH fleet syntaxes:
--   * double-brace  {{RANDOM|a|b|c}}  -> a   (spaces ok: {{ RANDOM | a | b }})
--   * RANDOM-less   {{a|b}}           -> a   (some authors write spin as plain {{opt1|opt2}})
--   * single-brace  {a|b}             -> a   (legacy Instantly {a|b} style)
-- KEPT VERBATIM (not spin): personalization tokens {{firstName}}, {{companyName|there}} (fallback),
--   {companyName } (single-brace), and Liquid {% ... %} control tags (spin INSIDE their branches
--   IS resolved). Personalization is identified by a known-variable allowlist (see the entity).
--
-- SKIPPED GRACEFULLY (no row, no error): campaigns with empty/unparseable sequence_raw
--   (completed/deleted campaigns naturally fall here), and individual variants whose raw copy
--   is malformed (unbalanced braces / spin that will not fully resolve). Per 100%-or-wipe these
--   are flagged-by-omission, never half-filled.
--
-- CHANNEL: 'instantly' only. Sendivo + Iskra/WhatsApp have NO per-variant copy TEMPLATE object
--   in the warehouse (only rendered, per-recipient sent messages -- no spintax, no variant
--   structure). `channel` is carried for future extension; those channels are intentionally
--   absent here, not silently merged.
--
-- NOT THE SAME as main.raw_pipeline_variant_copy. That is a RAW pipeline-supabase mirror of
--   CURRENT-state per-(campaign,variant) copy (has a `variant` column, uses *_unspintaxed
--   column names, carries v_disabled). THIS table (core.variant_copy) is the warehouse-native,
--   DERIVED, accumulate-only HISTORY keyed by (campaign_id, step, content_hash) with no variant
--   column and *_clean column names. The two are intentionally distinct grains; do not join them
--   on content_hash (different hash inputs) and do not treat *_clean and *_unspintaxed as the same
--   surface. core.variant_copy is the canonical clean-copy surface for copy-performance / the
--   RevOps workbench. NULL subject/body are coerced to '' by the entity before hashing, so the
--   0x1F separator is always present and the PK is collision-safe.

CREATE TABLE IF NOT EXISTS core.variant_copy (
  campaign_id      VARCHAR     NOT NULL,
  step             INTEGER     NOT NULL,
  content_hash     VARCHAR     NOT NULL,   -- md5(subject_raw || 0x1F || body_raw)
  workspace_id     VARCHAR,
  channel          VARCHAR,                -- 'instantly'
  sequence_index   INTEGER,                -- 1-based sequence ordinal (informational; almost always 1)
  variant_index    INTEGER,                -- 1-based variant slot where this content was first seen (informational)
  step_type        VARCHAR,                -- e.g. 'email'
  subject_raw      VARCHAR,
  subject_clean    VARCHAR,
  body_raw         VARCHAR,
  body_clean       VARCHAR,
  first_seen_at    TIMESTAMPTZ NOT NULL,   -- when this exact copy first entered the warehouse
  _run_id          VARCHAR     NOT NULL,
  PRIMARY KEY (campaign_id, step, content_hash)
);
