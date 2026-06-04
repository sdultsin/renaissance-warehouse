-- Phase: extend mv_esp_send_matrix with reply-type breakdown (Workstream H).
-- Applied at schema version 28 by scripts/setup_db.py / orchestrator DDL applier.
--
-- Adds three reply-type counts alongside the existing human_replies, so the dashboard
-- can switch the reply-rate numerator between total / human / auto / positive:
--   total_replies    = COUNT(*)                                   (every reply_data row)
--   auto_replies     = COUNT(*) FILTER (intent = 'auto_reply')    (UNDER-tagged: ~1.4% of corpus)
--   positive_replies = COUNT(*) FILTER (intent = 'positive')      (intent/quality signal)
-- human_replies stays as-is = COUNT(*) FILTER (intent IS DISTINCT FROM 'auto_reply').
-- Invariants: total_replies >= human_replies >= 0; total_replies >= auto_replies + positive_replies.
--
-- IF NOT EXISTS keeps this idempotent across re-runs of the DDL applier.

ALTER TABLE mv_esp_send_matrix ADD COLUMN IF NOT EXISTS total_replies    BIGINT;
ALTER TABLE mv_esp_send_matrix ADD COLUMN IF NOT EXISTS auto_replies     BIGINT;
ALTER TABLE mv_esp_send_matrix ADD COLUMN IF NOT EXISTS positive_replies BIGINT;
