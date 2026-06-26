-- 1019 — Sendivo per-BLAST send breakdown (G4 — now buildable).
--
-- DDL 66 recorded "G4/G8 are not buildable Sendivo-side" (2026-06-14: no per-blast field on
-- /sms/logs). That changed 2026-06-26: Larry (Sendivo) added a `blast` object to each /sms/logs
-- record — confirmed live as `blast: {id, name}` where the NAME is the script identity
-- (e.g. "Script 23 - June 25th #3"), 100% populated on real sends forward. This is the
-- finer-than-campaign granularity that answers "which scripts/blasts are landing" on the SEND side.
--
-- Source-side change lives in entities/sendivo_logs.py (the _blast() extractor + a SEPARATE
-- blast_agg folded in the same single pass over each day's /sms/logs). Kept as its OWN grain — NOT
-- merged into raw_sendivo_campaign_daily — so the existing campaign rollup + v_sms_campaign_performance
-- grain and sums are untouched (same lean-rollup discipline as G1/G3). The entity also creates this
-- table via IF NOT EXISTS; declared here so setup_db materialises it (and the view below) on a fresh DB.
--
-- Forward-only by design: historical blast coverage is patchy (51% of real sends on 2026-05-20, 0% on
-- 2026-06-10 — blast objects only ever existed for certain send paths), so there is no clean backfill;
-- treat blast attribution as a forward metric from 2026-06-26. Reply-side blast attribution is separate
-- (comms.sendivo_outbound_recovered.blast_id, comms migration 011).

-- One row per (day, sub, campaign, blast, status_group, run). Aggregated on ingest.
CREATE TABLE IF NOT EXISTS raw_sendivo_blast_daily (
    metric_date        DATE,
    sub_account_id     BIGINT,
    sub_account_name   VARCHAR,
    campaign_id        BIGINT,
    campaign_name      VARCHAR,
    blast_id           BIGINT,     -- Sendivo /sms/logs blast.id (NULL for system/unsubscribe msgs)
    blast_name         VARCHAR,    -- Sendivo blast.name == the script identity
    status_group       VARCHAR,    -- DELIVERED / UNDELIVERABLE / REJECTED / EXPIRED / PENDING
    n_messages         BIGINT,
    segments           BIGINT,
    cost_usd           DOUBLE,
    _loaded_at         TIMESTAMPTZ NOT NULL,
    _run_id            VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_blastd_date  ON raw_sendivo_blast_daily (metric_date);
CREATE INDEX IF NOT EXISTS ix_sv_blastd_blast ON raw_sendivo_blast_daily (blast_id);

-- Consumer view — per-blast SEND funnel by day, latest run per metric_date (re-pulls supersede).
-- Mirrors v_sms_send_by_hour / the outbound CTE of v_sms_campaign_performance. This is the
-- "which scripts are landing" surface on the send side (delivery by blast/script).
CREATE OR REPLACE VIEW v_sms_blast_performance AS
WITH run_rank AS (
  SELECT metric_date, _run_id,
         ROW_NUMBER() OVER (PARTITION BY metric_date ORDER BY max(_loaded_at) DESC) rn
  FROM raw_sendivo_blast_daily GROUP BY metric_date, _run_id
),
latest AS (
  SELECT b.* FROM raw_sendivo_blast_daily b
  JOIN run_rank r ON b.metric_date = r.metric_date AND b._run_id = r._run_id AND r.rn = 1
)
SELECT
  metric_date, sub_account_id, sub_account_name, campaign_id, campaign_name,
  blast_id, blast_name,
  sum(n_messages)                                                    AS sent,
  sum(n_messages) FILTER (WHERE status_group = 'DELIVERED')          AS delivered,
  sum(n_messages) FILTER (WHERE status_group IN ('UNDELIVERABLE','REJECTED','EXPIRED')) AS failed,
  sum(n_messages) FILTER (WHERE status_group = 'PENDING')            AS pending,
  sum(segments)                                                      AS segments,
  sum(cost_usd)                                                      AS cost_usd
FROM latest
GROUP BY ALL;
