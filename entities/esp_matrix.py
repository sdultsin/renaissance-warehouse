"""mv_esp_send_matrix — the ESP×ESP (sender ESP × recipient ESP) send/reply matrix.

The payoff surface: how does each sender ESP (our Google/Outlook/OTD infra) perform
into each recipient ESP (Google/Microsoft/Yahoo/...). Pure aggregation, no new sweep:

  sender_esp    = raw_pipeline_campaigns.infra_type  (per campaign)
  recipient_esp = core.recipient_domain.recipient_esp (per lead_domain; 'unknown' if unclassified)
  sends         = SUM(contact_frequency_campaign_daily.sent_count)  via postgres_scanner
  human_replies = canonical reply intent rows (is_auto_reply = false), attributed lead_email→domain→recipient_esp

⚠ 2026-06-14 (Sam source-of-truth, memory reference_warehouse_reply_and_tag_truth_20260614):
the human_replies / auto_replies / total_replies / positive_replies columns here derive from the
BROKEN core.reply.is_auto_reply heuristic (~3.5% auto vs ~63% native truth) and the unfunded
reply-intent classifier. They are NON-CANONICAL and must NOT be surfaced as reply counts anywhere
(dashboard_data.py was updated to drop them). The ONLY trustworthy reply numbers are Instantly
native: unique_replies / unique_replies_automatic in raw_pipeline_campaign_daily_metrics. This mv's
`sends` column is fine; treat the reply columns as a deprecated internal artifact only.

Built as a materialized table (not a view) because it aggregates an attached-Postgres
source (postgres_scanner can't live inside a VIEW). Registers under the 'derived' phase,
AFTER recipient_domain (dns_sweep phase) has classified the recipient side.
"""
from __future__ import annotations

import logging
import os

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.esp_matrix")

WINDOW_DAYS = int(os.environ.get("ESP_MATRIX_DAYS", "90"))


def register(registry: Registry) -> None:
    registry.add_phase("derived", "esp_matrix", run_esp_matrix)


def run_esp_matrix(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    pg_url = ctx.credentials.require("PIPELINE_SUPABASE_DB_URL")

    conn.execute("INSTALL postgres"); conn.execute("LOAD postgres")
    try:
        conn.execute("DETACH pg")
    except Exception:
        pass
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")

    # arg_max over the upsert-mirror: raw_pipeline_campaigns is 1 row/campaign, so a
    # _run_id=(latest) filter silently drops frozen/deleted-upstream campaigns from the
    # infra split once _run_id ever diverges (same bug class as the now-fixed
    # dashboard_data.py:108; mirrors dash.lens_overview__sending_daily, sql/ddl/86). Same
    # output today (single _run_id), no freeze bug going forward.
    latest_campaigns = (
        "(SELECT campaign_id, arg_max(infra_type, _loaded_at) AS infra_type "
        " FROM raw_pipeline_campaigns GROUP BY campaign_id)"
    )
    latest_reply_intent = (
        "(SELECT * FROM raw_pipeline_reply_intent_classifications "
        " WHERE source_table = 'conversation_messages' "
        "   AND _run_id = (SELECT _run_id FROM raw_pipeline_reply_intent_classifications "
        "                  ORDER BY _loaded_at DESC LIMIT 1))"
    )
    try:
        conn.execute("DELETE FROM mv_esp_send_matrix")
        conn.execute(
            f"""
            INSERT INTO mv_esp_send_matrix
              (week_start, sender_esp, recipient_esp, sends, human_replies,
               total_replies, auto_replies, positive_replies,
               reply_per_1k, domains_covered, _resolved_at)
            WITH sends AS (
              SELECT date_trunc('week', cf.send_date)::DATE AS week_start,
                     COALESCE(camp.infra_type, 'unknown')   AS sender_esp,
                     COALESCE(rd.recipient_esp, 'unknown')  AS recipient_esp,
                     SUM(cf.sent_count)::BIGINT             AS sends,
                     COUNT(DISTINCT cf.lead_domain)::BIGINT AS domains_covered
              FROM pg.public.contact_frequency_campaign_daily cf
              LEFT JOIN {latest_campaigns} camp ON camp.campaign_id = CAST(cf.campaign_id AS VARCHAR)
              LEFT JOIN core.recipient_domain rd ON rd.domain = lower(cf.lead_domain)
              WHERE cf.send_date >= current_date - INTERVAL '{WINDOW_DAYS} days'
              GROUP BY 1, 2, 3
            ),
            replies AS (
              SELECT date_trunc('week', r.reply_timestamp)::DATE AS week_start,
                     COALESCE(camp.infra_type, 'unknown')        AS sender_esp,
                     COALESCE(rd.recipient_esp, 'unknown')       AS recipient_esp,
                     COUNT(*) FILTER (WHERE COALESCE(r.is_auto_reply, false) = false)::BIGINT AS human_replies,
                     COUNT(*)::BIGINT                                            AS total_replies,
                     COUNT(*) FILTER (WHERE COALESCE(r.is_auto_reply, false) = true)::BIGINT AS auto_replies,
                     COUNT(*) FILTER (WHERE r.intent = 'positive')::BIGINT       AS positive_replies
              FROM {latest_reply_intent} r
              LEFT JOIN {latest_campaigns} camp ON camp.campaign_id = CAST(r.campaign_id AS VARCHAR)
              LEFT JOIN core.recipient_domain rd ON rd.domain = lower(split_part(r.lead_email, '@', 2))
              WHERE r.reply_timestamp >= current_date - INTERVAL '{WINDOW_DAYS} days'
              GROUP BY 1, 2, 3
            )
            SELECT
              COALESCE(s.week_start, rp.week_start),
              COALESCE(s.sender_esp, rp.sender_esp),
              COALESCE(s.recipient_esp, rp.recipient_esp),
              COALESCE(s.sends, 0),
              COALESCE(rp.human_replies, 0),
              COALESCE(rp.total_replies, 0),
              COALESCE(rp.auto_replies, 0),
              COALESCE(rp.positive_replies, 0),
              ROUND(COALESCE(rp.human_replies, 0) * 1000.0 / NULLIF(s.sends, 0), 3),
              COALESCE(s.domains_covered, 0),
              now()
            FROM sends s
            FULL OUTER JOIN replies rp
              ON s.week_start = rp.week_start AND s.sender_esp = rp.sender_esp
             AND s.recipient_esp = rp.recipient_esp
            """
        )
    finally:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass

    n = conn.execute("SELECT count(*) FROM mv_esp_send_matrix").fetchone()[0]
    tot = conn.execute(
        "SELECT SUM(sends), SUM(human_replies), SUM(total_replies), "
        "SUM(auto_replies), SUM(positive_replies) FROM mv_esp_send_matrix"
    ).fetchone()
    logger.info(
        "mv_esp_send_matrix: %d cells, %s sends, %s total / %s human / %s auto / %s positive replies (%dd)",
        n, tot[0], tot[2], tot[1], tot[3], tot[4], WINDOW_DAYS)
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"window_days": WINDOW_DAYS, "sends": tot[0],
                              "human_replies": tot[1], "total_replies": tot[2],
                              "auto_replies": tot[3], "positive_replies": tot[4]})
