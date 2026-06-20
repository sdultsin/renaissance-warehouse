"""Generate the dashboard-gallery data feed from the warehouse (read-only).

Runs ON the droplet (where warehouse.duckdb + duckdb live). Prints ONE combined JSON
object to stdout — the single feed the static gallery app reads as data.json.

Usage (run on the droplet):
    cd <repo> && source .venv/bin/activate && python scripts/dashboard_data.py > data.json

This is also the daily-sync mechanism: run it nightly (or on demand), commit/deploy the
refreshed data.json. Read-only — safe anytime except during the 03:30-04:15 write window.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import duckdb

DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
# Revenue-per-meeting assumption for dashboard rollups. Read from env so the figure
# is not committed; defaults to 0 (dashboards show meeting counts, revenue = 0) when unset.
REVENUE_PER_MEETING = float(os.environ.get("REVENUE_PER_MEETING", "0"))

# Active campaign managers only — all other CMs were let go and are archived from dashboards.
ACTIVE_CMS = ("SAMUEL", "EYVER", "LEO", "IDO", "SAM")
_CM_IN = "UPPER(cm) IN ('SAMUEL','EYVER','LEO','IDO','SAM')"

# Channel of a booked meeting. AUTHORITATIVE source = the curated core.meeting.channel column
# (sheet rows carry an explicit channel; SMS/Call/WhatsApp/LinkedIn are tagged there). The old
# raw_text LIKE heuristic (dumped every untagged row into 'email', counted email ~2x and SMS ~3x
# under) is RETIRED — it ignored the channel column entirely. Consumers below read the channel
# column directly (NULL/blank -> '(unknown)' bucket, or channel='SMS' for the SMS funnel).

# Hybrid EMAIL filter ported verbatim from scripts/portal_data.py (EMAIL_IS / EMAIL_WHERE).
# For source='sheet' rows (>=Jun-1) the explicit channel column is the truth (channel='Email').
# For Slack-era rows (no channel) fall back to the SMS-exclusion raw_text regex. This is the
# canonical email-meeting filter (v_kpi_email / warehouse-query-prompt.md); without it the
# email leaderboards count SMS/Call/WhatsApp (nearly all hard-assigned cm='IDO'). `m` = alias.
_REGEX_EMAIL = (
    "NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),"
    "'sendivo|\\bsms\\b|whatsapp|iskra')"
)
EMAIL_IS = (
    "(CASE WHEN m.source = 'sheet' THEN m.channel = 'Email' "
    f"ELSE {_REGEX_EMAIL} END)"
)
EMAIL_WHERE = EMAIL_IS  # alias for readability in WHERE clauses

# Partner-label normalization. core.meeting.partner_key is NOT reliably populated, so we
# normalize via the curated core.funding_partner.aliases array (display_name <- any alias),
# falling back to partner_key when present. This merges BTC/Big Think, Qualifi/GoQualifi,
# GreenBridge/GreenBridge Capital into one canonical display_name. `m`/`fp` = aliases.
PARTNER_JOIN = (
    "LEFT JOIN core.funding_partner fp "
    "ON (m.partner_key = fp.partner_key OR list_contains(fp.aliases, m.partner))"
)
PARTNER_NAME = "COALESCE(fp.display_name, m.partner, '(none)')"

conn = duckdb.connect(DB, read_only=True)


def q(sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def one(sql: str):
    r = conn.execute(sql).fetchone()
    return r[0] if r else None


def table_exists(name: str) -> bool:
    return (one(f"SELECT count(*) FROM information_schema.tables WHERE table_name='{name}'") or 0) > 0


data: dict = {"generated_at": datetime.now(timezone.utc).isoformat()}

# ---------------------------------------------------------------- Performance (email)
# Window = stable trailing-4-weeks (incl. the current partial week), anchored to the start of
# the current week so the bucket count doesn't drift day-to-day (B8). `sends`/`replies` are this
# windowed volume from v_campaign_opportunities; OPPS is the cumulative ABSOLUTE from
# v_campaign_metrics (G6) — v_campaign_opportunities.opportunities is a per-week TREND and must
# never be SUM'd as a total. opp_per_1k therefore = absolute_opps / windowed_sends (a mixed-grain
# efficiency ratio, intentional). by_cm/by_offer are Funding-5-scoped; by_offer is also pinned to
# offer='FUNDING' so its sub-rows reconcile to by_cm (B9). by_infra is account-grain ESP (B1/G1).
_PERF_WINDOW = "week_start >= date_trunc('week', current_date) - INTERVAL 21 DAY"
data["performance"] = {
    "by_cm": q(f"""
        WITH abs_opp AS (
          SELECT cm_name cm, SUM(opportunities) opps FROM v_campaign_metrics
          WHERE UPPER(cm_name) IN ('SAMUEL','EYVER','LEO','IDO','SAM') GROUP BY 1
        )
        SELECT o.cm, SUM(o.sends) sends, SUM(o.unique_replies) replies,
               a.opps,
               ROUND(a.opps*1000.0/NULLIF(SUM(o.sends),0),2) opp_per_1k
        FROM v_campaign_opportunities o LEFT JOIN abs_opp a ON a.cm = o.cm
        WHERE o.{_PERF_WINDOW} AND UPPER(o.cm) IN ('SAMUEL','EYVER','LEO','IDO','SAM')
        GROUP BY o.cm, a.opps ORDER BY a.opps DESC NULLS LAST"""),
    "by_offer": q(f"""
        WITH abs_opp AS (
          SELECT COALESCE(offer,'(unmatched)') offer, SUM(opportunities) opps FROM v_campaign_metrics
          WHERE UPPER(cm_name) IN ('SAMUEL','EYVER','LEO','IDO','SAM') AND offer = 'FUNDING' GROUP BY 1
        )
        SELECT COALESCE(o.offer,'(unmatched)') offer, SUM(o.sends) sends,
               a.opps,
               ROUND(a.opps*1000.0/NULLIF(SUM(o.sends),0),2) opp_per_1k
        FROM v_campaign_opportunities o
        LEFT JOIN abs_opp a ON a.offer = COALESCE(o.offer,'(unmatched)')
        WHERE o.{_PERF_WINDOW} AND {_CM_IN} AND o.offer = 'FUNDING'
        GROUP BY COALESCE(o.offer,'(unmatched)'), a.opps ORDER BY a.opps DESC NULLS LAST"""),
    # By Sender ESP — account-grain ESP + actual_sends (core.sending_account_daily), the ONLY
    # OTD-splittable source. Campaign infra_type lumped OTD into google (B1/G1). No opps column:
    # account grain is not joinable to opportunities by campaign. ~19% esp=NULL -> 'unknown';
    # source lags ~2 days behind the email-perf window.
    "by_infra": q("""
        SELECT COALESCE(esp,'unknown') sender_esp, SUM(actual_sends) sends
        FROM core.sending_account_daily WHERE date >= current_date - 30
        GROUP BY 1 ORDER BY sends DESC"""),
    "weekly": q("""
        SELECT week_start::VARCHAR AS "week", SUM(sends) sends, SUM(unique_replies) replies, SUM(opportunities) opps
        FROM v_campaign_opportunities GROUP BY 1 ORDER BY 1"""),
}

# ---------------------------------------------------------------- Sending Truth (inventory)
data["sending_truth"] = {
    "totals": q("""
        SELECT COUNT(*) FILTER (WHERE is_active) active_inboxes,
               COUNT(*) total_ever,
               SUM(daily_limit) FILTER (WHERE is_active) daily_capacity
        FROM core.sending_account""")[0],
    "by_esp": q("""
        SELECT esp, COUNT(*) inboxes, SUM(daily_limit) daily_capacity
        FROM core.sending_account WHERE is_active AND esp IS NOT NULL
        GROUP BY esp ORDER BY inboxes DESC"""),
    "by_workspace": q("""
        SELECT COALESCE(w.name, sa.workspace_slug) workspace, COUNT(*) inboxes,
               SUM(sa.daily_limit) daily_capacity
        FROM core.sending_account sa LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
        WHERE sa.is_active GROUP BY 1 ORDER BY inboxes DESC"""),
    "by_lifecycle": q("""
        SELECT lifecycle_state, COUNT(*) inboxes FROM core.sending_account
        WHERE is_active GROUP BY 1 ORDER BY inboxes DESC"""),
    # Daily send ACTIVITY (real history). The day TOTAL is the pipeline daily metric
    # (raw_pipeline_campaign_daily_metrics.sent); the google/outlook/otd SPLIT is the
    # account-grain ESP proportion from core.sending_account_daily for that date (B2). The old
    # campaign→infra_type join lumped OTD into google (~4.3x google inflation, ~75% OTD undercount)
    # because infra_type has no 'otd' value. outlook+microsoft -> outlook. CRITICAL: ESP data lags
    # ~2 days, so we INNER-JOIN on the date — trailing dates with no ESP rows (e.g. the partial
    # generation day) are OMITTED rather than emitting a false 100%-google bar. Last 45 days.
    "daily": q("""
        WITH tot AS (
          SELECT date, SUM(sent) total_sent
          FROM raw_pipeline_campaign_daily_metrics
          WHERE date >= current_date - 45 GROUP BY 1
        ),
        esp AS (
          SELECT date,
                 CASE WHEN lower(COALESCE(esp,''))='google' THEN 'google'
                      WHEN lower(COALESCE(esp,'')) IN ('outlook','microsoft') THEN 'outlook'
                      WHEN lower(COALESCE(esp,''))='otd' THEN 'otd' ELSE 'other' END bucket,
                 SUM(actual_sends) s
          FROM core.sending_account_daily WHERE date >= current_date - 45 GROUP BY 1, 2
        ),
        espday AS (SELECT date, SUM(s) day_total FROM esp GROUP BY 1)
        SELECT t.date::VARCHAR d, t.total_sent sends,
               ROUND(t.total_sent * COALESCE(SUM(e.s) FILTER (WHERE e.bucket='google'),0)  / ed.day_total)::BIGINT google,
               ROUND(t.total_sent * COALESCE(SUM(e.s) FILTER (WHERE e.bucket='outlook'),0) / ed.day_total)::BIGINT outlook,
               ROUND(t.total_sent * COALESCE(SUM(e.s) FILTER (WHERE e.bucket='otd'),0)     / ed.day_total)::BIGINT otd
        FROM tot t
        JOIN espday ed ON ed.date = t.date
        LEFT JOIN esp e ON e.date = t.date
        WHERE ed.day_total > 0
        GROUP BY t.date, t.total_sent, ed.day_total ORDER BY 1"""),
    "daily_by_workspace": q("""
        SELECT date::VARCHAR d, COALESCE(workspace_name,'(unknown)') workspace, SUM(sent) sends
        FROM raw_pipeline_campaign_daily_metrics
        WHERE date >= current_date - 45 GROUP BY 1, 2 ORDER BY 1, sends DESC"""),
}

# ---------------------------------------------------------------- ESP Distribution (workspace x esp)
data["esp_distribution"] = q("""
    SELECT COALESCE(w.name, sa.workspace_slug) workspace, sa.esp, COUNT(*) inboxes
    FROM core.sending_account sa LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
    WHERE sa.is_active AND sa.esp IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1, inboxes DESC""")

# ---------------------------------------------------------------- SMS Performance (Sendivo send-side)
# CANON send-side source = v_sms_campaign_performance (per-campaign, mapped sends — the same view
# v_kpi_sms + lens-sms build on). The old v_sms_performance read raw_sendivo_delivery_metrics (all
# sends) and disagreed with lens-sms by ~830K/30d (G4). delivery_rate is recomputed from counts
# (SUM(delivered)/SUM(sent)); opt_out_rate = opt_outs/sent. inbound/response_rate are dropped: this
# view has no inbound_sms_received, and its `replies` carries the upstream duplicate-webhook blowup
# (G3, a separate DDL fix) so it must not be surfaced as an inbound count here.
data["sms"] = {
    "send_30d": q("""
        SELECT SUM(sent) sent, SUM(segments) segments,
               ROUND(100.0*SUM(delivered)/NULLIF(SUM(sent),0),1) delivery_rate,
               ROUND(100.0*SUM(opt_outs)/NULLIF(SUM(sent),0),2) opt_out_rate
        FROM v_sms_campaign_performance WHERE metric_date >= current_date - 30""")[0],
    "daily": q("""SELECT metric_date::VARCHAR d, SUM(sent) sms_sent,
                         ROUND(100.0*SUM(delivered)/NULLIF(SUM(sent),0),1) delivery_rate
                  FROM v_sms_campaign_performance WHERE metric_date >= current_date - 30
                  GROUP BY 1 ORDER BY 1"""),
    "spend_billed": one("SELECT ROUND(SUM(total_usd),2) FROM core.cost_ledger WHERE vendor='sendivo'"),
    # Outcomes (curated): booked meetings on the SMS channel via the authoritative
    # core.meeting.channel column (the old raw_text 'sms' heuristic undercounted ~3x).
    "booked_30d": one("SELECT COUNT(*) FROM core.meeting WHERE channel = 'SMS' AND posted_at >= current_date - 30"),
    "booked_7d": one("SELECT COUNT(*) FROM core.meeting WHERE channel = 'SMS' AND posted_at >= current_date - 7"),
    "weekly_booked": q("""
        SELECT date_trunc('week', posted_at)::VARCHAR AS "week", COUNT(*) booked
        FROM core.meeting WHERE channel = 'SMS' AND posted_at >= current_date - 90
        GROUP BY 1 ORDER BY 1"""),
    "opportunities_unique": one("SELECT COUNT(*) FROM core.opportunity WHERE source='sendivo' AND state <> 'duplicate'"),
}

# ---------------------------------------------------------------- Deliverability / Infra Health
data["deliverability"] = {
    # pct_listed keys off Spamhaus DBL — the list mailbox providers actually act on.
    # SURBL is a URI/link list (filters domains found *inside* spam bodies), heavily
    # polluted by bulk-registered .co domains; it does NOT block sending, so it's shown
    # separately as context rather than as the headline risk number.
    "blacklist_by_esp": q("""
        SELECT esp, COUNT(*) domains,
               COUNT(*) FILTER (WHERE listed_on LIKE '%spamhaus_dbl%') listed,
               ROUND(100.0*COUNT(*) FILTER (WHERE listed_on LIKE '%spamhaus_dbl%')/COUNT(*),2) pct_listed,
               COUNT(*) FILTER (WHERE listed_on LIKE '%surbl%') surbl_listed,
               ROUND(100.0*COUNT(*) FILTER (WHERE listed_on LIKE '%surbl%')/COUNT(*),1) pct_surbl
        FROM core.domain GROUP BY esp ORDER BY domains DESC"""),
    "dns_signature_clusters": q("""
        SELECT dns_signature, COUNT(*) domains, MIN(domain) example, MAX(esp) esp
        FROM core.domain WHERE dns_signature IS NOT NULL
        GROUP BY dns_signature HAVING COUNT(*) > 100 ORDER BY domains DESC LIMIT 15"""),
    "ip24_clusters": q("""
        SELECT a_record_24, COUNT(*) domains, MAX(esp) esp FROM core.domain
        WHERE a_record_24 IS NOT NULL GROUP BY 1 HAVING COUNT(*) > 50 ORDER BY domains DESC LIMIT 15"""),
}

# ---------------------------------------------------------------- Meetings & Revenue
# Time-bounded (last 30d) — no all-time cumulative. Flat REVENUE_PER_MEETING incl. SMS;
# rev-share partner economics deliberately NOT modeled (no revenue source yet — bookmarked).
data["meetings"] = {
    "booked_30d": one("SELECT COUNT(*) FROM core.meeting WHERE posted_at >= current_date - 30"),
    "booked_7d": one("SELECT COUNT(*) FROM core.meeting WHERE posted_at >= current_date - 7"),
    "revenue_30d": (one("SELECT COUNT(*) FROM core.meeting WHERE posted_at >= current_date - 30") or 0) * REVENUE_PER_MEETING,
    # Email-CM leaderboard — MUST apply the hybrid EMAIL filter (B3/G2). Without it the count
    # included SMS/Call/WhatsApp meetings, nearly all hard-assigned cm='IDO' (IDO 2,567 -> 1,173
    # email-only, 30d). channel='Email' for sheet rows; SMS-exclusion regex for Slack-era rows.
    "by_cm": q(f"""
        SELECT m.cm, COUNT(*) meetings, COUNT(*)*{REVENUE_PER_MEETING} revenue
        FROM core.meeting m
        WHERE UPPER(m.cm) IN ('SAMUEL','EYVER','LEO','IDO','SAM')
              AND m.posted_at >= current_date - 30 AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    # Partner rollup — normalized to one canonical display_name via the curated
    # core.funding_partner.aliases array (BTC/Big Think, Qualifi/GoQualifi, GreenBridge variants
    # merge). partner_key is unreliably populated so the alias-array match carries the join (G8).
    "by_partner": q(f"""
        SELECT {PARTNER_NAME} partner, COUNT(*) meetings
        FROM core.meeting m {PARTNER_JOIN}
        WHERE m.posted_at >= current_date - 30 GROUP BY 1 ORDER BY meetings DESC LIMIT 20"""),
    # Partner rollup (30d) with commercial-model / tier labels from core.funding_partner.
    # LEFT JOIN keeps unmatched partners / NULL visible as '(unattributed)'. No revenue math.
    "by_partner_labeled": q("""
        SELECT COALESCE(fp.display_name, m.partner, '(unattributed)') partner,
               fp.commercial_model, fp.tier, COUNT(*) meetings
        FROM core.meeting m
        LEFT JOIN core.funding_partner fp ON m.partner_key = fp.partner_key
        WHERE m.posted_at >= current_date - 30
        GROUP BY 1, 2, 3 ORDER BY meetings DESC"""),
    # Channel mix — AUTHORITATIVE core.meeting.channel column (Email 2,023 / SMS 1,161 /
    # Call 95 / WhatsApp 91 / LinkedIn 2, 30d). The old raw_text heuristic dumped SMS/Call/
    # WhatsApp + all NULL-channel rows into 'email' (~2x over) and mis-bucketed SMS ~3x under
    # (B5). NULL/blank channel is surfaced as its own '(unknown)' bucket, not reclassified.
    "by_channel": q("""
        SELECT COALESCE(NULLIF(channel,''),'(unknown)') channel, COUNT(*) meetings
        FROM core.meeting WHERE posted_at >= current_date - 30 GROUP BY 1 ORDER BY 2 DESC"""),
    "weekly": q("""
        SELECT date_trunc('week', posted_at)::VARCHAR AS "week", COUNT(*) meetings
        FROM core.meeting WHERE posted_at >= current_date - 180 GROUP BY 1 ORDER BY 1"""),
}

# ---------------------------------------------------------------- ESP x ESP matrix (conditional)
# Frontend computes RR% from raw counts. Sender 'unknown' → 'otd' (only unknown infra we run);
# recipient yahoo/isp/apple/other → 'other'. Re-aggregated after remap so cells merge.
if table_exists("mv_esp_send_matrix") and (one("SELECT COUNT(*) FROM mv_esp_send_matrix") or 0) > 0:
    # NOTE 2026-06-14 (Sam source-of-truth decision, memory
    # reference_warehouse_reply_and_tag_truth_20260614): the human/auto/total/positive
    # reply columns in mv_esp_send_matrix derive from the BROKEN core.reply.is_auto_reply
    # heuristic (~3.5% auto vs ~63% native truth) and the never-funded intent classifier.
    # Do NOT surface them. Only `sends` here is a trustworthy native number. Reply truth
    # comes ONLY from Instantly native (unique_replies / unique_replies_automatic), surfaced
    # via the email-performance blocks above — never from this matrix.
    matrix = q("""
        SELECT
          CASE WHEN sender_esp = 'unknown' THEN 'otd' ELSE sender_esp END        AS sender_esp,
          CASE WHEN recipient_esp IN ('yahoo','isp','apple','other') THEN 'other'
               ELSE recipient_esp END                                            AS recipient_esp,
          SUM(sends)            AS sends
        FROM mv_esp_send_matrix
        GROUP BY 1, 2
        ORDER BY sends DESC""")
    total_sends = sum(r["sends"] or 0 for r in matrix)
    # 'unknown' recipient = MX not yet classified → counts as uncovered volume
    unknown_sends = sum((r["sends"] or 0) for r in matrix if r["recipient_esp"] == "unknown")
    data["esp_matrix"] = {
        "cells": matrix,
        "coverage_pct": round(100.0 * (total_sends - unknown_sends) / total_sends, 1) if total_sends else 0,
        "total_sends": total_sends,
    }
else:
    data["esp_matrix"] = None

# ---------------------------------------------------------------- Reply / Intent (cross-channel)
# DISABLED 2026-06-14 (Sam source-of-truth decision, memory
# reference_warehouse_reply_and_tag_truth_20260614). The EMAIL leg of derived.reply_intent
# is the broken/never-funded reply-intent classifier (~3.5% auto vs ~63% native truth); we do
# NOT surface email reply intent / auto-vs-human in the warehouse. Email reply truth comes ONLY
# from Instantly native (unique_replies / unique_replies_automatic), already surfaced in the
# email-performance blocks above. We emit null so any consumer falls back rather than reads a
# wrong number. (SMS intent is a separate coarse conversation-state proxy; if an SMS-only intent
# tile is ever wanted, query derived.reply_intent WHERE channel='sms' explicitly — never email.)
data["reply_intent"] = None

# ---------------------------------------------------------------- SMS Opportunities (AIM-classified)
# Sendivo has no native booking webhook — opportunities are AIM-surfaced into call_opportunity.
_callopp_latest = "(SELECT _run_id FROM raw_comms_call_opportunity ORDER BY _loaded_at DESC LIMIT 1)"
data["sms_opportunities"] = {
    "unique": one(f"SELECT COUNT(*) FROM raw_comms_call_opportunity WHERE _run_id = {_callopp_latest} AND source='sendivo' AND status <> 'duplicate'"),
    "by_status": q(f"""
        SELECT status, COUNT(*) AS n FROM raw_comms_call_opportunity
        WHERE _run_id = {_callopp_latest} AND source='sendivo' GROUP BY 1 ORDER BY n DESC"""),
}

print(json.dumps(data, default=str, indent=None))
conn.close()
