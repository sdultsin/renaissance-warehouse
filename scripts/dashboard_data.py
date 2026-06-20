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

# Channel of a booked meeting, classified from the Slack booking post (core.meeting.raw_text).
# This is the correct source for SMS/WhatsApp booked counts — the upstream SMS provider's
# calendly_event_uri funnel is dead (always NULL).
# NOTE: daily channel precision is approximate — some SMS lines are posted without an explicit
# tag, so counts won't tie out exactly to the team's manual daily tally. (bookmark: tighten.)
CHANNEL_CASE = (
    "CASE WHEN lower(raw_text) LIKE '%whatsapp%' THEN 'whatsapp' "
    "WHEN lower(raw_text) LIKE '%sendivo%' OR lower(raw_text) LIKE '%sms%' THEN 'sms' "
    "WHEN lower(raw_text) LIKE '%linkedin%' THEN 'linkedin' "
    "WHEN lower(raw_text) LIKE '%sdr%' THEN 'sdr' ELSE 'email' END"
)

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
data["performance"] = {
    "by_cm": q(f"""
        SELECT cm, SUM(sends) sends, SUM(unique_replies) replies,
               SUM(opportunities) opps,
               ROUND(SUM(opportunities)*1000.0/NULLIF(SUM(sends),0),2) opp_per_1k
        FROM v_campaign_opportunities WHERE week_start >= current_date - 30 AND {_CM_IN}
        GROUP BY 1 ORDER BY opps DESC NULLS LAST"""),
    "by_offer": q("""
        SELECT COALESCE(offer,'(unmatched)') offer, SUM(sends) sends, SUM(opportunities) opps,
               ROUND(SUM(opportunities)*1000.0/NULLIF(SUM(sends),0),2) opp_per_1k
        FROM v_campaign_opportunities WHERE week_start >= current_date - 30
        GROUP BY 1 ORDER BY opps DESC NULLS LAST"""),
    "by_infra": q("""
        SELECT COALESCE(infra_type,'(unknown)') sender_esp, SUM(sends) sends, SUM(opportunities) opps,
               ROUND(SUM(opportunities)*1000.0/NULLIF(SUM(sends),0),2) opp_per_1k
        FROM v_campaign_opportunities WHERE week_start >= current_date - 30
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
    # Daily send ACTIVITY (real history from the pipeline daily metrics). This is the
    # day-by-day "actual sends" series; infra split via campaign→infra_type join
    # (unknown/NULL → otd, the only unclassified infra we run). Last 45 days.
    "daily": q("""
        WITH camp AS (
          SELECT campaign_id, infra_type FROM raw_pipeline_campaigns
          WHERE _run_id = (SELECT _run_id FROM raw_pipeline_campaigns ORDER BY _loaded_at DESC LIMIT 1)
        )
        SELECT m.date::VARCHAR d, SUM(m.sent) sends,
               SUM(m.sent) FILTER (WHERE lower(c.infra_type)='google') google,
               SUM(m.sent) FILTER (WHERE lower(c.infra_type)='outlook') outlook,
               SUM(m.sent) FILTER (WHERE lower(c.infra_type) NOT IN ('google','outlook') OR c.infra_type IS NULL) otd
        FROM raw_pipeline_campaign_daily_metrics m LEFT JOIN camp c ON c.campaign_id = m.campaign_id
        WHERE m.date >= current_date - 45 GROUP BY 1 ORDER BY 1"""),
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
# Real send funnel from the Sendivo API (v_sms_performance). Rates are VOLUME-WEIGHTED
# (Σ(sent×rate)/Σsent) — the true period rate, NOT a naive average of daily rates.
data["sms"] = {
    "send_30d": q("""
        SELECT SUM(sms_sent) sent, SUM(segments_sent) segments, SUM(inbound_sms_received) inbound,
               ROUND(SUM(sms_sent*delivery_rate)/NULLIF(SUM(sms_sent),0),1) delivery_rate,
               ROUND(SUM(sms_sent*opt_out_rate)/NULLIF(SUM(sms_sent),0),2) opt_out_rate,
               ROUND(SUM(sms_sent*response_rate)/NULLIF(SUM(sms_sent),0),2) response_rate
        FROM v_sms_performance WHERE metric_date >= current_date - 30""")[0],
    "daily": q("""SELECT metric_date::VARCHAR d, sms_sent, delivery_rate
                  FROM v_sms_performance WHERE metric_date >= current_date - 30 ORDER BY metric_date"""),
    "spend_billed": one("SELECT ROUND(SUM(total_usd),2) FROM core.cost_ledger WHERE vendor='sendivo'"),
    # Outcomes (Slack-sourced): booked meetings attributed to the SMS channel.
    "booked_30d": one(f"SELECT COUNT(*) FROM core.meeting WHERE {CHANNEL_CASE} = 'sms' AND posted_at >= current_date - 30"),
    "booked_7d": one(f"SELECT COUNT(*) FROM core.meeting WHERE {CHANNEL_CASE} = 'sms' AND posted_at >= current_date - 7"),
    "weekly_booked": q(f"""
        SELECT date_trunc('week', posted_at)::VARCHAR AS "week", COUNT(*) booked
        FROM core.meeting WHERE {CHANNEL_CASE} = 'sms' AND posted_at >= current_date - 90
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
    "by_cm": q(f"""
        SELECT cm, COUNT(*) meetings, COUNT(*)*{REVENUE_PER_MEETING} revenue
        FROM core.meeting WHERE {_CM_IN} AND posted_at >= current_date - 30 GROUP BY 1 ORDER BY meetings DESC"""),
    "by_partner": q("""
        SELECT COALESCE(partner,'(none)') partner, COUNT(*) meetings
        FROM core.meeting WHERE posted_at >= current_date - 30 GROUP BY 1 ORDER BY meetings DESC LIMIT 20"""),
    # Partner rollup (30d) with commercial-model / tier labels from core.funding_partner.
    # LEFT JOIN keeps unmatched partners / NULL visible as '(unattributed)'. No revenue math.
    "by_partner_labeled": q("""
        SELECT COALESCE(fp.display_name, m.partner, '(unattributed)') partner,
               fp.commercial_model, fp.tier, COUNT(*) meetings
        FROM core.meeting m
        LEFT JOIN core.funding_partner fp ON m.partner_key = fp.partner_key
        WHERE m.posted_at >= current_date - 30
        GROUP BY 1, 2, 3 ORDER BY meetings DESC"""),
    "by_channel": q(f"""
        SELECT {CHANNEL_CASE} channel, COUNT(*) meetings
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
