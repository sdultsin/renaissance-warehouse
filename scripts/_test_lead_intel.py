"""In-memory smoke test for entities/lead_intel.py (WS-I).

Builds minimal core.lead / core.reply / core.reply_intent / core.lead_disposition /
core.opportunity / core.conversion_event sample tables with a DUAL-SIGNAL lead (a reply+intent
AND a partner disposition), applies the 46 DDL, runs _build(), and asserts the WS-I DoD.
Not part of the nightly — run directly: python scripts/_test_lead_intel.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from entities.lead_intel import _build, _DDL  # noqa: E402

db = duckdb.connect(":memory:")
db.execute("CREATE SCHEMA IF NOT EXISTS core")

# ── minimal source tables (subset of the real DDLs, exact column names) ────────
db.execute("""
CREATE TABLE core.lead (
  lead_key VARCHAR PRIMARY KEY, email VARCHAR, phone_e164 VARCHAR,
  first_name VARCHAR, company VARCHAR, segment VARCHAR, industry VARCHAR,
  lead_source VARCHAR, resolution_confidence VARCHAR,
  first_seen_at TIMESTAMPTZ, resolved_at TIMESTAMPTZ)""")
db.execute("""
CREATE TABLE core.reply (
  reply_id VARCHAR PRIMARY KEY, lead_email VARCHAR, campaign_id VARCHAR, workspace_id VARCHAR,
  step INTEGER, variant VARCHAR, subject VARCHAR, reply_text VARCHAR,
  reply_timestamp TIMESTAMPTZ, is_auto_reply BOOLEAN, source VARCHAR,
  _loaded_at TIMESTAMPTZ, _run_id VARCHAR)""")
db.execute("""
CREATE TABLE core.reply_intent (
  reply_id VARCHAR PRIMARY KEY, primary_intent VARCHAR, intent_tags VARCHAR[], sentiment VARCHAR,
  is_question BOOLEAN, is_objection BOOLEAN, objection_type VARCHAR, is_unsubscribe BOOLEAN,
  is_referral BOOLEAN, is_wrong_person BOOLEAN, summary VARCHAR, classifier_model VARCHAR,
  classifier_version INTEGER, confidence DOUBLE, classified_at TIMESTAMPTZ)""")
db.execute("""
CREATE TABLE core.lead_disposition (
  lead_email VARCHAR, source_period VARCHAR, disposition VARCHAR, disposition_class VARCHAR,
  rep VARCHAR, business_name VARCHAR, industry VARCHAR, id_confidence VARCHAR,
  rep_notes VARCHAR, resolved_at TIMESTAMPTZ,
  PRIMARY KEY (lead_email, source_period))""")
db.execute("""
CREATE TABLE core.opportunity (
  opportunity_id VARCHAR PRIMARY KEY, source VARCHAR, source_event_id VARCHAR, lead_email VARCHAR,
  campaign_id VARCHAR, workspace_id VARCHAR, opened_at TIMESTAMPTZ, state VARCHAR,
  state_updated_at TIMESTAMPTZ, is_duplicate_of VARCHAR, cost_per_opp_usd_estimated DOUBLE,
  raw VARCHAR, _resolved_at TIMESTAMPTZ)""")
db.execute("""
CREATE TABLE core.conversion_event (
  event_id VARCHAR PRIMARY KEY, lead_key VARCHAR, lead_email VARCHAR, phone_e164 VARCHAR,
  source_channel VARCHAR, conversion_agent VARCHAR, conversion_type VARCHAR,
  occurred_at TIMESTAMPTZ, campaign_id VARCHAR, warm_caller_id VARCHAR, resolved_at TIMESTAMPTZ)""")

# ── sample data ────────────────────────────────────────────────────────────────
# Lead A = DUAL-SIGNAL: 2 replies (interested + price-objection question) AND a partner
#          disposition (LIVE OPPORTUNITY) AND an opportunity AND a meeting.
# Lead B = reply only (objection_timing), no disposition.
# Lead C = phone-only Sendivo lead with a conversion_event keyed by lead_key, no reply.
db.execute("""INSERT INTO core.lead VALUES
  ('keyA','alice@acme.com',NULL,'Alice','Acme Co','MCA','retail','MCA - Isaac','email',now(),now()),
  ('keyB','bob@beta.io',NULL,'Bob','Beta','HELOC','saas','Ben','email',now(),now()),
  ('keyC',NULL,'+15551230000','Carol','Gamma','MCA','services',NULL,'phone',now(),now())""")

db.execute("""INSERT INTO core.reply VALUES
  ('r1','alice@acme.com','camp1','ws1',1,NULL,'re: hi','Sounds interesting, tell me more',
     TIMESTAMP '2026-06-01 10:00:00+00', false,'instantly',now(),'run1'),
  ('r2','alice@acme.com','camp1','ws1',2,NULL,'re: hi','But how much does it cost?',
     TIMESTAMP '2026-06-03 12:00:00+00', false,'instantly',now(),'run1'),
  ('r3','bob@beta.io','camp2','ws1',1,NULL,'re: x','Not right now, maybe Q3?',
     TIMESTAMP '2026-06-02 09:00:00+00', false,'instantly',now(),'run1'),
  ('r4','alice@acme.com','camp1','ws1',1,NULL,'auto','Out of office',
     TIMESTAMP '2026-05-30 08:00:00+00', true,'instantly',now(),'run1')""")

db.execute("""INSERT INTO core.reply_intent VALUES
  ('r1','interested',['interested','warm'],'positive',false,false,NULL,false,false,false,
     'Lead is interested, wants more info','m',1,0.9,now()),
  ('r2','info_request',['pricing'],'neutral',true,true,'price',false,false,false,
     'Asks about pricing','m',1,0.85,now()),
  ('r3','objection_timing',['timing'],'neutral',false,true,'timing',false,false,false,
     'Wants to wait until Q3','m',1,0.8,now())""")
# r4 (auto-reply) intentionally has no intent row and is excluded by is_auto_reply filter.

db.execute("""INSERT INTO core.lead_disposition VALUES
  ('alice@acme.com','2026-06-MTD','LIVE OPPORTUNITY','live','RepJoe','Acme Co','retail','High',
     'Strong fit, follow up', now())""")

db.execute("""INSERT INTO core.opportunity VALUES
  ('instantly:o1','instantly','o1','alice@acme.com','camp1','ws1',now(),'interested',now(),
     NULL,NULL,'{}',now())""")

db.execute("""INSERT INTO core.conversion_event VALUES
  ('e1','keyA','alice@acme.com',NULL,'cold_email','im','meeting_booked',
     TIMESTAMP '2026-06-06 15:00:00+00','camp1',NULL,now()),
  ('e2','keyC',NULL,'+15551230000','sms','warm_caller','appointment_set',
     TIMESTAMP '2026-06-04 11:00:00+00',NULL,'wc1',now())""")

# ── apply DDL (creates derived schema + derived.lead_intel) and run the build ──
db.execute(_DDL.read_text())
stats = _build(db)
print("BUILD STATS:", stats)

print("\n=== one row per core.lead? ===")
n_lead = db.execute("SELECT count(*) FROM core.lead").fetchone()[0]
n_intel = db.execute("SELECT count(*) FROM derived.lead_intel").fetchone()[0]
print(f"core.lead={n_lead}  derived.lead_intel={n_intel}  match={n_lead == n_intel}")

print("\n=== DUAL-SIGNAL lead (alice): reply intent AND disposition in ONE row ===")
row = db.execute("""
  SELECT lead_email, n_replies, dominant_intent, all_intent_tags, has_question, has_objection,
         top_objection_type, last_reply_text, last_sentiment,
         partner_disposition, disposition_class, partner_rep,
         is_opportunity, is_meeting, conversion_agent, funnel_stage, engagement_score
  FROM derived.lead_intel WHERE lead_email = 'alice@acme.com'""").fetchall()
for r in row:
    print(r)

print("\n=== full table (compact) ===")
for r in db.execute("""SELECT lead_key, lead_email, phone_e164, n_replies, dominant_intent,
       disposition_class, is_opportunity, is_meeting, funnel_stage, engagement_score
       FROM derived.lead_intel ORDER BY lead_key""").fetchall():
    print(r)

print("\n=== v_intent_distribution ===")
for r in db.execute("SELECT * FROM v_intent_distribution").fetchall():
    print(r)
print("\n=== v_objection_library ===")
for r in db.execute("SELECT * FROM v_objection_library").fetchall():
    print(r)
print("\n=== v_question_library ===")
for r in db.execute("SELECT * FROM v_question_library").fetchall():
    print(r)

# ── assertions ─────────────────────────────────────────────────────────────────
assert n_lead == n_intel == 3, "one row per core.lead"
a = db.execute("""SELECT n_replies, dominant_intent, has_question, has_objection,
   top_objection_type, last_reply_text, partner_disposition, disposition_class,
   is_opportunity, is_meeting, conversion_agent, funnel_stage, engagement_score
   FROM derived.lead_intel WHERE lead_email='alice@acme.com'""").fetchone()
assert a[0] == 2, f"alice has 2 human replies (auto excluded), got {a[0]}"
assert a[1] in ('interested', 'info_request'), f"dominant_intent set, got {a[1]}"
assert a[2] is True, "alice has_question"
assert a[3] is True, "alice has_objection"
assert a[4] == 'price', f"top_objection_type=price, got {a[4]}"
assert a[5] == 'But how much does it cost?', f"last_reply_text = most recent, got {a[5]!r}"
assert a[6] == 'LIVE OPPORTUNITY', "alice partner_disposition present (DUAL SIGNAL)"
assert a[7] == 'live', "alice disposition_class=live (DUAL SIGNAL)"
assert a[8] is True and a[9] is True, "alice is_opportunity AND is_meeting"
assert a[10] == 'im', f"alice conversion_agent=im, got {a[10]}"
assert a[11] == 'meeting', f"alice funnel_stage=meeting, got {a[11]}"
assert a[12] == 100, f"alice engagement_score capped at 100, got {a[12]}"

# Carol: phone-only, meeting via lead_key join, no reply
c = db.execute("""SELECT n_replies, is_meeting, conversion_agent, funnel_stage, phone_e164
   FROM derived.lead_intel WHERE lead_key='keyC'""").fetchone()
assert c[0] == 0 and c[1] is True and c[2] == 'warm_caller' and c[3] == 'meeting' and c[4] == '+15551230000', \
    f"carol phone-only conversion via lead_key, got {c}"

# views return >= 0 rows without error
assert db.execute("SELECT count(*) FROM v_intent_distribution").fetchone()[0] >= 0
assert db.execute("SELECT count(*) FROM v_objection_library").fetchone()[0] >= 0
assert db.execute("SELECT count(*) FROM v_question_library").fetchone()[0] >= 0

# idempotency: re-run yields identical row count
_build(db)
assert db.execute("SELECT count(*) FROM derived.lead_intel").fetchone()[0] == 3, "idempotent re-run"

print("\nALL ASSERTIONS PASSED ✅")
