#!/usr/bin/env python3
"""MD warehouse migration watchdog — server-side, droplet cron every 15m.
Watches the ACTIVE serving color (from the pointer file) for: reachability, TOKEN/TRIAL
validity (catches free-trial expiry / credit exhaustion), 207t/185v structural integrity,
a canary query, and FRESHNESS (build-marker age — catches a dead daily publish). Alerts
#cc-sam ONLY on 2+ consecutive HARD failures (gated) + posts a recovery note on heal.
Freshness is a soft WARN (logged; escalates to alert only when very stale)."""
import duckdb, json, os, sys, time, subprocess, urllib.request
from datetime import datetime, timezone

ENV = "/root/renaissance-warehouse/.env"
POINTER = "/opt/duckdb/md_serving_db"
STATE = "/root/md-migration/md_watchdog_state.json"
CH = "C0AR0EA21C1"                     # #cc-sam
EXPECT_TABLES, EXPECT_VIEWS = 200, 180
STALE_WARN_H, STALE_ALERT_H = 30, 50   # publish is daily; >50h build = dead publish

def env(k):
    for path in (ENV, "/root/.env"):
        try:
            for line in open(path):
                if line.startswith(k + "="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except FileNotFoundError:
            pass
    return None

def slack(text):
    tok = env("CC_SLACK_BOT_TOKEN")
    if not tok:
        print("WARN no CC_SLACK_BOT_TOKEN"); return
    data = json.dumps({"channel": CH, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=data,
          headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req, timeout=10); print("slack:", r.read()[:60])
    except Exception as e:
        print("slack err:", str(e)[:100])

def active_color():
    try:
        v = open(POINTER).read().strip()
        return v or "warehouse_a"
    except OSError:
        return "warehouse_a"

def check():
    os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN") or ""
    color = active_color()
    last = None
    for _ in range(3):                       # self-heal transient
        try:
            c = duckdb.connect(f"md:{color}")
            t = c.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=?", [color]).fetchone()[0]
            v = c.execute("SELECT count(*) FROM duckdb_views() WHERE database_name=?", [color]).fetchone()[0]
            t0 = time.time(); r = c.execute("SELECT count(*) FROM core.reply").fetchone()[0]; ms = (time.time()-t0)*1000
            # freshness from build marker
            age_h = None
            try:
                bt = c.execute("SELECT built_at_utc FROM main._md_build_info").fetchone()[0]
                age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(bt.replace("Z","+00:00"))).total_seconds()/3600
            except Exception:
                pass
            c.close()
            if t < EXPECT_TABLES or v < EXPECT_VIEWS:
                return False, f"{color} structural shrink: {t}t/{v}v", age_h
            if r <= 0:
                return False, f"{color} canary core.reply=0", age_h
            return True, f"{color} {t}t/{v}v canary={r:,} {ms:.0f}ms age={age_h:.1f}h" if age_h is not None else f"{color} {t}t/{v}v canary={r:,} {ms:.0f}ms", age_h
        except Exception as e:
            last = str(e)[:180]; time.sleep(5)
    return False, f"{color}: {last}", None

def main():
    arm = "--arm" in sys.argv
    ok, detail, age_h = check()
    try:
        st = json.load(open(STATE))
    except Exception:
        st = {"consecutive_fail": 0, "alerted": False, "stale_alerted": False}
    ts = subprocess.check_output(["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"]).decode().strip()
    if ok:
        if st.get("alerted"):
            slack(f":large_green_circle: MD warehouse watchdog — RECOVERED: {detail}. (was alerting)")
        # freshness soft-check (only meaningful once the daily publish cron is live)
        stale_alerted = st.get("stale_alerted", False)
        if age_h is not None and age_h > STALE_ALERT_H and not stale_alerted:
            slack(f":warning: MD serving is STALE — active color built {age_h:.0f}h ago (>{STALE_ALERT_H}h). "
                  f"The daily publish may have died; readers in md-mode are serving old data. — Sam, heads up.")
            stale_alerted = True
        elif age_h is not None and age_h <= STALE_WARN_H:
            stale_alerted = False
        st = {"consecutive_fail": 0, "alerted": False, "stale_alerted": stale_alerted}
        print(f"{ts} OK {detail}")
    else:
        st["consecutive_fail"] = st.get("consecutive_fail", 0) + 1
        print(f"{ts} FAIL[{st['consecutive_fail']}] {detail}")
        if st["consecutive_fail"] >= 2 and not st.get("alerted"):
            slack(f":red_circle: *MD warehouse migration watchdog* — health FAIL x{st['consecutive_fail']} (~30m).\n"
                  f"`{detail}`\nThe MotherDuck serving copy is unreachable/degraded — likely **free-trial expiry / "
                  f"credit exhaustion**, a dropped/half-built color, or an MD outage. Check Settings → Billing. "
                  f"Droplet is still primary; the flip is BLOCKED until healthy. — Sam, heads up.")
            st["alerted"] = True
    json.dump(st, open(STATE, "w"))
    if arm:
        slack(":eyes: *MD migration watchdog re-armed* — now tracking the ACTIVE serving color (pointer-based) "
              "+ freshness every 15m. Alerts here only on 2+ consecutive failures or very-stale serving. Silent while healthy.")
        print(f"{ts} ARMED")

main()
