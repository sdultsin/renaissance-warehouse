#!/usr/bin/env python3
"""MotherDuck migration SHEPHERD — droplet cron, keeps the migration moving after all chats close.
Droplet-native (has ssh-local, the .env, the Slack bot token) — unlike a cloud routine which can't
reach any of this. It cannot do Claude-engineering itself, so its job is: verify the done-infra is
healthy, and keep the migration on Sam's radar in #cc-sam with the CURRENT next action — so it never
silently stalls. Posts every ~48h (gated) + immediately on any health failure. A future chat that
advances a phase updates /root/md-migration/migration_next.txt."""
import duckdb, json, os, time, subprocess, urllib.request
from datetime import datetime, timezone

ENV = "/root/renaissance-warehouse/.env"
STATE = "/root/md-migration/shepherd_state.json"
NEXT = "/root/md-migration/migration_next.txt"
HANDOFF = "handoffs/2026-07-10-motherduck-migration-state-and-next.md"
CH = "C0AR0EA21C1"  # #cc-sam
POST_EVERY_H = 48

def env(k, path=ENV):
    try:
        for l in open(path):
            if l.startswith(k+"="): return l.split("=",1)[1].strip().strip('"').strip("\r")
    except FileNotFoundError: pass
    return None
def slack(text):
    tok = env("CC_SLACK_BOT_TOKEN") or env("CC_SLACK_BOT_TOKEN", "/root/.env")
    if not tok: return
    d = json.dumps({"channel": CH, "text": text}).encode()
    r = urllib.request.Request("https://slack.com/api/chat.postMessage", data=d,
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try: urllib.request.urlopen(r, timeout=10)
    except Exception: pass

def checks():
    out = {}
    # MotherDuck alive + md:warehouse queryable?
    try:
        os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN") or ""
        c = duckdb.connect("md:warehouse")
        t = c.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse'").fetchone()[0]
        c.close(); out["motherduck"] = f"alive ({t} tables)"
    except Exception as e:
        out["motherduck"] = f"UNREACHABLE — trial expired / not paid? ({str(e)[:50]})"
    # crons present?
    try:
        cron = subprocess.check_output(["crontab","-l"]).decode()
        out["crons"] = {n: ("on" if n in cron else "MISSING") for n in
                        ["escrow_export.py","escrow_watchdog.py","md_load_tables_v3.py"]}
    except Exception: out["crons"] = "crontab unreadable"
    # md refresh ran recently?
    try:
        mt = os.path.getmtime("/root/md-migration/md_refresh.log")
        out["md_refresh_age_h"] = round((time.time()-mt)/3600, 1)
    except Exception: out["md_refresh_age_h"] = None
    return out

def main():
    try: st = json.load(open(STATE))
    except Exception: st = {"last_post": 0}
    now = time.time()
    o = checks()
    unhealthy = ("UNREACHABLE" in str(o.get("motherduck")) or "MISSING" in str(o.get("crons"))
                 or (o.get("md_refresh_age_h") or 0) > 30)
    due = (now - st.get("last_post", 0)) > POST_EVERY_H*3600
    nxt = "Convert the MotherDuck trial → PAID (blocks the write-path). Then open a chat on the handoff to build the write-path → authoritative."
    try: nxt = open(NEXT).read().strip() or nxt
    except Exception: pass
    if unhealthy or due:
        flag = ":red_circle:" if unhealthy else ":compass:"
        slack(f"{flag} *MotherDuck migration shepherd* — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
              f"• MotherDuck: {o.get('motherduck')}\n• crons: {o.get('crons')}\n"
              f"• md:warehouse refresh: {o.get('md_refresh_age_h')}h ago\n"
              f"*NEXT →* {nxt}\n(Follow-through: point a fresh Claude chat at `{HANDOFF}`. This shepherd keeps nudging until the migration is done.)")
        st["last_post"] = now
    json.dump(st, open(STATE, "w"))
    print(datetime.now(timezone.utc).isoformat(), "healthy" if not unhealthy else "UNHEALTHY", "posted" if (unhealthy or due) else "quiet")

main()
