#!/usr/bin/env python3
"""Watchdog for the never-lose-data escrow + read-API instrumentation. Server-side cron (every 4h),
outlives any chat. Monitors OUTCOMES, self-heals, alerts #cc-sam only on PERSISTENT real failure.

Checks:
  1) ESCROW COMPLETENESS + FRESHNESS: a COMPLETE escrow (every snapshot base table + seed_data + manifest)
     landed within the last ~26h. If today's is missing/partial (e.g. an OOM mid-run), SELF-HEAL by
     re-running the resumable escrow_export (skip-if-present fills gaps), re-check, alert only if still bad.
  2) READ API: /healthz ok AND query-logging still flowing (a 'query' event in the last 24h) — so a
     stalled/silent read path or dead instrumentation surfaces.
Gated: alerts on 2+ consecutive bad cycles (state file); posts recovery when healthy again."""
import duckdb, boto3, json, os, sys, time, subprocess, urllib.request
from datetime import datetime, timezone, timedelta
from botocore.config import Config

ENV = "/root/renaissance-warehouse/.env"
BUCKET = "renaissance-warehouse-escrow"
STATE = "/root/md-migration/escrow_watchdog_state.json"
CH = "C0AR0EA21C1"  # #cc-sam

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def env(k, path=ENV):
    try:
        for l in open(path):
            if l.startswith(k+"="): return l.split("=",1)[1].strip().strip('"').strip("\r")
    except FileNotFoundError: pass
    return None
def slack(text):
    tok = env("CC_SLACK_BOT_TOKEN") or env("CC_SLACK_BOT_TOKEN", "/root/.env")
    if not tok: return
    data = json.dumps({"channel": CH, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=data,
          headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try: urllib.request.urlopen(req, timeout=10)
    except Exception: pass
def s3c():
    a = env("R2_ACCOUNT_ID")
    return boto3.client("s3", endpoint_url=f"https://{a}.r2.cloudflarestorage.com",
        aws_access_key_id=env("R2_ACCESS_KEY_ID"), aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"), region_name="auto")

def escrow_status():
    """Return (dt, missing_tables, has_seed, has_manifest) for the most recent escrow date."""
    s3 = s3c()
    # discover escrow dates
    dates = set()
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="escrow/dt=", Delimiter="/"):
        for cp in pg.get("CommonPrefixes", []):
            dates.add(cp["Prefix"].split("dt=")[1].strip("/"))
    if not dates: return None, ["<no escrow at all>"], False, False
    dt = max(dates)
    present = set()
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"escrow/dt={dt}/"):
        for o in pg.get("Contents", []): present.add(o["Key"].split(f"escrow/dt={dt}/")[1])
    SNAP = subprocess.check_output(["readlink","-f","/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
    c = duckdb.connect(SNAP, read_only=True)
    tbls = c.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type='BASE TABLE'").fetchall()
    c.close()
    missing = [f"{s}.{t}" for s,t in tbls if f"{s}.{t}.parquet" not in present]
    has_seed = any(k.startswith("seed_data/") for k in present)
    has_manifest = "_manifest.json" in present
    return dt, missing, has_seed, has_manifest

def read_api_ok():
    try:
        r = urllib.request.urlopen("http://127.0.0.1:8899/healthz", timeout=15)
        h = json.loads(r.read())
    except Exception as e:
        try: h = json.loads(e.read())
        except Exception: return False, f"healthz unreachable: {str(e)[:60]}"
    if not h.get("ok"): return False, f"healthz not ok: {str(h)[:80]}"
    # query logging still flowing? (a query event in last 24h)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24))
    fresh = False
    try:
        for line in reversed(open("/opt/duckdb/logs/mcp_access.jsonl").read().splitlines()[-3000:]):
            d = json.loads(line)
            if d.get("event") == "query":
                if datetime.fromisoformat(d["ts"].replace("Z","+00:00")) > cutoff: fresh = True
                break
    except Exception: pass
    if not fresh: return False, "no query events logged in 24h (instrumentation stalled?)"
    return True, "read API ok + logging live"

def self_heal():
    """Re-run the resumable escrow export (flock so it can't collide with the daily cron)."""
    log("SELF-HEAL: re-running escrow_export (skip-if-present fills gaps)…")
    subprocess.run("/usr/bin/flock -n /tmp/escrow.lock /bin/bash -c "
                   "'ESCROW_BUCKET=renaissance-warehouse-escrow /usr/bin/python3 /root/md-migration/escrow_export.py "
                   ">> /root/md-migration/escrow.log 2>&1'", shell=True, timeout=3000)

def main():
    arm = "--arm" in sys.argv
    try: st = json.load(open(STATE))
    except Exception: st = {"fails": 0, "alerted": False}
    now = datetime.now(timezone.utc)
    problems = []

    # --- escrow check (+ self-heal) ---
    try:
        dt, missing, has_seed, has_manifest = escrow_status()
        stale = (dt is None) or (now.date() - datetime.strptime(dt, "%Y-%m-%d").date()).days > 1
        incomplete = bool(missing) or not has_seed or not has_manifest
        if stale or incomplete:
            log(f"escrow needs heal: dt={dt} missing={len(missing)} seed={has_seed} manifest={has_manifest}")
            try: self_heal()
            except Exception as e: log(f"self-heal err {str(e)[:80]}")
            dt, missing, has_seed, has_manifest = escrow_status()  # re-check
            stale = (dt is None) or (now.date() - datetime.strptime(dt, "%Y-%m-%d").date()).days > 1
            incomplete = bool(missing) or not has_seed or not has_manifest
            if stale or incomplete:
                problems.append(f"ESCROW still bad after self-heal: dt={dt} missing={len(missing)}{missing[:5]} seed={has_seed}")
        log(f"escrow: dt={dt} tables_missing={len(missing)} seed={has_seed} manifest={has_manifest}")
    except Exception as e:
        problems.append(f"ESCROW check errored: {str(e)[:100]}")

    # --- read API check ---
    ok, detail = read_api_ok()
    if not ok: problems.append(f"READ-API: {detail}")
    log(f"read-api: {detail}")

    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    if problems:
        st["fails"] = st.get("fails", 0) + 1
        log(f"{ts} PROBLEMS x{st['fails']}: {problems}")
        if st["fails"] >= 2 and not st.get("alerted"):
            slack(":red_circle: *Warehouse/escrow watchdog* — persistent issue (self-heal did not resolve):\n• " +
                  "\n• ".join(problems) + "\nNever-lose-data or read path may be degraded — needs a look. — Sam.")
            st["alerted"] = True
    else:
        if st.get("alerted"):
            slack(":large_green_circle: Warehouse/escrow watchdog — RECOVERED (escrow complete + read API healthy).")
        st = {"fails": 0, "alerted": False}
        log(f"{ts} OK — escrow complete + fresh, read API healthy")
    json.dump(st, open(STATE, "w"))
    if arm:
        slack(":eyes: *Warehouse/escrow watchdog ARMED* — every 4h it verifies a complete+fresh immutable escrow "
              "(self-heals partial/missing days) and the read API + query-logging are alive. Silent unless a real "
              "problem persists through a self-heal.")
        print(f"{ts} ARMED")

main()
