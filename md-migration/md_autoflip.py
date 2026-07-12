#!/usr/bin/env python3
"""Auto-flip the mcp read API to MotherDuck the moment a valid READ-SCOPED token lands.
Cron every 5m. Idempotent + self-gated:
  - no MOTHERDUCK_TOKEN_RO in .env         -> wait (quiet).
  - token present but WRITE-capable        -> DON'T flip; alert #cc-sam (wrong token type).
  - token present + read-only confirmed     -> FLIP: inject WAREHOUSE_BACKEND=md + the RO token into
    the mcp-server.service via a drop-in, restart, verify /healthz (ok+read_only+md snapshot). On any
    verify failure -> immediate auto-ROLLBACK (rm drop-in, restart) + alert. On success -> alert + mark.
Rollback is always: rm the drop-in dir + daemon-reload + restart mcp-server."""
import os, json, subprocess, time, urllib.request, urllib.error

ENV = "/root/renaissance-warehouse/.env"
DROPIN_DIR = "/etc/systemd/system/mcp-server.service.d"
DROPIN = f"{DROPIN_DIR}/md-backend.conf"
TOKENFILE = "/opt/duckdb/md_serving.env"
DONE = "/root/md-migration/flip_done.json"
CH = "C0AR0EA21C1"

def env(k, path=ENV):
    try:
        for line in open(path):
            if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
    except FileNotFoundError: pass
    return None

def slack(text):
    tok = env("CC_SLACK_BOT_TOKEN") or env("CC_SLACK_BOT_TOKEN","/root/.env")
    if not tok: return
    data = json.dumps({"channel": CH, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=data,
          headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try: urllib.request.urlopen(req, timeout=10)
    except Exception: pass

def sh(cmd): return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def healthz():
    # unauthenticated /healthz on the mcp API
    try:
        r = urllib.request.urlopen("http://127.0.0.1:8899/healthz", timeout=15)
        return json.loads(r.read())
    except Exception as e:
        try:  # /healthz may 4xx but still return a body
            return json.loads(e.read())
        except Exception:
            return {"ok": False, "detail": f"healthz unreachable: {str(e)[:80]}"}

STATEF = "/root/md-migration/autoflip_state.json"
def load_state():
    try: return json.load(open(STATEF))
    except Exception: return {"healthz_fails": 0}
def save_state(st):
    try: json.dump(st, open(STATEF, "w"))
    except Exception: pass

def already_flipped():
    return os.path.exists(DROPIN)

def rollback(reason):
    sh(f"rm -rf {DROPIN_DIR}"); sh("systemctl daemon-reload"); sh("systemctl restart mcp-server.service")
    time.sleep(6)
    slack(f":arrows_counterclockwise: *MD flip AUTO-ROLLED BACK* — {reason}. mcp read API is back on LOCAL. "
          f"/healthz={json.dumps(healthz())[:160]}. Migration safe; will retry when healthy. — Sam, heads up.")

def main():
    ro = env("MOTHERDUCK_TOKEN_RO")
    if already_flipped():
        # post-flip bake monitor: rollback only after 3 consecutive bad /healthz (~15m), not a blip
        h = healthz(); st = load_state()
        if h.get("ok") and h.get("read_only"):
            st["healthz_fails"] = 0; save_state(st); return
        st["healthz_fails"] = st.get("healthz_fails", 0) + 1; save_state(st)
        if st["healthz_fails"] >= 3:
            rollback(f"post-flip /healthz degraded x{st['healthz_fails']}: {h.get('detail','')[:80]}")
            st["healthz_fails"] = 0; save_state(st)
        return
    if not ro:
        return  # waiting for Sam to add the read-scoped token

    # validate the token is genuinely READ-ONLY (write must be rejected)
    os.environ["motherduck_token"] = ro
    import duckdb
    try:
        c = duckdb.connect("md:warehouse_a")
        try:
            c.execute("CREATE DATABASE _roflip_probe"); c.execute("DROP DATABASE IF EXISTS _roflip_probe"); c.close()
            slack(":warning: *MD flip HELD* — the MOTHERDUCK_TOKEN_RO you added is WRITE-capable, not read-only. "
                  "Create a **read-scaling / read-only** access token in MotherDuck (Settings) and paste that as "
                  "MOTHERDUCK_TOKEN_RO. I'll flip automatically once it's genuinely read-only. — Sam.")
            return
        except Exception:
            c.close()  # write rejected -> genuinely read-only -> proceed
    except Exception as e:
        slack(f":warning: MD flip held — can't validate MOTHERDUCK_TOKEN_RO: {str(e)[:80]}"); return

    # FLIP: inject env into the service via drop-in
    os.makedirs(DROPIN_DIR, exist_ok=True)
    with open(TOKENFILE, "w") as f: f.write(f"MOTHERDUCK_TOKEN_RO={ro}\n")
    os.chmod(TOKENFILE, 0o600)
    with open(DROPIN, "w") as f:
        f.write("[Service]\nEnvironment=WAREHOUSE_BACKEND=md\n"
                f"EnvironmentFile={TOKENFILE}\n")
    sh("systemctl daemon-reload"); sh("systemctl restart mcp-server.service")
    time.sleep(8)
    h = healthz()
    if h.get("ok") and h.get("read_only") and str(h.get("snapshot_id","")).startswith("md:"):
        json.dump({"flipped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "healthz": h}, open(DONE, "w"))
        slack(f":white_check_mark: *MD read API FLIPPED to MotherDuck* — /healthz ok+read_only, serving "
              f"`{h.get('snapshot_id')}`. All 32 query-API consumers now read from MD. Baking with auto-rollback; "
              f"local is one env-flip away. — done.")
    else:
        rollback(f"post-flip /healthz not ok+read_only+md ({json.dumps(h)[:120]})")

if __name__ == "__main__":
    main()
