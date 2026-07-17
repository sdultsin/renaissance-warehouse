#!/usr/bin/env python3
"""MotherDuck WRITE-PATH runner — nightly build -> staging color -> validate/canaries -> atomic swap.

Staged 2026-07-12 (Lane B, execute-only from Tue 2026-07-14 after the paid upgrade lands).
This is the "final form" of md_load_tables_v3.py + md_publish.py:

  1. GATE      — refuse to publish unless tonight's LOCAL nightly committed cleanly
                 (core.sync_run success/partial, campaign_daily fresh, pre-canaries on the
                 snapshot). This is the guard the 09:00Z count-skip refresh lacked: on
                 2026-07-12 it copied a mid-repair snapshot and left md:warehouse with
                 core.reply=0 for a day. The gate makes that class impossible.
  2. BUILD     — full-fidelity copy of the promoted serving snapshot into the INACTIVE
                 blue/green color (warehouse_a|warehouse_b), rowid-chunked big tables with
                 per-chunk retry + reconnect (v3 robustness), macros then views multi-pass,
                 HEAVY_VIEWS materialized as tables.
  3. VALIDATE  — exact per-table row parity vs the snapshot + every view executes +
                 canaries.json (the parity-time canary queries, with floors) + build marker.
  4. SWAP      — atomic pointer-file flip (readers on the shim never see mid-rebuild), then
                 republish the canonical `md:warehouse` name via zero-copy clone (probed at
                 runtime; MotherDuck has no ALTER DATABASE RENAME as of 2026-07-09).

Exit codes: 0 = published+swapped · 1 = gate not ready (cron retries later, NO alert)
            2 = build/validation failure (alert, pointer untouched) · 3 = swap/canonical failure (alert).

Rollback:   md_write_path.py --rollback         # flip the pointer back to the previous color
Canonical:  CREATE DATABASE warehouse FROM warehouse_prev   (kept by the canonical republish)

Flags: --force (skip the gate) · --no-canonical (pointer flip only) · --no-flip (dark build)
       --rollback (pointer back) · --dry-run (gate + plan only, no MotherDuck writes)
"""
import duckdb, os, sys, time, json, subprocess, re, urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = "/root/renaissance-warehouse/.env"
POINTER = "/opt/duckdb/md_serving_db"            # shim reads this (common.md_serving_db)
STATE = os.path.join(HERE, "md_write_path_state.json")
CANARIES = os.path.join(HERE, "canaries.json")
TMP = "/mnt/volume_nyc1_1781398428838/tmp_mdload"
CHUNK, BIG = 4_000_000, 3_000_000
GATE_MAX_AGE_H = 30                               # newest committed nightly must be younger than this
CANONICAL = "warehouse"                           # the name Sam/web-UI queries
CANONICAL_PREV = "warehouse_prev"                 # instant canonical rollback
CH = "C0AR0EA21C1"                                # #cc-sam — failure-only

# Views too heavy to serve as views on MotherDuck (>60s API timeout cold) and on no automated
# consumer path — materialize as TABLES (serving is a per-publish snapshot; consistent).
HEAVY_VIEWS = {"derived.v_reply_canonical", "core.reply_attribution"}

os.makedirs(TMP, exist_ok=True)

def log(m): print(f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {m}", flush=True)

def env(k, path=ENV):
    try:
        for line in open(path):
            if line.startswith(k + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return None

def slack(text):
    tok = env("CC_SLACK_BOT_TOKEN") or env("CC_SLACK_BOT_TOKEN", "/root/.env")
    if not tok:
        log("WARN no CC_SLACK_BOT_TOKEN — alert not sent"); return
    data = json.dumps({"channel": CH, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=data,
          headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log(f"slack err: {str(e)[:120]}")

def load_state():
    try:
        return json.load(open(STATE))
    except Exception:
        return {}

def save_state(st):
    tmp = STATE + ".tmp"
    json.dump(st, open(tmp, "w"), indent=2)
    os.replace(tmp, STATE)

def read_pointer():
    try:
        v = open(POINTER).read().strip()
        return v if re.match(r"^warehouse_[a-z0-9_]+$", v) else None
    except OSError:
        return None

def flip_pointer(target):
    tmp = POINTER + ".tmp"
    open(tmp, "w").write(target)
    os.replace(tmp, POINTER)   # atomic

def canaries():
    try:
        return json.load(open(CANARIES))
    except Exception as e:
        log(f"WARN canaries.json unreadable ({e}) — running with structural checks only")
        return []

def run_canaries(con, prefix, label):
    """Run every canary with schema refs qualified by `prefix` (e.g. 'snap.' or 'warehouse_b.').
    Returns list of failures."""
    fails = []
    for c in canaries():
        sql = re.sub(r"\b(core|derived|dash|main)\.", prefix + r"\1.", c["sql"])
        try:
            got = con.execute(sql).fetchone()[0] or 0
            if got < c["min"]:
                fails.append(f"{c['name']}: {got} < min {c['min']}")
        except Exception as e:
            fails.append(f"{c['name']}: ERROR {str(e)[:100]}")
    if fails:
        log(f"CANARIES FAIL on {label}: {fails}")
    else:
        log(f"canaries OK on {label} ({len(canaries())} checks)")
    return fails

# ---------------------------------------------------------------- rollback --
if "--rollback" in sys.argv:
    st = load_state()
    prev = st.get("previous_color")
    if not prev:
        print("no previous_color in state — nothing to roll back to"); sys.exit(2)
    cur = read_pointer()
    flip_pointer(prev)
    log(f"POINTER ROLLED BACK: {cur} -> {prev}. Canonical rollback (if republished): "
        f"CREATE DATABASE {CANONICAL} FROM {CANONICAL_PREV} (drop the broken one first).")
    sys.exit(0)

# ---------------------------------------------------------------- connect ---
tok = env("MOTHERDUCK_TOKEN")
if not tok:
    log("FATAL: MOTHERDUCK_TOKEN missing from .env"); sys.exit(2)
os.environ["motherduck_token"] = tok

SNAP = subprocess.check_output(["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
SNAP_BASE = os.path.basename(SNAP)

def connect(attempts=6):
    last = None
    for i in range(attempts):
        try:
            c = duckdb.connect("md:")
            c.execute("SET preserve_insertion_order=false")
            c.execute("SET memory_limit='9GB'")
            c.execute(f"SET temp_directory='{TMP}'")
            c.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
            return c
        except Exception as e:
            last = e; log(f"  connect attempt {i+1} failed: {str(e)[:100]}"); time.sleep(6)
    raise RuntimeError(f"could not connect to MotherDuck: {last}")

# ---------------------------------------------------------------- gate ------
def gate(con):
    """Publish only a COMMITTED, healthy nightly. Returns (ok, reason)."""
    st = load_state()
    if st.get("last_success_snapshot") == SNAP_BASE and "--force" not in sys.argv:
        return False, f"snapshot {SNAP_BASE} already published (state)"
    try:
        row = con.execute(
            "SELECT status, started_at, ended_at, phase_count FROM snap.core.sync_run "
            "WHERE status IN ('success','partial') AND phase_count >= 5 "
            "ORDER BY started_at DESC LIMIT 1").fetchone()
    except Exception as e:
        return False, f"cannot read core.sync_run: {str(e)[:100]}"
    if not row:
        return False, "no committed nightly (success/partial, phase_count>=5) found in core.sync_run"
    status, started, ended, phases = row
    age_h = (datetime.now(timezone.utc) - started).total_seconds() / 3600
    if age_h > GATE_MAX_AGE_H:
        return False, f"latest committed nightly is {age_h:.0f}h old (> {GATE_MAX_AGE_H}h)"
    if ended is None:
        return False, "latest nightly has not ended"
    fails = run_canaries(con, "snap.", f"SOURCE snapshot {SNAP_BASE}")
    if fails:
        return False, f"pre-canaries failed on the snapshot: {fails}"
    return True, f"nightly {status} {started} ({phases} phases, {age_h:.1f}h ago); snapshot canaries OK"

con = connect()
ok, reason = gate(con)
force = "--force" in sys.argv
if not ok and not force:
    log(f"GATE NOT READY: {reason}")
    con.close(); sys.exit(1)
log(f"GATE {'OK' if ok else 'FORCED past: ' + reason}: {reason if ok else ''}")

active = read_pointer() or "warehouse_a"
target = "warehouse_b" if active == "warehouse_a" else "warehouse_a"
log(f"snapshot={SNAP_BASE} active={active} -> building INACTIVE target={target}")

if "--dry-run" in sys.argv:
    log("--dry-run: gate passed; would build the above. Exiting without writes."); con.close(); sys.exit(0)

# ---------------------------------------------------------------- build -----
t_all = time.time()
con.execute(f"DROP DATABASE IF EXISTS {target}")
con.execute(f"CREATE DATABASE {target}")
tschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name='snap'").fetchall()]
vschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_views() WHERE database_name='snap'").fetchall()]
for s in set(tschemas) | set(vschemas):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {target}.{s}")

tbls = con.execute("SELECT schema_name, table_name, estimated_size FROM duckdb_tables() "
                   "WHERE database_name='snap' ORDER BY estimated_size").fetchall()
bad, errs = [], []
for i, (s, n, est) in enumerate(tbls, 1):
    fq, sq = f'{target}.{s}."{n}"', f'snap.{s}."{n}"'
    try:
        ct = con.execute(f'SELECT count(*) FROM {sq}').fetchone()[0]
        if ct > BIG:
            con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq} LIMIT 0')
            # Chunk over the ACTUAL rowid domain, not count(*): DuckDB rowids are
            # physical and go SPARSE after UPDATE/DELETE churn — bounding by count
            # silently drops every row with rowid >= count. Proven 2026-07-17:
            # core.account_census count=10,292,972 max(rowid)=15,065,228 after DDL
            # 1117's row rewrites -> 797,920 rows dropped = the parity_mismatch
            # that failed this night's publish. Empty ranges cost ~nothing.
            mx = con.execute(f'SELECT max(rowid) FROM {sq}').fetchone()[0]
            k = 0
            while k <= mx:
                for attempt in range(4):
                    try:
                        con.execute(f'INSERT INTO {fq} SELECT * FROM {sq} WHERE rowid >= {k} AND rowid < {k+CHUNK}')
                        break
                    except Exception as ce:
                        log(f"    chunk {k} retry {attempt+1}: {str(ce)[:90]}")
                        try: con.close()
                        except Exception: pass
                        con = connect(); time.sleep(3)
                        if attempt == 3: raise
                k += CHUNK
            log(f"[{i}/{len(tbls)}] {s}.{n} rows={ct:,} (chunked)")
        else:
            con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq}')
        got = con.execute(f'SELECT count(*) FROM {fq}').fetchone()[0]
        if got != ct:
            bad.append((f"{s}.{n}", ct, got))
    except Exception as e:
        errs.append((f"{s}.{n}", str(e)[:150]))
        log(f"[{i}/{len(tbls)}] {s}.{n} ERROR {str(e)[:120]}")
        try: con.close()
        except Exception: pass
        con = connect()
log(f"tables built {len(tbls)} in {time.time()-t_all:.0f}s; parity_mismatch={bad[:5]} errors={errs[:5]}")

# macros then views (multi-pass; HEAVY_VIEWS materialized)
con.execute(f"USE {target}")
for s, fn, params, body in con.execute(
        "SELECT schema_name, function_name, parameters, macro_definition FROM duckdb_functions() "
        "WHERE database_name='snap' AND function_type ILIKE '%macro%' AND internal=false").fetchall():
    ps = ", ".join(params); tbl = body.lstrip().upper().startswith("SELECT")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    con.execute(f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS TABLE ({body})' if tbl
                else f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS {body}')
vw = con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() "
                 "WHERE database_name='snap' AND internal=false").fetchall()
pending = list(vw)
for _p in range(4):
    still = []
    for s, n, sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
            if f"{s}.{n}" in HEAVY_VIEWS:
                con.execute(re.sub(r"^\s*CREATE\s+VIEW\s", "CREATE TABLE ", sql, count=1, flags=re.I))
            else:
                con.execute(sql)
        except Exception:
            still.append((s, n, sql))
    if not still or len(still) == len(pending):
        pending = still; break
    pending = still
views_ok = len(vw) - len(pending)
log(f"macros+views ported: views {views_ok}/{len(vw)} (failed: {[f'{s}.{n}' for s, n, _ in pending][:10]})")

# ---------------------------------------------------------------- validate --
vfail = []
for s, v in con.execute("SELECT schema_name, view_name FROM duckdb_views() "
                        "WHERE database_name=? AND internal=false", [target]).fetchall():
    try:
        con.execute(f'SELECT count(*) FROM {target}.{s}."{v}"').fetchone()
    except Exception as e:
        vfail.append((f"{s}.{v}", str(e)[:80]))
cfails = run_canaries(con, f"{target}.", f"TARGET {target}")
ok = (not bad) and (not errs) and (views_ok == len(vw)) and (not vfail) and (not cfails)
con.execute(f"CREATE SCHEMA IF NOT EXISTS {target}.main")
con.execute(f"CREATE OR REPLACE TABLE {target}.main._md_build_info AS "
            "SELECT ? AS snapshot_id, ? AS built_at_utc, ? AS color",
            [SNAP_BASE, time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), target])
log(f"VALIDATION {'PASS' if ok else 'FAIL'}: parity_ok={not bad} copy_errs={len(errs)} "
    f"views={views_ok}/{len(vw)} view_exec_fail={len(vfail)} canary_fail={len(cfails)}")

if not ok:
    slack(f":rotating_light: MotherDuck write-path: VALIDATION FAILED building `{target}` from "
          f"`{SNAP_BASE}` — pointer NOT flipped (readers still on `{active}`). "
          f"parity={bad[:3]} errs={errs[:3]} views={views_ok}/{len(vw)} canaries={cfails[:3]} "
          f"— see /root/md-migration/md_write_path.log")
    con.close(); sys.exit(2)

if "--no-flip" in sys.argv:
    log(f"--no-flip: built+validated {target}; pointer UNCHANGED (still {active})."); con.close(); sys.exit(0)

# ---------------------------------------------------------------- swap ------
flip_pointer(target)
st = load_state()
st.update({"previous_color": active, "active_color": target,
           "last_success_snapshot": SNAP_BASE, "swapped_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})
save_state(st)
log(f"POINTER FLIPPED: {active} -> {target} (shim readers in md-mode now serve {target})")

# canonical `md:warehouse` republish via zero-copy clone (probed; MD has no RENAME as of 07-09)
if "--no-canonical" not in sys.argv:
    rc3 = False
    try:
        con.execute("DROP DATABASE IF EXISTS _mdwp_cloneprobe")
        con.execute(f"CREATE DATABASE _mdwp_cloneprobe FROM {target}")
        n = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='_mdwp_cloneprobe'").fetchone()[0]
        con.execute("DROP DATABASE _mdwp_cloneprobe")
        if n < len(tbls):
            raise RuntimeError(f"clone probe table count {n} < {len(tbls)}")
        log("zero-copy clone PROBE OK")
        con.execute(f"DROP DATABASE IF EXISTS {CANONICAL_PREV}")
        try:
            con.execute(f"CREATE DATABASE {CANONICAL_PREV} FROM {CANONICAL}")
        except Exception as e:
            log(f"WARN could not snapshot current canonical to {CANONICAL_PREV}: {str(e)[:100]}")
        con.execute(f"DROP DATABASE IF EXISTS {CANONICAL}")
        try:
            con.execute(f"CREATE DATABASE {CANONICAL} FROM {target}")
        except Exception as e:
            log(f"CANONICAL REPUBLISH FAILED mid-swap: {str(e)[:150]} — attempting restore from {CANONICAL_PREV}")
            con.execute(f"CREATE DATABASE {CANONICAL} FROM {CANONICAL_PREV}")
            raise
        nt = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=?", [CANONICAL]).fetchone()[0]
        log(f"CANONICAL republished: md:{CANONICAL} <- {target} ({nt} tables)")
        st["canonical_republished"] = True; save_state(st)
        rc3 = True
    except Exception as e:
        slack(f":warning: MotherDuck write-path: pointer swap SUCCEEDED (`{target}` serving via shim) "
              f"but the canonical `md:warehouse` republish failed/unsupported: `{str(e)[:150]}`. "
              f"Web-UI users of `md:warehouse` are stale until fixed (fallback: keep the 09:00Z "
              f"md_load_tables_v3 refresh cron). Rollback note: CREATE DATABASE warehouse FROM warehouse_prev")
        st["canonical_republished"] = False; save_state(st)
        con.close(); sys.exit(3)

con.close()
log(f"DONE in {time.time()-t_all:.0f}s — {target} live (pointer + canonical), snapshot {SNAP_BASE}")
sys.exit(0)
