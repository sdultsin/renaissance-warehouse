# RUNBOOK — MotherDuck write-path go-live [staged 2026-07-12 · execute Tue 2026-07-14+]

**Who runs this:** the MOF master-orchestrator chat (owns migration execution per
`migration_next.txt`). **Precondition:** Sam's paid MotherDuck upgrade has landed (calendared Tue
07-14). Nothing here runs on the trial. All scripts referenced live in `/root/md-migration/` and are
version-controlled in `renaissance-warehouse/md-migration/` (commit+push before modifying any of them).

**What ships tonight-after-you-run-this:** every green local nightly is published full-fidelity into
a MotherDuck staging color, validated (exact row parity + all views execute + canary floors), and
atomically swapped live (pointer flip for the API shim + zero-copy republish of the canonical
`md:warehouse` name). Bad nightlies are REFUSED (alert, readers keep yesterday). Then the read API
flips to MotherDuck, reversible in one command.

**Mechanism decision (read once):** this is the **publish-based write-path** — the local nightly
remains the build factory (untouched, instant fallback), and its validated output is what lands in
MotherDuck. The deeper "orchestrator writes directly into md: during the 4.5h build" variant is NOT
staged: it would freeze the local build (can't double-pull), break the direct-file consumers
(D1 publish, dashboards, daily report all read the local build file), and its over-the-wire build
runtime is unmeasured. It becomes the retire-local phase. See PREMISES at the bottom.

---

## 0 · Preflight (5 min, all read-only)

```bash
# 0.1 paid plan is live (Sam confirms in MotherDuck UI: Settings -> Billing = Business, not trial)

# 0.2 droplet can reach MotherDuck + both tokens present
grep -c '^MOTHERDUCK_TOKEN=' /root/renaissance-warehouse/.env      # expect 1
grep -c '^MOTHERDUCK_TOKEN_RO=' /root/renaissance-warehouse/.env   # expect 1
export motherduck_token=$(grep '^MOTHERDUCK_TOKEN=' /root/renaissance-warehouse/.env | cut -d= -f2- | tr -d '"')
/opt/duckdb/venv/bin/python -c "import duckdb; print([r[0] for r in duckdb.connect('md:').execute('SHOW DATABASES').fetchall()])"
# expect: warehouse, warehouse_a (+ my_db, sample_data). warehouse_b appears after the first run.

# 0.3 RO token is genuinely read-scoped (must FAIL with a permission error)
/opt/duckdb/venv/bin/python -c "
import os,duckdb
os.environ['motherduck_token']=open('/root/renaissance-warehouse/.env').read().split('MOTHERDUCK_TOKEN_RO=')[1].splitlines()[0].strip('\"')
c=duckdb.connect('md:warehouse_a')
try: c.execute('CREATE SCHEMA zz_probe'); print('!! RO TOKEN CAN WRITE — do NOT flip readers with it')
except Exception as e: print('OK read-scoped:', str(e)[:80])"

# 0.4 last night's nightly is green (the gate will re-check this itself)
tail -5 /root/renaissance-warehouse/logs/nightly.log

# 0.5 KNOWN STATE [2026-07-12]: md:warehouse had core.reply=0 (the 09:00Z count-skip refresh copied a
# mid-ART-repair snapshot). The 07-13 09:00Z refresh should have healed it; verify:
/opt/duckdb/venv/bin/python -c "import duckdb; print(duckdb.connect('md:warehouse').execute('SELECT count(*) FROM core.reply').fetchone())"
# expect ~1.05M+. If still 0, don't panic — step (a)'s first write-path run fixes it properly.
```

## (a) · Enable the write-path

```bash
# a.1 code current (the box tracks origin/main; divergence guard reverts strays)
git -C /root/renaissance-warehouse pull --ff-only

# a.2 dry-run: proves the gate + connection without writing anything
/usr/bin/python3 /root/md-migration/md_write_path.py --dry-run
# expect "GATE OK ..." + "would build ... target=warehouse_b". rc=1 "GATE NOT READY" means
# tonight's nightly hasn't committed yet — wait, or investigate the printed reason.

# a.3 first real run, manual + watched (~25–40 min; full copy + validate + swap)
/usr/bin/flock -n /tmp/md_write_path.lock /root/md-migration/md_write_path.sh 2>&1 | tee -a /root/md-migration/md_write_path.log
# watch for: "tables built N", "VALIDATION PASS", "POINTER FLIPPED: warehouse_a -> warehouse_b",
# "zero-copy clone PROBE OK", "CANONICAL republished".
# If the clone PROBE fails (CREATE DATABASE ... FROM ... unsupported on this MotherDuck version):
# rc=3 + Slack warn; the pointer flip still happened (shim readers fine). Decide per PREMISES #2.

# a.4 verify the result (read-only)
cat /opt/duckdb/md_serving_db                                    # expect warehouse_b
cat /root/md-migration/md_write_path_state.json                  # active_color, last_success_snapshot = tonight's
/opt/duckdb/venv/bin/python -c "import duckdb; c=duckdb.connect('md:warehouse');
print(c.execute('SELECT * FROM main._md_build_info').fetchone());
print('reply:', c.execute('SELECT count(*) FROM core.reply').fetchone())"

# a.5 install the crons (crontab -e):
#   REPLACE the 09:00Z storefront-refresh line
#     0 9 * * * ... md_load_tables_v3.py ...           # daily MD storefront refresh [2026-07-10]
#   WITH (comment the old line out, keep it as the fallback):
*/30 6-15 * * * /usr/bin/flock -n /tmp/md_write_path.lock /root/md-migration/md_write_path.sh >> /root/md-migration/md_write_path.log 2>&1  # MotherDuck write-path: publish committed nightly -> staging color -> validate -> swap [2026-07-14]
#   RE-ARM the health watchdog (cron was removed 07-10 to stop trial-credit burn):
*/15 * * * * /usr/bin/flock -n /tmp/md_watchdog.lock /usr/bin/python3 /root/md-migration/md_health_watchdog.py >> /root/md-migration/md_watchdog.log 2>&1  # MotherDuck serving health + freshness [re-armed 2026-07-14]
#   KEEP untouched: escrow 10:00Z, escrow_watchdog */4h, shepherd 14:00Z, and the ENTIRE local
#   nightly block (nightly.sh 05:30Z, backup.sh, snapshot machinery) — local stays the factory + fallback.

# a.6 update /root/md-migration/migration_next.txt (write-path LIVE, next = dual-run days then reader
#     flip on Sam's go) + drop a one-liner in #cc-sam.
```

**Rollback (a):** re-comment the write-path cron, un-comment the md_load_tables_v3 line,
`python3 /root/md-migration/md_write_path.py --rollback` (pointer back to the previous color).
Canonical back: `DROP DATABASE warehouse; CREATE DATABASE warehouse FROM warehouse_prev;` (python
one-liner with the write token). Local was never touched.

## (b) · Dual-run verification (target: 3 consecutive green days)

Daily, ~1 min each (days can overlap the reader flip — (c) needs day 1 green only, Sam's go):

1. `grep -E "VALIDATION|FLIPPED|CANONICAL|GATE" /root/md-migration/md_write_path.log | tail -8` —
   today shows GATE OK → VALIDATION PASS → POINTER FLIPPED → CANONICAL republished.
2. `tail -2 /root/md-migration/md_watchdog.log` — watchdog OK, age < 30h, canary ≥ ~1M.
3. Row-parity spot (local vs MotherDuck — the runner already asserted EXACT per-table parity at
   publish time; this is the independent recheck):
   `/opt/duckdb/venv/bin/python -c "import duckdb,subprocess; s=subprocess.check_output(['readlink','-f','/opt/duckdb/warehouse_current.duckdb']).decode().strip(); print('local:', duckdb.connect(s,read_only=True).execute('SELECT count(*) FROM core.reply').fetchone()); print('md:', duckdb.connect('md:warehouse').execute('SELECT count(*) FROM core.reply').fetchone())"`
4. Escrow watchdog still green: `tail -2 /root/md-migration/escrow_watchdog.log` (belt stays on
   regardless of backend — never retire OWNED R2/parquet backups).
5. D1 read-model unaffected (it reads the LOCAL build, which still runs): no alerts from
   `wgr_nightly_d1_watchdog`; daily report rendered in Slack as usual.
6. No new Slack alerts from the write-path runner (it is failure-only).

A REFUSED day (gate/validation fail) is the system working: readers keep yesterday's data, the alert
says why. Fix the nightly, let the next 30-min tick publish. It does NOT reset the 3-day count if
the refusal was correct behavior (bad nightly), only if the runner itself malfunctioned.

## (c) · Reader flip (Sam's go — open question #2 in the 07-10 handoff)

```bash
# c.1 flip (injects WAREHOUSE_BACKEND=md + the RO token via systemd drop-in, restarts, verifies,
#     AUTO-ROLLS-BACK if healthz doesn't come up ok+md within ~30s)
/root/md-migration/reader_flip.sh apply

# c.2 verify through the real consumer path (bearer-authenticated REST query)
TOK=$(grep -v '^#' /opt/duckdb/allowed_tokens.txt | head -1 | awk '{print $1}')
curl -s -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT count(*) FROM core.reply"}' http://localhost:8899/query | head -c 400
# expect rows + "snapshot_id":"md:warehouse_b" (or _a). Latency 20–300ms is normal.

# c.3 D1-publish diff (the CC-enforcement invariant): D1 is published FROM the local build (path
#     unchanged by this flip — PARITY-REPORT.md: direct-file consumers don't ride the shim), and the
#     runner proved local == MotherDuck exactly at publish time, so D1 == MotherDuck transitively.
#     Verify both halves anyway:
/root/wgr_nightly_d1_watchdog.sh          # D1 vs local build — must stay quiet/green
# + step (b).3 parity spot                # local vs MotherDuck
# One spot pair (yesterday's sends, D1-backed dashboards vs md):
/opt/duckdb/venv/bin/python -c "import duckdb; print(duckdb.connect('md:warehouse').execute(\"SELECT count(*), sum(sent) FROM core.campaign_daily WHERE date = current_date - 1\").fetchone())"
# compare against the Campaign Control dashboard / daily report number for the same day.

# c.4 observe 1 business day (watchdog + query log /opt/duckdb/logs/mcp_access.jsonl), then note in
#     migration_next.txt that readers are on MotherDuck.
```

**Freshness caveat (know before flipping):** pre-flip, API consumers ride intraday local snapshot
promotes (5–6×/day). Post-flip they get the once-daily post-nightly publish. If an intraday consumer
complains, either run an extra publish (`rm /root/md-migration/.md_write_path_done && flock -n
/tmp/md_write_path.lock /root/md-migration/md_write_path.sh`) or widen the cron window — publishes
are ~25–40 min and MotherDuck compute is paid now (Sam: cost not the concern). Same remedy for
read-after-write after a warehouse-ship apply (writes land local first — this fully resolves only at
the retire-local/direct-build phase).

**Rollback (c):** `/root/md-migration/reader_flip.sh rollback` — back on the local snapshot in
seconds. (Same mechanism as the proven 07-10 flip + rollback.)

## (d) · Rollback summary (any step, any time)

| After | Command | Effect |
|---|---|---|
| (a) publish | `md_write_path.py --rollback` | pointer back to previous color (readers on shim) |
| (a) canonical | `CREATE DATABASE warehouse FROM warehouse_prev` (drop broken first) | canonical name back to yesterday |
| (a) crons | swap the cron lines back (old lines kept commented) | 09:00Z count-skip refresh resumes |
| (c) flip | `reader_flip.sh rollback` | API back on local snapshot in seconds |
| everything | local nightly + snapshots + escrow were NEVER touched | full local fallback intact |

---

## PREMISES the docs left ambiguous — decided here, executor may override

1. **Publish-based vs direct-build write-path.** The 07-10 handoff's "env-gate core/db.py write
   target to md:" (orchestrator writes into MotherDuck for the whole 4.5h build) is NOT what's
   staged, deliberately: it freezes local (can't double-pull), breaks D1/dashboards/daily-report
   (they read the local build file), and its remote-build runtime is unmeasured. Staged instead:
   local builds → validated full-fidelity publish → MotherDuck (readers + canonical current within
   ~40 min of a green nightly). Direct-build = the retire-local phase, AFTER the direct-file
   consumers are re-sourced from MotherDuck (droplet-exit Wave 1/3 work). If the executor wants
   direct-build anyway, core/db.py env-gating is NOT written — that's a build task, not execute-only.
2. **"Atomic swap to md:warehouse".** MotherDuck has no `ALTER DATABASE RENAME` (verified 07-09), so
   a literal in-place atomic rename is impossible. Staged: atomic pointer flip (API shim readers,
   truly atomic) + canonical `md:warehouse` republish via zero-copy clone
   (`CREATE DATABASE ... FROM ...`), which leaves a seconds-level gap on the canonical NAME only,
   at ~07:00–10:00Z. The runner PROBES clone support at runtime; if unsupported, it alerts and the
   fallback is keeping the 09:00Z v3 refresh for the canonical name (shim readers unaffected).
3. **Publish cadence.** Once per green nightly (default). Intraday freshness for API consumers
   regressed vs local promotes — see the (c) caveat for the knob. Executor/Sam call.
4. **Team-write routing (moderator / warehouse-ship → MotherDuck).** Deliberately NOT staged —
   droplet-exit plan §6.1 assigns DDL governance redesign to the warehouse-ops lane, and writes keep
   landing on the local factory (correct in publish-mode). Read-after-write via the API resolves at
   the retire-local phase.
5. **`warehouse_a` (07-09 parity color)** stays parked; the color rotation reuses it on run #2.
   `my_db` / `sample_data` are MotherDuck defaults — ignore.
6. **Canary floors** (canaries.json) set from 07-12 actuals (reply 1.08M→floor 900k, meetings 38k→10k,
   raw_instantly_email 379k→300k). Ratchet them up occasionally; they are shrink-guards, not exactness.
