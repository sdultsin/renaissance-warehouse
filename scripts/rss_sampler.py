#!/usr/bin/env python3
"""Per-phase peak-RSS profiler for the warehouse nightly (monitoring only — never touches data).

Two modes:

  sample  — every INTERVAL s, record a JSONL row with the summed RSS of every live
            python3/duckdb process on the box, the single largest such process, and the
            kernel's MemAvailable. Self-terminates after DURATION_MIN (so a stuck nightly
            can't leave it running forever). Launch it detached at the TOP of nightly.sh.

  report  — join the samples against core.sync_run's per-ingest [started_at, ended_at]
            intervals (read-only) and print peak summed-RSS + peak single-proc RSS + min
            MemAvailable PER PHASE/INGEST. This is the "peak RAM per phase" deliverable.
            Also flags any phase whose peak > ALERT_GB.

No dependency on parsing the nightly log; sync_run is the source of truth for phase timing.
"""
from __future__ import annotations
import glob, json, os, re, sys, time
from datetime import datetime, timezone

INTERVAL = float(os.environ.get("RSS_SAMPLE_INTERVAL", "3"))
DURATION_MIN = float(os.environ.get("RSS_SAMPLE_DURATION_MIN", "720"))  # 12h cap
OUT = os.environ.get("RSS_SAMPLE_OUT", "/root/core/rss_profile.jsonl")
ALERT_GB = float(os.environ.get("RSS_ALERT_GB", "10"))
DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")


def _meminfo_available_kb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return -1


def _procs():
    """Yield (pid, comm, rss_kb) for python3/duckdb processes."""
    for p in glob.glob("/proc/[0-9]*"):
        try:
            with open(f"{p}/comm") as f:
                comm = f.read().strip()
            if not (comm.startswith("python") or comm == "duckdb"):
                continue
            rss_kb = 0
            with open(f"{p}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1]); break
            yield int(os.path.basename(p)), comm, rss_kb
        except (OSError, ValueError):
            continue


def sample():
    deadline = time.time() + DURATION_MIN * 60
    with open(OUT, "a") as out:
        while time.time() < deadline:
            ts = datetime.now(timezone.utc).isoformat()
            procs = list(_procs())
            total = sum(r for _, _, r in procs)
            top_pid, top_comm, top_rss = (0, "", 0)
            for pid, comm, rss in procs:
                if rss > top_rss:
                    top_pid, top_comm, top_rss = pid, comm, rss
            rec = {"ts": ts, "sum_rss_kb": total, "top_pid": top_pid,
                   "top_comm": top_comm, "top_rss_kb": top_rss,
                   "mem_avail_kb": _meminfo_available_kb(), "nproc": len(procs)}
            out.write(json.dumps(rec) + "\n"); out.flush()
            time.sleep(INTERVAL)


def report(since=None):
    import duckdb
    # samples
    samples = []
    with open(OUT) as f:
        for line in f:
            try:
                r = json.loads(line)
                r["_t"] = datetime.fromisoformat(r["ts"])
                samples.append(r)
            except Exception:
                continue
    if not samples:
        print("no samples in", OUT); return
    c = duckdb.connect(DB, read_only=True)
    c.execute("SET memory_limit='2GB'")
    # latest run's phases (or all since a run_id date)
    rows = c.execute("""
        SELECT phase_name, ingest_name, started_at, ended_at, status
        FROM core.sync_run_phase
        WHERE started_at >= now() - INTERVAL '18 hours'
        ORDER BY started_at
    """).fetchall()
    c.close()
    print(f"{'phase':<18} {'ingest':<26} {'dur_s':>6} {'peakSumGB':>9} {'peakProcGB':>10} {'minAvailGB':>10} {'status'}")
    worst = []
    for phase, ingest, st, en, status in rows:
        st = st.replace(tzinfo=timezone.utc) if st.tzinfo is None else st
        en = (en or datetime.now(timezone.utc))
        en = en.replace(tzinfo=timezone.utc) if en.tzinfo is None else en
        win = [s for s in samples if st <= s["_t"] <= en]
        if not win:
            continue
        peak_sum = max(s["sum_rss_kb"] for s in win) / 1024 / 1024
        peak_proc = max(s["top_rss_kb"] for s in win) / 1024 / 1024
        min_avail = min(s["mem_avail_kb"] for s in win) / 1024 / 1024
        dur = (en - st).total_seconds()
        print(f"{phase:<18} {ingest:<26} {dur:>6.0f} {peak_sum:>9.1f} {peak_proc:>10.1f} {min_avail:>10.1f} {status}")
        worst.append((peak_proc, phase, ingest))
    worst.sort(reverse=True)
    print("\nTop offenders by peak single-proc RSS:")
    for pk, ph, ing in worst[:8]:
        flag = "  <== >ALERT" if pk > ALERT_GB else ""
        print(f"  {pk:6.1f} GB  {ph}.{ing}{flag}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "sample"
    if mode == "sample":
        sample()
    elif mode == "report":
        report()
