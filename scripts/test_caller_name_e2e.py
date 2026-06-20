#!/usr/bin/env python3
"""Standalone e2e QA for caller_name — no orchestrator needed.

Creates a test Close lead + call, runs the close entity directly against
the live warehouse (write-locks for just the rebuild), asserts caller_name
lands correctly, then cleans up both Close and the warehouse row.

Run: python scripts/test_caller_name_e2e.py
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

import httpx
import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from entities.close_calls import parse_caller_name, upsert_raw, rebuild_core, resolve_lead_attrs
from sources.close import CloseClient

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/root/core/warehouse.duckdb")
TEST_NOTE = "TestRep - QA caller name sync"
TEST_NAME = "Testrep"

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def main() -> None:
    if not CLOSE_API_KEY:
        print(f"{FAIL}  CLOSE_API_KEY not set")
        sys.exit(1)

    lead_id = None
    call_id = None
    ok = True

    http = httpx.Client(
        base_url="https://api.close.com/api/v1",
        auth=(CLOSE_API_KEY, ""),
        headers={"User-Agent": "renaissance-warehouse/test"},
        timeout=30.0,
    )

    try:
        # 1. Create test lead + call in Close.
        print("── Creating test records in Close ──")
        r = http.post("/lead/", json={
            "name": "_TEST_WARM_CALLER_QA",
            "contacts": [{"name": "QA", "phones": [{"phone": "+10000000001"}]}],
        })
        r.raise_for_status()
        lead_id = r.json()["id"]
        print(f"  lead:  {lead_id}")

        r = http.post("/activity/call/", json={
            "lead_id": lead_id,
            "direction": "outbound",
            "disposition": "answered",
            "duration": 90,
            "note": TEST_NOTE,
        })
        r.raise_for_status()
        call_raw = r.json()
        call_id = call_raw["id"]
        print(f"  call:  {call_id}  note={TEST_NOTE!r}")
        time.sleep(1)

        # 2. Fetch the call back through CloseClient (same path as the real sync).
        print("\n── Fetching via CloseClient + running rebuild ──")
        now = datetime.now(timezone.utc)

        with duckdb.connect(WAREHOUSE_PATH) as conn:
            # Apply DDL (idempotent — adds caller_name column if missing).
            ddl = (ROOT / "sql" / "ddl" / "42_close_calls.sql").read_text()
            conn.execute(ddl)

            # Upsert just this call into raw.
            upsert_raw(conn, call_raw, "test_run", now)

            # Resolve the lead.
            with CloseClient(CLOSE_API_KEY) as client:
                lead = client.get_lead(lead_id)
            lead_cache = {lead_id: resolve_lead_attrs(lead)}

            # Rebuild core.call (full rebuild — will include existing calls).
            stats = rebuild_core(conn, lead_cache, now)
            print(f"  rebuild stats: {stats}")

            # Assert.
            rows = conn.execute(
                "SELECT call_id, caller_name FROM core.call WHERE call_id = ?",
                [call_id],
            ).fetchall()

        if not rows:
            print(f"  {FAIL}  call {call_id} not found in core.call")
            ok = False
        else:
            actual = rows[0][1]
            if actual == TEST_NAME:
                print(f"  {PASS}  core.call.caller_name = {actual!r}")
            else:
                print(f"  {FAIL}  caller_name = {actual!r}  (expected {TEST_NAME!r})")
                ok = False

        # 3. Coverage snapshot.
        print("\n── Caller attribution across core.call ──")
        with duckdb.connect(WAREHOUSE_PATH, read_only=True) as conn:
            coverage = conn.execute("""
                SELECT
                    coalesce(caller_name, '(unknown)') AS caller,
                    count(*) AS n,
                    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
                FROM core.call
                GROUP BY 1
                ORDER BY 2 DESC
            """).fetchall()
        for caller, n, pct in coverage:
            print(f"  {caller:20s}  {n:5d}  ({pct}%)")

    except Exception as exc:
        print(f"  {FAIL}  {exc}")
        ok = False

    finally:
        print("\n── Cleanup ──")
        if call_id:
            r = http.delete(f"/activity/call/{call_id}/")
            status = PASS if r.status_code in (200, 204, 404) else FAIL
            print(f"  {status}  deleted call  {call_id}")
        if lead_id:
            r = http.delete(f"/lead/{lead_id}/")
            status = PASS if r.status_code in (200, 204, 404) else FAIL
            print(f"  {status}  deleted lead  {lead_id}")
        http.close()

    print(f"\n{'All checks passed.' if ok else 'SOME CHECKS FAILED.'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
