#!/usr/bin/env python3
"""QA test — warm caller name parsing via Close call notes.

What this does:
  1. Unit-tests parse_caller_name() with known inputs.
  2. Creates a disposable Close lead + call activity (note="TestRep").
  3. Runs the full close_calls rebuild against the live warehouse.
  4. Asserts caller_name="Testrep" appears in core.call.
  5. Deletes the test lead + activity from Close.

Run from the renaissance-warehouse root:
    python scripts/test_caller_name_sync.py

Requires CLOSE_API_KEY and WAREHOUSE_PATH in env (or .env).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Load .env if present.
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

from entities.close_calls import parse_caller_name


CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY", "")
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/root/core/warehouse.duckdb")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


# ── 1. Unit tests ────────────────────────────────────────────────────────────

def run_unit_tests() -> bool:
    cases = [
        ("Jamie",                       "Jamie"),
        ("jamie",                       "Jamie"),
        ("ELLE",                        "Elle"),
        ("Elle",                        "Elle"),
        ("Elle - set appointment",      "Elle"),
        ("Jamie: not interested",       "Jamie"),
        ("sarah | booked 10am",         "Sarah"),
        ("Marcus,notes here",           "Marcus"),
        ("",                            None),
        (None,                          None),
        ("   ",                         None),
        ("L",                           None),   # single char — too short
        ("123",                         None),   # not alpha
        ("123abc",                      None),   # not alpha
        ("not interested",              None),   # outcome keyword
        ("DNC",                         None),   # outcome keyword
        ("No",                          None),   # outcome keyword
        ("booked 10am",                 None),   # outcome keyword
        ("voicemail left",              None),   # outcome keyword
    ]
    ok = True
    print("\n── Unit tests: parse_caller_name() ──")
    for note, expected in cases:
        result = parse_caller_name(note)
        status = PASS if result == expected else FAIL
        if result != expected:
            ok = False
        print(f"  {status}  parse_caller_name({note!r:35s}) = {result!r:12s}  (expected {expected!r})")
    return ok


# ── 2. Close API helpers ─────────────────────────────────────────────────────

def close_client() -> httpx.Client:
    if not CLOSE_API_KEY:
        print(f"{FAIL}  CLOSE_API_KEY not set — skipping end-to-end test")
        sys.exit(1)
    return httpx.Client(
        base_url="https://api.close.com/api/v1",
        auth=(CLOSE_API_KEY, ""),
        headers={"User-Agent": "renaissance-warehouse/test", "Accept": "application/json"},
        timeout=30.0,
    )


def create_test_lead(client: httpx.Client) -> str:
    resp = client.post("/lead/", json={
        "name": "_TEST_WARM_CALLER_QA",
        "contacts": [{"name": "QA Test Contact", "phones": [{"phone": "+10000000001"}]}],
    })
    resp.raise_for_status()
    lead_id = resp.json()["id"]
    print(f"  Created test lead: {lead_id}")
    return lead_id


def create_test_call(client: httpx.Client, lead_id: str, note: str) -> str:
    resp = client.post("/activity/call/", json={
        "lead_id": lead_id,
        "direction": "outbound",
        "disposition": "answered",
        "duration": 90,
        "note": note,
    })
    resp.raise_for_status()
    call_id = resp.json()["id"]
    print(f"  Created test call activity: {call_id}  (note={note!r})")
    return call_id


def delete_call(client: httpx.Client, call_id: str) -> None:
    resp = client.delete(f"/activity/call/{call_id}/")
    if resp.status_code not in (200, 204, 404):
        print(f"  Warning: DELETE call {call_id} -> {resp.status_code}")
    else:
        print(f"  Deleted test call: {call_id}")


def delete_lead(client: httpx.Client, lead_id: str) -> None:
    resp = client.delete(f"/lead/{lead_id}/")
    if resp.status_code not in (200, 204, 404):
        print(f"  Warning: DELETE lead {lead_id} -> {resp.status_code}")
    else:
        print(f"  Deleted test lead: {lead_id}")


# ── 3. Warehouse sync + assertion ────────────────────────────────────────────

def run_close_phase() -> None:
    """Run just the close_calls entity against the live warehouse on this machine."""
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "core.orchestrator", "--phase", "close"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"  Orchestrator stderr:\n{result.stderr[-2000:]}")
        raise RuntimeError(f"Orchestrator phase 'close' failed (rc={result.returncode})")
    print("  Orchestrator close phase: OK")


def assert_caller_name_in_warehouse(call_id: str, expected_name: str) -> bool:
    try:
        conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        rows = conn.execute(
            "SELECT call_id, caller_name FROM core.call WHERE call_id = ?",
            [call_id],
        ).fetchall()
        conn.close()
    except Exception as exc:
        print(f"  {FAIL}  warehouse query failed: {exc}")
        return False

    if not rows:
        print(f"  {FAIL}  call {call_id} not found in core.call after sync")
        return False

    actual = rows[0][1]
    if actual == expected_name:
        print(f"  {PASS}  core.call.caller_name = {actual!r}  (call {call_id})")
        return True
    else:
        print(f"  {FAIL}  core.call.caller_name = {actual!r}  (expected {expected_name!r})")
        return False


def check_coverage_improvement() -> None:
    """Show before/after caller attribution coverage."""
    try:
        conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        rows = conn.execute("""
            SELECT
                caller_name,
                count(*) AS n,
                round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
            FROM core.call
            GROUP BY 1
            ORDER BY 2 DESC
        """).fetchall()
        conn.close()
    except Exception as exc:
        print(f"  Could not check coverage: {exc}")
        return

    print("\n  Caller attribution across core.call:")
    for name, n, pct in rows:
        label = name if name else "(unknown)"
        print(f"    {label:20s}  {n:5d}  ({pct}%)")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    all_ok = True

    # 1. Unit tests (no API, no DB needed).
    all_ok = run_unit_tests() and all_ok

    # 2. End-to-end: Close API + warehouse.
    print("\n── End-to-end: Close API + warehouse sync ──")
    test_note = "TestRep - QA caller name sync"
    expected_name = "Testrep"

    lead_id = None
    call_id = None
    try:
        with close_client() as client:
            lead_id = create_test_lead(client)
            call_id = create_test_call(client, lead_id, test_note)

        # Give Close a moment to commit the activity.
        time.sleep(2)

        # Run the close phase (pulls incremental — will pick up our new call).
        print("  Running orchestrator close phase…")
        run_close_phase()

        # Assert.
        all_ok = assert_caller_name_in_warehouse(call_id, expected_name) and all_ok

        # Coverage report.
        check_coverage_improvement()

    except Exception as exc:
        print(f"  {FAIL}  end-to-end test error: {exc}")
        all_ok = False
    finally:
        # Always clean up, even if something failed.
        if lead_id or call_id:
            print("\n── Cleanup ──")
            with close_client() as client:
                if call_id:
                    delete_call(client, call_id)
                if lead_id:
                    delete_lead(client, lead_id)

    print(f"\n{'All tests passed.' if all_ok else 'SOME TESTS FAILED.'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
