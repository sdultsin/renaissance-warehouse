#!/usr/bin/env python3
"""parity_g4.py — G4 parity vs Instantly `?lead=` ground truth (QA-CHECKLIST G4).

For a stratified sample of replied leads (per-workspace floor >=38, >=300 total), compare
the warehouse's stored messages for each lead against a LIVE `?lead=` pull:

  * per-lead message_id Jaccard >= 0.99 for >= 99% of sampled leads
    (deleted-at-source ids removed from BOTH sets and enumerated separately, NOT a mismatch);
  * direction matches 100% on the intersecting ids;
  * body_text non-empty for >= 99% of WH rows whose live counterpart is non-deleted.

LIVE side reads per-ORG keys (King MCP clients.api_key_new_2026_04_13 is current; the
.env.instantly key store is STALE), curl User-Agent (Instantly fingerprint-blocks python),
and paginates `next_starting_after`. WH side reads the query API (POST /query).

Writes:
  --out-summary   {n_leads, n_per_ws, jaccard_pass_pct, body_nonempty_pct, direction_match_pct}
  --out-mismatch  CSV: lead_email,workspace,missing_in_wh[],extra_in_wh[],deleted_at_source[],empty_body_ids[]

Stdlib-only (urllib) so it runs on any laptop with no extra deps.

ENV:
  BASE                  default https://renaissance-droplet.tailae5c80.ts.net
  WAREHOUSE_API_TOKEN   warehouse read token
  INSTANTLY_KEY_<SLUG>  per-workspace Instantly key (slug upper, '-'->'_'); maps the
                        sample's workspace_id (slug) back to its live key.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request

BASE = os.environ.get("BASE", "https://renaissance-droplet.tailae5c80.ts.net").rstrip("/")
WAREHOUSE_API_TOKEN = os.environ.get("WAREHOUSE_API_TOKEN", "")
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"
_UA = "curl/8.4.0"  # Instantly blocks python UAs (matches sources/instantly.py)


# ── warehouse read ──────────────────────────────────────────────────────────────
def wh_query(sql: str) -> list[list]:
    req = urllib.request.Request(
        BASE + "/query",
        data=json.dumps({"sql": sql}).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {WAREHOUSE_API_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
        payload = json.loads(r.read().decode())
    # The query API returns {"rows": [[...], ...]} (or {"data": ...}); tolerate both.
    return payload.get("rows") or payload.get("data") or []


def wh_messages_for_lead(lead_email: str, workspace_id: str) -> dict[str, dict]:
    """{message_id: {direction, body_len}} from core.email_message for one lead+ws."""
    safe = lead_email.replace("'", "''")
    ws = workspace_id.replace("'", "''")
    rows = wh_query(
        "SELECT message_id, direction, length(coalesce(body_text,'')) "
        "FROM core.email_message "
        f"WHERE lead_email='{safe}' AND workspace_id='{ws}'"
    )
    out: dict[str, dict] = {}
    for r in rows:
        out[str(r[0])] = {"direction": r[1], "body_len": int(r[2] or 0)}
    return out


# ── live Instantly read (per-org key, curl UA, paginated) ───────────────────────
def _key_env_for_slug(slug: str) -> str:
    return "INSTANTLY_KEY_" + slug.upper().replace("-", "_")


def live_messages_for_lead(lead_email: str, api_key: str) -> dict[str, dict]:
    """{item_id: {ue_type, direction, body_len, deleted}} from the live ?lead= pull."""
    out: dict[str, dict] = {}
    cursor = None
    pages = 0
    while True:
        params = f"lead={urllib.parse.quote(lead_email.lower().strip())}&limit=100"
        if cursor:
            params += f"&starting_after={urllib.parse.quote(cursor)}"
        req = urllib.request.Request(
            f"{INSTANTLY_BASE}/emails?{params}",
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": _UA,
                     "Accept": "application/json"},
        )
        ctx = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
                payload = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                time.sleep(65 if e.code == 429 else 5)
                continue
            raise
        for it in payload.get("items") or []:
            mid = it.get("id")
            if not mid:
                continue
            ue = it.get("ue_type")
            try:
                ue = int(ue) if ue is not None else None
            except (TypeError, ValueError):
                ue = None
            body = it.get("body") or {}
            blen = len((body.get("text") or body.get("html") or "")) if isinstance(body, dict) else 0
            out[str(mid)] = {
                "ue_type": ue,
                "direction": "inbound" if ue == 2 else "outbound",
                "body_len": blen,
                "deleted": bool(it.get("deleted")),
            }
        cursor = payload.get("next_starting_after")
        pages += 1
        if not cursor or pages > 1000:
            break
        time.sleep(0.05)
    return out


# ── compare ─────────────────────────────────────────────────────────────────────
def compare(sample_rows: list[tuple[str, str]]) -> tuple[dict, list[dict]]:
    """sample_rows = [(lead_email, workspace_id), ...]. Returns (summary, mismatches)."""
    n_per_ws: dict[str, int] = {}
    jaccard_pass = 0
    # ROW-grain body coverage (QA-CHECKLIST G4: "body_text non-empty for >=99% of WH ROWS whose
    # live counterpart is non-deleted"). Accumulate over ROWS across all leads — NOT per-lead
    # (a lead with 1 empty body out of 50 must not fail the whole lead; that diverges from the
    # row-level >=99% and is not monotonically equivalent to it).
    body_nonempty_rows = 0
    body_total_rows = 0
    direction_mismatch_total = 0
    direction_compared_total = 0
    mismatches: list[dict] = []

    for lead_email, workspace_id in sample_rows:
        n_per_ws[workspace_id] = n_per_ws.get(workspace_id, 0) + 1
        api_key = os.environ.get(_key_env_for_slug(workspace_id))
        if not api_key:
            mismatches.append({
                "lead_email": lead_email, "workspace": workspace_id,
                "missing_in_wh": [], "extra_in_wh": [], "deleted_at_source": [],
                "empty_body_ids": [], "note": f"NO LIVE KEY env {_key_env_for_slug(workspace_id)}",
            })
            continue

        wh = wh_messages_for_lead(lead_email, workspace_id)
        live = live_messages_for_lead(lead_email, api_key)

        deleted = {mid for mid, v in live.items() if v.get("deleted")}
        live_ids = set(live) - deleted
        wh_ids = set(wh) - deleted  # a deleted-at-source id present in WH is not a mismatch

        inter = wh_ids & live_ids
        union = wh_ids | live_ids
        jacc = (len(inter) / len(union)) if union else 1.0
        if jacc >= 0.99:
            jaccard_pass += 1

        # direction 100% on the intersect
        dir_mismatch_ids = [
            mid for mid in inter
            if wh[mid]["direction"] != live[mid]["direction"]
        ]
        direction_compared_total += len(inter)
        direction_mismatch_total += len(dir_mismatch_ids)

        # body non-empty for WH ROWS whose live counterpart is non-deleted (row-grain — G4).
        body_rows = [mid for mid in (set(wh) & live_ids)]
        empty_body_ids = [mid for mid in body_rows if wh[mid]["body_len"] == 0]
        body_total_rows += len(body_rows)
        body_nonempty_rows += len(body_rows) - len(empty_body_ids)

        missing_in_wh = sorted(live_ids - wh_ids)
        extra_in_wh = sorted(wh_ids - live_ids)
        if jacc < 0.99 or dir_mismatch_ids or empty_body_ids or missing_in_wh or extra_in_wh:
            mismatches.append({
                "lead_email": lead_email, "workspace": workspace_id,
                "missing_in_wh": missing_in_wh, "extra_in_wh": extra_in_wh,
                "deleted_at_source": sorted(deleted), "empty_body_ids": empty_body_ids,
                "direction_mismatch_ids": dir_mismatch_ids, "jaccard": round(jacc, 4),
            })

    n = len(sample_rows)
    summary = {
        "n_leads": n,
        "n_per_ws": n_per_ws,
        "jaccard_pass_pct": round(jaccard_pass / n, 4) if n else None,
        # ROW-grain body coverage (G4): non-empty WH rows / total WH rows whose live counterpart
        # is non-deleted, across ALL leads.
        "body_nonempty_pct": round(body_nonempty_rows / body_total_rows, 4) if body_total_rows else None,
        "body_total_rows": body_total_rows,
        "body_nonempty_rows": body_nonempty_rows,
        "direction_match_pct": (
            round(1 - direction_mismatch_total / direction_compared_total, 4)
            if direction_compared_total else 1.0
        ),
    }
    return summary, mismatches


def _load_sample(path: str) -> list[tuple[str, str]]:
    """CSV of replied leads. Accepts header lead_email[,workspace_id] or a bare email column.

    If workspace_id is absent, resolve it from the warehouse (one ws per lead is the
    common case; if a lead spans workspaces, each (lead,ws) is emitted)."""
    rows: list[tuple[str, str]] = []
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader, None)
        has_ws = header and len(header) >= 2 and "workspace" in (header[1] or "").lower()
        lead_idx = 0
        # if the first line wasn't a header, treat it as data
        if header and "@" in (header[0] or ""):
            rows.append((header[0].strip().lower(), header[1].strip() if has_ws and len(header) > 1 else ""))
        for r in reader:
            if not r:
                continue
            lead = r[lead_idx].strip().lower()
            ws = r[1].strip() if has_ws and len(r) > 1 else ""
            if lead:
                rows.append((lead, ws))
    # resolve missing workspaces from the warehouse
    resolved: list[tuple[str, str]] = []
    for lead, ws in rows:
        if ws:
            resolved.append((lead, ws))
            continue
        safe = lead.replace("'", "''")
        for wsrow in wh_query(
            f"SELECT DISTINCT workspace_id FROM core.email_message WHERE lead_email='{safe}'"
        ):
            resolved.append((lead, str(wsrow[0])))
    return resolved


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", required=True, help="CSV of sampled replied leads")
    ap.add_argument("--out-mismatch", default="g4_mismatches.csv")
    ap.add_argument("--out-summary", default="g4_summary.json")
    args = ap.parse_args(argv)

    if not WAREHOUSE_API_TOKEN:
        print("WAREHOUSE_API_TOKEN not set", file=sys.stderr)
        return 2

    sample = _load_sample(args.sample)
    if not sample:
        print("empty sample", file=sys.stderr)
        return 2

    # ── SAMPLING-FLOOR gate (QA-CHECKLIST G4: "Sample >=300 total WITH per-workspace floor >=38,
    # or all replied leads if a ws has fewer"). A deficient sample (e.g. 50 leads, one ws with 5)
    # must NOT exit 0 / mark G4 green just because the parity RATIOS pass — the floor is part of
    # the gate. We honor the documented exception: a ws is allowed below 38 ONLY if the sample
    # already covers ALL of that ws's replied (inbound) leads. ──
    n_per_ws_pre: dict[str, int] = {}
    for _lead, _ws in sample:
        n_per_ws_pre[_ws] = n_per_ws_pre.get(_ws, 0) + 1
    floor_failures: list[str] = []
    if len(sample) < 300:
        floor_failures.append(f"total sample {len(sample)} < 300")
    for ws, cnt in n_per_ws_pre.items():
        if cnt >= 38:
            continue
        # below 38 — allowed ONLY if we've sampled ALL of this ws's replied leads.
        ws_total = None
        try:
            safe_ws = ws.replace("'", "''")
            rows = wh_query(
                "SELECT count(DISTINCT lead_email) FROM core.email_message "
                f"WHERE workspace_id='{safe_ws}' AND direction='inbound'"
            )
            ws_total = int(rows[0][0]) if rows and rows[0] else None
        except Exception as exc:  # noqa: BLE001
            print(f"warn: could not size ws {ws} for floor exception ({exc})", file=sys.stderr)
        if ws_total is None or cnt < ws_total:
            floor_failures.append(
                f"ws {ws}: sampled {cnt} < 38 and < its {ws_total} replied leads (floor breach)"
            )
    if floor_failures:
        print("G4 SAMPLING FLOOR FAILED:", file=sys.stderr)
        for fm in floor_failures:
            print(f"  - {fm}", file=sys.stderr)
        # still emit the summary so the operator sees coverage, but FAIL the gate.
        with open(args.out_summary, "w") as f:
            json.dump({"floor_failures": floor_failures, "n_leads": len(sample),
                       "n_per_ws": n_per_ws_pre}, f, indent=2)
        return 1

    summary, mismatches = compare(sample)

    with open(args.out_summary, "w") as f:
        json.dump(summary, f, indent=2)
    with open(args.out_mismatch, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["lead_email", "workspace", "missing_in_wh", "extra_in_wh",
                    "deleted_at_source", "empty_body_ids", "direction_mismatch_ids", "jaccard"])
        for m in mismatches:
            w.writerow([
                m.get("lead_email"), m.get("workspace"),
                ";".join(m.get("missing_in_wh", [])), ";".join(m.get("extra_in_wh", [])),
                ";".join(m.get("deleted_at_source", [])), ";".join(m.get("empty_body_ids", [])),
                ";".join(m.get("direction_mismatch_ids", [])), m.get("jaccard", ""),
            ])

    print(json.dumps(summary, indent=2))
    # exit nonzero if any threshold misses, so a CI/harness can gate on it.
    ok = (
        (summary["jaccard_pass_pct"] or 0) >= 0.99
        and (summary["body_nonempty_pct"] or 0) >= 0.99
        and (summary["direction_match_pct"] or 0) >= 1.0
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
