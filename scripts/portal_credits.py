"""Pull Instantly Lead Credits for the portal (the ONE portal datum with no warehouse table).

The portal's "Accounts ▸ Lead Credits" tab reads a Supabase `instantly_credits`
table (cols: date, workspace, used, lim, remaining, pct_used). There is NO warehouse
equivalent (Phase-0 §3 GAP #1 / inventory §9 GAP #1). The closest live, read-only
source is the Instantly billing API:

    GET https://api.instantly.ai/api/v2/workspace-billing/plan-details
    Authorization: Bearer <per-workspace API key>

Scope is READ-ONLY (`workspace_billing:read`).

IMPORTANT (verified live 2026-06-14): the `subscriptions.credits` pool
(total_credits / available_credits) is **org-wide / shared** — every workspace key
returns the SAME `available_credits`, and only the org that holds the Hyper-Credits
subscription has a real `total_credits` (the rest report the 100 base). So the credit
pool is NOT per-workspace. The per-WORKSPACE quota that the portal's Lead-Credits tab
actually tracks is the OUTREACH LEAD-LIST limit:

    used      = subscriptions.outreach.current_lead_count   (this workspace's leads loaded)
    lim       = subscriptions.outreach.total_lead_limit     (this workspace's lead cap)
    remaining = lim - used
    pct_used  = round(100 * used / lim)

These are genuinely per-workspace and map 1:1 onto the portal's
Used / Limit / Remaining / %Used columns. The org-wide CREDIT pool is emitted ONCE in
`credit_pool` (not per row) so the portal can show it as a single org figure if wanted.

Keys live in `.env.instantly` as `INSTANTLY_KEY_<NAME>` (the raw base64 value IS the
bearer token). Each key is a separate Instantly *organization/workspace*; we label the
row by the `organization_name` the API returns (drift-proof — survives the workspace
renames; see reference_instantly_workspace_renames_20260530).

Emits a JSON object on stdout that scripts/portal_data.py reads and merges into
window.PORTAL_DATA.credits. SAFE / read-only — no warehouse, no DB, no writes.

Usage (Mac or droplet — needs only network + .env.instantly):
    python scripts/portal_credits.py > portal_credits.json

CONFIG (env):
    INSTANTLY_ENV_FILE   path to the env file with INSTANTLY_KEY_* (default: search
                         ./.env.instantly then ~/Documents/.../Renaissance/.env.instantly)
    INSTANTLY_API_BASE   default https://api.instantly.ai/api/v2
    PORTAL_CREDITS_TIMEOUT  per-request timeout seconds (default 25)
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date

API_BASE = os.environ.get("INSTANTLY_API_BASE", "https://api.instantly.ai/api/v2")
TIMEOUT = float(os.environ.get("PORTAL_CREDITS_TIMEOUT", "25"))

_DEFAULT_ENV_PATHS = [
    os.environ.get("INSTANTLY_ENV_FILE", ""),
    ".env.instantly",
    os.path.expanduser("~/Documents/Claude Code/Renaissance/.env.instantly"),
]


def load_keys() -> dict[str, str]:
    """Return {ENV_VAR_NAME: bearer_token} for every INSTANTLY_KEY_* in the env file.

    Falls back to keys already present in os.environ (so a droplet that exports them
    works without the file)."""
    keys: dict[str, str] = {}
    for path in _DEFAULT_ENV_PATHS:
        if path and os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("INSTANTLY_KEY_") and "=" in line:
                        k, v = line.split("=", 1)
                        keys[k.strip()] = v.strip().strip('"').strip("'")
            break
    # Merge any exported in the environment (env wins).
    for k, v in os.environ.items():
        if k.startswith("INSTANTLY_KEY_") and v:
            keys[k] = v
    # SAM_TEST / PERSONAL are not production funding workspaces — skip noise.
    for skip in ("INSTANTLY_KEY_SAM_TEST",):
        keys.pop(skip, None)
    return keys


def fetch_plan(token: str) -> dict:
    req = urllib.request.Request(
        f"{API_BASE}/workspace-billing/plan-details",
        headers={"Authorization": f"Bearer {token}", "User-Agent": "renaissance-portal-credits/1.0"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return json.loads(resp.read().decode())


def row_from_plan(env_name: str, plan: dict) -> dict:
    subs = plan.get("subscriptions") or {}
    outreach = subs.get("outreach") or {}
    ws = plan.get("organization_name") or env_name.replace("INSTANTLY_KEY_", "").title()

    # Per-WORKSPACE lead-list quota — the figures the portal's Lead-Credits tab tracks.
    lim = outreach.get("total_lead_limit")
    used = outreach.get("current_lead_count")
    remaining = (lim - used) if (lim is not None and used is not None) else None
    pct_used = round(100.0 * used / lim) if (used is not None and lim) else None

    return {
        "workspace": ws,
        "env_key": env_name,
        "used": used,           # leads loaded in this workspace
        "lim": lim,             # this workspace's lead cap
        "remaining": remaining,
        "pct_used": pct_used,
        "plan": outreach.get("plan_name"),
    }


def credit_pool_from_plan(plan: dict) -> dict | None:
    """The org-wide credit pool (shared across workspaces) — emitted once, not per row."""
    credits = (plan.get("subscriptions") or {}).get("credits") or {}
    total = credits.get("total_credits")
    avail = credits.get("available_credits")
    if total is None and avail is None:
        return None
    used = (total - avail) if (total is not None and avail is not None) else None
    return {
        "organization": plan.get("organization_name"),
        "plan": credits.get("plan_name"),
        "total_credits": total,
        "available_credits": avail,
        "used_credits": used,
        "pct_used": round(100.0 * used / total) if (used is not None and total) else None,
    }


def main() -> int:
    keys = load_keys()
    by_ws: dict[str, dict] = {}   # dedup by workspace/org name (two keys -> same org)
    pools: list[dict] = []
    errors: list[dict] = []
    if not keys:
        print("[portal_credits] WARN: no INSTANTLY_KEY_* found in env/file", file=sys.stderr)
    for env_name, token in sorted(keys.items()):
        try:
            plan = fetch_plan(token)
            row = row_from_plan(env_name, plan)
            # Dedup: keep the row with the larger lead cap if a workspace has 2 keys.
            prev = by_ws.get(row["workspace"])
            if not prev or (row.get("lim") or 0) >= (prev.get("lim") or 0):
                by_ws[row["workspace"]] = row
            pool = credit_pool_from_plan(plan)
            if pool:
                pools.append(pool)
        except urllib.error.HTTPError as e:  # noqa: PERF203
            errors.append({"env_key": env_name, "error": f"HTTP {e.code}"})
            print(f"[portal_credits] WARN {env_name}: HTTP {e.code}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001
            errors.append({"env_key": env_name, "error": str(e)})
            print(f"[portal_credits] WARN {env_name}: {e}", file=sys.stderr)
        time.sleep(0.3)  # be gentle on the API

    rows = sorted(by_ws.values(), key=lambda r: (r.get("lim") or 0), reverse=True)
    # The real org credit pool = the one with the largest total_credits (the Hyper sub).
    credit_pool = max(
        (p for p in pools if (p.get("total_credits") or 0) > 1000),
        key=lambda p: p.get("total_credits") or 0,
        default=None,
    )
    out = {
        "generated_at": date.today().isoformat(),
        "date": date.today().isoformat(),  # matches instantly_credits.date semantics
        "source": "Instantly billing API plan-details (read-only) via scripts/portal_credits.py",
        "note": "rows = per-workspace OUTREACH lead-list quota (used/lim/remaining/pct_used). "
                "credit_pool = org-wide shared credit balance (single figure).",
        "rows": rows,
        "totals": {
            "used": sum(r["used"] for r in rows if r.get("used") is not None),
            "lim": sum(r["lim"] for r in rows if r.get("lim") is not None),
            "remaining": sum(r["remaining"] for r in rows if r.get("remaining") is not None),
        },
        "credit_pool": credit_pool,
        "errors": errors,
    }
    json.dump(out, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
