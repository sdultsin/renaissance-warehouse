#!/usr/bin/env python3
"""Push the booking-form email-KPI snapshot from the WAREHOUSE to the Portal cache.

Reader-1 repoint (MOF-10 pipeline-Supabase retirement, 2026-07-18):
The Portal booking-form email KPI reads a Portal-local cache (kpi_workspace_daily /
kpi_campaign_ws / kpi_workspaces / kpi_ws_alias) via the portal RPC `kpi_compute`.
That cache USED to be refreshed by the portal pg_cron `kpi_workspace_sync` -> edge fn
`kpi-sync`, which read the PIPELINE Supabase RPC `kpi_portal_snapshot()`. This script
replaces that read: it rebuilds the *identical* snapshot from the warehouse (the
warehouse `raw_pipeline_*` mirrors of pipeline `campaign_daily_metrics` / `campaigns`)
and pushes it into the portal via the portal's own `kpi_ingest_snapshot(jsonb)` RPC.
Result: same cache, same user-facing compute, zero reads of the pipeline project.

Parity proven 2026-07-18: workspace_daily SENT byte-identical to the pipeline snapshot
(Jul 4-17, all 8 workspaces); opps within <=2 (cumulative-counter sync jitter);
campaign_ws ncamp set identical (3013), 4/3013 ws diffs (2 slug->desk improvements,
2 test campaigns). The KPI headline is Sent / Booked (booked from im_bookings), so
SENT parity is what preserves the displayed number.

Reads (durable, MotherDuck-backed, survives droplet death):
  WAREHOUSE_API_URL + WAREHOUSE_API_TOKEN  (read-only query API, POST {url}/query)
Writes:
  PORTAL_SUPABASE_URL (default https://pxrdmjjaxtqycuxhxmgi.supabase.co) +
  RENAISSANCE_PORTAL_SUPABASE_SERVICE_ROLE_KEY | IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY

Emits per (date,ws): sent, opps, replies (HUMAN reply_count; drives sealed-day
Human/Positive RR on the KPI tab). The intraday feed owns the live [today-K..today]
window with the same three fields; this push owns the disjoint sealed tail.

Cron (LIVE on sync-runner-1 since 2026-07-22 droplet migration; /etc/cron.d/renaissance,
offset :17/:47 to avoid colliding with the intraday feed's :06/:36 single-writer tick):
  17,47 * * * * root cd /root/renaissance-warehouse && python3 scripts/push_booking_kpi_to_portal.py >> /root/renaissance-warehouse/logs/push_booking_kpi_to_portal.log 2>&1
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# --- static config (mirrors pipeline ws_rename / kpi_workspaces / kpi_ws_alias) -----
# ws_real(): coalesce(ws_rename.real for old, old). Rename map (old display -> real).
WS_RENAME = [
    ("Koi and Destroy", "Funding 4 (Sam)"), ("Prospects Power", "Funding 3 (Leo)"),
    ("Renaissance 2", "Funding 5 (Eyver)"), ("Renaissance 4", "Funding 1 (Samuel)"),
    ("Renaissance 5", "Funding 2 (Ido)"), ("Renaissance 1", "Renaissance 1 (Instantly)"),
    ("Section 125 1", "R&D Credit"), ("Tariffs + Funding", "Tariffs"),
    ("Warm Leads", "Warm leads"), ("The Gatekeepers", "Max's workspace"),
    ("Section 125 2", "Section 125"), ("section-125-1", "Section 125"),
    ("section-125-2", "Section 125"),
]
KPI_WORKSPACES = [
    ("Funding 1 (Samuel)", 1), ("Funding 2 (Ido)", 2), ("Funding 3 (Leo)", 3),
    ("Funding 4 (Sam)", 4), ("Funding 5 (Eyver)", 5), ("Renaissance 1 (Instantly)", 6),
    ("Warm leads", 7), ("Max's workspace", 8), ("Tariffs", 9), ("Section 125", 10),
]
KPI_WS_ALIAS = [
    ("funding 1", "Funding 1 (Samuel)"), ("f1", "Funding 1 (Samuel)"),
    ("funding 2", "Funding 2 (Ido)"), ("f2", "Funding 2 (Ido)"),
    ("funding 3", "Funding 3 (Leo)"), ("f3", "Funding 3 (Leo)"),
    ("funding 4", "Funding 4 (Sam)"), ("f4", "Funding 4 (Sam)"),
    ("funding 5", "Funding 5 (Eyver)"), ("f5", "Funding 5 (Eyver)"),
    ("max ws", "Max's workspace"), ("max's workspace", "Max's workspace"),
    ("max's ws", "Max's workspace"), ("warm leads", "Warm leads"),
    ("renaissance 1", "Renaissance 1 (Instantly)"), ("r1", "Renaissance 1 (Instantly)"),
    ("funding 1 (samuel)", "Funding 1 (Samuel)"), ("funding 2 (ido)", "Funding 2 (Ido)"),
    ("funding 3 (leo)", "Funding 3 (Leo)"), ("funding 4 (sam)", "Funding 4 (Sam)"),
    ("funding 5 (eyver)", "Funding 5 (Eyver)"),
    ("renaissance 1 (instantly)", "Renaissance 1 (Instantly)"),
    ("tariffs", "Tariffs"), ("section 125", "Section 125"),
]


def _sql_lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _values(rows) -> str:
    return ",\n ".join("(" + ", ".join(_sql_lit(str(c)) for c in r) + ")" for r in rows)


# ws_rename + kpi_ws CTEs shared by both warehouse queries.
_CTES = f"""WITH ws_rename(old, real) AS (VALUES
 {_values(WS_RENAME)}
),
kpi_ws(name) AS (VALUES
 {_values([(n,) for n, _ in KPI_WORKSPACES])}
),
dims AS (
  SELECT DISTINCT ON (campaign_id) campaign_id, name, workspace_name
  FROM raw_pipeline_campaigns ORDER BY campaign_id, _loaded_at DESC
)"""

# workspace_daily [{d, ws, sent, opps}] — exact port of kpi_portal_snapshot's query.
SQL_WORKSPACE_DAILY = _CTES + """
SELECT CAST(mt.date AS VARCHAR) AS d,
       COALESCE(r.real, d.workspace_name) AS ws,
       CAST(sum(mt.sent) AS BIGINT) AS sent,
       CAST(sum(mt.opportunities) AS BIGINT) AS opps,
       -- HUMAN reply count (Instantly-native `replies`; `replies_automatic` is a
       -- SEPARATE mirror column and is NOT summed here — matches the intraday feed
       -- and the standing ruling that reply_count is human, auto tracked separately).
       -- Sealed-day Human RR = replies/sent and Positive RR = opps/replies derive
       -- from this tab-side, so older ranges stop showing blank RR.
       CAST(sum(mt.replies) AS BIGINT) AS replies
FROM raw_pipeline_campaign_daily_metrics mt
JOIN dims d ON d.campaign_id = mt.campaign_id
LEFT JOIN ws_rename r ON r.old = d.workspace_name
WHERE COALESCE(r.real, d.workspace_name) IN (SELECT name FROM kpi_ws)
  -- Seal ONLY days older than the intraday live settling window. The live feed
  -- (push_booking_kpi_intraday.py) owns [today-K..today] direct from Instantly; this
  -- warehouse push owns [..today-(K+1)] — disjoint (date,ws) keyspace, both upsert-only.
  AND CAST(mt.date AS DATE) <= DATE '__SEAL_FLOOR__'
GROUP BY 1, 2
-- Publish only real (positive) sent days. kpi_ingest_snapshot is UPSERT-only, so
-- never emitting a 0 means a day is never regressed to 0 in the cache. In
-- particular TODAY (raw_pipeline_campaign_daily_metrics mirrors nightly, so the
-- current day reads 0 until the next load) keeps its last-known cached value
-- instead of flipping to 0 the moment we cut over. Complete past days always
-- carry sent>0 and refresh exactly. (Intraday freshness for the current day needs
-- an intraday warehouse metrics feed — flagged separately; the pipeline path had
-- the same gap post-mof10.)
HAVING sum(mt.sent) > 0
"""

# campaign_ws [{ncamp, ws}] — exact port of v_campaign_ws with ws_real applied.
SQL_CAMPAIGN_WS = _CTES + """,
sent AS (SELECT campaign_id, sum(sent) s FROM raw_pipeline_campaign_daily_metrics GROUP BY 1),
norm AS (
  SELECT nullif(trim(regexp_replace(
           regexp_replace(
             regexp_replace(
               regexp_replace(lower(coalesce(c.name,'')), '^\\s*bounced\\s+', ''),
               '\\s*\\(copy\\)\\s*', ' ', 'g'),
             '\\s+rv\\s*$', ''),
           '\\s+', ' ', 'g')), '') AS ncamp,
         c.workspace_name AS ws_raw,
         COALESCE(s.s, 0) AS s
  FROM dims c
  LEFT JOIN sent s ON s.campaign_id = c.campaign_id
  WHERE c.name IS NOT NULL AND c.workspace_name IS NOT NULL
),
pick AS (
  SELECT DISTINCT ON (ncamp) ncamp, ws_raw
  FROM norm WHERE ncamp IS NOT NULL
  ORDER BY ncamp, s DESC
)
SELECT p.ncamp AS ncamp, COALESCE(r.real, p.ws_raw) AS ws
FROM pick p LEFT JOIN ws_rename r ON r.old = p.ws_raw
"""


def _load_repo_env():
    """Populate os.environ from the repo .env for any missing keys. Robust to
    comment/quote lines that break bash `source` (the warehouse .env has some).
    Looks next to the repo root (../.env from scripts/), overridable via ENV_FILE."""
    path = os.environ.get("ENV_FILE") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def _env(*names, default=None):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


def wh_query(sql: str) -> list[list]:
    url = _env("WAREHOUSE_API_URL")
    tok = _env("WAREHOUSE_API_TOKEN")
    if not url or not tok:
        raise SystemExit("WAREHOUSE_API_URL / WAREHOUSE_API_TOKEN not set")
    req = urllib.request.Request(
        url.rstrip("/") + "/query",
        data=json.dumps({"sql": sql}).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        out = json.load(r)
    if "rows" not in out:
        raise SystemExit(f"warehouse query error: {out}")
    return out["rows"]


def portal_ingest(snapshot: dict) -> dict:
    base = _env("PORTAL_SUPABASE_URL", "IM_BOOKINGS_SUPABASE_URL",
                default="https://pxrdmjjaxtqycuxhxmgi.supabase.co")
    key = _env("RENAISSANCE_PORTAL_SUPABASE_SERVICE_ROLE_KEY",
               "IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        raise SystemExit("portal service-role key not set")
    req = urllib.request.Request(
        base.rstrip("/") + "/rest/v1/rpc/kpi_ingest_snapshot",
        data=json.dumps({"p": snapshot}).encode(),
        method="POST",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def main() -> int:
    _load_repo_env()
    dry = "--dry" in sys.argv
    # Seal-floor: own ONLY completed days older than the intraday live settling window
    # [today-K..today] (push_booking_kpi_intraday.py). K MUST match that feed's value.
    K = int(_env("KPI_INTRADAY_SETTLE_DAYS", default="5"))
    seal_floor = (datetime.now(ZoneInfo("America/New_York")).date()
                  - timedelta(days=K + 1)).isoformat()
    wd = wh_query(SQL_WORKSPACE_DAILY.replace("__SEAL_FLOOR__", seal_floor))
    cw = wh_query(SQL_CAMPAIGN_WS)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_daily": [{"d": d, "ws": ws, "sent": sent, "opps": opps,
                             "replies": replies}
                            for d, ws, sent, opps, replies in wd],
        "campaign_ws": [{"ncamp": ncamp, "ws": ws} for ncamp, ws in cw],
        "workspaces": [{"name": n, "sort_order": so} for n, so in KPI_WORKSPACES],
        "ws_alias": [{"label": lbl, "ws": ws} for lbl, ws in KPI_WS_ALIAS],
    }
    if dry:
        # Per-ws sent/replies for a couple of sealed historical dates, to eyeball parity.
        for probe in ("2026-07-14", "2026-07-15", "2026-07-16"):
            agg = {}
            for row in snapshot["workspace_daily"]:
                if row["d"] == probe:
                    s, rp = agg.get(row["ws"], (0, 0))
                    agg[row["ws"]] = (s + row["sent"], rp + row.get("replies", 0))
            print(f"[dry] sent / replies by ws for {probe}:")
            for ws in sorted(agg):
                print(f"        {ws:28s} sent={agg[ws][0]:>9} replies={agg[ws][1]:>6}")
        print(f"[dry] workspace_daily rows={len(snapshot['workspace_daily'])} "
              f"campaign_ws={len(snapshot['campaign_ws'])} (NOT written)")
        return 0
    res = portal_ingest(snapshot)
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] pushed booking-kpi snapshot -> portal: ingest={res} "
          f"(sent workspace_daily={len(snapshot['workspace_daily'])}, "
          f"campaign_ws={len(snapshot['campaign_ws'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
