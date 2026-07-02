"""Full both-direction Instantly email-thread sync (FINALIZED-SPEC 2026-06-28).

For EVERY replied lead, pull that lead's ENTIRE thread via GET /api/v2/emails?lead=
(cold sends ue_type=1, prospect replies ue_type=2, our/IM replies ue_type=3 — the
ACTUAL rendered emails) and UPSERT into raw_instantly_email_message (PK = the
per-email Instantly id). core.email_message (view) + core.email_thread (rollup view)
make a "thread" a trivial group-by. See sql/ddl/1037_email_message.sql.

TWO-PHASE, LOCK-DISCIPLINED (R8 — blocking):
  Phase A (NO writer lock):  enumerate orgs -> discover replied leads -> pull each
                             lead's full history into a JSONL staging file. NO db
                             writes, NO httpx while any flock is held.
  Phase B (writer flock held): bulk-load staging -> TEMP table -> ONE upsert
                             transaction -> CHECKPOINT. No per-row execute, no httpx.

Mirrors the proven shape of scripts/backfill_im_outbound_bodies.py (append-only JSONL
checkpoint, fsync batches, resume-on-restart, manifest for rollback) but at the
whole-thread grain and with the §0 conflict resolutions folded in:

  R2  dedup by organization_id at run start (first key per org wins; dead keys with
      org_id=None skipped); STORE the canonical core.workspace SLUG in workspace_id,
      never the org UUID.
  R7  watermark is PER WORKSPACE (max(message_at) WHERE workspace_id=ws), not global.
  R8  network pull outside the lock; bulk upsert under it.
  R9  direction from ue_type ALONE.
  R1/R1a/R5/R6  thread_key=campaign_id (anchor-fallback), step_path raw string,
      PK from item['id'].

RUN MODES:
  * register() wires a flag-gated nightly hook (WAREHOUSE_PULL_THREADS=1) in the
    `instantly` phase AFTER instantly_replies. It runs the full two-phase flow with
    its OWN connection management so the network pull never holds ctx.db's lock.
  * CLI: `python -m entities.email_thread_sync fetch|apply|run` for backfills,
    matching the backfill script's fetch/apply split so the heavy pull can run
    un-locked and the apply runs under scripts/with_warehouse_lock.sh.

Flag-gated: does nothing unless WAREHOUSE_PULL_THREADS=1.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from core import db as db_module
from core.config import DB_PATH, REPO_ROOT
from core.credentials import Credentials, load_credentials
from core.email_body_clean import clean_body, clean_subject
from core.registry import Registry
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.email_thread_sync")

_OVERLAP = timedelta(days=2)
# Per-org bounded concurrency WITHIN a workspace; SERIAL across orgs (feedback_instantly
# _list_accounts_serial_only). 4-8 workers per the spec §4.C.
_LEAD_WORKERS = int(os.environ.get("WAREHOUSE_THREADS_LEAD_WORKERS", "6"))

# Default staging + manifest locations (next to the live DB on the droplet; overridable).
_STAGE_DEFAULT = os.environ.get(
    "WAREHOUSE_THREADS_STAGE",
    str(Path(DB_PATH).parent / "email_thread_stage.jsonl"),
)

# Atom columns, in DDL order (sql/ddl/1037_email_message.sql §3a), minus PK handling in SET.
_COLS = [
    "message_id", "rfc_message_id", "thread_id", "thread_key", "lead_anchor_key",
    "workspace_id", "organization_id", "campaign_id", "lead_email", "direction",
    "ue_type", "step_path", "subject", "body_text", "body_html", "from_email",
    "to_emails", "eaccount", "message_at", "source", "api_response_raw",
    "_loaded_at", "_run_id",
]

# Columns that are IMMUTABLE for a given message_id (R6/E): never flip on re-pull.
# (We assert-equal these by simply NOT updating them in the ON CONFLICT SET clause.)
_IMMUTABLE = {"direction", "ue_type", "step_path", "lead_email", "workspace_id"}

# Business-payload columns (EXCLUDES _loaded_at/_run_id/api_response_raw) — the exact
# set G2 hashes (QA-CHECKLIST G2 SQL). A no-op re-pull must leave this hash unchanged, so
# we compute messages_upserted_changed = count of staged ids whose pre-row payload hash
# differs from the post-row payload hash (a genuine no-op re-run reports 0 — G2 evidence).
_PAYLOAD_COLS = [
    "message_id", "thread_id", "thread_key", "lead_anchor_key", "workspace_id",
    "campaign_id", "lead_email", "direction", "ue_type", "step_path",
    "subject", "body_text", "body_html", "from_email", "to_emails", "eaccount",
    "message_at",
]


def _payload_hash_sql(alias: str) -> str:
    """md5 over the business payload of one row (matches QA-CHECKLIST G2's concat_ws hash).
    `alias` is the table/temp alias the columns are read from."""
    parts = []
    for c in _PAYLOAD_COLS:
        col = f"{alias}.{c}"
        if c == "campaign_id":
            parts.append(f"coalesce({col},'')")
        elif c in ("step_path", "subject", "body_text", "body_html", "to_emails", "eaccount"):
            parts.append(f"coalesce({col},'')")
        elif c == "ue_type":
            parts.append(f"coalesce({col}::varchar,'')")
        elif c == "message_at":
            parts.append(f"coalesce({col}::varchar,'')")
        else:
            parts.append(f"coalesce({col}::varchar,'')")
    return "md5(concat_ws('|', " + ", ".join(parts) + "))"


# Explicit staging column TYPES for read_json (CRITICAL): read_json_auto INFERS a column as
# JSON when every SAMPLED value is null (e.g. an all-null rfc_message_id / body_html page),
# and a JSON-typed value then fails the VARCHAR COALESCE/NULLIF + INSERT ("Malformed JSON …
# input length is 0"). We pin every column's type so inference can never misfire. fetched_at is
# the staging-only dedup key (not an atom column).
_STAGE_COL_TYPES = {
    "message_id": "VARCHAR", "rfc_message_id": "VARCHAR", "thread_id": "VARCHAR",
    "thread_key": "VARCHAR", "lead_anchor_key": "VARCHAR", "workspace_id": "VARCHAR",
    "organization_id": "VARCHAR", "campaign_id": "VARCHAR", "lead_email": "VARCHAR",
    "direction": "VARCHAR", "ue_type": "INTEGER", "step_path": "VARCHAR", "subject": "VARCHAR",
    "body_text": "VARCHAR", "body_html": "VARCHAR", "from_email": "VARCHAR", "to_emails": "VARCHAR",
    "eaccount": "VARCHAR", "message_at": "TIMESTAMPTZ", "source": "VARCHAR",
    "api_response_raw": "VARCHAR", "fetched_at": "TIMESTAMPTZ",
}


def _read_stage_sql(stage_path: str) -> str:
    """read_json with EXPLICIT columns so no field is mis-inferred as JSON (all-null pages)."""
    cols = ", ".join(f"'{c}': '{t}'" for c, t in _STAGE_COL_TYPES.items())
    return (
        f"read_json('{stage_path}', format='newline_delimited', "
        f"columns={{{cols}}}, ignore_errors=false)"
    )


def _manifest_path(run_id: str) -> Path:
    """Per-run manifest path G7 greps verbatim: core/email_thread_manifest_<run_id>.txt.
    (REPO_ROOT/core so the QA-CHECKLIST `wc -l core/email_thread_manifest_<run_id>.txt`
    target resolves; NO extra timestamp suffix — the run_id alone keys the run.)"""
    d = REPO_ROOT / "core"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        d = Path(DB_PATH).parent
    return d / f"email_thread_manifest_{run_id}.txt"


def register(registry: Registry) -> None:
    """INTENTIONALLY a no-op for the network pull (G8/R8).

    This entity must NOT run inside the orchestrator. The orchestrator opens ONE read-write
    connection at run start (core/orchestrator.py) which takes the single-writer flock and
    holds it for the ENTIRE run; any in-phase hook therefore executes its network pull UNDER
    the held writer lock, starving every other writer for the full network duration — exactly
    the multi-network-I/O-under-lock starvation R8 (BLOCKING) forbids and the anti-pattern the
    spec cites in instantly_replies.py.

    The two-phase lock split (Phase A pull un-locked, Phase B apply under the flock) only
    delivers its R8 benefit when the two phases run as SEPARATE processes. So the nightly path
    is re-homed to POST-orchestrator steps in scripts/nightly.sh (run AFTER the orchestrator
    releases the writer lock), matching the existing lock-free post-orchestrator steps:
        WAREHOUSE_PULL_THREADS=1 python -m entities.email_thread_sync fetch     # un-locked pull
        scripts/with_warehouse_lock.sh python -m entities.email_thread_sync apply --stage <run_stage>

    `register()` deliberately wires NOTHING so the orchestrator never holds the lock during the
    pull. (Kept as a registered module so discovery does not error; the body is a no-op.)
    """
    logger.debug(
        "email_thread_sync: register() is a no-op — the network pull runs POST-orchestrator "
        "(scripts/nightly.sh: `fetch` un-locked then `apply` under with_warehouse_lock.sh) so it "
        "is never executed under the orchestrator's held writer flock (R8/G8)."
    )
    return None


# ── org enumeration + dedup (FINALIZED-SPEC §4.A / R2) ──────────────────────────
def enumerate_orgs(
    creds: Credentials, read_conn: duckdb.DuckDBPyConnection
) -> tuple[dict[str, tuple[str, str, str | None]], dict]:
    """Return ({ws_uuid: (canonical_slug, api_key, organization_id)}, diagnostics).

    Dedup by the WORKSPACE id (`w['id']` — the workspace UUID), first live key per
    DISTINCT workspace wins; dead keys (no `id`) skipped. The two canonical entities
    (entities/instantly_replies.py, entities/workspace.py) BOTH dedup on `w.get('id')`
    and read `organization_id` only as a separate provenance field — this mirrors them.

    Why NOT dedup by organization_id: /workspaces/current returns BOTH `id` (the
    workspace UUID) and `organization_id` (the PARENT org) as DISTINCT values, and two
    DISTINCT workspaces can share ONE org. Deduping by organization_id would collapse
    those two workspaces to one and SILENTLY DROP the second workspace's replied-lead
    threads (violates "all workspaces" / DoD-2 / G6). Deduping by the workspace UUID
    keeps every distinct workspace; the only true duplicate (the same workspace exposed
    under two env slugs) still collapses to one.

    The canonical slug is resolved via core.workspace (keyed on the Instantly workspace
    UUID `id`) so the STORED workspace_id is the joinable slug, never a UUID. Falls back
    to the env slug if core.workspace lacks the id. The REAL organization_id is retained
    in the tuple as provenance AND so the local discovery query can match
    raw_instantly_email.workspace_id (DDL 36 populated it with `organization_id or ws.id`).
    """
    # core.workspace.workspace_id == the Instantly workspace UUID (the `id` from
    # /workspaces/current); slug is the canonical join key. Build id -> slug.
    id_to_slug: dict[str, str] = {}
    try:
        for wsid, slug in read_conn.execute(
            "SELECT workspace_id, slug FROM core.workspace"
        ).fetchall():
            if wsid and slug:
                id_to_slug[str(wsid)] = str(slug)
    except duckdb.Error:
        logger.warning("core.workspace not readable — falling back to env slugs only")

    keys = creds.instantly_workspace_keys()
    # Optional ALLOWLIST (handoff 2026-06-28: pin the canonical reply-thread source to EXACTLY the 9
    # verified workspaces — drop the 12 dead keys, "The Eagles" (free plan, dormant), and the PREIPO
    # duplicate). WAREHOUSE_THREADS_ORG_ALLOWLIST = comma-separated env-slugs
    # (INSTANTLY_KEY_<X> -> x.lower().replace('_','-'); e.g. "new-maxs,new-funding1samuel"). When SET,
    # ONLY those slugs are even probed — so dead keys are never hit (no wasted 402/401 round-trips) and
    # the canonical source can contain ONLY Sam's 9. This also pins the NIGHTLY to the 9 (set the var in
    # /root/core/.env.threads). When UNSET, every key is enumerated (back-compat / unchanged behaviour).
    allow_raw = os.environ.get("WAREHOUSE_THREADS_ORG_ALLOWLIST", "").strip()
    allowlist = (
        {s.strip().lower() for s in allow_raw.split(",") if s.strip()} if allow_raw else None
    )
    skipped_not_allowlisted: list[str] = []
    if allowlist is not None:
        logger.info("workspace allowlist active: %d slugs -> %s", len(allowlist), sorted(allowlist))

    # ws_uuid -> (canonical_slug, api_key, organization_id). DEDUP KEY = the workspace UUID
    # (w['id']), so two DISTINCT workspaces that share one parent org are BOTH kept (never
    # collapsed). The real organization_id rides along so (a) it is preserved as provenance and
    # (b) the LOCAL discovery query can match raw_instantly_email.workspace_id, which DDL 36
    # populates with `organization_id or ws.id` — so discovery must match BOTH the org UUID and
    # the ws UUID (the R2 pre-existing bug stored a UUID, not the canonical slug).
    workspaces: dict[str, tuple[str, str, str | None]] = {}
    dead_keys: list[str] = []
    dup_collapsed: list[str] = []  # same WORKSPACE id under a second env slug -> collapsed
    key_errors: list[dict] = []

    for slug in sorted(keys.keys()):
        if allowlist is not None and slug not in allowlist:
            skipped_not_allowlisted.append(slug)  # not in the pinned 9 -> never probed
            continue
        api_key = keys[slug]
        try:
            with InstantlyClient(api_key) as client:
                w = client.get_current_workspace()
        except InstantlyError as exc:
            key_errors.append({"slug": slug, "error": str(exc)[:200]})
            continue
        except Exception as exc:  # noqa: BLE001
            key_errors.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:200]})
            continue
        ws_uuid = w.get("id")
        organization_id = w.get("organization_id")  # real parent org (separate provenance field)
        if not ws_uuid:
            dead_keys.append(slug)  # e.g. section-125-2 -> no workspace id
            continue
        if ws_uuid in workspaces:
            dup_collapsed.append(slug)  # the SAME workspace exposed under a second env slug
            continue
        # canonical slug: prefer core.workspace's slug for this workspace UUID; else the org UUID
        # mapping (compat), else the env slug.
        canon = (
            id_to_slug.get(str(ws_uuid))
            or (id_to_slug.get(str(organization_id)) if organization_id else None)
            or slug
        )
        workspaces[ws_uuid] = (
            canon, api_key, str(organization_id) if organization_id else None
        )

    diag = {
        "distinct_workspaces": len(workspaces),
        "dead_keys": dead_keys,
        "dup_collapsed": dup_collapsed,
        "key_errors": key_errors,
        "workspaces": sorted({v[0] for v in workspaces.values()}),
        "allowlist": sorted(allowlist) if allowlist is not None else None,
        "skipped_not_allowlisted": skipped_not_allowlisted,
    }
    logger.info(
        "workspace enum: distinct_workspaces=%d dead_keys=%d dup_collapsed=%d key_errors=%d",
        len(workspaces), len(dead_keys), len(dup_collapsed), len(key_errors),
    )
    return workspaces, diag


# ── discovery (FINALIZED-SPEC §4.B / R7) ────────────────────────────────────────
# ── explicit per-workspace drain watermark (429-hardening, 2026-07-02) ──────────
# WHY A FILE, not max(message_at): committed max(message_at) only equals "drained through T"
# when a fetch ran to COMPLETION. To let a 429-storm night commit PARTIAL progress (instead of
# discarding the whole stage, which froze core.email_message at 2026-06-29 for 3 nights), the
# apply must be able to commit whatever leads DID get pulled without the watermark silently
# jumping past the leads that did NOT. So the fetch drains leads in ASCENDING last-reply order,
# tracks the contiguously-completed prefix, and the apply — after the rows durably commit —
# advances this explicit per-ws watermark only through that prefix. workspace_watermark()
# PREFERS the file; the legacy max(message_at) derivation remains the fallback for workspaces
# that have never had a file entry (identical behavior to before on the happy path).
def _watermark_file_path() -> Path:
    return Path(os.environ.get(
        "WAREHOUSE_THREADS_WATERMARK",
        str(Path(DB_PATH).parent / "threads_watermark.json"),
    ))


def _load_watermarks() -> dict[str, str]:
    """{ws_slug: iso-ts 'drained through'} — empty on absent/corrupt file (fallback applies)."""
    p = _watermark_file_path()
    try:
        with open(p) as f:
            data = json.load(f)
        return {str(k): str(v) for k, v in data.items() if v}
    except (OSError, ValueError):
        return {}


def _parse_iso_utc(s: str):
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _advance_watermarks(updates: dict) -> dict[str, str]:
    """MONOTONICALLY advance the explicit watermark file (atomic tmp+rename). A ws entry only
    moves FORWARD (max of existing and new) so a re-applied old stage can never regress it.
    Returns the entries actually advanced ({ws: new_iso})."""
    if not updates:
        return {}
    p = _watermark_file_path()
    current = _load_watermarks()
    advanced: dict[str, str] = {}
    for ws, new_dt in updates.items():
        if new_dt is None:
            continue
        old = _parse_iso_utc(current[ws]) if ws in current else None
        if old is None or new_dt > old:
            current[ws] = new_dt.astimezone(timezone.utc).isoformat()
            advanced[ws] = current[ws]
    if not advanced:
        return {}
    tmp = str(p) + ".tmp"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(current, f, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except OSError as exc:
        logger.warning("could not persist thread watermarks to %s: %s", p, exc)
        return {}
    return advanced


class _DrainTracker:
    """Contiguous-prefix drain tracker for the ordered lead pull.

    Leads are submitted in ASCENDING last-reply order; `drained_through()` is the highest
    last-reply timestamp T such that EVERY lead with key <= T has completed (rows durably
    staged, or recorded in failed.jsonl — failed leads don't block the prefix because
    failed.jsonl owns their one-retry, the historic §4.F contract). Completion order is not
    submission order (worker pool), so out-of-order completions park until the prefix
    closes. None keys (prior-failed retries with no local reply row) sort FIRST and never
    advance the watermark themselves."""

    def __init__(self, ordered_keys: list):
        self._keys = ordered_keys
        self._done = [False] * len(ordered_keys)
        self._ptr = 0
        self._best = None  # max non-None key in the completed prefix

    def mark_done(self, idx: int) -> None:
        self._done[idx] = True
        while self._ptr < len(self._done) and self._done[self._ptr]:
            k = self._keys[self._ptr]
            if k is not None and (self._best is None or k > self._best):
                self._best = k
            self._ptr += 1

    def drained_through(self):
        return self._best

    def all_done(self) -> bool:
        return self._ptr == len(self._done)


def _progress_sidecar_path(stage_path: str) -> str:
    return stage_path + ".progress"


def _write_progress_sidecar(stage_path: str, progress: dict) -> None:
    """Atomically persist per-ws fetch progress ({ws: {drained_through, complete, mode}}).
    Written ONLY right after a stage fsync so it never claims more than what is durably
    staged; survives a SIGKILL'd fetch so the apply can advance watermarks for the drained
    prefix (partial-progress commit)."""
    path = _progress_sidecar_path(stage_path)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(progress, f, default=str, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning("could not write progress sidecar for %s: %s", stage_path, exc)


def _read_progress_sidecar(stage_path: str) -> dict:
    try:
        with open(_progress_sidecar_path(stage_path)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def workspace_watermark(read_conn: duckdb.DuckDBPyConnection, ws_slug: str):
    """Effective incremental lower bound for THIS workspace (R7 — per-workspace, never global).

    PREFERS the explicit drain watermark file (exact 'drained through' state, advanced only
    after an apply commits — see _advance_watermarks): after a PARTIAL apply, committed
    max(message_at) overshoots the true drained state, so the file is authoritative once it
    exists. Falls back to the historic max(message_at) derivation otherwise. Both paths keep
    the 2-day overlap re-pull."""
    wm = _load_watermarks().get(ws_slug)
    if wm:
        dt = _parse_iso_utc(wm)
        if dt is not None:
            return dt - _OVERLAP
    try:
        row = read_conn.execute(
            "SELECT max(message_at) FROM raw_instantly_email_message WHERE workspace_id = ?",
            [ws_slug],
        ).fetchone()
    except duckdb.Error:
        return None  # table not created yet (first ever run)
    if row and row[0] is not None:
        return row[0] - _OVERLAP
    return None


def discover_replied_leads(
    read_conn: duckdb.DuckDBPyConnection,
    client: InstantlyClient,
    org_id: str,
    ws_uuid: str | None,
    since_ws,
    skip_live_walk: bool = False,
) -> tuple[dict[str, "datetime | None"], dict]:
    """{lowercased lead -> last known reply ts (drain key)} to (re)pull for one org since
    `since_ws`. The timestamp keys the ORDERED DRAIN (429-hardening 2026-07-02): leads are
    pulled ascending by last-reply time so a partial fetch's contiguously-completed prefix is a
    valid 'drained through T' watermark. A None ts (unparseable) orders first and never
    advances the watermark.

    DISCOVERY SOURCE = the ALREADY-SYNCED LOCAL inbound atom (raw_instantly_email, DDL 36 —
    the received/inbound stream the native pipe #36/#77 keeps current), NOT a live full /emails
    API walk. FINALIZED-SPEC §2 (lines 118-120) names "the received stream … reused as the
    replied-lead discovery + watermark source (per org)" — i.e. this local table. A live
    all_emails() walk over the WHOLE workspace stream is catastrophic on the large workspaces:
    renaissance-4 = 3.96M emails (~39,600 pages) vs _paginate's 1000-page ceiling (~2.5% of the
    stream), so a since_ws=None full backfill would ALWAYS hit the ceiling, HARD-FAIL the
    workspace, and re-quarantine it every run — the two biggest workspaces could never complete a
    backfill (defeating DoD #2 / G6). The local query is O(~25k rows) with ZERO API calls.

    raw_instantly_email.workspace_id is UUID-keyed (DDL 36 stores `organization_id or ws.id` —
    the R2 pre-existing bug), NOT the canonical slug — so we match on the org/workspace UUIDs we
    enumerated, not the slug. Every row in raw_instantly_email is an inbound reply (email_type=
    received), so DISTINCT lead_email over it IS the replied-lead set; ?lead= then re-pulls each
    one's FULL both-direction history (the upsert collapses).

    INCREMENTAL broadened trigger (idempotency lens): for an INCREMENTAL run (since_ws is not
    None — a small recent delta) we ALSO walk the live full /emails stream (all_emails, newest-
    first, stops at the cutoff) so a late ue_type=3 reply with NO new inbound row is still caught
    (the local inbound atom would miss it). This live walk is gated to non-null since_ws ONLY and
    is NEVER run for since_ws=None (the first-run full backfill), where it would be catastrophic.
    On the bounded incremental walk a pagination-ceiling hit is still surfaced (HARD FAIL — no
    watermark advance) the same as a per-lead pull truncation (FINALIZED-SPEC §4.C).
    """
    leads: dict[str, datetime | None] = {}

    def _note(lead: str, ts) -> None:
        """Keep the MAX known reply ts per lead (the drain key); None never displaces a ts."""
        ts = _coerce_utc(ts)
        cur = leads.get(lead)
        if lead not in leads or (ts is not None and (cur is None or ts > cur)):
            leads[lead] = ts

    # --- LOCAL discovery from the synced inbound atom (zero API calls) ---
    # Match raw_instantly_email.workspace_id against BOTH the org_id and the ws UUID (DDL 36
    # writes `organization_id or ws.id`, so either may be present depending on the API payload).
    ids = [str(x) for x in {org_id, ws_uuid} if x]
    placeholders = ", ".join(["?"] * len(ids))
    local_n = 0
    try:
        if since_ws is None:
            rows = read_conn.execute(
                f"SELECT lower(trim(lead_email)), max(reply_timestamp) FROM raw_instantly_email "
                f"WHERE workspace_id IN ({placeholders}) AND lead_email IS NOT NULL "
                f"GROUP BY 1",
                ids,
            ).fetchall()
        else:
            rows = read_conn.execute(
                f"SELECT lower(trim(lead_email)), max(reply_timestamp) FROM raw_instantly_email "
                f"WHERE workspace_id IN ({placeholders}) AND lead_email IS NOT NULL "
                f"AND reply_timestamp > ? GROUP BY 1",
                ids + [since_ws],
            ).fetchall()
        for lead, ts in rows:
            if lead:
                _note(lead, ts)
                local_n += 1
    except duckdb.Error as exc:
        # raw_instantly_email not present (e.g. a brand-new warehouse before #36 ran) -> empty
        # local set; the incremental live walk below (if since_ws set) still finds new activity.
        logger.warning("discovery: raw_instantly_email not queryable (%s) — local set empty", exc)

    # --- INCREMENTAL ONLY: broadened live trigger for a late ue_type=3 with no new inbound ---
    # skip_live_walk forces LOCAL-ONLY discovery even for a non-null since_ws — used by a bounded
    # `--since-override`/`--local-only` window BACKFILL (e.g. the IM first-reply baseline pull),
    # where the all_emails walk is both unnecessary (we want the local replied-lead set in a fixed
    # window, not late-ue3 deltas) AND risky (a big workspace's recent stream can cross the
    # pagination ceiling and HARD-FAIL the ws). The nightly incremental (no flags) is unaffected.
    ceiling_hit = False
    n_seen = 0
    if since_ws is not None and not skip_live_walk:
        cutoff_iso = since_ws.astimezone(timezone.utc).isoformat()
        flag: dict = {"hit": False}
        for e in client.all_emails(since=cutoff_iso, ceiling_flag=flag):
            lead = (e.get("lead") or "").lower().strip()
            if lead:
                _note(lead, e.get("timestamp_email") or e.get("timestamp_created"))
            n_seen += 1
        ceiling_hit = bool(flag.get("hit"))

    return leads, {
        "local_replied_leads": local_n,
        "incremental_emails_scanned": n_seen,
        "discovery_ceiling_hit": ceiling_hit,
    }


def _coerce_utc(ts):
    """Best-effort tz-AWARE UTC datetime from a DuckDB TIMESTAMP(TZ) / ISO string; None if
    unparseable. Naive values are assumed UTC (the warehouse convention) so drain keys are
    always mutually comparable."""
    if ts is None:
        return None
    if isinstance(ts, str):
        return _parse_iso_utc(ts)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    return None


# ── transform (FINALIZED-SPEC §4.D) ─────────────────────────────────────────────
def transform_item(item: dict, org_id: str, ws_slug: str, fetched_at: str) -> dict | None:
    """Map one Instantly /emails item -> a staging row dict (atom column names).

    Pure function (no network/db) so it is unit-testable. Returns None if the item
    has no usable id (cannot be a PK).
    """
    message_id = item.get("id")  # PK (UUID). NOT item['message_id'] (RFC822 header). R6.
    if not message_id:
        return None
    rfc_message_id = item.get("message_id")  # RFC822 header -> separate column
    # organization_id provenance: the item's own org, else the caller's resolved org for this
    # workspace (the REAL organization_id, NOT the workspace UUID — R2). May be None (nullable).
    organization_id = item.get("organization_id") or org_id
    campaign_id = item.get("campaign_id")
    thread_id = item.get("thread_id") or ""
    # thread_id = '<campaign_id[:2]>-<per-lead-suffix>'; the SUFFIX is the per-lead anchor (QA only).
    if "-" in thread_id:
        _, suffix = thread_id.split("-", 1)
    else:
        suffix = ""
    lead_anchor_key = suffix
    # conversation key (R1/R1a): campaign_id, or 'unattributed:'||anchor for null-campaign IM replies.
    thread_key = campaign_id if campaign_id else ("unattributed:" + suffix)

    ue_type = item.get("ue_type")
    try:
        ue_type = int(ue_type) if ue_type is not None else None
    except (TypeError, ValueError):
        ue_type = None
    # direction from ue_type ALONE (R9): inbound iff ue_type==2, else outbound.
    direction = "inbound" if ue_type == 2 else "outbound"

    step_path = item.get("step")  # raw composite '0_0_2' (R5); NEVER int(); NULL on replies.
    if step_path is not None:
        step_path = str(step_path)

    message_at = item.get("timestamp_email") or item.get("timestamp_created")

    body = item.get("body")
    body_html = body.get("html") if isinstance(body, dict) else None
    body_text = clean_body(body)  # §7: html+quote stripped, spintax/merge can't survive

    lead_email = (item.get("lead") or "").lower().strip()

    return {
        "message_id": message_id,
        "rfc_message_id": rfc_message_id,
        "thread_id": thread_id or None,
        "thread_key": thread_key,
        "lead_anchor_key": lead_anchor_key,
        "workspace_id": ws_slug,
        "organization_id": organization_id,
        "campaign_id": campaign_id,
        "lead_email": lead_email,
        "direction": direction,
        "ue_type": ue_type,
        "step_path": step_path,
        # subject is scanned by G3 too (incl. source='template') — spintax/merge-strip it so a
        # raw {a|b}/{{field}} can never survive in the stored subject (FINALIZED-SPEC §7).
        "subject": clean_subject(item.get("subject")),
        "body_text": body_text,
        "body_html": body_html,
        "from_email": item.get("from_address_email"),
        "to_emails": _join_recipients(item.get("to_address_email_list")),
        "eaccount": item.get("eaccount"),
        "message_at": str(message_at) if message_at is not None else None,
        "source": "instantly",
        "api_response_raw": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    }


def _join_recipients(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, list):
        return ", ".join(str(x) for x in v if x)
    return str(v)


# ── Phase A: pull (NO lock) -> JSONL staging (FINALIZED-SPEC §4.C/F) ─────────────
def _assert_no_writer_lock_held() -> None:
    """G8/R8 guard: the network pull (Phase A) must NEVER run while the warehouse writer
    flock is held — otherwise httpx I/O executes under the single-writer lock and starves
    every other writer for the network duration (the exact anti-pattern R8 forbids). The
    flock wrapper + core/db.py both export WAREHOUSE_WRITE_LOCK_HELD=1 while the lock is held,
    so if we see it set here we are about to pull under the lock — REFUSE loudly.

    Allow an explicit opt-out (WAREHOUSE_THREADS_ALLOW_LOCKED_PULL=1) only for a deliberate
    dev/all-in-one run where the operator accepts the starvation.
    """
    if os.environ.get("WAREHOUSE_THREADS_ALLOW_LOCKED_PULL") == "1":
        return
    if os.environ.get("WAREHOUSE_WRITE_LOCK_HELD") == "1":
        raise RuntimeError(
            "email_thread_sync Phase A (network pull) refuses to run with the warehouse writer "
            "flock held (WAREHOUSE_WRITE_LOCK_HELD=1) — that would do httpx I/O under the "
            "single-writer lock and starve other writers (R8/G8 violation). Run `fetch` UN-locked "
            "(post-orchestrator) and `apply` under scripts/with_warehouse_lock.sh. Override only "
            "with WAREHOUSE_THREADS_ALLOW_LOCKED_PULL=1 for a deliberate dev all-in-one run."
        )


def _read_prior_failed(failed_path: str) -> dict[str, set[str]]:
    """Read the prior run's failed.jsonl into {ws_slug: {lead, ...}} (FINALIZED-SPEC §4.F).

    These are leads whose ?lead= pull 4xx/5xx-failed (after _get's retries) last run. They are
    unioned back into THIS run's discovery so they get exactly one retry, then failed.jsonl is
    REWRITTEN (not appended) with only the leads that fail again — so a transient failure heals
    and a persistent one stays visible (never silently/permanently dropped)."""
    out: dict[str, set[str]] = {}
    if not os.path.exists(failed_path):
        return out
    try:
        with open(failed_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                lead = (rec.get("lead") or "").lower().strip()
                ws = rec.get("ws") or ""
                if lead and lead != "?":  # drop legacy useless '?' entries
                    out.setdefault(ws, set()).add(lead)
    except OSError as exc:
        logger.warning("could not read prior failed.jsonl %s: %s", failed_path, exc)
    return out


def _write_failed(failed_path: str, records: list[dict]) -> None:
    """Rewrite failed.jsonl with this run's surviving failures (§4.F one-retry semantics)."""
    try:
        with open(failed_path, "w") as f:
            for rec in records:
                f.write(json.dumps(rec, default=str) + "\n")
    except OSError as exc:
        logger.warning("could not write failed.jsonl %s: %s", failed_path, exc)


def _append_failed_durable(fh, rec: dict) -> bool:
    """DURABLY append one failure record to the open failed.jsonl handle (write+flush+fsync).

    Adversarial-review B1: a failed lead may only close its drain slot (letting the watermark
    advance past it) once its retry record is ON DISK — an in-memory-only `new_failed` entry
    dies with a 60m-timeout SIGKILL, and a watermark that advanced past it would then skip the
    lead FOREVER. Returns False on any OSError (caller must then NOT close the slot — the
    prefix stalls, which is the fail-safe direction)."""
    try:
        fh.write(json.dumps(rec, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
        return True
    except (OSError, ValueError) as exc:  # ValueError = closed handle; both mean NOT durable
        logger.warning("could not durably append failed-lead record (%s) — drain slot stays open", exc)
        return False


def _ensure_trailing_newline(path: str) -> None:
    """Terminate a torn final line before appending (adversarial-review M2): a hard-killed
    fetch can leave a partial line with no newline; a resumed fetch's first row would
    concatenate onto it, corrupting an INTACT row that sanitize would then drop. Appending a
    newline isolates the torn fragment as its own (sanitize-dropped) line."""
    try:
        with open(path, "rb+") as f:
            f.seek(0, 2)
            if f.tell() == 0:
                return
            f.seek(-1, 2)
            if f.read(1) != b"\n":
                f.write(b"\n")
                f.flush()
                os.fsync(f.fileno())
    except FileNotFoundError:
        return
    except OSError as exc:
        logger.warning("could not newline-terminate stage %s: %s", path, exc)


def _resume_done(stage_path: str) -> set[str]:
    """message_ids already in the staging checkpoint (restart resumes, never re-pulls)."""
    done: set[str] = set()
    if not os.path.exists(stage_path):
        return done
    with open(stage_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(json.loads(line)["message_id"])
            except (ValueError, KeyError):
                continue
    return done


def run_fetch(
    creds: Credentials,
    run_id: str,
    stage_path: str,
    since_override=None,
    full_backfill: bool = False,
    local_only: bool = False,
) -> dict:
    """Phase A. Enumerate orgs, discover replied leads per org (per-workspace watermark),
    pull each lead's full thread, append rows to the JSONL staging file. NO DB WRITES.

    Returns a diagnostics dict including per-ws RUNLOG metrics and ceiling-hit workspaces
    whose watermark must NOT advance.

    `since_ws` per workspace is chosen as (highest precedence first):
      * `full_backfill=True`  -> since_ws=None  (FULL local discovery, ignore the watermark). This is
        what makes a re-runnable full backfill safe AFTER the atom is already partly populated (e.g.
        after a focused window pull): the watermark would otherwise be non-null and silently turn the
        big historical pull into a tiny incremental delta, dropping every pre-watermark replier.
      * `since_override` set -> since_ws=that datetime for every ws (a fixed window backfill, e.g. the
        baseline pull `--since-override 2026-06-22`). Pair with `local_only=True` to skip the all_emails
        walk (a fixed window wants the local replied-lead set, not late-ue3 deltas).
      * else -> the per-workspace watermark (R7 — the unchanged nightly incremental path).
    `local_only=True` forces local-only discovery (no all_emails live walk) even for a non-null since_ws.
    With no flags, behaviour is exactly the original watermark-based incremental.

    LOCK DISCIPLINE (parallel-safe, beyond R8): every DuckDB read is done on a SHORT-LIVED read-only
    connection that is CLOSED before the long per-lead network pull. A read-only DuckDB connection takes a
    *shared* file lock, and DuckDB refuses a read-WRITE open while any shared lock is held — so holding a
    read conn across a multi-hour pull (as the original single-conn shape did) would block the nightly
    orchestrator's RW open AND every apply for the whole run. The serial+post-orchestrator spec never hit
    this; running N pullers in parallel ACROSS the nightly window does. So the pull holds NO DuckDB lock.
    """
    _assert_no_writer_lock_held()  # G8/R8: never pull over the network under the writer lock
    _enum_conn = db_module.connect(read_only=True)
    try:
        workspaces, diag = enumerate_orgs(creds, _enum_conn)
    finally:
        _enum_conn.close()
    fetched_at = datetime.now(timezone.utc).isoformat()
    failed_path = stage_path + ".failed"

    done = _resume_done(stage_path)
    if done:
        logger.info("fetch resume: %d message_ids already staged in %s", len(done), stage_path)

    per_ws_runlog: list[dict] = []
    ceiling_hit_ws: list[str] = []
    # Ordered-drain progress ({ws: {drained_through, complete, mode}}) — persisted to the
    # .progress sidecar after every stage fsync so a KILLED fetch (the 60m nightly timeout /
    # SIGKILL) leaves an exact, durable "drained through T" record the apply can trust.
    # mode drives whether the apply may advance the explicit watermark: watermark/full yes,
    # override no (a fixed window says nothing about completeness before its lower bound).
    mode = "override" if since_override is not None else ("full" if full_backfill else "watermark")
    progress: dict[str, dict] = {}
    total_429 = 0

    # First retry the leads that 4xx/5xx-failed their ?lead= pull on a PRIOR run (FINALIZED-SPEC
    # §4.F: "one retry on the next full run — not permanently skipped"). We read the prior
    # failed.jsonl into a per-ws set BEFORE truncating it, union those leads back into discovery
    # so they get re-attempted this run, then rewrite failed.jsonl with only the leads that fail
    # AGAIN this run.
    prior_failed_by_ws = _read_prior_failed(failed_path)

    # Serial across orgs (never parallelize workspaces). The failed.jsonl handle is opened in
    # APPEND mode alongside the stage: each failure is durably appended the moment it happens
    # (B1 — see _append_failed_durable); the end-of-run _write_failed rewrite still performs the
    # §4.F one-retry consolidation on a clean finish. _ensure_trailing_newline isolates a torn
    # tail before a RESUMED fetch appends (M2).
    _ensure_trailing_newline(stage_path)
    with open(stage_path, "a") as stage, open(failed_path, "a") as failed_out:
        new_failed: list[dict] = []
        retried_ws: set[str] = set()  # ws whose pull loop ran (its prior-failed were retried)
        for ws_uuid, (ws_slug, api_key, organization_id) in workspaces.items():
            ws_ceiling = False
            try:
                with InstantlyClient(api_key) as client:
                    # ── discovery phase: a SHORT-LIVED read-only conn, CLOSED before the pull ──
                    # so the long per-lead network pull below holds NO DuckDB shared lock (which would
                    # otherwise block the nightly's RW open + every apply for the whole run).
                    discovery_started = datetime.now(timezone.utc)
                    disc_conn = db_module.connect(read_only=True)
                    try:
                        if full_backfill:
                            since_ws = None                       # FULL discovery — ignore the watermark
                        elif since_override is not None:
                            since_ws = since_override              # fixed-window backfill (baseline pull)
                        else:
                            since_ws = workspace_watermark(disc_conn, ws_slug)  # nightly incremental (R7)
                        # discovery matches raw_instantly_email.workspace_id against BOTH the org UUID
                        # AND the ws UUID (DDL 36 stored `organization_id or ws.id`) so no existing
                        # lead is missed regardless of which the inbound row carries.
                        lead_keys, dd = discover_replied_leads(
                            disc_conn, client, organization_id, ws_uuid, since_ws,
                            skip_live_walk=(local_only or full_backfill),
                        )
                        total_cold_sends_window = _cold_send_count(disc_conn, ws_slug, since_ws)
                        # pre-run effective drained-through (for the M1 seed below): the value
                        # discovery is ABOUT to be correct against, whatever the mode.
                        pre_run_wm = since_ws if mode == "watermark" else workspace_watermark(disc_conn, ws_slug)
                    finally:
                        disc_conn.close()   # release the shared lock BEFORE the multi-hour pull
                    # M1 (adversarial review): durably SEED this ws's watermark-file entry with
                    # its pre-run effective state BEFORE any of its rows can commit. If the apply
                    # later dies between COMMIT and the sidecar merge, workspace_watermark() must
                    # NOT fall back to max(message_at) — which the partial commit just pushed
                    # past the true drained state. Monotone max-merge makes this a no-op for
                    # file-backed workspaces and a one-time (bounded, 2d) conservative pin for
                    # legacy-fallback ones. A BRAND-NEW ws (no prior state at all) pins at the
                    # 1970 epoch so a crashed first partial run re-discovers in full rather
                    # than trusting its own partial max(message_at); override windows are
                    # operator-managed and keep their historic semantics (no pin).
                    seeded = _coerce_utc(pre_run_wm)
                    if seeded is None and mode != "override":
                        seeded = datetime(1970, 1, 1, tzinfo=timezone.utc)
                    if seeded is not None:
                        _advance_watermarks({ws_slug: seeded})
                    # union back the prior-run failures for this ws (one retry — §4.F); no local
                    # reply row -> None drain key (ordered first, never advances the watermark).
                    for lead in prior_failed_by_ws.get(ws_slug, set()):
                        lead_keys.setdefault(lead, None)
                    if dd.get("discovery_ceiling_hit"):
                        # Discovery scan itself truncated -> the replied-lead SET is incomplete.
                        # HARD FAIL the workspace exactly like a per-lead pull truncation: do NOT
                        # advance its watermark, exclude its rows from apply (§4.C, both halves).
                        ws_ceiling = True
                        ceiling_hit_ws.append(ws_slug)
                        _write_ceiling_sidecar(stage_path, ceiling_hit_ws)  # durable IMMEDIATELY
                        logger.error(
                            "DISCOVERY PAGINATION CEILING on ws=%s — replied-lead set truncated; "
                            "HARD FAIL, watermark NOT advanced, ws rows quarantined.", ws_slug,
                        )
                    replied_lead_delta = len(lead_keys)
                    leads_pulled = 0
                    api_calls = 0
                    messages_upserted = 0

                    # Baseline progress entry BEFORE the pull: "drained through since_ws" is
                    # already true of the pre-run state on the watermark path (since_ws is the
                    # prior watermark minus the overlap; _advance_watermarks is monotone so this
                    # can never regress anything). Ensures every ws that stages ANY row this run
                    # has a durable progress entry — a mid-ws kill can then never let the apply
                    # fall back to the overshooting max(message_at) derivation.
                    baseline = _coerce_utc(since_ws) if mode == "watermark" else None
                    progress[ws_slug] = {
                        "drained_through": baseline.isoformat() if baseline else None,
                        "complete": False, "mode": mode,
                    }
                    _write_progress_sidecar(stage_path, progress)

                    def pull_one(lead: str):
                        items, hit = client.lead_emails_window(lead)
                        return lead, items, hit

                    # ── ORDERED DRAIN (429-hardening 2026-07-02) ──────────────────────────────
                    # Leads are pulled ASCENDING by last-reply time (None keys — prior-failed
                    # retries — first). The _DrainTracker's contiguously-completed prefix is a
                    # valid per-ws "drained through T" even if this process is KILLED mid-pull
                    # (the 60m nightly timeout during a sustained 429 storm): everything with a
                    # key <= T is durably staged or DURABLY in failed.jsonl at the instant its
                    # slot closes (B1: failures fsync via _append_failed_durable BEFORE
                    # mark_done; failed.jsonl is preserved across nightly runs). The sidecar is
                    # only written right after a stage fsync so it never claims un-staged work.
                    # Map each future -> its ordinal so a pull that raises after _get's retries
                    # records the ACTUAL lead in failed.jsonl (§4.F: one retry on the next run —
                    # NOT permanently skipped) AND still closes its slot in the drain prefix
                    # (failed.jsonl owns the retry; the watermark must not stall on it).
                    epoch = datetime.min.replace(tzinfo=timezone.utc)
                    ordered = sorted(lead_keys.items(),
                                     key=lambda kv: (kv[1] is not None, kv[1] or epoch))
                    tracker = _DrainTracker([k for _, k in ordered])

                    def _sync_progress() -> None:
                        dt = tracker.drained_through() or baseline
                        progress[ws_slug] = {
                            "drained_through": dt.isoformat() if dt else None,
                            "complete": False, "mode": mode,
                        }
                        _write_progress_sidecar(stage_path, progress)

                    with ThreadPoolExecutor(max_workers=_LEAD_WORKERS) as ex:
                        fut_to_idx = {ex.submit(pull_one, lead): i
                                      for i, (lead, _key) in enumerate(ordered)}
                        for n, fut in enumerate(as_completed(fut_to_idx), 1):
                            idx = fut_to_idx[fut]
                            lead = ordered[idx][0]
                            try:
                                lead, items, hit = fut.result()
                            except InstantlyError as exc:
                                rec = {"lead": lead, "ws": ws_slug, "error": str(exc)[:200],
                                       "fetched_at": fetched_at}
                                new_failed.append(rec)
                                # B1: the slot closes ONLY once the retry record is ON DISK —
                                # failed.jsonl owns the retry (§4.F); an fsync failure keeps the
                                # slot open so the watermark can never pass an unrecorded lead.
                                if _append_failed_durable(failed_out, rec):
                                    tracker.mark_done(idx)
                                continue
                            except Exception as exc:  # noqa: BLE001 — any pull error -> retry next run
                                rec = {"lead": lead, "ws": ws_slug,
                                       "error": f"{type(exc).__name__}: {exc}"[:200],
                                       "fetched_at": fetched_at}
                                new_failed.append(rec)
                                if _append_failed_durable(failed_out, rec):
                                    tracker.mark_done(idx)
                                continue
                            leads_pulled += 1
                            # 1 cursor walk = >=1 api call; approximate (page count unknown here).
                            api_calls += max(1, len(items) // 100 + 1)
                            if hit:
                                if not ws_ceiling:
                                    ws_ceiling = True  # pagination ceiling => HARD FAIL for this ws
                                    ceiling_hit_ws.append(ws_slug)
                                    _write_ceiling_sidecar(stage_path, ceiling_hit_ws)  # durable NOW
                                # a TRUNCATED lead's slot never closes -> the drain watermark can
                                # never advance past it (its rows are quarantined at apply anyway).
                            for item in items:
                                # org provenance fallback = the REAL organization_id (when the
                                # item omits its own); ws_slug is the canonical stored workspace_id.
                                row = transform_item(item, organization_id, ws_slug, fetched_at)
                                if row is None:
                                    continue
                                if row["message_id"] in done:
                                    continue
                                stage.write(json.dumps(row, default=str) + "\n")
                                done.add(row["message_id"])
                                # per-ws RUNLOG `messages_upserted` = rows STAGED this run for this
                                # ws (excludes resume-`done` ids). G6 uses leads_pulled/cold-sends,
                                # not this. The AUTHORITATIVE run-total (G7: == manifest line count)
                                # is the RUNLOG-APPLY line's messages_upserted emitted by _apply_core
                                # — on a fresh (non-resumed) stage the per-ws sum equals it.
                                messages_upserted += 1
                            if not hit:
                                tracker.mark_done(idx)  # rows written to the stage buffer above
                            if n % 200 == 0:
                                stage.flush()
                                os.fsync(stage.fileno())
                                _sync_progress()  # ONLY after fsync: never claim un-staged work
                    stage.flush()
                    os.fsync(stage.fileno())
                    # M4: only NOW did this ws's prior-failed leads genuinely get their retry
                    # (the pool drained; every outcome is durable). Marking earlier let a
                    # mid-pool ws-level error erase the §4.F ledger for leads never retried.
                    retried_ws.add(ws_slug)

                    # ws pull loop finished. Clean completion (no ceiling) drains through the
                    # DISCOVERY time: everything discoverable as of that instant was pulled or
                    # recorded failed; replies landing after it belong to the next window.
                    if ws_ceiling:
                        _sync_progress()  # keep the last honest partial prefix
                    else:
                        progress[ws_slug] = {
                            "drained_through": discovery_started.isoformat(),
                            "complete": True, "mode": mode,
                        }
                        _write_progress_sidecar(stage_path, progress)

                    ws_429 = client.rate_limit_hits
                    total_429 += ws_429
                    rl = {
                        "ws": ws_slug,
                        "replied_lead_delta": replied_lead_delta,
                        "leads_pulled": leads_pulled,
                        "api_calls": api_calls,
                        "messages_upserted": messages_upserted,
                        "total_cold_sends_window": total_cold_sends_window,
                        "ceiling_hit": ws_ceiling,
                        "rate_limit_429s": ws_429,
                        "drained_through": progress[ws_slug]["drained_through"],
                        "complete": progress[ws_slug]["complete"],
                    }
                    per_ws_runlog.append(rl)
                    # G6/G7 parse this exact line (new fields appended AFTER the historic ones).
                    logger.info(
                        "RUNLOG run_id=%s ws=%s replied_lead_delta=%d leads_pulled=%d "
                        "api_calls=%d messages_upserted=%d total_cold_sends_window=%d "
                        "rate_limit_429s=%d drained_through=%s complete=%s",
                        run_id, ws_slug, replied_lead_delta, leads_pulled, api_calls,
                        messages_upserted, total_cold_sends_window, ws_429,
                        progress[ws_slug]["drained_through"], progress[ws_slug]["complete"],
                    )
                    if ws_ceiling:
                        # HARD FAIL: do NOT advance this ws watermark; escalate.
                        logger.error(
                            "PAGINATION CEILING on ws=%s (full backfill=%s) — HARD FAIL, "
                            "watermark NOT advanced, escalate.",
                            ws_slug, since_ws is None,
                        )
            except InstantlyError as exc:
                logger.error("ws=%s API error: %s", ws_slug, exc)
                diag.setdefault("ws_errors", []).append({"ws": ws_slug, "error": str(exc)[:200]})
            except Exception as exc:  # noqa: BLE001
                logger.exception("ws=%s unexpected error", ws_slug)
                diag.setdefault("ws_errors", []).append(
                    {"ws": ws_slug, "error": f"{type(exc).__name__}: {exc}"[:200]})

    # Rewrite failed.jsonl (§4.F "one retry on the next full run"): this run's failures, PLUS any
    # prior-failed leads for a workspace that errored out before its pull loop ran (so they are not
    # silently lost — they get their retry on a run where the ws is reachable). A prior-failed lead
    # for a ws that DID retry is intentionally NOT re-carried unless it failed AGAIN this run
    # (now in new_failed) — that is the "one retry, then it surfaces as a persistent failure" rule.
    carried = list(new_failed)
    for ws_slug, leads_set in prior_failed_by_ws.items():
        if ws_slug in retried_ws:
            continue  # already retried this run; only its re-failures (in new_failed) carry forward
        for lead in leads_set:
            carried.append({"lead": lead, "ws": ws_slug, "error": "ws_unreachable_not_retried",
                            "fetched_at": fetched_at})
    _write_failed(failed_path, carried)
    diag["failed_leads"] = len(carried)

    diag["per_ws_runlog"] = per_ws_runlog
    diag["ceiling_hit_ws"] = ceiling_hit_ws
    diag["stage_path"] = stage_path
    diag["rate_limit_429s"] = total_429
    diag["progress"] = progress
    # Persist the ceiling-hit workspace list next to the stage file so a SEPARATE apply
    # invocation (the CLI fetch/apply split) can EXCLUDE those workspaces' partial rows
    # from the upsert — otherwise a truncated ws's rows would commit and ADVANCE its
    # max(message_at) watermark, silently skipping the unreached middle of its history
    # (FINALIZED-SPEC §4.C/§4.E HARD FAIL). The apply reads this sidecar. (Also written
    # INCREMENTALLY at first detection above, so a killed fetch still quarantines.)
    _write_ceiling_sidecar(stage_path, ceiling_hit_ws)
    # One-line per-run FETCH summary so the QA can tell partial progress under a 429 storm
    # from total failure at a glance (grep RUN-SUMMARY-FETCH in the nightly log).
    logger.info(
        "RUN-SUMMARY-FETCH run_id=%s mode=%s workspaces=%d ws_errors=%d leads_discovered=%d "
        "leads_pulled=%d leads_failed=%d messages_staged=%d rate_limit_429s=%d "
        "ceiling_hit_ws=%s complete_ws=%d/%d",
        run_id, mode, len(workspaces), len(diag.get("ws_errors", [])),
        sum(r["replied_lead_delta"] for r in per_ws_runlog),
        sum(r["leads_pulled"] for r in per_ws_runlog),
        len(new_failed), sum(r["messages_upserted"] for r in per_ws_runlog), total_429,
        sorted(ceiling_hit_ws),
        sum(1 for p in progress.values() if p.get("complete")), len(workspaces),
    )
    return diag


def _ceiling_sidecar_path(stage_path: str) -> str:
    return stage_path + ".ceiling"


def _write_ceiling_sidecar(stage_path: str, ceiling_hit_ws: list[str]) -> None:
    """Persist the ceiling-hit ws slugs (one per line) so apply can exclude them."""
    try:
        with open(_ceiling_sidecar_path(stage_path), "w") as f:
            for ws in ceiling_hit_ws:
                f.write(ws + "\n")
    except OSError as exc:
        logger.warning("could not write ceiling sidecar for %s: %s", stage_path, exc)


def _read_ceiling_sidecar(stage_path: str) -> set[str]:
    """Read the ceiling-hit ws slugs persisted by run_fetch (empty if none / absent)."""
    path = _ceiling_sidecar_path(stage_path)
    out: set[str] = set()
    if not os.path.exists(path):
        return out
    try:
        with open(path) as f:
            for line in f:
                ws = line.strip()
                if ws:
                    out.add(ws)
    except OSError:
        pass
    return out


def _cold_send_count(read_conn: duckdb.DuckDBPyConnection, ws_slug: str, since_ws) -> int:
    """Total cold sends in the window (G6 denominator: leads_pulled << 0.01*cold_sends).

    Sourced from the existing sent-volume mirror (raw_pipeline_conversation_messages
    ue_type=1) for the same workspace + window. Best-effort: 0 if unavailable, which
    makes G6's vs-sends ratio conservative (a 0 denominator is reported, not asserted).
    """
    try:
        if since_ws is None:
            row = read_conn.execute(
                "SELECT count(*) FROM raw_pipeline_conversation_messages "
                "WHERE workspace_id = ? AND ue_type = 1",
                [ws_slug],
            ).fetchone()
        else:
            row = read_conn.execute(
                "SELECT count(*) FROM raw_pipeline_conversation_messages "
                "WHERE workspace_id = ? AND ue_type = 1 AND message_timestamp >= ?",
                [ws_slug, since_ws],
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except duckdb.Error:
        return 0


# ── Phase B: bulk upsert UNDER the flock (FINALIZED-SPEC §4.E) ───────────────────
def run_apply(stage_path: str, run_id: str, db_path: Path | None = None) -> dict:
    """Phase B. Bulk-load staging JSONL -> TEMP -> ONE non-destructive upsert -> CHECKPOINT.

    MUST be invoked under scripts/with_warehouse_lock.sh (or core.db.connect's in-process
    flock) — this is the ONLY DB-write section; no httpx here. Idempotent + non-destructive:
    COALESCE(NULLIF(...)) never wipes a good body; immutable cols are not flipped; staging is
    DISTINCT-latest on message_id so a crash-duplicated JSONL line can't make ambiguous rows.
    Writes a manifest of upserted message_ids for O(1) rollback (G7).

    HARD-FAIL gate (§4.C/§4.E): a workspace that hit the pagination ceiling in Phase A
    (persisted in the `.ceiling` sidecar) has PARTIAL, mid-history-truncated rows. Applying
    them would ADVANCE that ws's max(message_at) watermark and silently skip its unreached
    history. The apply EXCLUDES those workspaces' rows so the watermark does NOT advance, and
    raises if it cannot (so a CLI `run`/`apply` aborts rather than commit truncated data).
    """
    if not os.path.exists(stage_path):
        logger.info("apply: no staging file at %s — nothing to apply", stage_path)
        return {"messages_upserted": 0, "manifest": None, "messages_upserted_changed": 0}

    # PARTIAL-FETCH tolerance [2026-07-02]: a fetch killed mid-write (the 60m nightly timeout
    # under a 429 storm) can leave ONE torn (non-JSON) trailing line in the stage. read_json
    # runs ignore_errors=false (strict — data-quality invariant for intact lines), so a torn
    # tail would abort the WHOLE apply and lose the night's progress. Drop unparseable lines
    # (loudly) before loading; the drain watermark never advanced past un-fsynced work, so a
    # dropped torn line is re-pulled next run by construction.
    _sanitize_stage(stage_path)

    ceiling_excluded = _read_ceiling_sidecar(stage_path)
    # read-write connect acquires the writer flock (unless a wrapper already holds it).
    con = db_module.connect(db_path)
    try:
        return _apply_core(con, stage_path, run_id, ceiling_excluded)
    finally:
        con.close()


def _sanitize_stage(stage_path: str) -> int:
    """Drop unparseable JSONL lines (torn tail from a killed fetch) — STREAMING (M3).

    Pass 1 counts bad lines with O(1) memory; only when something is bad does pass 2
    stream-rewrite to a tmp file + atomic replace (a full-backfill stage can be multi-GB —
    accumulating kept lines in memory OOMs the droplet on the happy path). Returns dropped."""
    bad = 0
    try:
        with open(stage_path) as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    json.loads(s)
                except ValueError:
                    bad += 1
    except OSError as exc:
        logger.warning("apply: could not sanitize stage %s: %s", stage_path, exc)
        return 0
    if not bad:
        return 0
    kept = 0
    tmp = stage_path + ".sanitized"
    try:
        with open(stage_path) as srcf, open(tmp, "w") as dst:
            for line in srcf:
                s = line.strip()
                if not s:
                    continue
                try:
                    json.loads(s)
                except ValueError:
                    continue
                dst.write(s + "\n")
                kept += 1
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp, stage_path)
    except OSError as exc:
        logger.warning("apply: sanitize rewrite failed for %s: %s", stage_path, exc)
        return 0
    logger.warning(
        "apply: dropped %d unparseable stage line(s) (torn tail from a killed fetch) — "
        "kept %d intact rows; the drain watermark never covered un-fsynced work, so the "
        "dropped lines re-pull next run.", bad, kept,
    )
    return bad


def _apply_core(
    con: duckdb.DuckDBPyConnection,
    stage_path: str,
    run_id: str,
    ceiling_excluded: set[str] | None = None,
) -> dict:
    """Shared Phase-B body (used by BOTH the CLI run_apply and the post-orchestrator path).

    Operates on an ALREADY-OPEN read-write connection. Loads the JSONL staging, DISTINCT-latest
    on message_id, EXCLUDES ceiling-hit workspaces (so their truncated rows do NOT commit and
    do NOT advance their watermark — §4.C HARD FAIL), measures messages_upserted_changed (G2),
    upserts non-destructively, writes the G7 manifest, and CHECKPOINTs.
    """
    ceiling_excluded = ceiling_excluded or set()
    now = datetime.now(timezone.utc)

    con.execute("DROP TABLE IF EXISTS _stage_email_message_all")
    con.execute(
        f"""
        CREATE TEMP TABLE _stage_email_message_all AS
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (
              PARTITION BY message_id ORDER BY fetched_at DESC NULLS LAST
          ) AS rn
          FROM {_read_stage_sql(stage_path)}
        ) WHERE rn = 1
        """
    )
    # Quarantine: drop ceiling-hit workspaces' partial rows BEFORE the upsert so their
    # watermark (max(message_at) of committed rows) is never advanced by truncated history.
    con.execute("DROP TABLE IF EXISTS _stage_email_message")
    if ceiling_excluded:
        placeholders = ", ".join(["?"] * len(ceiling_excluded))
        con.execute(
            f"""CREATE TEMP TABLE _stage_email_message AS
                SELECT * FROM _stage_email_message_all
                WHERE workspace_id NOT IN ({placeholders})""",
            list(ceiling_excluded),
        )
        excluded_n = con.execute(
            "SELECT count(*) FROM _stage_email_message_all"
        ).fetchone()[0] - con.execute(
            "SELECT count(*) FROM _stage_email_message"
        ).fetchone()[0]
        logger.error(
            "apply: EXCLUDED %d staged rows from ceiling-hit workspaces %s "
            "(HARD FAIL — watermark NOT advanced for those ws).",
            excluded_n, sorted(ceiling_excluded),
        )
    else:
        con.execute(
            "CREATE TEMP TABLE _stage_email_message AS SELECT * FROM _stage_email_message_all"
        )
    con.execute("DROP TABLE IF EXISTS _stage_email_message_all")

    staged = con.execute("SELECT count(*) FROM _stage_email_message").fetchone()[0]
    if not staged:
        logger.info("apply: staging empty after dedup/ceiling-exclude — nothing to write")
        con.execute("DROP TABLE IF EXISTS _stage_email_message")
        # An empty stage can still carry real drain progress (e.g. every discovered lead's
        # messages were already committed by a prior run) — advance watermarks anyway.
        advanced = _merge_drain_watermarks(stage_path, ceiling_excluded)
        return {"messages_upserted": 0, "manifest": None, "messages_upserted_changed": 0,
                "ceiling_excluded": sorted(ceiling_excluded),
                "watermarks_advanced": advanced}

    # messages_upserted_changed (G2): count staged ids whose business payload differs from the
    # CURRENTLY-committed row (a brand-new id counts as changed; an unchanged re-pull does NOT).
    changed = con.execute(
        f"""
        SELECT count(*) FROM _stage_email_message s
        LEFT JOIN raw_instantly_email_message r USING (message_id)
        WHERE r.message_id IS NULL
           OR {_payload_hash_sql('s')} <> {_payload_hash_sql('r')}
        """
    ).fetchone()[0]

    insert_select = ", ".join(
        ("s." + c) if c not in ("_loaded_at", "_run_id")
        else ("$loaded_at" if c == "_loaded_at" else "$run_id")
        for c in _COLS
    )
    set_clause = _build_update_set()

    con.execute("BEGIN")
    con.execute(
        f"""
        INSERT INTO raw_instantly_email_message ({", ".join(_COLS)})
        SELECT {insert_select}
        FROM _stage_email_message s
        ON CONFLICT (message_id) DO UPDATE SET {set_clause}
        """,
        {"loaded_at": now, "run_id": run_id},
    )
    con.execute("COMMIT")

    ids = [r[0] for r in con.execute(
        "SELECT message_id FROM _stage_email_message"
    ).fetchall()]
    manifest = _manifest_path(run_id)
    manifest.write_text("\n".join(ids) + "\n")

    con.execute("CHECKPOINT")
    con.execute("DROP TABLE IF EXISTS _stage_email_message")
    guard = interest_status_guard(con)
    # PARTIAL-PROGRESS WATERMARK COMMIT [2026-07-02]: the rows are now durably committed
    # (post-CHECKPOINT), so it is safe to advance the explicit per-ws drain watermark through
    # the fetch's contiguously-drained prefix (the .progress sidecar). This is what turns a
    # 429-storm night from "whole night discarded" into "whatever landed, landed" — the next
    # run resumes from the drained prefix instead of re-losing everything.
    advanced = _merge_drain_watermarks(stage_path, ceiling_excluded)
    # Run-level summary line (G2 reads messages_upserted_changed; G7 reconciles
    # messages_upserted == manifest line count; new fields appended AFTER the historic ones).
    logger.info(
        "RUNLOG-APPLY run_id=%s messages_upserted=%d messages_upserted_changed=%d "
        "ceiling_excluded=%s manifest=%s watermarks_advanced=%s",
        run_id, len(ids), changed, sorted(ceiling_excluded), manifest, advanced,
    )
    return {
        "messages_upserted": len(ids),
        "messages_upserted_changed": int(changed),
        "manifest": str(manifest),
        "interest_guard": guard,
        "ceiling_excluded": sorted(ceiling_excluded),
        "watermarks_advanced": advanced,
    }


def _merge_drain_watermarks(stage_path: str, ceiling_excluded: set[str] | None) -> dict[str, str]:
    """Advance the explicit per-ws watermark file from the fetch's .progress sidecar.

    Only AFTER the staged rows durably committed (callers invoke this post-CHECKPOINT / on the
    provably-empty path). Skips: ceiling-hit workspaces (their rows were quarantined — §4.C),
    override-mode entries (a fixed window proves nothing about completeness before its lower
    bound), and entries with no drained_through. Monotone by construction (_advance_watermarks
    takes max), so replaying an old sidecar can never regress a watermark."""
    ceiling_excluded = ceiling_excluded or set()
    progress = _read_progress_sidecar(stage_path)
    updates: dict[str, datetime] = {}
    for ws, entry in progress.items():
        if ws in ceiling_excluded:
            continue
        if not isinstance(entry, dict) or entry.get("mode") == "override":
            continue
        dt = _parse_iso_utc(entry.get("drained_through") or "") if entry.get("drained_through") else None
        if dt is not None:
            updates[ws] = dt
    advanced = _advance_watermarks(updates)
    if advanced:
        logger.info("drain watermarks advanced: %s (file=%s)", advanced, _watermark_file_path())
    return advanced


def interest_status_guard(con: duckdb.DuckDBPyConnection) -> dict:
    """Post-apply guard (moderator WARN): core.email_thread.lead_interest_status is sourced by a
    slug-keyed join to the superseded raw_pipeline_conversation_messages. If that table is ever
    dropped or its workspace_id namespace drifts off the slug, the LEFT JOIN degrades to a SILENT
    all-NULL. This catches that: if there ARE threads but NONE has a non-null interest_status, log
    a loud WARNING so a silent all-NULL is surfaced (not gated — it degrades gracefully, but it
    must not pass unnoticed). Returns {threads, with_interest, all_null}.
    """
    try:
        threads = con.execute("SELECT count(*) FROM core.email_thread").fetchone()[0]
        with_i = con.execute(
            "SELECT count(*) FROM core.email_thread WHERE lead_interest_status IS NOT NULL"
        ).fetchone()[0]
    except duckdb.Error as exc:
        logger.warning("interest_status_guard: could not evaluate (%s)", exc)
        return {"threads": None, "with_interest": None, "all_null": None}
    all_null = bool(threads and not with_i)
    if all_null:
        logger.warning(
            "interest_status_guard: %d threads but 0 have lead_interest_status — possible "
            "namespace drift / drop of raw_pipeline_conversation_messages (re-home interest "
            "per the DDL OPEN-1 guard).", threads,
        )
    return {"threads": threads, "with_interest": with_i, "all_null": all_null}


# Columns whose ON CONFLICT update is ALWAYS an unconditional overwrite. These are the ONLY
# columns a degraded re-pull may legitimately clobber: api_response_raw is a pure drill-through of
# the LATEST raw item (newest wins, by design), and _loaded_at/_run_id are run provenance that MUST
# reflect this run. Every OTHER mutable column is non-destructive (COALESCE) so a later re-pull with
# a NULL/'' value can never wipe committed-good data (idempotency lens — blocking finding).
_UNCONDITIONAL_OVERWRITE = {"api_response_raw", "_loaded_at", "_run_id"}


def _build_update_set() -> str:
    """ON CONFLICT SET — fully non-destructive (idempotency lens, blocking).

    A degraded re-pull (a lead pulled again where Instantly now returns a NULL/'' for a field that
    WAS populated) must NEVER null out committed-good data. So EVERY mutable column is wrapped:

      * message_at        -> COALESCE(excluded.message_at, <table>.message_at). COALESCE ALONE —
                             it is TIMESTAMPTZ and NULLIF('') would type-error. message_at is the
                             worst column to wipe: it is the per-workspace watermark AND the
                             core.email_thread ordering key.
      * rfc_message_id    -> COALESCE(excluded.col, <table>.col) (already nullable; no '' case).
      * all other VARCHAR metadata (campaign_id, thread_key, lead_anchor_key, thread_id,
        organization_id, subject, body_text, body_html, from_email, to_emails, eaccount, source)
                          -> COALESCE(NULLIF(excluded.col, ''), <table>.col): treat '' as NULL so a
                             blank later pull never displaces a good value.
      * api_response_raw/_loaded_at/_run_id -> unconditional overwrite (latest raw + run provenance).
      * immutable cols (direction/ue_type/step_path/lead_email/workspace_id) -> OMITTED (assert-equal
        by not flipping them for an existing id).
    """
    tbl = "raw_instantly_email_message"
    parts = []
    for c in _COLS:
        if c in _IMMUTABLE:
            continue  # never flip an immutable col for an existing id
        if c in _UNCONDITIONAL_OVERWRITE:
            parts.append(f"{c} = excluded.{c}")
        elif c == "message_at":
            # TIMESTAMPTZ — COALESCE ALONE (NULLIF('') would type-error on a non-VARCHAR).
            parts.append(f"{c} = COALESCE(excluded.{c}, {tbl}.{c})")
        elif c == "rfc_message_id":
            # nullable VARCHAR header; no empty-string sentinel — plain COALESCE.
            parts.append(f"{c} = COALESCE(excluded.{c}, {tbl}.{c})")
        else:
            # every other mutable VARCHAR: treat '' as NULL so a blank pull never wipes a good value.
            parts.append(f"{c} = COALESCE(NULLIF(excluded.{c}, ''), {tbl}.{c})")
    return ", ".join(parts)


# NOTE (R8/G8): there is DELIBERATELY no in-orchestrator nightly hook. The orchestrator holds
# the single-writer flock for its whole run, so a hook would pull over the network UNDER the
# lock. The nightly path is the CLI fetch/apply split wired into scripts/nightly.sh AFTER the
# orchestrator releases the lock (see register() docstring + scripts/nightly.sh). `fetch` runs
# un-locked; `apply` runs under scripts/with_warehouse_lock.sh.


# ── CLI (backfill + nightly driver — fetch un-locked, apply under the lock) ──────
def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="Phase A — pull replied-lead threads to JSONL (NO lock)")
    f.add_argument("--stage", default=_STAGE_DEFAULT)
    f.add_argument("--full-backfill", action="store_true",
                   help="FULL local discovery (since_ws=None) ignoring the watermark — the safe "
                        "re-runnable historical backfill even after the atom is partly populated.")
    f.add_argument("--since-override", default=None,
                   help="ISO8601 lower bound used as since_ws for EVERY workspace (fixed-window "
                        "backfill, e.g. the baseline pull --since-override 2026-06-22). Pair with "
                        "--local-only. Mutually exclusive with --full-backfill.")
    f.add_argument("--local-only", action="store_true",
                   help="Local-only discovery (skip the all_emails live walk) even for a non-null "
                        "since_ws — for a bounded window backfill that must not risk the page ceiling.")
    a = sub.add_parser("apply", help="Phase B — bulk upsert staging (run under with_warehouse_lock.sh)")
    a.add_argument("--stage", default=_STAGE_DEFAULT)
    r = sub.add_parser("run", help="fetch then apply (DEV ONLY — apply takes the writer lock)")
    r.add_argument("--stage", default=_STAGE_DEFAULT)
    r.add_argument("--full-backfill", action="store_true")
    r.add_argument("--since-override", default=None)
    r.add_argument("--local-only", action="store_true")
    args = ap.parse_args(argv)

    since_override = None
    if getattr(args, "since_override", None):
        if getattr(args, "full_backfill", False):
            print("--since-override and --full-backfill are mutually exclusive.", file=sys.stderr)
            return 2
        since_override = datetime.fromisoformat(args.since_override.replace("Z", "+00:00"))
        if since_override.tzinfo is None:
            since_override = since_override.replace(tzinfo=timezone.utc)

    if os.environ.get("WAREHOUSE_PULL_THREADS") != "1":
        print("WAREHOUSE_PULL_THREADS != 1 — refusing to run (flag-gated).", file=sys.stderr)
        return 3

    creds = load_credentials()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    ceiling_hit = False
    if args.cmd in ("fetch", "run"):
        # run_fetch opens its OWN short-lived read-only connections (one per discovery, closed before
        # each pull) so the multi-hour network pull holds no DuckDB shared lock — parallel pullers must
        # not block the nightly's RW open or apply (see run_fetch docstring).
        diag = run_fetch(
            creds, run_id, args.stage,
            since_override=since_override,
            full_backfill=getattr(args, "full_backfill", False),
            local_only=getattr(args, "local_only", False),
        )
        print(json.dumps({k: v for k, v in diag.items() if k != "per_ws_runlog"}, default=str, indent=2))
        if diag.get("ceiling_hit_ws"):
            ceiling_hit = True
            print(f"HARD FAIL: pagination ceiling hit on {diag['ceiling_hit_ws']} — "
                  f"those workspaces' rows are QUARANTINED (excluded from apply, watermark NOT "
                  f"advanced); the rest applies. Investigate the ceiling-hit ws.", file=sys.stderr)
    if args.cmd in ("apply", "run"):
        # apply EXCLUDES ceiling-hit workspaces (read from the .ceiling sidecar) so their
        # truncated rows never commit / advance their watermark; complete workspaces DO apply.
        applied = run_apply(args.stage, run_id)
        print(json.dumps(applied, default=str, indent=2))
        if applied.get("ceiling_excluded"):
            ceiling_hit = True
            print(f"HARD FAIL: excluded ceiling-hit workspaces {applied['ceiling_excluded']} "
                  f"from the upsert (watermark NOT advanced for them).", file=sys.stderr)
    # Non-zero exit when ANY workspace hit the ceiling so the nightly/watchdog flags the run as
    # degraded (complete workspaces still landed; the truncated ones are excluded + must be retried).
    return 4 if ceiling_hit else 0


if __name__ == "__main__":
    sys.exit(main())
