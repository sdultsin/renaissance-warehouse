"""Fold locally-cached OUTBOUND into raw_instantly_email_message (bridge for the lagging per-lead drain).

WHY (root cause: deliverables/2026-07-19-report-consolidation/forensics/OUTBOUND-CAPTURE-ROOTCAUSE.md):
  Our outbound (ue_type=1 cold sends, ue_type=3 our/AIM replies) reaches the warehouse ONLY via the
  slow, rate-limited per-lead full-thread drain (entities/email_thread_sync.py). That lane is ~3d
  behind and was paused 2026-07-18, so recently-replied leads show inbound-only threads: of 7,074 AIM
  auto-sends only 36.6% are captured, and ~50% of AIM-answered leads still read
  reply_attribution.answered=FALSE. The MISSING outbound already sits on the box in two local logs,
  freshly pulled from the SAME /emails feed — this entity merges them into the canonical atom so
  core.email_message / core.reply_attribution reflect our real sends WITHOUT any new API load and
  WITHOUT the per-lead drain's write-contention (it reads local files only).

SOURCES (both local files; NO network):
  1. AIM auto-send log  (/root/core/aim_v3_threads.jsonl, mode=Send) — each line's top-level `msg_id`
     IS the Instantly /emails item['id'] == raw_instantly_email_message.message_id (the PK). REAL id
     -> source='aim_fold', ai_agent_id set (is_aim=true). When the per-lead drain later pulls the same
     id it UPSERTs the SAME PK (source flips to 'instantly', api_response_raw -> the real item) — so
     the AIM fold is naturally reconciled, never duplicated.
  2. Reply-QA overlay    (/root/core/reply_qa/thread_overlay.jsonl) — per-lead full threads pulled
     from /emails?lead=, but it persists only {ts, role, text_raw} (NO per-message id/campaign/ai
     flag). So overlay outbound gets a DETERMINISTIC SYNTHETIC PK ('ovl:'||sha1(lead|ue|ts)),
     source='overlay_bridge', ai_agent_id NULL (the overlay carries no AI flag — is_aim stays false,
     which is the honest value; `answered` only needs a ue3 to exist). These synthetic rows SELF-RETIRE
     via the supersede-cleanup once the real drain lands the same message with its real id.

CONVERSATION IDENTITY IS INHERITED, NOT GUESSED. Neither log carries the canonical workspace SLUG or
the campaign_id/thread_key needed for the `answered` join. But the lead's INBOUND reply (ue_type=2) is
ALREADY in the atom (that lane is fast) — it carries the correct workspace_id (slug), campaign_id,
thread_key, thread_id, lead_anchor_key, organization_id. The apply INHERITS those from the lead's own
inbound ue2 row (the reply this outbound answers: the ue2 with the greatest message_at <= the
outbound's message_at, else the closest). This (a) sidesteps the display-name->slug minefield entirely
(the AIM log's ws "Funding 2 (Ido)" is actually slug 'renaissance-5'), and (b) GUARANTEES the folded
ue3 shares (workspace_id, lead_email, thread_key) with the ue2 it answers, so core.reply_attribution's
`answered` join fires. An outbound row whose lead has NO captured inbound is SKIPPED (it can't satisfy
workspace_id NOT NULL, and `answered` could not be evaluated for it anyway) — counted as
skipped_no_inbound.

TWO-PHASE, LOCK-DISCIPLINED (mirrors email_thread_sync):
  build  (NO lock, NO network, NO db):   parse the two logs -> a JSONL staging file.
  apply  (writer flock held):            load staging -> TEMP -> inherit identity -> ONE non-destructive
                                         UPSERT -> supersede-cleanup -> CHECKPOINT.

Idempotent + bridge-safe: AIM rows keyed on the REAL id; overlay rows on a deterministic hash; the
INSERT is ON CONFLICT (message_id) DO NOTHING — the fold provides a row only while the id is absent and
NEVER clobbers a row the per-lead drain has already captured with the real /emails item (the drain's own
upsert overwrites the folded AIM row when it lands; the fold re-run is then a no-op). Fully reversible:
  DELETE FROM raw_instantly_email_message WHERE source IN ('aim_fold','overlay_bridge');

Flag-gated: does NOTHING unless WAREHOUSE_FOLD_OUTBOUND=1.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from core import db as db_module
from core.config import DB_PATH, REPO_ROOT
# Reuse the canonical atom column list so this loader can NEVER drift from email_thread_sync's
# INSERT contract (single source of truth for the atom columns).
from entities.email_thread_sync import _COLS, _manifest_path

logger = logging.getLogger("entities.email_outbound_fold")

_AIM_LOG_DEFAULT = os.environ.get("WAREHOUSE_FOLD_AIM_LOG", "/root/core/aim_v3_threads.jsonl")
_OVERLAY_DEFAULT = os.environ.get("WAREHOUSE_FOLD_OVERLAY", "/root/core/reply_qa/thread_overlay.jsonl")
_STAGE_DEFAULT = os.environ.get(
    "WAREHOUSE_FOLD_STAGE", str(Path(DB_PATH).parent / "email_outbound_fold_stage.jsonl")
)
# Supersede tolerance: an overlay_bridge row is retired once a REAL (non-bridge) outbound row lands for
# the same (workspace_id, lead_email, ue_type) within this many seconds of it.
_SUPERSEDE_TOL_S = int(os.environ.get("WAREHOUSE_FOLD_SUPERSEDE_TOL_S", "600"))

# overlay role string -> ue_type (outbound only; 'Lead' = inbound ue2, already captured, skipped)
_OVERLAY_ROLE_UE = {
    "Cold email (us)": 1,
    "Reply (us)": 3,
    "Us": 3,  # unknown-direction outbound -> treat as our reply
}

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(s: str | None) -> str | None:
    """Cheap text approximation for body_text (the atom stores cleaned text + raw html separately).
    Not load-bearing for `answered`; kept simple + dependency-free."""
    if not s:
        return None
    txt = _TAG_RE.sub(" ", s)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt or None


def _norm_ts(ts: str | None) -> str | None:
    """Return an ISO-8601 string DuckDB TIMESTAMPTZ can read, or None if unusable."""
    if not ts:
        return None
    ts = str(ts).strip()
    if not ts:
        return None
    return ts  # '2026-07-17T17:54:23.000Z' / '...+00:00' both parse as TIMESTAMPTZ


def _synth_pk(lead: str, ue: int, ts: str) -> str:
    h = hashlib.sha1(f"{lead}|{ue}|{ts}".encode()).hexdigest()[:24]
    return "ovl:" + h


def _stage_row(
    message_id: str,
    lead_email: str,
    message_at: str,
    ue_type: int,
    source: str,
    ai_agent_id: str | None,
    subject: str | None,
    body_text: str | None,
    body_html: str | None,
    from_email: str | None,
    fold_kind: str,
) -> dict:
    """Assemble one staging row. api_response_raw is SYNTHESIZED here (build phase) carrying exactly the
    fields the curated view reads: ai_agent_id (-> is_aim), id, ue_type, lead, timestamp_email, plus a
    _fold_source provenance marker. When the real drain later pulls a real-id row it overwrites
    api_response_raw with the true item (unconditional-overwrite col), so is_aim recomputes from the
    real ai_agent_id — convergent."""
    api_raw = {
        "id": message_id,
        "ue_type": ue_type,
        "ai_agent_id": ai_agent_id,  # None for overlay; a value (is_aim=true) for AIM sends
        "lead": lead_email,
        "timestamp_email": message_at,
        "_fold_source": fold_kind,
    }
    return {
        "message_id": message_id,
        "lead_email": lead_email,
        "message_at": message_at,
        "ue_type": ue_type,
        "source": source,
        "ai_agent_id": ai_agent_id,
        "subject": subject,
        "body_text": body_text,
        "body_html": body_html,
        "from_email": from_email,
        "api_response_raw": json.dumps(api_raw, default=str),
        "fold_kind": fold_kind,
    }


# ── build (Phase A — NO lock/network/db) ────────────────────────────────────────
def build_stage(aim_log: str, overlay: str, stage_path: str) -> dict:
    """Parse the two local logs into a de-duplicated JSONL staging file. Pure file I/O."""
    seen: set[str] = set()
    n_aim = n_overlay = n_dup = 0
    with open(stage_path, "w") as out:
        # 1) AIM auto-send log — REAL ids, is_aim=true.
        if os.path.exists(aim_log):
            with open(aim_log) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    if d.get("mode") != "Send":
                        continue
                    mid = d.get("msg_id")
                    lead = (d.get("email") or "").lower().strip()
                    ts = _norm_ts(d.get("sent_ts"))
                    if not (mid and lead and ts):
                        continue
                    if mid in seen:
                        n_dup += 1
                        continue
                    seen.add(mid)
                    # subject/from_email from the ue3 entry of the rendered thread, if present.
                    subj = from_email = None
                    for m in reversed(d.get("thread") or []):
                        if m.get("ue") == 3:
                            subj = m.get("subj")
                            from_email = m.get("frm")
                            break
                    out.write(json.dumps(_stage_row(
                        message_id=mid, lead_email=lead, message_at=ts, ue_type=3,
                        source="aim_fold", ai_agent_id=(d.get("agent") or "AIM V3"),
                        subject=subj, body_text=(d.get("ai_message") or None), body_html=None,
                        from_email=from_email, fold_kind="aim_v3_threads",
                    ), default=str) + "\n")
                    n_aim += 1
        else:
            logger.warning("AIM log not found at %s — skipping AIM fold", aim_log)

        # 2) Reply-QA overlay — synthetic ids, is_aim=false (honest; no flag in the overlay).
        if os.path.exists(overlay):
            with open(overlay) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    lead = (d.get("le") or "").lower().strip()
                    if not lead:
                        continue
                    for m in d.get("msgs") or []:
                        ue = _OVERLAY_ROLE_UE.get(m.get("role"))
                        if ue is None:  # 'Lead' inbound or unknown -> skip (inbound already captured)
                            continue
                        ts = _norm_ts(m.get("ts"))
                        if not ts:
                            continue
                        pk = _synth_pk(lead, ue, ts)
                        if pk in seen:
                            n_dup += 1
                            continue
                        seen.add(pk)
                        raw = m.get("text_raw") or ""
                        is_html = raw.lstrip().startswith("<")
                        out.write(json.dumps(_stage_row(
                            message_id=pk, lead_email=lead, message_at=ts, ue_type=ue,
                            source="overlay_bridge", ai_agent_id=None, subject=None,
                            body_text=(_strip_html(raw) if is_html else (raw.strip() or None)),
                            body_html=(raw if is_html else None), from_email=None,
                            fold_kind="thread_overlay",
                        ), default=str) + "\n")
                        n_overlay += 1
        else:
            logger.warning("overlay not found at %s — skipping overlay fold", overlay)

    diag = {"aim_rows": n_aim, "overlay_rows": n_overlay, "duplicates_dropped": n_dup,
            "staged_total": n_aim + n_overlay, "stage_path": stage_path}
    logger.info("BUILD-STAGE aim_rows=%d overlay_rows=%d dup_dropped=%d stage=%s",
                n_aim, n_overlay, n_dup, stage_path)
    return diag


_STAGE_COL_TYPES = {
    "message_id": "VARCHAR", "lead_email": "VARCHAR", "message_at": "TIMESTAMPTZ",
    "ue_type": "INTEGER", "source": "VARCHAR", "ai_agent_id": "VARCHAR", "subject": "VARCHAR",
    "body_text": "VARCHAR", "body_html": "VARCHAR", "from_email": "VARCHAR",
    "api_response_raw": "VARCHAR", "fold_kind": "VARCHAR",
}


def _read_stage_sql(stage_path: str) -> str:
    cols = ", ".join(f"'{c}': '{t}'" for c, t in _STAGE_COL_TYPES.items())
    return (
        f"read_json('{stage_path}', format='newline_delimited', "
        f"columns={{{cols}}}, ignore_errors=true, maximum_object_size=268435456)"
    )


# ── apply (Phase B — writer flock held) ─────────────────────────────────────────
def run_apply(stage_path: str, run_id: str, db_path: Path | None = None) -> dict:
    if not os.path.exists(stage_path):
        logger.info("apply: no staging file at %s — nothing to apply", stage_path)
        return {"messages_upserted": 0}
    con = db_module.connect(db_path)  # acquires the writer flock (unless a wrapper already holds it)
    try:
        return _apply_core(con, stage_path, run_id)
    finally:
        con.close()


def _apply_core(con, stage_path: str, run_id: str) -> dict:
    now = datetime.now(timezone.utc)

    con.execute("DROP TABLE IF EXISTS _stage_fold")
    con.execute(
        f"""
        CREATE TEMP TABLE _stage_fold AS
        SELECT * EXCLUDE (rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY message_id ORDER BY message_at DESC NULLS LAST) AS rn
          FROM {_read_stage_sql(stage_path)}
        ) WHERE rn = 1
        """
    )
    staged = con.execute("SELECT count(*) FROM _stage_fold").fetchone()[0]
    if not staged:
        con.execute("DROP TABLE IF EXISTS _stage_fold")
        logger.info("apply: staging empty — nothing to write")
        return {"messages_upserted": 0, "staged": 0}

    # Inherit conversation identity from the lead's own captured inbound ue2 row (the reply this
    # outbound answers). AMBIGUITY GUARD (moderator deep-review, blocking-class metric-corruption risk):
    # a lead active in MULTIPLE concurrent campaigns has inbound replies under >1 thread_key, and a bare
    # nearest-inbound pick could attach the outbound to the WRONG thread — silently flipping
    # reply_attribution.answered=TRUE for a campaign that never got that reply. So we ONLY inherit when
    # the candidate inbound set collapses to exactly ONE thread_key; otherwise the outbound is SKIPPED
    # (skipped_ambiguous) rather than guessed. Candidate set = the lead's inbounds AT/BEFORE the outbound
    # (the replies it could be answering); if none are at/before (e.g. a ue1 cold send that predates the
    # first reply) fall back to ALL the lead's inbounds. Within the chosen unambiguous set, pick the
    # closest inbound (greatest at/before, else nearest).
    con.execute("DROP TABLE IF EXISTS _fold_ident")
    con.execute("DROP TABLE IF EXISTS _fold_cand")
    con.execute(
        """
        CREATE TEMP TABLE _fold_cand AS
        WITH inb AS (
          SELECT lower(trim(lead_email)) AS le, workspace_id, organization_id, campaign_id,
                 thread_key, thread_id, lead_anchor_key, message_at
          FROM raw_instantly_email_message
          WHERE ue_type = 2 AND workspace_id IS NOT NULL
        ),
        cand AS (
          SELECT s.message_id, s.message_at AS out_at,
                 i.workspace_id, i.organization_id, i.campaign_id, i.thread_key,
                 i.thread_id, i.lead_anchor_key, i.message_at AS in_at,
                 (i.message_at <= s.message_at) AS at_or_before
          FROM _stage_fold s JOIN inb i ON i.le = s.lead_email
        ),
        flags AS (
          SELECT message_id, max(CASE WHEN at_or_before THEN 1 ELSE 0 END) AS has_before
          FROM cand GROUP BY message_id
        )
        -- scope to the at/before inbounds when any exist, else all of the lead's inbounds
        SELECT c.* FROM cand c JOIN flags f USING (message_id)
        WHERE (f.has_before = 1 AND c.at_or_before) OR (f.has_before = 0)
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE _fold_ident AS
        WITH amb AS (
          SELECT message_id, count(DISTINCT thread_key) AS n_threads FROM _fold_cand GROUP BY message_id
        ),
        pick AS (
          SELECT c.message_id, c.workspace_id, c.organization_id, c.campaign_id,
                 c.thread_key, c.thread_id, c.lead_anchor_key,
                 row_number() OVER (
                   PARTITION BY c.message_id
                   ORDER BY c.at_or_before DESC, abs(epoch(c.in_at) - epoch(c.out_at)) ASC
                 ) AS rn
          FROM _fold_cand c JOIN amb a USING (message_id)
          WHERE a.n_threads = 1              -- unambiguous single-thread lead only
        )
        SELECT * EXCLUDE (rn) FROM pick WHERE rn = 1
        """
    )
    inheritable = con.execute("SELECT count(*) FROM _fold_ident").fetchone()[0]
    with_inbound = con.execute(
        "SELECT count(DISTINCT message_id) FROM _fold_cand"
    ).fetchone()[0]
    skipped_no_inbound = staged - with_inbound
    skipped_ambiguous = with_inbound - inheritable

    # INSERT ... ON CONFLICT DO NOTHING. Column order == _COLS (imported from email_thread_sync so it
    # can't drift). DO NOTHING (not DO UPDATE) is the correct BRIDGE precedence: the fold provides a row
    # ONLY while the id is absent; if the per-lead drain later captures the same message (AIM rows share
    # the REAL id) its own INSERT...ON CONFLICT DO UPDATE overwrites the row with the true /emails item,
    # and a subsequent fold re-run is a no-op — so the fold can NEVER clobber real drain-captured data
    # back to synthesized values. Fully idempotent.
    const = {
        "rfc_message_id": "NULL",
        "direction": "'outbound'",
        "step_path": "NULL",
        "to_emails": "NULL",
        "eaccount": "NULL",
        "_loaded_at": "$loaded_at",
        "_run_id": "$run_id",
    }
    ident = {"thread_id", "thread_key", "lead_anchor_key", "workspace_id",
             "organization_id", "campaign_id"}
    stage_cols = {"message_id", "lead_email", "ue_type", "subject", "body_text",
                  "body_html", "from_email", "message_at", "source", "api_response_raw"}
    select_parts = []
    for c in _COLS:
        if c in const:
            select_parts.append(f"{const[c]} AS {c}")
        elif c in ident:
            select_parts.append(f"d.{c} AS {c}")
        elif c in stage_cols:
            select_parts.append(f"s.{c} AS {c}")
        else:
            raise RuntimeError(f"unmapped atom column in fold INSERT: {c}")
    select_sql = ",\n               ".join(select_parts)

    # Net-new ids this run (absent from the atom before the insert) — the exact set DO NOTHING will
    # insert, and the precise per-run rollback manifest (deleting these can never touch a drain-owned row).
    con.execute("DROP TABLE IF EXISTS _fold_new")
    con.execute(
        """
        CREATE TEMP TABLE _fold_new AS
        SELECT s.message_id FROM _stage_fold s JOIN _fold_ident d USING (message_id)
        LEFT JOIN raw_instantly_email_message r USING (message_id)
        WHERE r.message_id IS NULL
        """
    )
    changed = con.execute("SELECT count(*) FROM _fold_new").fetchone()[0]

    con.execute("BEGIN")
    con.execute(
        f"""
        INSERT INTO raw_instantly_email_message ({", ".join(_COLS)})
        SELECT {select_sql}
        FROM _stage_fold s
        JOIN _fold_ident d USING (message_id)
        ON CONFLICT (message_id) DO NOTHING
        """,
        {"loaded_at": now, "run_id": run_id},
    )
    con.execute("COMMIT")

    upserted = con.execute(
        "SELECT count(*) FROM _stage_fold s JOIN _fold_ident d USING (message_id)"
    ).fetchone()[0]

    # Supersede-cleanup: retire an overlay_bridge row once a REAL (non-bridge) outbound row for the
    # same (workspace_id, lower(lead_email), ue_type) has landed within +/- tolerance of it. This makes
    # the synthetic-PK bridge SELF-RETIRING as the per-lead drain catches up (matching the overlay's
    # designed retirement) so ue3/ue1 counts don't stay double-counted.
    con.execute("BEGIN")
    superseded = con.execute(
        f"""
        WITH bridge AS (
          SELECT message_id, workspace_id, lower(trim(lead_email)) AS le, ue_type, message_at
          FROM raw_instantly_email_message WHERE source = 'overlay_bridge'
        ),
        real AS (
          SELECT workspace_id, lower(trim(lead_email)) AS le, ue_type, message_at
          FROM raw_instantly_email_message
          WHERE direction = 'outbound' AND source <> 'overlay_bridge'
        )
        SELECT count(*) FROM bridge b
        WHERE EXISTS (
          SELECT 1 FROM real r
          WHERE r.workspace_id = b.workspace_id AND r.le = b.le AND r.ue_type = b.ue_type
            AND abs(epoch(r.message_at) - epoch(b.message_at)) <= {_SUPERSEDE_TOL_S}
        )
        """
    ).fetchone()[0]
    if superseded:
        con.execute(
            f"""
            DELETE FROM raw_instantly_email_message b
            WHERE b.source = 'overlay_bridge'
              AND EXISTS (
                SELECT 1 FROM raw_instantly_email_message r
                WHERE r.direction = 'outbound' AND r.source <> 'overlay_bridge'
                  AND r.workspace_id = b.workspace_id
                  AND lower(trim(r.lead_email)) = lower(trim(b.lead_email))
                  AND r.ue_type = b.ue_type
                  AND abs(epoch(r.message_at) - epoch(b.message_at)) <= {_SUPERSEDE_TOL_S}
              )
            """
        )
    con.execute("COMMIT")

    ids = [r[0] for r in con.execute("SELECT message_id FROM _fold_new").fetchall()]
    manifest = _manifest_path(f"fold_{run_id}")
    manifest.write_text(("\n".join(ids) + "\n") if ids else "")

    con.execute("CHECKPOINT")
    con.execute("DROP TABLE IF EXISTS _stage_fold")
    con.execute("DROP TABLE IF EXISTS _fold_cand")
    con.execute("DROP TABLE IF EXISTS _fold_ident")
    con.execute("DROP TABLE IF EXISTS _fold_new")

    out = {
        "staged": int(staged),
        "inheritable": int(inheritable),
        "skipped_no_inbound": int(skipped_no_inbound),
        "skipped_ambiguous": int(skipped_ambiguous),
        "messages_upserted": int(upserted),
        "newly_inserted": int(changed),
        "overlay_superseded_deleted": int(superseded),
        "manifest": str(manifest),
    }
    logger.info(
        "RUNLOG-FOLD run_id=%s staged=%d inheritable=%d skipped_no_inbound=%d skipped_ambiguous=%d "
        "upserted=%d newly_inserted=%d overlay_superseded=%d manifest=%s",
        run_id, staged, inheritable, skipped_no_inbound, skipped_ambiguous, upserted, changed,
        superseded, manifest,
    )
    return out


def register(registry) -> None:  # noqa: ANN001
    """No-op: like email_thread_sync, this runs POST-orchestrator (scripts/nightly.sh) so its apply
    never executes under the orchestrator's held writer flock. Kept registered so discovery doesn't
    error."""
    return None


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="Phase A — parse the local logs into a JSONL stage (NO lock)")
    b.add_argument("--aim-log", default=_AIM_LOG_DEFAULT)
    b.add_argument("--overlay", default=_OVERLAY_DEFAULT)
    b.add_argument("--stage", default=_STAGE_DEFAULT)
    a = sub.add_parser("apply", help="Phase B — upsert the stage (run under with_warehouse_lock.sh)")
    a.add_argument("--stage", default=_STAGE_DEFAULT)
    r = sub.add_parser("run", help="build then apply (apply takes the writer lock)")
    r.add_argument("--aim-log", default=_AIM_LOG_DEFAULT)
    r.add_argument("--overlay", default=_OVERLAY_DEFAULT)
    r.add_argument("--stage", default=_STAGE_DEFAULT)
    args = ap.parse_args(argv)

    if os.environ.get("WAREHOUSE_FOLD_OUTBOUND") != "1":
        print("WAREHOUSE_FOLD_OUTBOUND != 1 — refusing to run (flag-gated).", file=sys.stderr)
        return 3

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.cmd in ("build", "run"):
        diag = build_stage(args.aim_log, args.overlay, args.stage)
        print(json.dumps(diag, default=str, indent=2))
    if args.cmd in ("apply", "run"):
        applied = run_apply(args.stage, run_id)
        print(json.dumps(applied, default=str, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
