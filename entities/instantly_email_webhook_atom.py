"""Instantly email-webhook → thread-atom drain (2026-07-17).

Drains the real-time Instantly email-BODY webhook capture
(raw_comms_instantly_email_event ← comms.instantly_email_event, mig 045) into the
canonical both-direction thread atom raw_instantly_email_message (DDL 1037). This is
the PUSH path that fills the OUTBOUND gap the rate-limited per-lead /emails?lead= drain
(entities/email_thread_sync.py) can't keep current: email_sent carries our cold sends
(ue1) + our replies (ue3) with the body in email_html; reply_received carries prospect
replies (ue2) with reply_text/html.

WHY THIS IS COLLAPSE-SAFE (verified 2026-07-17, read-only, live API):
  * The webhook `email_id` EQUALS the GET /emails item `id`, which IS the atom PK
    `message_id`. So a webhook-sourced atom row and a per-lead-drain row for the same
    physical email share message_id and are ONE row — never a duplicate.
  * This drain is INSERT-ONLY via ON CONFLICT (message_id) DO NOTHING: it only fills
    message_ids the atom does NOT already have (the gap). It NEVER overwrites a row the
    per-lead drain wrote — email_thread_sync stays authoritative for repliers (richer:
    composite step_path, rfc_message_id, i_status/ai_agent_id in api_response_raw). When
    the per-lead drain later pulls a replier whose sends we pre-filled, ITS ON CONFLICT DO
    UPDATE enriches the mutable columns; the atom's IMMUTABLE cols (direction/ue_type/
    step_path/lead_email/workspace_id) keep the first (webhook) writer's values by the
    atom's own contract — see the field-mapping notes below.

MAPPING (webhook capture → atom columns):
  message_id      = email_id (PK; == /emails id).
  rfc_message_id  = NULL       (webhook carries no RFC822 header).
  thread_id       = NULL       (webhook carries no thread_id) → lead_anchor_key = '' (QA-only).
  thread_key      = campaign_id, or 'unattributed:' when campaign_id is NULL (R1/R1a). Every
                    observed webhook event carries campaign_id, so this matches the per-lead
                    drain's thread_key (= campaign_id).
  workspace_id    = workspace  (canonical slug from the webhook ?ws= param; NOT NULL; the atom
                    uses the SAME slug — verified: 'prospects-power' for Funding 3).
  organization_id = organization_id (Instantly org UUID; provenance).
  direction       = 'inbound' (reply_received / ue2) else 'outbound' (from ue_type — R9).
  ue_type         = as captured (1 send / 2 reply / 3 our reply). NB the webhook cannot see
                    ai_agent_id, so a ue3 AIM reply lands is_aim=false in the curated view
                    until the per-lead drain re-pulls it (api_response_raw is unconditionally
                    refreshed by that drain). Documented residual.
  step_path       = the webhook integer step as a STRING (a bare 1-based sequence index, e.g.
                    '5'), NULL on replies/ue3. NB this differs in FORMAT from the per-lead
                    drain's composite path ('0_4_0'); step_path is IMMUTABLE so whichever
                    writer lands a given message_id first sets it. Functionally equivalent for
                    thread reconstruction; a resend captured by both paths could, in the rare
                    cross-format case, count as 2 distinct steps in core.email_thread.n_seq_sends.
  subject         = clean_subject(subject).
  body_text       = clean_html(body_html, body_text)  — the CANONICAL §7 cleaner (quote-cut +
                    tag-strip + spintax-strip), reused from core.email_body_clean so webhook
                    rows read IDENTICALLY to per-lead-drain rows. (For email_sent the payload
                    text is empty and the body lives in email_html; the worker already derived
                    a plaintext body_text, but we re-clean from the raw html here for parity.)
  body_html       = body_html (raw).
  from_email      = eaccount (outbound) / lead_email (inbound).
  to_emails       = lead_email (outbound) / eaccount (inbound).
  eaccount        = eaccount.
  message_at      = message_at.
  source          = 'instantly_webhook' — provenance. A row the per-lead drain also pulls has
                    its source healed to 'instantly' by that drain's ON CONFLICT; a row that
                    stays 'instantly_webhook' is one ONLY the webhook captured (the filled gap).
                    source is NOT in the G2 payload hash, so this never perturbs change counts.
  api_response_raw= the raw webhook payload JSON (drill-through / re-derivation).

READS THE LOCAL raw_comms_instantly_email_event snapshot (refreshed by the comms_mirror
_TABLES REPLACE step earlier in the SAME "comms_mirror" phase — deterministic: comms_mirror.py
imports before instantly_email_webhook_atom.py, so its phase entity runs first), anti-joined
against the atom so only NOT-yet-present message_ids are processed (bounded work; robust to
arbitrary mirror-outage backfill — it always reconciles the full gap). Single-writer-safe:
runs INSIDE the orchestrator's serialized writer window, so it opens no second writer and adds
no contention to the real-time path.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from core.email_body_clean import clean_html, clean_subject
from core.registry import PhaseResult

if TYPE_CHECKING:
    from core.registry import RunContext

logger = logging.getLogger(__name__)

SNAPSHOT = "raw_comms_instantly_email_event"
ATOM = "raw_instantly_email_message"

# Atom columns in DDL 1037 order (minus workspace_slug_norm — nullable, filled by the nightly
# mof_bi_history enrichment, exactly as the per-lead drain leaves it).
_ATOM_COLS = [
    "message_id", "rfc_message_id", "thread_id", "thread_key", "lead_anchor_key",
    "workspace_id", "organization_id", "campaign_id", "lead_email", "direction",
    "ue_type", "step_path", "subject", "body_text", "body_html", "from_email",
    "to_emails", "eaccount", "message_at", "source", "api_response_raw",
    "_loaded_at", "_run_id",
]

# Source columns pulled from the local snapshot for the rows not yet in the atom.
_SELECT_NEW = f"""
    SELECT
        s.email_id, s.event_type, s.workspace, s.organization_id, s.campaign_id,
        s.lead_email, s.eaccount, s.direction, s.ue_type, s.step,
        s.subject, s.body_text, s.body_html, s.message_at, s.raw_json
    FROM {SNAPSHOT} s
    LEFT JOIN {ATOM} r ON r.message_id = s.email_id
    WHERE r.message_id IS NULL
      AND s.email_id  IS NOT NULL
      AND s.lead_email IS NOT NULL AND trim(s.lead_email) <> ''
      AND s.workspace  IS NOT NULL AND trim(s.workspace)  <> ''
"""


def _snapshot_exists(conn) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [SNAPSHOT],
    ).fetchone()
    return bool(row and row[0])


def _build_atom_row(rec: tuple, loaded_at, run_id: str) -> tuple:
    """Map one snapshot row (tuple in _SELECT_NEW order) → an atom row tuple in _ATOM_COLS order."""
    (email_id, _event_type, workspace, organization_id, campaign_id,
     lead_email, eaccount, direction, ue_type, step,
     subject, body_text, body_html, message_at, raw_json) = rec

    lead_email = (lead_email or "").lower().strip()
    is_outbound = direction == "outbound"
    thread_key = campaign_id if campaign_id else "unattributed:"
    step_path = str(step) if step is not None else None
    # Canonical §7 clean (quote-cut + tag-strip + spintax) from the RAW html, with the
    # captured text as fallback — identical semantics to the per-lead drain (clean_body).
    clean_text = clean_html(body_html, body_text) or None
    clean_subj = clean_subject(subject)
    from_email = eaccount if is_outbound else lead_email
    to_emails = lead_email if is_outbound else eaccount
    # api_response_raw: keep the exact captured payload JSON (already a JSON string in the
    # snapshot; normalize to a compact string).
    if isinstance(raw_json, (dict, list)):
        api_raw = json.dumps(raw_json, default=str)
    else:
        api_raw = raw_json if raw_json is not None else None

    return (
        email_id,            # message_id
        None,                # rfc_message_id
        None,                # thread_id
        thread_key,          # thread_key
        "",                  # lead_anchor_key
        workspace,           # workspace_id (slug)
        organization_id,     # organization_id
        campaign_id,         # campaign_id
        lead_email,          # lead_email
        direction,           # direction
        ue_type,             # ue_type
        step_path,           # step_path
        clean_subj,          # subject
        clean_text,          # body_text
        body_html,           # body_html (raw)
        from_email,          # from_email
        to_emails,           # to_emails
        eaccount,            # eaccount
        message_at,          # message_at
        "instantly_webhook", # source
        api_raw,             # api_response_raw
        loaded_at,           # _loaded_at
        run_id,              # _run_id
    )


def _run(ctx: "RunContext") -> PhaseResult:
    from datetime import datetime, timezone

    conn = ctx.db
    if not _snapshot_exists(conn):
        logger.info("%s absent — comms_mirror has not created it yet; skipping (no-op)", SNAPSHOT)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "snapshot_absent"})

    new_rows = conn.execute(_SELECT_NEW).fetchall()
    if not new_rows:
        logger.info("instantly_email_webhook_atom: 0 new webhook rows to drain into %s", ATOM)
        return PhaseResult(rows_in=0, rows_out=0, notes={"new": 0})

    loaded_at = datetime.now(timezone.utc)
    atom_rows = [_build_atom_row(r, loaded_at, ctx.run_id) for r in new_rows]

    placeholders = "(" + ", ".join(["?"] * len(_ATOM_COLS)) + ")"
    insert_sql = (
        f"INSERT INTO {ATOM} ({', '.join(_ATOM_COLS)}) VALUES {placeholders} "
        f"ON CONFLICT (message_id) DO NOTHING"
    )

    before = conn.execute(f"SELECT count(*) FROM {ATOM}").fetchone()[0]
    conn.execute("BEGIN")
    try:
        conn.executemany(insert_sql, atom_rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    after = conn.execute(f"SELECT count(*) FROM {ATOM}").fetchone()[0]
    inserted = after - before

    logger.info(
        "instantly_email_webhook_atom: %d candidate webhook rows, %d inserted into %s "
        "(DO NOTHING skipped %d already-present)",
        len(atom_rows), inserted, ATOM, len(atom_rows) - inserted,
    )
    return PhaseResult(
        rows_in=len(atom_rows), rows_out=inserted,
        notes={"candidates": len(atom_rows), "inserted": inserted},
    )


def register(registry) -> None:
    # Same phase as the comms mirror so it runs in the serialized writer window, AFTER the
    # comms_mirror _TABLES REPLACE has refreshed raw_comms_instantly_email_event (deterministic
    # alphabetical registration order: comms_mirror.py < instantly_email_webhook_atom.py).
    registry.add_phase("comms_mirror", "instantly_email_webhook_atom", _run)
