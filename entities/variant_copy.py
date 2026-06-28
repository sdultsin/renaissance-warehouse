"""Per-variant copy: unspintax into 4 clean columns, accumulate-only.

Registered in the 'derived' phase. Populates core.variant_copy (DDL: variant_copy
table) with one row per (campaign_id, step, content_hash) for every Instantly
campaign's copy, exposing exactly four copy columns:

    subject_raw / subject_clean / body_raw / body_clean

where *_clean = the *_raw text with every spin block resolved to its FIRST option,
recursively. See the DDL header for the full contract. Key points:

  * KEY = (campaign_id, step, content_hash) where content_hash = md5(subject_raw +
    0x1F + body_raw). INSERT ... ON CONFLICT DO NOTHING, so identical copy already
    present is a no-op and only new/changed copy adds a row.
  * STRICTLY NON-DESTRUCTIVE: we only ever INSERT. A variant or campaign that
    disappears from Instantly is never deleted or blanked -- its rows persist. (This
    is why it is a TABLE, not a view.)
  * SKIP GRACEFULLY (no row, no error): campaigns with empty/unparseable sequence_raw
    (completed/deleted campaigns land here), and individual variants whose raw copy is
    malformed (unbalanced braces, or spin that will not fully resolve).
  * The first nightly run is the initial backfill.

UNSPINTAX handles BOTH fleet syntaxes -- double-brace {{RANDOM|a|b}} (RANDOM keyword,
spaces tolerated) and RANDOM-less {{a|b}}, plus legacy single-brace {a|b}. It KEEPS
personalization tokens ({{firstName}}, {{companyName|there}}, {companyName }) and Liquid
{% ... %} control tags verbatim (spin inside Liquid branches IS resolved). Personalization
is recognised by a known-variable allowlist (PERSONALIZATION_VARS) -- so a RANDOM-less
double-brace block whose first token is NOT a known var (e.g. {{We work with|We help}}) is
correctly treated as spin. Validated 2026-06-28: byte-identical to an independent reference
on all 10,757 well-formed variants of the live fleet; the only divergences are malformed
source copy, which this skips.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.variant_copy")

# Known personalization / enrichment variables (lowercased). A double-brace block
# {{VAR}} or {{VAR|fallback}} whose first token is one of these is KEPT VERBATIM, not
# treated as spin. Derived from the live fleet's no-pipe {{token}} vocabulary (2026-06-28)
# plus common standard merge tags. Extend this list if new merge variables are introduced.
PERSONALIZATION_VARS = {
    "accountsignature", "ai_bridge", "ai_icp", "ai_observation", "ai_subject",
    "aibusinesschallenge", "annualsavings", "cashgap", "categories", "city",
    "companyname", "companyuse", "customline", "derived_chip", "derived_hs_category",
    "estimatedsavings", "first_name", "firstname", "fundingnumber", "general_industry",
    "industry", "jobtitle", "location", "lookslike", "peergroup", "phone",
    "proof_number", "ratings", "revive", "sendername", "sendingaccountfirstname",
    "sendingaccountname", "shanghailane", "si", "socialproof", "stage_v1_identitychip",
    "stage_v6_opener", "text", "thrive", "topdepartureport", "v_use",
    # common standard merge tags (proactive, may not appear yet):
    "lastname", "last_name", "company", "companydomain", "email", "title", "website",
    "state", "country", "linkedin", "sendingaccountlastname",
}

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


# ---------------------------------------------------------------------------
# Unspintax parser (recursive, first-option). Self-contained reference parser.
# ---------------------------------------------------------------------------
def _scan_block(s: str, i: int) -> tuple[int, bool]:
    """s[i] == '{'. Return (end_exclusive, is_double) for the brace block via generic
    brace-depth counting; Liquid {% ... %} inside is skipped (its braces don't count)."""
    n = len(s)
    depth = 0
    j = i
    is_double = (i + 1 < n and s[i + 1] == "{")
    while j < n:
        if s[j] == "{" and j + 1 < n and s[j + 1] == "%":
            k = s.find("%}", j)
            if k == -1:
                j += 1
                continue
            j = k + 2
            continue
        if s[j] == "{":
            depth += 1
            j += 1
            continue
        if s[j] == "}":
            depth -= 1
            j += 1
            if depth == 0:
                return j, is_double
            continue
        j += 1
    return n, is_double


def _split_top(inner: str) -> list[str]:
    """Split on top-level '|' respecting nested braces and Liquid tags."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    i = 0
    n = len(inner)
    while i < n:
        c = inner[i]
        if c == "{" and i + 1 < n and inner[i + 1] == "%":
            k = inner.find("%}", i)
            if k == -1:
                buf.append(c)
                i += 1
                continue
            buf.append(inner[i:k + 2])
            i = k + 2
            continue
        if c == "{":
            depth += 1
            buf.append(c)
            i += 1
            continue
        if c == "}":
            depth -= 1
            buf.append(c)
            i += 1
            continue
        if c == "|" and depth == 0:
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    parts.append("".join(buf))
    return parts


def _is_personalization(parts: list[str]) -> bool:
    """A double-brace block is personalization (keep verbatim) iff its first top-level
    token is a known merge variable. RANDOM and unknown tokens => spin."""
    first = parts[0].strip()
    if first.upper() == "RANDOM":
        return False
    return bool(_IDENT.match(first)) and first.lower() in PERSONALIZATION_VARS


def unspin(s: str | None, _depth: int = 0) -> str | None:
    """Resolve every spin block to its first option, recursively. Keep personalization
    tokens and Liquid tags verbatim."""
    if s is None:
        return None
    if _depth > 60:
        return s
    out: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == "{" and i + 1 < n and s[i + 1] == "%":          # Liquid tag: keep
            k = s.find("%}", i)
            if k == -1:
                out.append(c)
                i += 1
                continue
            out.append(s[i:k + 2])
            i = k + 2
            continue
        if c == "{":
            end, is_double = _scan_block(s, i)
            block = s[i:end]
            inner = block[2:-2] if is_double else block[1:-1]
            parts = _split_top(inner)
            if len(parts) <= 1:                                  # no pipe: keep verbatim
                out.append(block)
            elif is_double:
                if parts[0].strip().upper() == "RANDOM":
                    out.append(unspin(parts[1] if len(parts) > 1 else "", _depth + 1))
                elif _is_personalization(parts):
                    out.append(block)                            # personalization w/ fallback
                else:
                    out.append(unspin(parts[0], _depth + 1))     # RANDOM-less double spin
            else:
                out.append(unspin(parts[0], _depth + 1))         # single-brace spin
            i = end
            continue
        out.append(c)
        i += 1
    return "".join(out)


_SPIN_RESIDUE = re.compile(r"\{\{\s*RANDOM", re.IGNORECASE)


def _is_unparseable(subject_raw: str, body_raw: str, subject_clean: str, body_clean: str) -> bool:
    """Malformed copy: unbalanced braces in the raw, or spin that did not fully resolve."""
    for raw in (subject_raw, body_raw):
        if raw.count("{") != raw.count("}"):
            return True
    for clean in (subject_clean, body_clean):
        if _SPIN_RESIDUE.search(clean):
            return True
    return False


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
def register(registry: Registry) -> None:
    registry.add_phase("derived", "variant_copy", run_variant_copy)


def _content_hash(subject_raw: str, body_raw: str) -> str:
    return hashlib.md5((subject_raw + "\x1f" + body_raw).encode("utf-8")).hexdigest()


def run_variant_copy(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    now = datetime.now(timezone.utc)

    # Latest raw row per campaign (the current copy snapshot). History rows are ignored;
    # accumulation across copy CHANGES happens in core.variant_copy itself (new hash -> new row).
    rows = db.execute(
        """
        SELECT campaign_id, workspace_id, sequence_raw
        FROM raw_instantly_campaign
        QUALIFY row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) = 1
        """
    ).fetchall()

    variants_seen = 0
    inserted = 0
    skipped_campaigns = 0
    skipped_variants = 0

    for campaign_id, workspace_id, sequence_raw in rows:
        if not sequence_raw or sequence_raw in ("[]", "null", ""):
            skipped_campaigns += 1
            continue
        try:
            sequences = json.loads(sequence_raw)
        except Exception:
            skipped_campaigns += 1
            continue
        if isinstance(sequences, dict):
            sequences = [sequences]
        if not isinstance(sequences, list):
            skipped_campaigns += 1
            continue

        for seq_idx, seq in enumerate(sequences, start=1):
            if not isinstance(seq, dict):
                continue
            for step_idx, step in enumerate(seq.get("steps") or [], start=1):
                if not isinstance(step, dict):
                    continue
                step_type = step.get("type")
                for var_idx, variant in enumerate(step.get("variants") or [], start=1):
                    if not isinstance(variant, dict):
                        continue
                    variants_seen += 1
                    subject_raw = variant.get("subject") or ""
                    body_raw = variant.get("body") or ""
                    subject_clean = unspin(subject_raw)
                    body_clean = unspin(body_raw)
                    if _is_unparseable(subject_raw, body_raw, subject_clean, body_clean):
                        skipped_variants += 1
                        continue
                    h = _content_hash(subject_raw, body_raw)
                    before = db.execute(
                        "SELECT count(*) FROM core.variant_copy "
                        "WHERE campaign_id = ? AND step = ? AND content_hash = ?",
                        [campaign_id, step_idx, h],
                    ).fetchone()[0]
                    if before:
                        continue  # identical copy already present -> no-op
                    db.execute(
                        """
                        INSERT INTO core.variant_copy
                          (campaign_id, step, content_hash, workspace_id, channel,
                           sequence_index, variant_index, step_type,
                           subject_raw, subject_clean, body_raw, body_clean,
                           first_seen_at, _run_id)
                        VALUES (?, ?, ?, ?, 'instantly', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (campaign_id, step, content_hash) DO NOTHING
                        """,
                        [
                            campaign_id, step_idx, h, workspace_id,
                            seq_idx, var_idx, step_type,
                            subject_raw, subject_clean, body_raw, body_clean,
                            now, ctx.run_id,
                        ],
                    )
                    inserted += 1

    logger.info(
        "variant_copy: %d variants seen, %d new rows inserted, "
        "%d campaigns skipped (empty/unparseable seq), %d variants skipped (malformed)",
        variants_seen, inserted, skipped_campaigns, skipped_variants,
    )
    return PhaseResult(
        rows_in=variants_seen,
        rows_out=inserted,
        notes={
            "skipped_campaigns": skipped_campaigns,
            "skipped_variants": skipped_variants,
        },
    )
