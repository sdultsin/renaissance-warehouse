"""Campaign + tag ingest.

Per workspace key (serial):
  1. List campaigns -> write raw_instantly_campaign rows
  2. List custom-tags -> build tag_id -> tag_label map
  3. For each campaign, resolve its email_tag_list -> raw_instantly_campaign_sending_tag
  4. List tag-mappings resource_type=2 -> raw_instantly_campaign_marker_tag

After all workspaces are ingested, run the resolution pass over the raws in
this run to refresh core.campaign / core.campaign_sending_tag /
core.campaign_marker_tag.

Tag-type finding (revised 2026-05-30):
  Instantly exposes ONE tag entity (GET /custom-tags) but TWO surfaces:
    - SENDING tags: referenced in campaign config `email_tag_list` (UUIDs).
      Pipeline-supabase pre-resolves these to labels in raw_pipeline_campaigns.tags
      (e.g. ["RG4843", "RG4844"] for the T-MailIn-GO campaign).
    - MARKER tags: applied via tag mappings with resource_type=2. Visible as
      the badge next to the campaign name in Instantly UI (e.g. "AIM Active").
      Only available via GET /tag-mappings?resource_type=2.

  Both populated in v1.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.campaign")

# Campaign-tag sync is DISABLED by default as of 2026-06-14 (Sam source-of-truth
# decision, memory reference_warehouse_reply_and_tag_truth_20260614): we no longer
# pull Instantly campaign tags into raw_instantly_campaign_sending_tag /
# core.campaign_sending_tag going forward. Tags are not a trustworthy attribution
# surface, so the nightly stops fetching them. EXISTING synced tags are KEPT (the
# resolution passes only touch last_seen_at on rows they re-see; with the fetch off
# no new raw rows arrive, so the existing core.campaign_sending_tag rows are left
# intact as fallback hints). Reversible: set WAREHOUSE_SYNC_CAMPAIGN_TAGS=1 to
# restore the old behavior.
SYNC_CAMPAIGN_TAGS = os.environ.get("WAREHOUSE_SYNC_CAMPAIGN_TAGS", "0") == "1"


# ---------------------------------------------------------------------
# Regex resolution rules for campaign-manager / offer / is_mca classification.
# ---------------------------------------------------------------------
_RE_MCA = re.compile(r"\b(isaac|mca|cheap leads)\b", re.IGNORECASE)

# Canonical CM token list — kept in sync with data-pipeline-v2/src/transforms.ts
# (CANONICAL_CMS + CM_ALIAS_MAP), the upstream source of truth that fills
# public.campaigns.cm_name (mirrored here as raw_pipeline_campaigns.cm_name, the
# PRIMARY cm source in v_kpi_email). This warehouse-native derivation is the
# COALESCE *fallback*; it must recognize the same 13 CMs so the fallback can't
# disagree with the primary. SAMUEL is kept distinct from SAM (separate CMs
# upstream). MARCO->MARCOS, ANDRE->ANDRES folded in. (2026-06-14: previously the
# list was {SAM,SAMUEL,LEO,IDO,EYVER,TOUKIR,TOMER,LUCAS,MAX} — missing 8 active
# historical CMs and carrying 4 tokens that never appear in any campaign name.)
_CM_ALIAS = {
    "MARCO": "MARCOS",
    "ANDRE": "ANDRES",
}
_CM_TOKENS = [
    "SAMUEL", "SAM", "LEO", "IDO", "EYVER", "ALEX", "ANDRES", "ANDRE",
    "BRENDAN", "CARLOS", "LAUTARO", "MARCOS", "MARCO", "SHAAN", "TOMI",
]
# Longest-first so SAMUEL wins over SAM, MARCOS over MARCO, ANDRES over ANDRE.
_RE_CM = re.compile(
    r"\b(" + "|".join(sorted(_CM_TOKENS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

_RE_HELOC = re.compile(r"\bHELOC\b", re.IGNORECASE)
_RE_TARIFFS = re.compile(r"\bTariff(s)?\b", re.IGNORECASE)
_RE_S125 = re.compile(r"\b(s125|section\s*125|125)\b", re.IGNORECASE)
_RE_RD = re.compile(r"\bR&?D\b|\bRD\b", re.IGNORECASE)
_RE_FUNDING = re.compile(r"\bFunding\b|\bMCA\b|\bIsaac\b", re.IGNORECASE)


def _derive_cm(name: str | None) -> str | None:
    """Match a canonical CM token (whole word, case-insensitive), alias-normalized.
    Multiple DISTINCT CMs in one name -> None (ambiguous)."""
    if not name:
        return None
    matches = {_CM_ALIAS.get(m.group(1).upper(), m.group(1).upper()) for m in _RE_CM.finditer(name)}
    if len(matches) != 1:
        return None
    return matches.pop()


def _derive_offer(name: str | None) -> str | None:
    if not name:
        return None
    if _RE_HELOC.search(name):
        return "HELOC"
    if _RE_TARIFFS.search(name):
        return "Tariffs"
    if _RE_S125.search(name):
        return "s125"
    if _RE_RD.search(name):
        return "R&D"
    if _RE_FUNDING.search(name):
        return "Funding"
    return None


def _derive_is_mca(name: str | None) -> bool:
    if not name:
        return False
    return bool(_RE_MCA.search(name))


# ---------------------------------------------------------------------
# WS7: canonical offer taxonomy + 3-pass resolver (v109 / 2026-06-21).
#
# core.campaign.offer is materialized here as the SOLE writer, resolved
#   1 workspace_map (deterministic)  2 name_regex (canonical)  3 llm_copy (cached).
# Replaces the name-only `_derive_offer` (which is no longer the offer writer; it
# still feeds nothing — _resolve_core_campaign now writes offer=NULL on first
# ingest and _resolve_campaign_offer fills it in the same nightly pass).
# Verified read-only on snapshot warehouse_20260621_063139_227.duckdb: the
# windowed per-offer split ties to workspace totals exactly
#   Business Funding 8,091,580 / Pre-IPO 107,443 (Σ 8,199,023, ex-warm-leads),
# with 0 windowed campaigns resolving to NULL.
# ---------------------------------------------------------------------
OFFER_BF, OFFER_RND, OFFER_TARIFFS, OFFER_PREIPO, OFFER_S125 = (
    "Business Funding", "R&D Credit", "Tariffs", "Pre-IPO", "Section 125")

# Single-offer workspaces, keyed by workspace_id (UUID). Verified WS7 2026-06-20 and
# RE-VERIFIED 2026-06-21 (snapshot 063139_227): the first 6 contribute the full
# 7,953,337 window send total (the other BF-mapped ws have 0 window volume today).
# MIXED workspaces are intentionally ABSENT so they fall to name_regex/LLM.
# (Slugs are pre-rename; do NOT key on them. Key on workspace_id UUID — stable.)
WORKSPACE_OFFER_MAP = {
    "d5ebf2bd-d7c8-4feb-8310-e57e6140e12a": OFFER_BF,       # Funding 3 (Leo)
    "88de6a7c-55db-4594-8851-ed7d56342a45": OFFER_BF,       # Funding 2 (Ido)
    "587765d7-e9ed-4057-85d1-eca48bcc9384": OFFER_BF,       # Renaissance 1 (Instantly)
    "6ab744f5-be81-4c5b-8333-c0c119a19b80": OFFER_BF,       # Funding 4 (Sam)
    "cdae94c6-5a88-4614-92e2-09e28a073a2e": OFFER_BF,       # Funding 1 (Samuel)
    "f02d3d50-0e9f-4687-981d-6134e789baa4": OFFER_BF,       # Funding 5 (Eyver)
    "ddbeb975-fafb-4412-ae8f-d3b478f6abff": OFFER_BF,       # Funding Canada (outlook-1)
    "7d4e8e68-db7c-427c-a5eb-9675c0d1f3e8": OFFER_BF,       # Funding UK (automated-applications)
    "a998ae0d-5b87-41a6-b62d-ab5764596cb7": OFFER_BF,       # Outlook 3
    "2aa14704-dd1d-4ca2-a527-bcc4cadf5af2": OFFER_BF,       # Renaissance 3
    "634b4eac-8903-48a6-9361-bf1d52a13476": OFFER_BF,       # RE Wholesale (equinox)
    "0424e5fb-8857-47e8-a3c2-00add9fb0a8c": OFFER_BF,       # The Eagles
    "0d9ed15e-8fb9-4427-860e-99a403cea081": OFFER_TARIFFS,  # Tariffs (frozen, label only)
}
# MIXED (no map -> name_regex then LLM):
#   396288e0 R&D Credit (window: all funding) ; 7adab6c6 Section 125 (frozen) ; 9e822ccc Max's workspace
# Warm-leads (58ae9dc4-9bc0-46d6-beb2-a1dc3e99cbf5) NOT here: its campaigns are not in core.campaign (structural).

# Canonical, longest/most-specific first. NOTE: distinct from the legacy _RE_*
# regexes above (which produce the OLD tokens). These produce canonical labels.
_RE_OFFER_PREIPO = re.compile(r"pre[\s-]?ipo", re.IGNORECASE)
_RE_OFFER_TARIFFS = re.compile(r"tariff", re.IGNORECASE)
_RE_OFFER_S125 = re.compile(r"section\s*125|s125", re.IGNORECASE)
_RE_OFFER_RD = re.compile(r"r&d|r and d|\br\s*&?\s*d\b", re.IGNORECASE)
_RE_OFFER_FUNDING = re.compile(
    r"funding|mca|\bloc\b|o2d|o2 d|isaac|construction|reseller|real[\s-]?estate",
    re.IGNORECASE,
)

# Cheap Haiku-class classifier (matches entities/reply_intent_llm.py convention).
# Anthropic key lives in the parent .env as ANTHROPIC_KEY (not ANTHROPIC_API_KEY).
_OFFER_MODEL = "claude-haiku-4-5"
_OFFER_KEY_CANDIDATES = (
    "ANTHROPIC_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_KEY_RENAISSANCE_OVERVIEW_CHATBOT",
)
_OFFER_LABELS = {OFFER_BF, OFFER_RND, OFFER_TARIFFS, OFFER_PREIPO, OFFER_S125}
_OFFER_SYSTEM_PROMPT = (
    "You classify a cold-email campaign into exactly ONE offer label, based on its "
    "name and email copy. The closed label set is: Business Funding | R&D Credit | "
    "Pre-IPO | Section 125 | Tariffs. Business Funding = revenue-based / merchant cash "
    "advance / line-of-credit business financing. R&D Credit = R&D tax credit. Pre-IPO "
    "= pre-IPO share / secondary / liquidity. Section 125 = Section 125 cafeteria / "
    "payroll benefit plan. Tariffs = tariff mitigation / duty recovery. Respond with "
    "ONLY the exact label string, or the single word __ambiguous__ if it does not "
    "clearly match one. No other text."
)


def _derive_offer_canonical(name):
    """Pass-2 name_regex. Canonical labels, longest/most-specific first."""
    if not name:
        return None
    if _RE_OFFER_PREIPO.search(name):
        return OFFER_PREIPO
    if _RE_OFFER_TARIFFS.search(name):
        return OFFER_TARIFFS
    if _RE_OFFER_S125.search(name):
        return OFFER_S125
    if _RE_OFFER_RD.search(name):
        return OFFER_RND
    if _RE_OFFER_FUNDING.search(name):
        return OFFER_BF
    return None


_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_SPINTAX = re.compile(r"\{\{[^{}]*\}\}")
_RE_WS = re.compile(r"\s+")


def _parse_sequence_text(seq_raw):
    """Flatten raw_instantly_campaign.sequence_raw (JSON: list of
    {steps:[{variants:[{subject,body}]}]}) into plain classifier text — subjects +
    HTML-stripped bodies, spintax markers removed. Returns '' on any parse failure.
    Runs in the GENERATOR only (it reads raw, never a serving view), so no
    TIMESTAMPTZ column is touched. Deterministic for the md5 content_hash."""
    if not seq_raw:
        return ""
    try:
        seqs = json.loads(seq_raw) if isinstance(seq_raw, str) else seq_raw
    except Exception:  # noqa: BLE001
        return ""
    parts: list[str] = []
    for seq in seqs or []:
        for step in (seq or {}).get("steps") or []:
            for variant in (step or {}).get("variants") or []:
                subj = (variant or {}).get("subject") or ""
                body = (variant or {}).get("body") or ""
                if subj:
                    parts.append(subj)
                if body:
                    parts.append(_RE_HTML_TAG.sub(" ", body))
    text = " ".join(parts)
    text = _RE_SPINTAX.sub(" ", text)
    return _RE_WS.sub(" ", text).strip()


def _latest_sequence_body(ctx, cid):
    """Deterministic: latest _loaded_at row per campaign_id (raw is ~5,870 rows /
    ~687 distinct cids). Runs in the GENERATOR (portal venv has pytz); never expose
    _loaded_at (TIMESTAMPTZ) via a serving view — the read API throws pytz on it."""
    row = ctx.db.execute(
        "SELECT sequence_raw FROM raw_instantly_campaign WHERE campaign_id = ? "
        "ORDER BY _loaded_at DESC LIMIT 1",
        [cid],
    ).fetchone()
    return _parse_sequence_text(row[0]) if row and row[0] else ""


def _call_offer_classifier(ctx, name, body):
    """LLM fallback (pass 3). Temp-0 Haiku, closed 5-label set; returns one canonical
    label or None (__ambiguous__). Skip-with-None (never raise) if the SDK or key is
    unavailable — an unresolved campaign WITH window sends is then caught by ASSERT 1,
    and the LLM only ever owns 0-window-send campaigns here, so it can't poison the
    windowed split."""
    try:
        import anthropic  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        logger.error("anthropic SDK unavailable (%s) — offer LLM classify skipped", exc)
        return None
    api_key = next(
        (ctx.credentials.optional(k) for k in _OFFER_KEY_CANDIDATES
         if ctx.credentials.optional(k)),
        None,
    )
    if not api_key:
        logger.error("No Anthropic key (%s) — offer LLM classify skipped", _OFFER_KEY_CANDIDATES)
        return None
    user_msg = "Campaign name: " + (name or "") + "\n\nEmail copy:\n" + (body or "")[:6000]
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=_OFFER_MODEL,
            max_tokens=16,
            system=_OFFER_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        label = "".join(
            b.text for b in resp.content if getattr(b, "type", None) == "text"
        ).strip()
    except Exception as exc:  # noqa: BLE001
        logger.error("offer LLM classify failed (%s): %s", type(exc).__name__, str(exc)[:200])
        return None
    return label if label in _OFFER_LABELS else None


def _classify_offer_llm(ctx, cid, name, now):
    """Pass-3 cached LLM classify. Cache key (campaign_id, md5(name+body)) so the
    nightly only calls the model when copy changes. Returns (offer, source) where
    source is 'llm_copy' (resolved) or 'unresolved' (__ambiguous__ -> NULL offer)."""
    body = _latest_sequence_body(ctx, cid)
    chash = hashlib.md5(f"{name}\n{body}".encode()).hexdigest()
    hit = ctx.db.execute(
        "SELECT offer FROM core.campaign_offer_llm_cache "
        "WHERE campaign_id = ? AND content_hash = ?",
        [cid, chash],
    ).fetchone()
    if hit is not None:
        return hit[0], ("llm_copy" if hit[0] else "unresolved")
    offer = _call_offer_classifier(ctx, name, body)
    ctx.db.execute(
        "INSERT OR REPLACE INTO core.campaign_offer_llm_cache "
        "(campaign_id, content_hash, offer, model, classified_at) VALUES (?, ?, ?, ?, ?)",
        [cid, chash, offer, _OFFER_MODEL, now],
    )
    return offer, ("llm_copy" if offer else "unresolved")


def _resolve_campaign_offer(ctx, now: datetime) -> None:
    """3-pass canonical offer -> core.campaign.offer + offer_source. SOLE writer of
    the offer column. 1 workspace_map  2 name_regex  3 llm_copy (cached). Runs every
    nightly inside run_campaign_ingest, AFTER _resolve_core_campaign (which leaves
    offer NULL). Idempotent."""
    # one-time idempotent legacy-token migration (Funding/R&D/s125 -> canonical).
    ctx.db.execute(
        "UPDATE core.campaign SET offer = CASE offer "
        "WHEN 'Funding' THEN 'Business Funding' WHEN 'R&D' THEN 'R&D Credit' "
        "WHEN 's125' THEN 'Section 125' ELSE offer END "
        "WHERE offer IN ('Funding', 'R&D', 's125')"
    )
    for cid, wsid, name in ctx.db.execute(
        "SELECT campaign_id, workspace_id, name FROM core.campaign"
    ).fetchall():
        if wsid in WORKSPACE_OFFER_MAP:
            offer, src = WORKSPACE_OFFER_MAP[wsid], "workspace_map"
        else:
            offer = _derive_offer_canonical(name)
            if offer:
                src = "name_regex"
            else:
                offer, src = _classify_offer_llm(ctx, cid, name, now)  # 'llm_copy'/'unresolved'
        ctx.db.execute(
            "UPDATE core.campaign SET offer = ?, offer_source = ? WHERE campaign_id = ?",
            [offer, src, cid],
        )


# ---------------------------------------------------------------------
# Status code -> label. Instantly's documented mapping (best-effort).
# ---------------------------------------------------------------------
_STATUS_LABEL = {
    0: "draft",
    1: "active",
    2: "paused",
    3: "completed",
    4: "running_subsequences",
    -1: "archived",
    -2: "deleted",
}


def _status_label(code: int | None) -> str | None:
    if code is None:
        return None
    return _STATUS_LABEL.get(code, f"unknown_{code}")


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        # Instantly returns ISO8601 with `Z`
        s = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register(registry: Registry) -> None:
    registry.add_phase("instantly", "campaign", run_campaign_ingest)


# ---------------------------------------------------------------------
# Main ingest
# ---------------------------------------------------------------------

def run_campaign_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping campaign ingest")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    rows_in = 0  # campaigns fetched
    rows_out = 0  # raw rows written across both tables
    failures: list[dict] = []
    workspaces_done: list[str] = []

    # Track which workspace_ids we've already processed in this run so
    # FUNDING_4 / KOI_AND_DESTROY (duplicate keys for one workspace) don't
    # double-insert.
    seen_workspace_ids: set[str] = set()

    for slug in sorted(keys.keys()):
        api_key = keys[slug]
        try:
            with InstantlyClient(api_key) as client:
                ws = client.get_current_workspace()
                workspace_id = ws.get("id")
                if not workspace_id:
                    failures.append({"slug": slug, "error": "missing_workspace_id"})
                    continue
                if workspace_id in seen_workspace_ids:
                    logger.info(
                        "Skipping duplicate workspace slug=%s (workspace_id %s already ingested this run)",
                        slug, workspace_id,
                    )
                    continue
                seen_workspace_ids.add(workspace_id)

                # 1. Tags first — small set, used to look up labels for email_tag_list refs.
                #    DISABLED by default (see SYNC_CAMPAIGN_TAGS): skip the /custom-tags
                #    fetch entirely so the nightly no longer pulls campaign tags.
                tag_id_to_label: dict[str, str] = {}
                if SYNC_CAMPAIGN_TAGS:
                    for tag in client.list_tags(workspace_id):
                        tid = tag.get("id")
                        label = tag.get("label")
                        if tid and label:
                            tag_id_to_label[tid] = label

                # 2. Campaigns
                w_campaigns = 0
                w_sending_tags = 0
                for camp in client.list_campaigns(workspace_id):
                    rows_in += 1
                    w_campaigns += 1
                    campaign_id = camp.get("id")
                    if not campaign_id:
                        continue
                    ctx.db.execute(
                        """
                        INSERT INTO raw_instantly_campaign
                          (_loaded_at, _run_id, workspace_id, campaign_id,
                           name, status, status_label,
                           created_at, updated_at,
                           email_gap, random_wait_max, daily_limit,
                           schedule_raw, sequence_raw, api_response_raw)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            now,
                            ctx.run_id,
                            workspace_id,
                            campaign_id,
                            camp.get("name"),
                            camp.get("status"),
                            _status_label(camp.get("status")),
                            _parse_ts(camp.get("timestamp_created")),
                            _parse_ts(camp.get("timestamp_updated")),
                            camp.get("email_gap"),
                            camp.get("random_wait_max"),
                            camp.get("daily_limit"),
                            json.dumps(camp.get("campaign_schedule") or {}),
                            json.dumps(camp.get("sequences") or []),
                            json.dumps(camp),
                        ],
                    )
                    rows_out += 1

                    # Sending tags from email_tag_list. See module docstring.
                    # DISABLED by default (SYNC_CAMPAIGN_TAGS) — with tag fetch off,
                    # tag_id_to_label is empty so every ref would be skipped anyway;
                    # the explicit guard makes the stop intentional + cheap (no lookups).
                    if not SYNC_CAMPAIGN_TAGS:
                        continue
                    for tid in camp.get("email_tag_list") or []:
                        label = tag_id_to_label.get(tid)
                        if not label:
                            # Tag was referenced but not in /custom-tags — skip rather
                            # than insert a NULL label (PK requires non-null).
                            continue
                        ctx.db.execute(
                            """
                            INSERT INTO raw_instantly_campaign_sending_tag
                              (_loaded_at, _run_id, workspace_id, campaign_id,
                               tag_id, tag_label, account_count)
                            VALUES (?, ?, ?, ?, ?, ?, NULL)
                            ON CONFLICT DO NOTHING
                            """,
                            [now, ctx.run_id, workspace_id, campaign_id, tid, label],
                        )
                        w_sending_tags += 1
                        rows_out += 1

                # 3. Marker tags: DEFERRED to v1.1.
                #
                # Marker tags (the badges next to campaign names in the Instantly UI,
                # e.g. "AIM Active") are not exposed by the public REST API.
                # The endpoint /api/v2/tag-mappings?resource_type=2 returns 404 in
                # production (verified 2026-05-30 across all workspace keys).
                # The Instantly MCP wrapper uses a private/admin endpoint we have
                # not yet reverse-engineered.
                #
                # core.campaign_marker_tag DDL is in place; populate it once we
                # identify the correct endpoint (likely requires Instantly support
                # ticket asking for the public-API equivalent of tag-mappings).
                w_marker_tags = 0

                workspaces_done.append(slug)
                logger.info(
                    "Workspace %s (id=%s): %d campaigns, %d sending-tags (marker-tags deferred)",
                    slug, workspace_id, w_campaigns, w_sending_tags,
                )
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    # ---- canonical resolution ------------------------------------------
    _resolve_core_campaign(ctx, now)
    _resolve_campaign_offer(ctx, now)  # WS7: sole writer of core.campaign.offer (3-pass)
    _resolve_core_campaign_sending_tag(ctx, now)
    _resolve_core_campaign_marker_tag(ctx, now)

    notes = {
        "workspaces_done": workspaces_done,
        "failures": failures,
    }
    return PhaseResult(rows_in=rows_in, rows_out=rows_out, notes=notes)


# ---------------------------------------------------------------------
# Resolution passes
# ---------------------------------------------------------------------

def _resolve_core_campaign(ctx, now: datetime) -> None:
    """Upsert into core.campaign from the latest raw rows in this run."""
    rows = ctx.db.execute(
        """
        SELECT workspace_id, campaign_id, name, status, status_label,
               created_at, email_gap, random_wait_max, daily_limit
        FROM raw_instantly_campaign
        WHERE _run_id = ?
        """,
        [ctx.run_id],
    ).fetchall()

    cols = ("workspace_id", "campaign_id", "name", "status", "status_label",
            "created_at", "email_gap", "random_wait_max", "daily_limit")
    for row in rows:
        d = dict(zip(cols, row))
        name = d["name"]
        cm = _derive_cm(name)
        # WS7: offer is owned by _resolve_campaign_offer (3-pass resolver), called
        # right after this pass. Write NULL here so the column is set on first
        # ingest, then the resolver fills it in the same nightly run. `_derive_offer`
        # stays defined (legacy token producer) but is no longer the offer writer.
        offer = None
        is_mca = _derive_is_mca(name)

        # Try insert; if conflict, fall through to update.
        existing = ctx.db.execute(
            "SELECT first_seen_at FROM core.campaign WHERE campaign_id = ?",
            [d["campaign_id"]],
        ).fetchone()
        if existing is None:
            ctx.db.execute(
                """
                INSERT INTO core.campaign
                  (campaign_id, workspace_id, name, status, status_label,
                   cm, offer, is_mca,
                   email_gap, random_wait_max, daily_limit,
                   created_at, is_active, first_seen_at, last_seen_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?)
                """,
                [
                    d["campaign_id"], d["workspace_id"], name,
                    d["status"], d["status_label"],
                    cm, offer, is_mca,
                    d["email_gap"], d["random_wait_max"], d["daily_limit"],
                    d["created_at"], now, now, now,
                ],
            )
        else:
            ctx.db.execute(
                """
                UPDATE core.campaign
                SET workspace_id = ?,
                    name = ?,
                    status = ?,
                    status_label = ?,
                    cm = ?,
                    offer = ?,
                    is_mca = ?,
                    email_gap = ?,
                    random_wait_max = ?,
                    daily_limit = ?,
                    created_at = COALESCE(created_at, ?),
                    is_active = TRUE,
                    last_seen_at = ?,
                    resolved_at = ?
                WHERE campaign_id = ?
                """,
                [
                    d["workspace_id"], name, d["status"], d["status_label"],
                    cm, offer, is_mca,
                    d["email_gap"], d["random_wait_max"], d["daily_limit"],
                    d["created_at"], now, now,
                    d["campaign_id"],
                ],
            )

    # Flip is_active for campaigns we did NOT see this run.
    # Same temp-table dance — keeps VARCHAR `_run_id` away from TIMESTAMPTZ placeholder inference.
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_campaign")
    ctx.db.execute(
        """
        CREATE TEMP TABLE _run_latest_campaign AS
        SELECT DISTINCT campaign_id FROM raw_instantly_campaign WHERE _run_id = ?
        """,
        [ctx.run_id],
    )
    ctx.db.execute(
        """
        UPDATE core.campaign
        SET is_active = FALSE,
            resolved_at = ?
        WHERE campaign_id NOT IN (SELECT campaign_id FROM _run_latest_campaign)
        """,
        [now],
    )
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_campaign")


def _resolve_core_campaign_marker_tag(ctx, now: datetime) -> None:
    """Upsert into core.campaign_marker_tag from this run's raw marker rows."""
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_marker_tag")
    ctx.db.execute(
        """
        CREATE TEMP TABLE _run_latest_marker_tag AS
        SELECT DISTINCT workspace_id, campaign_id, tag_label
        FROM raw_instantly_campaign_marker_tag
        WHERE _run_id = ?
        """,
        [ctx.run_id],
    )
    ctx.db.execute(
        """
        INSERT INTO core.campaign_marker_tag
          (workspace_id, campaign_id, tag_name, first_seen_at, last_seen_at)
        SELECT workspace_id, campaign_id, tag_label, ?, ?
        FROM _run_latest_marker_tag src
        WHERE NOT EXISTS (
          SELECT 1 FROM core.campaign_marker_tag t
          WHERE t.campaign_id = src.campaign_id AND t.tag_name = src.tag_label
        )
        """,
        [now, now],
    )
    ctx.db.execute(
        """
        UPDATE core.campaign_marker_tag
        SET last_seen_at = ?
        WHERE EXISTS (
          SELECT 1 FROM _run_latest_marker_tag src
          WHERE src.campaign_id = core.campaign_marker_tag.campaign_id
            AND src.tag_label = core.campaign_marker_tag.tag_name
        )
        """,
        [now],
    )
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_marker_tag")


def _resolve_core_campaign_sending_tag(ctx, now: datetime) -> None:
    """Upsert into core.campaign_sending_tag from the latest raw rows in this run.

    Key is (campaign_id, tag_name) — see spec 03; we want the workspace-scoped
    natural lookup, not the GUID, so labels that drift but keep the same ID
    are tracked by label.
    """
    # Materialize per-run set first to avoid DuckDB prepared-statement type
    # inference confusing VARCHAR `_run_id` with TIMESTAMPTZ placeholders.
    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_send_tag")
    ctx.db.execute(
        """
        CREATE TEMP TABLE _run_latest_send_tag AS
        SELECT DISTINCT workspace_id, campaign_id, tag_label
        FROM raw_instantly_campaign_sending_tag
        WHERE _run_id = ?
        """,
        [ctx.run_id],
    )

    # Insert net-new (campaign_id, tag_name) tuples.
    ctx.db.execute(
        """
        INSERT INTO core.campaign_sending_tag
          (workspace_id, campaign_id, tag_name, first_seen_at, last_seen_at)
        SELECT workspace_id, campaign_id, tag_label, ?, ?
        FROM _run_latest_send_tag src
        WHERE NOT EXISTS (
          SELECT 1 FROM core.campaign_sending_tag t
          WHERE t.campaign_id = src.campaign_id AND t.tag_name = src.tag_label
        )
        """,
        [now, now],
    )

    # Touch last_seen_at on existing rows.
    ctx.db.execute(
        """
        UPDATE core.campaign_sending_tag
        SET last_seen_at = ?
        WHERE EXISTS (
          SELECT 1 FROM _run_latest_send_tag src
          WHERE src.campaign_id = core.campaign_sending_tag.campaign_id
            AND src.tag_label = core.campaign_sending_tag.tag_name
        )
        """,
        [now],
    )

    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_send_tag")
