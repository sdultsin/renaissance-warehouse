"""Instantly REST client.

Just the endpoints the warehouse needs. Per-workspace Bearer auth.
User-Agent override because Instantly blocks `Python-urllib/*` and `python-httpx/*`
(see memory `reference_instantly_api_urllib_403_block.md`).
"""

from __future__ import annotations

import logging
import time
from typing import Iterator

import httpx

logger = logging.getLogger("sources.instantly")

BASE_URL = "https://api.instantly.ai/api/v2"
# Mimic curl — Instantly fingerprint-blocks Python clients on the default UA.
_UA = "curl/8.4.0"
# Conservative serial pace. Per `feedback_instantly_list_accounts_serial_only.md`,
# do not parallelize across workspaces or hit the API hot.
_REQUEST_TIMEOUT = 60.0  # 30s tripped on slow /emails pages (big workspaces), 2026-06-11


class InstantlyError(RuntimeError):
    """Raised on a non-2xx response. Caller decides whether to skip the workspace."""


class InstantlyClient:
    """One client per workspace key. Reuses an httpx.Client for keepalive."""

    def __init__(self, api_key: str):
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": _UA,
                "Accept": "application/json",
            },
            timeout=_REQUEST_TIMEOUT,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- helpers ---------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        import time as _time
        retries = 3
        for attempt in range(retries):
            # Transient transport failures (ReadTimeout on slow /emails pages, conn
            # resets) must retry, not kill the whole workspace pull — a single slow
            # page repeatedly aborted the-gatekeepers replies ingest (2026-06-11).
            try:
                resp = self._client.get(path, params=params)
            except httpx.TransportError as exc:
                if attempt + 1 >= retries:
                    raise
                wait = 15 * (attempt + 1)
                logger.warning(
                    "GET %s -> %s (attempt %d/%d), sleeping %ds",
                    path, type(exc).__name__, attempt + 1, retries, wait,
                )
                _time.sleep(wait)
                continue
            if resp.status_code == 429:
                wait = 65  # 429 = rate limit; wait >60s (window resets per minute)
                logger.warning(
                    "GET %s -> 429 rate-limit (attempt %d/%d), sleeping %ds",
                    path, attempt + 1, retries, wait,
                )
                _time.sleep(wait)
                continue
            if resp.status_code >= 500:
                # Instantly 5xx are transient ("please try again") — seen repeatedly
                # on deep /emails pagination (2026-06-11). Retry with backoff.
                if attempt + 1 < retries:
                    wait = 30 * (attempt + 1)
                    logger.warning(
                        "GET %s -> %d (attempt %d/%d), sleeping %ds",
                        path, resp.status_code, attempt + 1, retries, wait,
                    )
                    _time.sleep(wait)
                    continue
            if resp.status_code >= 400:
                body = resp.text[:500]
                raise InstantlyError(f"GET {path} -> {resp.status_code}: {body}")
            return resp.json()
        body = resp.text[:500]
        raise InstantlyError(f"GET {path} -> {resp.status_code} after {retries} retries: {body}")

    # Pagination page-count ceiling. Crossing it means we BAILED before the cursor
    # was exhausted, i.e. the result set is silently truncated mid-history. Callers
    # that need full coverage (email-thread-sync full backfill) MUST treat a ceiling
    # hit as a HARD FAIL and NOT advance their watermark (FINALIZED-SPEC §4.C/R8).
    PAGINATION_CEILING = 100_000  # runaway BACKSTOP, not a data limit (~10M records). Real
                                  # pulls complete via cursor exhaustion; never cut short silently.

    def _paginate(
        self,
        path: str,
        params: dict | None = None,
        limit: int = 100,
        ceiling_flag: dict | None = None,
    ) -> Iterator[dict]:
        """Yield all items across `next_starting_after` pagination.

        If `ceiling_flag` (a dict) is passed and the page-count ceiling is hit before
        the cursor is exhausted, sets `ceiling_flag['hit'] = True` so the caller can
        detect a silent mid-history truncation (vs a clean end-of-cursor). Without it
        the historic behaviour is unchanged (log + stop).
        """
        p = dict(params or {})
        p.setdefault("limit", limit)
        cursor: str | None = None
        seen_pages = 0
        seen_cursors: set[str] = set()
        while True:
            if cursor:
                p["starting_after"] = cursor
            payload = self._get(path, params=p)
            items = payload.get("items") or []
            for it in items:
                yield it
            cursor = payload.get("next_starting_after")
            seen_pages += 1
            if not cursor:
                return
            # Infinite-loop guard: a cursor that repeats is a real bug, not legitimate volume.
            if cursor in seen_cursors:
                raise InstantlyError(
                    f"{path}: pagination cursor not advancing at page {seen_pages} — aborting infinite loop")
            seen_cursors.add(cursor)
            time.sleep(0.05)  # gentle pace
            if seen_pages > self.PAGINATION_CEILING:
                # Never silently return a partial set. A caller that explicitly handles a short
                # pull gets the flag; anyone else fails LOUD instead of getting incomplete data.
                logger.error("Pagination backstop %d hit on %s — refusing to return a partial set",
                             self.PAGINATION_CEILING, path)
                if ceiling_flag is not None:
                    ceiling_flag["hit"] = True
                    return
                raise InstantlyError(
                    f"{path}: exceeded {seen_pages} pages — refusing to return a partial set")

    # --- endpoints -------------------------------------------------------

    def get_current_workspace(self) -> dict:
        """`GET /workspaces/current` — returns the one workspace this key authenticates."""
        return self._get("/workspaces/current")

    def list_campaigns(self, workspace_id: str | None = None) -> Iterator[dict]:
        """`GET /campaigns` (paginated). `workspace_id` arg is informational —
        the key already scopes the workspace; we accept it for clarity at call sites.
        """
        yield from self._paginate("/campaigns", params=None, limit=100)

    def get_campaign(self, campaign_id: str) -> dict:
        """`GET /campaigns/{id}` — full campaign detail. Currently unused by the
        warehouse since the list endpoint returns email_gap/daily_limit/random_wait_max
        already; kept for future entities (steps, variants) that may need it.
        """
        return self._get(f"/campaigns/{campaign_id}")

    def campaign_analytics(self, campaign_id: str | None = None) -> list[dict]:
        """`GET /campaigns/analytics` — campaign-GRAIN performance, one object per
        campaign. This is the ONLY source whose `emails_sent_count` /
        `reply_count_unique` / `total_opportunities` match the Instantly UI; the
        daily-metrics fact table's `unique_*` columns are per-day-distinct and
        cannot be summed (a lead unique on two days counts twice). See
        sql/ddl/32_campaign_analytics.sql.

        Omit `campaign_id` to get every campaign in the workspace in one call
        (the endpoint returns a bare JSON array, not a paginated `items` wrapper).
        """
        params = {"id": campaign_id} if campaign_id else None
        payload = self._get("/campaigns/analytics", params=params)
        if isinstance(payload, list):
            return payload
        # Defensive: some deployments wrap arrays in {items:[...]}.
        if isinstance(payload, dict):
            return payload.get("items") or []
        return []

    def received_emails(
        self,
        since: str | None = None,
        workspace_id: str | None = None,
    ) -> Iterator[dict]:
        """`GET /emails?email_type=received` (paginated) — inbound prospect replies.

        This is the DIRECT-Instantly source for what pipeline-supabase's
        `reply_data` table holds (cold-email replies). It lets the warehouse pull
        replies itself instead of mirroring n8n's reply_data table — the n8n
        webhook collector is the only producer of pipeline-supabase.public.reply_data
        and is not owned by us (see deliverables/2026-03-23-data-landscape-audit.md
        "reply_data Population").

        Each item carries the fields raw_instantly_email needs:
          id, campaign_id, lead, subject, body{html/text}, from_address_email,
          eaccount, step, timestamp_email, ue_type, thread_id, message_id.

        `since` (ISO8601) is a best-effort lower bound passed as the API's
        `i_status`-free time window if supported; we ALSO filter client-side on
        timestamp_email so an unsupported param can never widen the pull.
        The key already scopes the workspace; `workspace_id` is informational.
        """
        params: dict = {"email_type": "received"}
        # The endpoint sorts newest-first; we stop paginating once we cross `since`.
        from datetime import datetime, timezone

        cutoff = None
        if since:
            try:
                cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
            except ValueError:
                cutoff = None

        for item in self._paginate("/emails", params=params, limit=100):
            if cutoff is not None:
                ts = item.get("timestamp_email") or item.get("timestamp_created")
                if ts:
                    try:
                        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                        if t.tzinfo is None:
                            t = t.replace(tzinfo=timezone.utc)
                        if t < cutoff:
                            # Newest-first ordering: everything after this is older.
                            return
                    except ValueError:
                        pass
            yield item

    def all_emails(
        self,
        since: str | None = None,
        ceiling_flag: dict | None = None,
    ) -> Iterator[dict]:
        """`GET /emails` (paginated, NO email_type filter) — EVERY email in the workspace,
        all directions (ue_type 1 cold send / 2 prospect reply / 3 our/IM reply), newest-first.

        This is the DISCOVERY stream for email-thread-sync (FINALIZED-SPEC §4.B / R7): the
        trigger to (re)pull a lead is ANY new email since the per-workspace watermark —
        received OR sent OR our IM reply — NOT received-only. A received-only discovery
        (`received_emails`) would MISS a late ue_type=3 reply that produced no new inbound
        row, defeating R7's incremental-completeness guarantee (idempotency-lens MISS).

        Newest-first ordering lets us stop paginating once we cross `since` (cheap incremental).
        On a `since=None` full backfill this walks the WHOLE stream; if that crosses the
        pagination ceiling the lead set is silently truncated, so we thread `ceiling_flag`
        through `_paginate` and the caller treats a hit as a HARD FAIL for the workspace
        (no watermark advance) exactly like a per-lead pull truncation (FINALIZED-SPEC §4.C).
        """
        from datetime import datetime, timezone

        cutoff = None
        if since:
            try:
                cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
            except ValueError:
                cutoff = None

        for item in self._paginate("/emails", params=None, limit=100, ceiling_flag=ceiling_flag):
            if cutoff is not None:
                ts = item.get("timestamp_email") or item.get("timestamp_created")
                if ts:
                    try:
                        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                        if t.tzinfo is None:
                            t = t.replace(tzinfo=timezone.utc)
                        if t < cutoff:
                            # Newest-first: everything after this page is older than the watermark.
                            return
                    except ValueError:
                        pass
            yield item

    def lead_emails(self, lead_email: str) -> Iterator[dict]:
        """`GET /api/v2/emails?lead=<email>` (paginated) — a lead's COMPLETE message set.

        Returns every email for one lead across ALL directions and campaigns:
        ue_type 1 (cold sends), 2 (prospect replies), 3 (our/IM replies) — rendered
        (spintax already collapsed, merge fields filled), chronological newest-first
        (cursor = next_starting_after). This is the email-thread-sync pull primitive
        (FINALIZED-SPEC §2/§4.C): one call captures a replier's entire thread, so the
        upsert collapses on the per-email id.

        The `?lead=` API value is case-insensitive (`JESI@…` == lowercase), but the
        warehouse key is NOT — so we lowercase+trim before sending so the discovery
        set, the request value, and the stored key all agree (FINALIZED-SPEC §2).

        A nonexistent lead returns HTTP 200 with items=[] / next=null — NEVER a 404.
        Per-lead 404 handling is dead code; the only real failure is a dead workspace
        KEY (handled by the caller skipping that org). 5xx/429 use _get's retry path.

        Newest-first cursor: we yield in the API's native order (newest first) and do
        NOT filter by time here — the caller pulls the full history per replied lead
        (the thread is small) and the upsert is idempotent.
        """
        norm = (lead_email or "").lower().strip()
        if not norm:
            return
        yield from self._paginate("/emails", params={"lead": norm}, limit=100)

    def lead_emails_window(self, lead_email: str) -> tuple[list[dict], bool]:
        """Eager `lead_emails` that also reports whether the pagination ceiling was hit.

        Returns (items, ceiling_hit). A True ceiling_hit means the lead's history was
        truncated mid-cursor (>1000 pages — pathological for a single lead) and the
        caller MUST treat that lead/workspace as a HARD FAIL (do not advance the
        watermark; escalate). Used by the email-thread-sync full-backfill path.
        """
        norm = (lead_email or "").lower().strip()
        if not norm:
            return [], False
        flag: dict = {"hit": False}
        items = list(
            self._paginate("/emails", params={"lead": norm}, limit=100, ceiling_flag=flag)
        )
        return items, bool(flag.get("hit"))

    def list_tags(self, workspace_id: str | None = None) -> Iterator[dict]:
        """`GET /custom-tags` — workspace-level tag catalog.

        Empirical finding (2026-05-30): Instantly exposes ONE tag entity. The two
        semantic surfaces (marker vs sending) are distinguished by how the tag is
        APPLIED, not by tag type:
          - Sending tags: referenced in campaign config `email_tag_list` (tag UUIDs)
          - Marker tags: applied to a campaign as a tag mapping with resource_type=2

        See `list_tag_mappings` below.
        """
        yield from self._paginate("/custom-tags", params=None, limit=100)

    def list_tag_mappings(
        self,
        workspace_id: str | None = None,
        resource_type: int | None = None,
    ) -> Iterator[dict]:
        """`GET /tag-mappings` — every (tag, resource) link in this workspace.

        Args:
            resource_type: 1 = account-level tag mapping (sending account is tagged X),
                           2 = campaign-level tag mapping (campaign is tagged X — the
                               "marker tag" surface visible as a badge next to the
                               campaign name in the Instantly UI).

        Yields rows like {"tag_id": "...", "resource_id": "...", "resource_type": 1|2}.
        Join `tag_id` to `list_tags()` output to get the human label.
        """
        params: dict = {}
        if resource_type is not None:
            params["resource_type"] = resource_type
        yield from self._paginate("/tag-mappings", params=params or None, limit=100)

    def list_custom_tag_mappings(
        self,
        resource_type: int | None = None,
        workspace_id: str | None = None,
    ) -> Iterator[dict]:
        """`GET /custom-tag-mappings` — the PUBLIC bulk (tag, resource) edge list.

        Unlike `/tag-mappings` (admin-only; 404s on the public v2 surface — see
        `list_tag_mappings`), `/custom-tag-mappings` IS reachable with a workspace key
        and streams EVERY tag edge in one paginated pass (verified live 2026-06-26 on
        Funding 2). Each item: {"id","tag_id","resource_id","resource_type",...} where
        `resource_id` is the sending-account EMAIL when resource_type == 1. This is the
        efficient, faithful source for the full account<->tag mirror — no per-tag
        `/accounts?tag_ids=` iteration needed.
        """
        params: dict = {}
        if resource_type is not None:
            params["resource_type"] = resource_type
        yield from self._paginate("/custom-tag-mappings", params=params or None, limit=100)

    def list_accounts(
        self,
        tag_ids: str | None = None,
        status: int | None = None,
        workspace_id: str | None = None,
    ) -> Iterator[dict]:
        """`GET /accounts` (paginated) — sending accounts in this workspace.

        `tag_ids` is a SERVER-SIDE filter (comma-separated tag UUIDs): the endpoint
        returns only accounts carrying ANY of those tags. This is the documented
        public-v2 mechanism the account->tag sync relies on (verified 2026-06-23 via
        the Instantly v2 docs + the MCP wrapper: F2 'Reseller Active' tag returns 4,557
        accounts). It is the robust alternative to /tag-mappings?resource_type=1, which
        is the private/admin surface (the public tag-mappings endpoint 404s — see the
        marker-tag note in entities/campaign.py).

        Each account item carries the fields the tag sync needs:
          email, status, warmup_status, daily_limit, provider_code (when present),
          sending_gap, timestamp_created.

        The key already scopes the workspace; `workspace_id` is informational.
        """
        params: dict = {}
        if tag_ids:
            params["tag_ids"] = tag_ids
        if status is not None:
            params["status"] = status
        yield from self._paginate("/accounts", params=params or None, limit=100)
