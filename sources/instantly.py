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
_REQUEST_TIMEOUT = 30.0


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
        resp = self._client.get(path, params=params)
        if resp.status_code >= 400:
            # Surface body up to 500 chars for debugging without dumping mega-JSON.
            body = resp.text[:500]
            raise InstantlyError(
                f"GET {path} -> {resp.status_code}: {body}"
            )
        return resp.json()

    def _paginate(self, path: str, params: dict | None = None, limit: int = 100) -> Iterator[dict]:
        """Yield all items across `next_starting_after` pagination."""
        p = dict(params or {})
        p.setdefault("limit", limit)
        cursor: str | None = None
        seen_pages = 0
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
            # gentle pace + safety
            time.sleep(0.05)
            if seen_pages > 1000:
                logger.warning("Pagination ceiling on %s — bailing at %d pages", path, seen_pages)
                return

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
