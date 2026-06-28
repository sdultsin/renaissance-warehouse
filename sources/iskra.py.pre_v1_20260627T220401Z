"""Iskra public-API client (WhatsApp outreach — the WhatsApp analogue of Sendivo's SMS).

Read-only key (6 read scopes: messages, conversations, deals, meetings, stats, numbers).
Base: https://xglfamaaotmwulglwcui.supabase.co/functions/v1/public-api
Auth:  Authorization: Bearer <ISKRA_API_KEY>   (X-API-Key also accepted)
Rate limit: 60 req/min/key — ENFORCED here with a per-client min-interval gate (Sendivo had
no documented limit; Iskra does). Validated live 2026-06-18: every endpoint 200; `since=`
ACTUALLY bounds the result set (unlike Sendivo's /sms/logs global feed — SMS gap G9), so
incremental-by-watermark is clean.

List endpoints return {data:[...], next_cursor, has_more}; walk `cursor` until has_more=false.
`paginate()` reports whether the walk COMPLETED (reached has_more=false) vs hit the page cap —
the caller turns an incomplete walk into a loud reconciliation failure (SMS audit E3: no silent
truncation).

The key cannot be loaded with `source .env` (earlier .env lines have invalid shell identifiers
that abort the parse before it). The warehouse loads it via python-dotenv (dotenv_values), which
tolerates the bad lines — see core/credentials.py.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

logger = logging.getLogger("sources.iskra")

BASE_URL = "https://xglfamaaotmwulglwcui.supabase.co/functions/v1/public-api"
_TIMEOUT = 60.0
_PAGE_LIMIT = 100            # max page size the paginated list endpoints honour
MEETINGS_CAP = 500           # /meetings hard cap: no pagination, has_more always false (vendor gap)
_MIN_INTERVAL_S = 1.05       # 60 req/min/key -> >=1.0s between requests; 1.05 for headroom
_MAX_RETRIES = 4


class IskraError(RuntimeError):
    """Raised on a persistent non-2xx (after retries). Caller decides skip vs abort the phase."""


@dataclass
class PageWalk:
    """Result of walking a paginated list endpoint.

    completed=True means we paged until has_more=false (the full set). completed=False means we
    stopped at the MAX_PAGES safety cap WITHOUT the API signalling the end — a potential silent
    truncation; the caller MUST treat this as a reconciliation failure, never as a clean pull.
    """

    items: list[dict]
    pages: int
    completed: bool


class IskraClient:
    def __init__(self, api_key: str):
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        self._last_req = 0.0

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- transport -------------------------------------------------------
    def _throttle(self) -> None:
        """Hold to <=60 req/min: sleep so consecutive requests are >= _MIN_INTERVAL_S apart."""
        gap = time.monotonic() - self._last_req
        if gap < _MIN_INTERVAL_S:
            time.sleep(_MIN_INTERVAL_S - gap)
        self._last_req = time.monotonic()

    def _get(self, path: str, params: dict | None = None) -> dict:
        last = None
        for attempt in range(_MAX_RETRIES):
            self._throttle()
            try:
                resp = self._client.get(path, params=params)
            except httpx.TimeoutException:
                last = "timeout"
                time.sleep(2.0 * (attempt + 1))
                continue
            if resp.status_code == 429:
                # respect Retry-After when present, else back off
                wait = resp.headers.get("Retry-After")
                try:
                    wait_s = float(wait) if wait else 2.0 * (attempt + 1)
                except ValueError:
                    wait_s = 2.0 * (attempt + 1)
                last = "429"
                time.sleep(min(wait_s, 30.0))
                continue
            if resp.status_code in (500, 502, 503, 504):
                last = str(resp.status_code)
                time.sleep(2.0 * (attempt + 1))
                continue
            if resp.status_code >= 400:
                raise IskraError(f"GET {path} {params} -> {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        raise IskraError(f"GET {path} {params} -> repeated failure ({last})")

    def paginate(self, path: str, params: dict | None = None, max_pages: int = 5000,
                 stop_before: str | None = None, stop_field: str = "created_at") -> PageWalk:
        """Walk a cursor-paginated list endpoint to completion (or the safety cap).

        Each response is {data:[...], next_cursor, has_more}. We pass next_cursor back as `cursor`
        until has_more is false. `limit` defaults to the max page size so we make the fewest
        requests (rate-limit-friendly). Returns a PageWalk carrying the completed flag so the
        caller can fail loud on truncation.

        `stop_before` (client-side incremental early-stop): for endpoints that IGNORE the server
        `since` param (verified 2026-06-18: /conversations and /deals do — they return the full
        newest-first feed regardless), stop walking once a page's oldest `stop_field` value falls
        below `stop_before`. The feed is strictly newest-first so everything beyond is older. This
        is the same incremental trick sources/close.py uses on the call-activity feed. Reaching the
        boundary IS a complete walk (we have every row >= stop_before), so completed=True.
        """
        params = dict(params or {})
        params.setdefault("limit", _PAGE_LIMIT)
        items: list[dict] = []
        cursor = None
        pages = 0
        completed = False
        while pages < max_pages:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            payload = self._get(path, page_params)
            data = payload.get("data")
            if not isinstance(data, list):
                data = []
            items.extend(data)
            pages += 1
            if not payload.get("has_more"):
                completed = True
                break
            if stop_before and data:
                # newest-first => the LAST item on the page is the oldest seen so far.
                tail = data[-1].get(stop_field)
                if tail and str(tail) < str(stop_before):
                    completed = True  # crossed the watermark; the rest of the feed is older
                    break
            cursor = payload.get("next_cursor")
            if not cursor:
                # has_more=true but NO cursor to advance: we cannot page further AND have not
                # reached the true end -> a truncation (audit E3). Leave completed=False so
                # _require_complete fails loud rather than passing a partial pull as clean.
                break
        return PageWalk(items=items, pages=pages, completed=completed)

    # --- endpoints -------------------------------------------------------
    def whoami(self) -> dict:
        return self._get("/v1/whoami")

    def messages(self, since: str | None = None, direction: str | None = None,
                 max_pages: int = 5000) -> PageWalk:
        """GET /v1/messages/whatsapp — newest-first; `since` (ISO) bounds the lower edge (verified)."""
        params: dict = {}
        if since:
            params["since"] = since
        if direction:
            params["direction"] = direction
        return self.paginate("/v1/messages/whatsapp", params, max_pages=max_pages)

    def conversations(self, stop_before: str | None = None, max_pages: int = 5000) -> PageWalk:
        """GET /v1/conversations — newest-first by created_at; server `since` is IGNORED, so we use
        the client-side early-stop (stop_before) to stay incremental."""
        return self.paginate("/v1/conversations", {}, max_pages=max_pages,
                             stop_before=stop_before, stop_field="created_at")

    def meetings(self, since: str | None = None, max_pages: int = 2000) -> PageWalk:
        """GET /v1/meetings — VENDOR LIMITATION (verified 2026-06-18): no cursor pagination,
        has_more is ALWAYS false, and the endpoint hard-caps at MEETINGS_CAP (500) rows newest-first.
        `since` IS a working lower bound. So we request the max page (500); the caller treats a
        full-cap return as potential truncation (older tags beyond the newest 500 are unreachable).
        Daily tag volume is ~100-220, so an incremental since=watermark pull stays well under the
        cap and is complete go-forward; only a deep historical backfill hits the cap."""
        params = {"limit": MEETINGS_CAP}
        if since:
            params["since"] = since
        return self.paginate("/v1/meetings", params, max_pages=max_pages)

    def deals(self, stage_id: str | None = None, stop_before: str | None = None,
              max_pages: int = 5000) -> PageWalk:
        """GET /v1/deals — newest-first by created_at; server `since` is IGNORED, so we use the
        client-side early-stop (stop_before) to stay incremental. NOTE: stage/amount UPDATES on
        deals OLDER than the watermark are not re-pulled (the feed offers no updated_at access);
        acceptable for v1 — the WhatsApp deal pipeline is currently trivial (auto-created stubs,
        single stage, amount=null, deals_won=0). Widen via a periodic full walk if it matures."""
        params = {"stage_id": stage_id} if stage_id else {}
        return self.paginate("/v1/deals", params, max_pages=max_pages,
                             stop_before=stop_before, stop_field="created_at")

    def numbers(self, max_pages: int = 200) -> PageWalk:
        return self.paginate("/v1/numbers", {"limit": 500}, max_pages=max_pages)

    def numbers_snapshot(self) -> dict:
        """GET /v1/numbers/snapshot — aggregate {total, by_status, by_quality, total_daily_cap, captured_at}."""
        return self._get("/v1/numbers/snapshot")

    def stats_summary(self, channel: str, frm: str, to: str) -> dict:
        """GET /v1/stats/summary — the agency funnel for a window (the reconciliation SOT row).

        Returns the full payload; the WhatsApp funnel is under the 'whatsapp' key plus the
        top-level opportunities / meetings_booked / deals_won.
        """
        return self._get("/v1/stats/summary",
                         {"channel": channel, "from": frm, "to": to})
