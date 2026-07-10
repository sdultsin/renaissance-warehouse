"""Close CRM REST client.

Just the endpoints the warehouse needs for the warm-call (BOF) ingest:
  - GET /api/v1/activity/call/  — paginated call activity (newest-first), incremental
  - GET /api/v1/lead/{lead_id}/ — the Close lead (email∪phone + Source/Campaign custom fields)

Auth = HTTP basic with the API key as the username and an EMPTY password
(`-u "$CLOSE_API_KEY:"`). Pagination on the activity endpoint uses _skip/_limit and a
`has_more` flag on the response.

INCREMENTAL: the call activity feed is ordered newest-first, so `iter_calls(since=…)`
stops paginating as soon as it crosses a call whose `date_updated` is <= the watermark
(the max date_updated already in the warehouse). First run (since=None) does a full pull.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterator

import httpx

logger = logging.getLogger("sources.close")

BASE_URL = "https://api.close.com/api/v1"
# A normal UA (Close does not fingerprint-block like Instantly, but be polite/explicit).
_UA = "renaissance-warehouse/1.0"
_REQUEST_TIMEOUT = 60.0
_PAGE_LIMIT = 100


class CloseError(RuntimeError):
    """Raised on a non-2xx response. Caller decides whether to skip / abort the phase."""


def _parse_dt(v) -> datetime | None:
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
    except (TypeError, ValueError):
        return None


class CloseClient:
    """One client for the org the key authenticates. Reuses an httpx.Client."""

    def __init__(self, api_key: str):
        # Close basic auth: key as username, empty password.
        self._client = httpx.Client(
            base_url=BASE_URL,
            auth=(api_key, ""),
            headers={"User-Agent": _UA, "Accept": "application/json"},
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
        # Retry transient failures (read timeouts, transport blips, 429/5xx) with
        # backoff so one slow lead-fetch can't kill the whole phase.
        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                resp = self._client.get(path, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                logger.warning("GET %s transient %s (attempt %d/4)", path, type(exc).__name__, attempt + 1)
                time.sleep(1.5 * (attempt + 1))
                continue
            if resp.status_code in (429, 500, 502, 503, 504):
                last_exc = CloseError(f"GET {path} -> {resp.status_code}")
                time.sleep(1.5 * (attempt + 1))
                continue
            if resp.status_code >= 400:
                raise CloseError(f"GET {path} -> {resp.status_code}: {resp.text[:500]}")
            return resp.json()
        raise CloseError(f"GET {path} failed after retries: {last_exc}")

    # --- endpoints -------------------------------------------------------

    def iter_calls(self, since: datetime | None = None) -> Iterator[dict]:
        """Yield Close call-activity objects, newest-first, paginated via _skip/_limit.

        `since` = the max `date_updated` already in the warehouse. Because the feed is
        newest-first, we stop paginating once we reach a call with
        `date_updated <= since` (everything after it is older and already stored).
        First run (since=None) pulls the full history.
        """
        skip = 0
        seen_pages = 0
        while True:
            payload = self._get(
                "/activity/call/", params={"_limit": _PAGE_LIMIT, "_skip": skip}
            )
            data = payload.get("data") or []
            if not data:
                return
            for call in data:
                if since is not None:
                    du = _parse_dt(call.get("date_updated"))
                    if du is not None and du <= since:
                        # Newest-first: this and everything after it is already stored.
                        logger.info(
                            "iter_calls hit watermark (date_updated %s <= %s) — stopping",
                            du.isoformat(), since.isoformat(),
                        )
                        return
                yield call
            seen_pages += 1
            if not payload.get("has_more"):
                return
            skip += _PAGE_LIMIT
            time.sleep(0.1)  # gentle pace
            if seen_pages > 50_000:
                # Never silently stop with a partial feed (the newest-first watermark/has_more
                # are the real stops). Only a true runaway reaches this — fail LOUD.
                raise CloseError(
                    f"Close call pagination exceeded {seen_pages} pages — refusing to return a partial feed")

    # --- CRM-mirror endpoints (2026-07-10 Close→warehouse ingest) ---------
    # All read-only GETs. See entities/close_crm_mirror.py + sql/ddl/99_close_crm_mirror.sql.

    def iter_activities(self, activity_type: str, since: datetime | None = None) -> Iterator[dict]:
        """Yield Close activity objects of one type (email / sms / status_change),
        newest-first via _skip/_limit, stopping at the `since` watermark on
        date_created (same pattern as iter_calls; the caller passes a watermark
        with a safety overlap because activity feeds are only ~newest-first).

        `activity_type` is the URL segment: 'email', 'sms', 'status_change/lead'.
        """
        skip = 0
        seen_pages = 0
        while True:
            payload = self._get(
                f"/activity/{activity_type}/", params={"_limit": _PAGE_LIMIT, "_skip": skip}
            )
            data = payload.get("data") or []
            if not data:
                return
            for act in data:
                if since is not None:
                    dc = _parse_dt(act.get("date_created"))
                    if dc is not None and dc <= since:
                        logger.info(
                            "iter_activities(%s) hit watermark (date_created %s <= %s) — stopping",
                            activity_type, dc.isoformat(), since.isoformat(),
                        )
                        return
                yield act
            seen_pages += 1
            if not payload.get("has_more"):
                return
            skip += _PAGE_LIMIT
            time.sleep(0.1)
            if seen_pages > 50_000:
                raise CloseError(
                    f"Close {activity_type} pagination exceeded {seen_pages} pages — refusing to return a partial feed")

    def iter_leads(self, window_days: int = 15, start: str = "2025-06-01") -> Iterator[dict]:
        """Yield EVERY Close lead (full snapshot) via date_created windows.

        The /lead/ search endpoint caps _skip pagination at 10k results, so we
        window the query by date_created (15-day windows keep each window well
        under the cap at current volume ~20k leads total) and _skip within each
        window. Empty windows cost one request. Full snapshot each run: leads
        mutate (status, custom fields) without a reliable updated-feed, and 20k
        rows ≈ 100 requests — cheap enough nightly.
        """
        from datetime import timedelta
        cursor = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end = datetime.now(timezone.utc) + timedelta(days=1)
        while cursor < end:
            w_end = cursor + timedelta(days=window_days)
            query = (
                f'date_created >= "{cursor.date().isoformat()}" '
                f'date_created < "{w_end.date().isoformat()}" sort:created'
            )
            skip = 0
            while True:
                payload = self._get(
                    "/lead/", params={"query": query, "_limit": 200, "_skip": skip}
                )
                data = payload.get("data") or []
                for lead in data:
                    yield lead
                if not payload.get("has_more"):
                    break
                skip += 200
                time.sleep(0.1)
            cursor = w_end

    def get_dim(self, path: str) -> list[dict]:
        """Small dimension pulls (lead statuses, custom-field defs, smart views).
        One unpaginated GET; these are tiny (≤ dozens of rows)."""
        payload = self._get(path, params={"_limit": _PAGE_LIMIT})
        return payload.get("data") or []

    def get_lead(self, lead_id: str) -> dict | None:
        """`GET /lead/{lead_id}/` — the Close lead. Carries:
          - contacts[].emails[].email        (email identity; empty for Sendivo phone-only)
          - contacts[].phones[].phone        (E.164 phone identity)
          - custom.{Campaign,Source} AND flattened `custom.cf_*` keys (attribution)
        Returns None on a 404 (deleted lead) so the caller can flag-not-crash.
        """
        try:
            return self._get(f"/lead/{lead_id}/")
        except CloseError as exc:
            if " -> 404" in str(exc):
                logger.warning("Close lead %s not found (404)", lead_id)
                return None
            # Persistent non-404 failure (incl. exhausted retries): skip this lead
            # (it will be flagged as an orphan / unattributed) rather than kill the
            # whole phase. One bad lead-fetch must never sink the backfill.
            logger.warning("Close lead %s unresolved (%s) — skipping", lead_id, str(exc)[:120])
            return None
