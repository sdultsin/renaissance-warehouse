"""Sendivo REST client (SMS send-side). Bearer auth.

Just the read endpoints the warehouse needs (spec 14). Base: app.sendivo.io/api/v1.
Validated 2026-05-31: /campaigns returns the exact UI roster (30/16/14);
/delivery-metrics caps at a 30-day range (422 beyond).
"""
from __future__ import annotations

import logging
import time

import httpx

logger = logging.getLogger("sources.sendivo")

BASE_URL = "https://app.sendivo.io/api/v1"
_TIMEOUT = 30.0


class SendivoError(RuntimeError):
    pass


class SendivoClient:
    def __init__(self, api_key: str):
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _get(self, path: str, params: dict | None = None) -> dict:
        for attempt in range(3):
            resp = self._client.get(path, params=params)
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(1.5 * (attempt + 1))
                continue
            if resp.status_code >= 400:
                raise SendivoError(f"GET {path} {params} -> {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        raise SendivoError(f"GET {path} {params} -> repeated 5xx/429")

    def _get_list(self, path: str, params: dict | None = None) -> list[dict]:
        """GET a list endpoint, transparently following pagination IF the API exposes it.

        Validated 2026-06-14 against the live key: /campaigns (60), /brands (65) and
        /phone-numbers (262) each return the FULL set in ONE un-paginated response — the
        `page`/`per_page` params are ignored and there is no `meta`/`links` block. This guard
        is purely DEFENSIVE: if Sendivo ever switches on Laravel-style pagination we follow
        `meta.last_page` (or `links.next`) instead of silently truncating to page 1 (audit E3).
        """
        params = dict(params or {})
        payload = self._get(path, params)
        data = payload.get("data")
        if not isinstance(data, list):
            return data or []
        meta = payload.get("meta") or {}
        last_page = meta.get("last_page")
        if isinstance(last_page, int) and last_page > 1:
            for page in range(2, last_page + 1):
                more = self._get(path, {**params, "page": page}).get("data")
                if not isinstance(more, list) or not more:
                    break
                data.extend(more)
            return data
        # links.next style (only if there IS a next link — absent today)
        nxt = (payload.get("links") or {}).get("next")
        page = 2
        while nxt and page <= 100:  # hard stop so a malformed `next` can't loop forever
            p = self._get(path, {**params, "page": page})
            more = p.get("data")
            if not isinstance(more, list) or not more:
                break
            data.extend(more)
            nxt = (p.get("links") or {}).get("next")
            page += 1
        return data

    # --- endpoints -------------------------------------------------------
    def delivery_metrics(self, start_date: str, end_date: str, sub_account_id: int | None = None) -> dict:
        """Aggregate funnel for a date range (<=30 days). Returns the `data` dict.

        Pass sub_account_id for the PER-SUB-ACCOUNT funnel (validated 2026-06-14: the param is
        honoured — agency vs sub return different totals); omit it for the agency aggregate.
        """
        params = {"start_date": start_date, "end_date": end_date}
        if sub_account_id is not None:
            params["sub_account_id"] = sub_account_id
        payload = self._get("/delivery-metrics", params)
        return payload.get("data") or {}

    def campaigns(self) -> list[dict]:
        return self._get_list("/campaigns")

    def brands(self) -> list[dict]:
        return self._get_list("/brands")

    def phone_numbers(self) -> list[dict]:
        """/phone-numbers — full sending-asset inventory snapshot (audit G2). One row per number:
        id, phone_number, friendly_name, tags, number_status, messaging_status, phone_number_type,
        is_default, campaign{id,name,status}, brand{id,name}, sub_account_id, purchase/renewal_date.
        """
        return self._get_list("/phone-numbers")

    def billing_report(self, start_date: str, end_date: str) -> list[dict]:
        """Per-sub-account billing for a period. Returns the `data` list."""
        payload = self._get("/billing/report", {"start_date": start_date, "end_date": end_date})
        d = payload.get("data")
        return d if isinstance(d, list) else ([d] if d else [])

    def sms_logs_page(self, day: str, page: int, per_page: int = 1000) -> dict:
        """One page of /sms/logs for a single DAY. Returns the `data` dict {logs, pagination}.

        The date filter is day-granular (time-of-day is ignored), so pass the same date for
        start/end. This endpoint is heavy (the `total` is a full-table count) and times out
        intermittently — own retry covering ReadTimeout + 429/5xx, longer per-request timeout.
        """
        params = {"start_date": day, "end_date": day, "per_page": per_page, "page": page}
        last = None
        for attempt in range(5):
            try:
                resp = self._client.get("/sms/logs", params=params, timeout=90.0)
                if resp.status_code in (429, 500, 502, 503):
                    last = f"{resp.status_code}"
                    time.sleep(2.5 * (attempt + 1))
                    continue
                if resp.status_code >= 400:
                    raise SendivoError(f"GET /sms/logs {params} -> {resp.status_code}: {resp.text[:200]}")
                return resp.json().get("data") or {}
            except httpx.TimeoutException:
                last = "timeout"
                time.sleep(2.5 * (attempt + 1))
        raise SendivoError(f"GET /sms/logs {params} -> repeated failure ({last})")
