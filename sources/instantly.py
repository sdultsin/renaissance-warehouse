"""Instantly REST client.

Just the endpoints the warehouse needs. Per-workspace Bearer auth.
User-Agent override because Instantly blocks `Python-urllib/*` and `python-httpx/*`
(see memory `reference_instantly_api_urllib_403_block.md`).
"""

from __future__ import annotations

import logging
import os
import random
import threading
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

# ── adaptive 429 backoff [2026-07-02] ────────────────────────────────────────────
# The old policy (429 shares the flat 3-attempt loop, 65s each) is no match for a SUSTAINED
# rate-limit window: 3×65s ≈ 3.2 min, then the request hard-fails — which is how
# email_thread_sync lost 3 consecutive nightlies (every lead pull died inside the storm and
# core.email_message froze at 2026-06-29). 429s now retry on their OWN adaptive schedule:
# exponential from _429_BASE_WAIT_S doubling up to _429_MAX_WAIT_S between attempts
# (65s → 130 → 260 → 520 → 900 cap ≈ 15 min), with ±15% jitter so parallel workers
# de-synchronize, bounded by a per-request cumulative sleep budget _429_BUDGET_S. Exhausting
# the budget raises InstantlyError like any other terminal failure (callers' failed-lead /
# retry-next-run semantics unchanged). Transport/5xx retry behavior is untouched.
_429_BASE_WAIT_S = float(os.environ.get("INSTANTLY_429_BASE_WAIT_S", "65"))
_429_MAX_WAIT_S = float(os.environ.get("INSTANTLY_429_MAX_WAIT_S", "900"))
_429_BUDGET_S = float(os.environ.get("INSTANTLY_429_BUDGET_S", "1800"))


def backoff_429_wait(rl_attempt: int, base: float | None = None, cap: float | None = None) -> float:
    """Nominal (pre-jitter) wait before 429 retry number `rl_attempt` (0-based): base·2^n, capped."""
    b = _429_BASE_WAIT_S if base is None else base
    c = _429_MAX_WAIT_S if cap is None else cap
    return min(b * (2 ** rl_attempt), c)


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
        # Per-client 429 tally (thread-safe: email_thread_sync shares one client across its
        # per-lead worker pool) so callers can report the run's rate-limit pressure — the QA
        # needs to tell "partial progress under a 429 storm" apart from "total failure".
        self.rate_limit_hits = 0
        self._rl_lock = threading.Lock()

    def _note_rate_limit(self) -> None:
        with self._rl_lock:
            self.rate_limit_hits += 1

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- helpers ---------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        import time as _time
        retries = 3          # transport/5xx attempts (historic semantics, unchanged)
        attempt = 0
        rl_attempt = 0       # 429 attempts — separate ADAPTIVE schedule (see module header)
        rl_slept = 0.0
        while True:
            # Transient transport failures (ReadTimeout on slow /emails pages, conn
            # resets) must retry, not kill the whole workspace pull — a single slow
            # page repeatedly aborted the-gatekeepers replies ingest (2026-06-11).
            try:
                resp = self._client.get(path, params=params)
            except httpx.TransportError as exc:
                attempt += 1
                if attempt >= retries:
                    raise
                wait = 15 * attempt
                logger.warning(
                    "GET %s -> %s (attempt %d/%d), sleeping %ds",
                    path, type(exc).__name__, attempt, retries, wait,
                )
                _time.sleep(wait)
                continue
            if resp.status_code == 429:
                # Adaptive exponential backoff for SUSTAINED rate-limit windows [2026-07-02]:
                # does NOT consume the transport/5xx attempts; bounded by a cumulative
                # per-request sleep budget instead (fail like any terminal error past it).
                self._note_rate_limit()
                wait = backoff_429_wait(rl_attempt) * random.uniform(0.85, 1.15)
                if rl_slept + wait > _429_BUDGET_S:
                    raise InstantlyError(
                        f"GET {path} -> 429: adaptive backoff budget exhausted "
                        f"(~{int(rl_slept)}s slept over {rl_attempt} retries; "
                        f"budget {int(_429_BUDGET_S)}s)")
                logger.warning(
                    "GET %s -> 429 rate-limit (429-retry %d, sleeping %.0fs; %.0f/%.0fs budget used)",
                    path, rl_attempt + 1, wait, rl_slept, _429_BUDGET_S,
                )
                _time.sleep(wait)
                rl_slept += wait
                rl_attempt += 1
                continue
            if resp.status_code >= 500:
                # Instantly 5xx are transient ("please try again") — seen repeatedly
                # on deep /emails pagination (2026-06-11). Retry with backoff.
                attempt += 1
                if attempt < retries:
                    wait = 30 * attempt
                    logger.warning(
                        "GET %s -> %d (attempt %d/%d), sleeping %ds",
                        path, resp.status_code, attempt, retries, wait,
                    )
                    _time.sleep(wait)
                    continue
            if resp.status_code >= 400:
                body = resp.text[:500]
                raise InstantlyError(f"GET {path} -> {resp.status_code}: {body}")
            return resp.json()

    # Pagination page-count ceiling. Crossing it means we BAILED before the cursor
    # was exhausted, i.e. the result set is silently truncated mid-history. Callers
    # that need full coverage (email-thread-sync full backfill) MUST treat a ceiling
    # hit as a HARD FAIL and NOT advance their watermark (FINALIZED-SPEC §4.C/R8).
    PAGINATION_CEILING = 5_000    # [06-30 lowered from 100_000] runaway BACKSTOP (~500k records).
                                  # 100k never bit: the all-history tag-edge pull (custom-tag-mappings /
                                  # accounts?tag_ids) walked ~91k pages/15h, hanging the whole nightly on
                                  # the writer lock. 5k still clears every legit pull (campaigns/accounts/
                                  # analytics are <500 pages) but caps the runaway -> fails LOUD, and
                                  # per-phase isolation carries the run forward to the canonical phase.

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

    def account_daily_analytics(
        self, emails: list[str], start_date: str, end_date: str
    ) -> list[dict]:
        """`GET /accounts/analytics/daily?emails=<csv>&start_date=&end_date=` —
        per-ACCOUNT day-grain metrics (the sibling of campaign_analytics_daily at
        account grain). Returns a bare list of one object per (account, active-day):
          date, email_account, sent, bounced, contacted, new_leads_contacted,
          opened, unique_opened, replies, unique_replies, replies_automatic,
          unique_replies_automatic, clicks, unique_clicks.
        `unique_replies` = HUMAN (per-lead dedup); `unique_replies_automatic` = AUTO.

        MUST be chunked by the `emails` filter. The whole-workspace pull (no emails)
        413s once a workspace exceeds ~a few hundred accounts — "Payload Too Large:
        add an emails filter or request a smaller date range". The cap is account-COUNT
        (payload size), NOT date-range: even a single day 413s at workspace scope, and
        413 is NOT ret/split-able. Verified live 2026-07-18: renaissance-1 @ 13,607
        accounts whole-workspace 413s; emails-filtered batches of 200 return 200, 500
        413s. Callers pass batches of <=100 (entities/instantly_account_daily.py);
        a single filtered request may span the whole window (one row per account per day).
        """
        if not emails:
            return []
        params = {
            "emails": ",".join(emails),
            "start_date": start_date,
            "end_date": end_date,
        }
        payload = self._get("/accounts/analytics/daily", params=params)
        if isinstance(payload, list):
            return payload
        # Defensive: some deployments wrap the array in {items:[...]} / {daily:[...]}.
        if isinstance(payload, dict):
            return payload.get("items") or payload.get("daily") or []
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
        truncated mid-cursor (>PAGINATION_CEILING pages — pathological for a single lead) and the
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
        ceiling_flag: dict | None = None,
    ) -> Iterator[dict]:
        """`GET /custom-tag-mappings` — the PUBLIC bulk (tag, resource) edge list.

        Unlike `/tag-mappings` (admin-only; 404s on the public v2 surface — see
        `list_tag_mappings`), `/custom-tag-mappings` IS reachable with a workspace key
        and streams EVERY tag edge in one paginated pass (verified live 2026-06-26 on
        Funding 2). Each item: {"id","tag_id","resource_id","resource_type",
        "timestamp_created",...} where `resource_id` is the sending-account EMAIL when
        resource_type == 1.

        ORDERING + FILTERS (verified live 2026-06-30, entities/account_tags.py rewrite):
          * The stream is NEWEST-FIRST by `timestamp_created` (cursor walks backward in
            time), exactly like `/emails`. A caller pulling an INCREMENTAL delta should
            iterate and STOP (break) once it crosses its watermark — everything after is
            older. This is the ONLY safe way to use it: a full walk is ~9M edges across
            workspaces (every account ever × its tags), ~91k pages, hours — it hung the
            nightly 15h (2026-06-30). DON'T full-walk it nightly.
          * `resource_type` is NOT honored server-side (verified: rt=1 returns the SAME
            set as unfiltered, ~96% rt=1 / ~4% rt=2). Filter client-side on
            `resource_type == 1`. The param is sent for forward-compat only.
          * No server-side date filter exists (start_date/created_after/since all ignored)
            — the newest-first client-side STOP is the only incremental bound.

        `ceiling_flag` (a dict) is threaded to `_paginate`: if the page-count backstop is
        hit before the cursor is exhausted it sets `ceiling_flag['hit'] = True` (vs raising)
        so an incremental caller can fail-loud for that workspace WITHOUT merging a partial
        set. Pass it whenever you rely on a bounded incremental window.
        """
        params: dict = {}
        if resource_type is not None:
            params["resource_type"] = resource_type
        yield from self._paginate(
            "/custom-tag-mappings", params=params or None, limit=100, ceiling_flag=ceiling_flag
        )

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
