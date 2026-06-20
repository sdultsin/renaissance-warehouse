# 07a — Blocklist surveillance prep notes (recon for spec 07)

Read of `/root/renaissance-worker/jobs/blocklist-surveillance/` on `renaissance-worker`, 2026-05-30.
Companion to `specs/07-entity-domain-dns-sweep.md`. Phase 3 implementer should read this before starting.

## 1. How the registry is actually built

`registry.py` iterates **every `INSTANTLY_KEY_*` var** in `.env.instantly` (except `EXCLUDE_KEYS = {PERSONAL, SAM_TEST, WARM_LEADS}`), pages `/api/v2/accounts` with `starting_after` (NOT `skip`), extracts `email.split("@")[1]`, builds `{domain: {workspaces: {slug: {api_key, account_count}}}}`. Concurrency capped at 3 workspaces in flight. Output cached to `state/domain_registry.json` (11 MB, 51,832 domains) and **only rebuilt on `--rebuild-registry`** — daily cron uses the cache verbatim. Last rebuild was 2026-05-18; deleted/paused accounts only fall off when registry is manually rebuilt. **The spec's "intersection with `core.sending_account.domain`" is strictly stricter than what runs today** — there is no sheet or pipeline cross-check at all. We should keep the per-workspace pagination logic since `core.sending_account` is built the same way upstream.

## 2. Hidden assumptions in the cron

- Reads two env files: `.env` (BLS_* flags + Spamhaus key + Slack webhook) AND `.env.instantly` (workspace API keys). Both must be sourced.
- `BLS_DNS_RESOLVER=127.0.0.2` — points at a **local recursive resolver on the droplet**, not 8.8.8.8. SURBL/Spamhaus DNSBLs block queries from public resolvers; we must preserve this. Warehouse Phase 3 needs the same local resolver setup or it will get poisoned responses.
- Cron is daily 08:00 UTC after janitor at 07:00. Today's run took **66 minutes** (3,966s) for 51,832 domains × 2 DNSBLs + Spamhaus REST. The spec's "<30min" SLA is aspirational at current QPS=20.
- Workspace 4xx errors break the whole pagination loop for that workspace (`break` on `HTTPStatusError`) — partial results silently merge. No 401/402 handling beyond logging.
- Slack webhook is the same one used by the janitor (channel `C0B33FEE7LN`).

## 3. Empirical findings from today's log

- **51,832 domains scanned, 29,445 listed, 66min runtime, ~31,045 accounts "would-pause"** (60% of all sending domains in our portfolio).
- **SURBL accounts for 29,429 of 29,445 listings (99.9%).** Spamhaus DBL only flagged 67. Spamrl flagged 0. TLD spread: 19,566 `.info` / 9,555 `.co` / 204 `.com` / 120 `.org` — confirms our cheap-TLD batches are getting carpet-bombed by SURBL.
- 92 Spamhaus 504s + 3 500s out of 51,670 requests (0.18% error rate, retried-as-clean — silent false-negatives).
- Spamrl returned zero listings across 51k queries: **either spamrl is moribund or our local resolver isn't reaching it.** Worth probing before relying on it.

## 4. Spamhaus DQS key situation

There are **two keys** and neither is the documented DQS zone setup:
- Hardcoded fallback in `config.py` (long b64-ish string, 5dc415f1-…): I curl-tested it just now — returns **HTTP 500**. Dead.
- `.env` override `BLS_SPAMHAUS_DQS_KEY=zeatuqr2ocbgw5qczd4zv6xdku`: returns **HTTP 404** for clean domains, **200** for listed. Working. This is the one in actual use.

Both hit `apibl.spamhaus.net/lookup/v1/dbl/<domain>` (the WQS/free-trial REST endpoint), **not** the documented `<key>.dbl.dq.spamhaus.net` DNS zone the config comment describes. The comment is wrong. No way to tell from the code when the key was rotated or whether the trial is paid — flag for Sam.

## 5. Dry-run / live-mode

`BLS_DRY_RUN=1` in `.env` — has been DRY-RUN the entire history. In dry-run, `pauser.py` skips the per-account fetch and emits one log line per (domain, workspace) with `~account_count` from the cached registry. Today's "31,045 would-pause" is the sum of `account_count` across all 29,445 listed domains × workspaces (not a unique-account count — a domain in 2 workspaces double-counts).

The live path (`dry_run=False`) fetches accounts per listed domain, POSTs `/accounts/{email}/pause`, sleeps 0.3s. **Latent risk:** with today's volume this would fire ~31k pause calls in ~2.5 hours and nuke 60% of the portfolio. Anyone flipping the flag without first tightening the listing criteria (e.g. require SURBL + Spamhaus, or N-day persistence) would brick sends. Spec is right to keep warehouse observe-only in v1.

## 6. Recommended changes to spec 07

1. **Don't blindly add the 5 new DNSBLs.** Surbl is already 99.9% noise on `.co/.info`; SORBS/UCEPROTECT/SpamCop are likely worse on our portfolio. Start with the existing 3, add new ones one at a time with N-day-listed corroboration before treating any as actionable. The "would-pause 60% of portfolio" data point should change the spec's tone from "absorb + extend" to "absorb + measure baseline noise per blocklist before extending."
2. **Absorb the WHOIS age cache.** `state/domain_age_cache.json` (3,002 entries, 18k lines) is incremental at 500 queries/run with a 1.2s WHOIS delay. Spec 07 doesn't mention age at all but `domain_age.py` is a built feature — add `core.domain.acquisition_date` provenance: WHOIS cached value first, manual sheet second.
3. **Local DNS resolver is mandatory.** Spec doesn't mention `BLS_DNS_RESOLVER=127.0.0.2`. Add an explicit infra requirement: warehouse DNS workers must run on a host with a local recursive resolver, or the SURBL/Spamhaus DNS queries will return the false-positive sentinel IPs (`127.255.255.254`, etc.) that `checker.py:FALSE_POSITIVE_IPS` already guards against.
4. **DQS key + endpoint clarification.** Spec line 33 says "existing key in blocklist-surveillance config" — the config's documented fallback is dead. Use the `.env` key, hit the REST endpoint (not the `dbl.dq.spamhaus.net` DNS zone the comment describes), and add a monthly alive-check.
5. **Replace `--rebuild-registry` flag with continuous reconciliation.** The current daily cron uses a 12-day-old cache. In the warehouse, `core.sending_account` is already continuously refreshed — phase 3 should join against it rather than maintaining its own cached `domain_registry.json`. That alone fixes the deleted-account drift problem.

## 7. Other surprises

- **`run_at` in `last_run.json` is the start-time, not finish-time** — delta calculation compares listing snapshots taken ~66 minutes apart. New-listing alerts have ±1hr noise built in.
- **Multi-listed count is tiny (51/29,445).** Almost no overlap between SURBL and Spamhaus, which makes "N-blocklist majority vote" a viable suppression filter when we go live.
- **`compare_instantly.py`, `build_domain_provider_map.py`, `provider_for_blocklisted.py`, `tag_domain_map.py`** are one-shot analysis scripts (last touched 2026-05-19), not part of the cron. Their state files (`domain_provider.json` 6.5 MB, `tag_domain_map.json` 6.8 MB) are stale; absorb only as historical seed if useful, not as authority.
- **`auth_checker.py`** does SPF/DKIM/DMARC probing standalone — overlaps the spec's "DNS sweep" section. Worth reading before reimplementing.
- **The "would-pause 31,045" output isn't unique accounts**, it double-counts domain-in-multiple-workspaces. Any equivalent metric in `core.domain_blacklist_event` should be (`COUNT DISTINCT sending_account`), not summed `account_count`.
