"""Single source of truth for paths and sync window phase ordering."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# DuckDB file path. Overridable via env. Filename must NOT match any schema name
# inside the DB (DuckDB binds the file as the default catalog), so we use
# warehouse.duckdb and keep `core` as the canonical schema name inside it.
DEFAULT_DB_PATH = "/root/core/warehouse.duckdb"
DB_PATH = Path(os.environ.get("CORE_DB_PATH", DEFAULT_DB_PATH))

# Backup location. Nightly cp lands here. Droplet only.
BACKUP_DIR = Path(os.environ.get("CORE_BACKUP_DIR", "/root/archive/mac-offload/core"))
BACKUP_RETENTION_DAYS = 14

# .env search order. First match wins for any given key, but credentials.py merges
# across all of them so per-workspace Instantly keys can live in .env.instantly while
# other keys live in .env. An optional parent directory holding shared .env files can be
# pointed at via the RENAISSANCE_ENV_DIR environment variable (e.g. a local dev checkout).
_ENV_DIR = Path(os.environ.get("RENAISSANCE_ENV_DIR", str(REPO_ROOT.parent)))
ENV_FILE_CANDIDATES = [
    REPO_ROOT / ".env",
    _ENV_DIR / ".env",
    _ENV_DIR / ".env.instantly",
]

# Sync window phase order. Orchestrator runs phases in this order. Within a phase
# multiple ingests can register and they run sequentially in registration order.
# PHASE_ORDER is split into two PASSES by scripts/nightly.sh, with a SERVING PROMOTE between them.
# [2026-07-14] Why: the serving snapshot can only be published when the writer lock is free — i.e.
# when the orchestrator exits. With one 7h pass that meant the snapshot landed ~09:40 ET, so the
# Data Hub's morning rebuild always read YESTERDAY's snapshot. The fleet-health tables (census,
# sending_account, tags) are fast; the slow phases in front of them (instantly_replies ~90m,
# dns_sweep ~73m, CRM) are NOT needed for them. So PASS A now runs only what fleet health needs
# (~1.5h) and promotes at ~03:30 ET; PASS B runs everything else and promotes again at completion,
# exactly as before. Every ingest still runs EXACTLY ONCE per night (account_status_history is
# append-only — re-running a phase would double-insert), and no ingest was moved ahead of an
# upstream it reads. See PASS_A_PHASES / PASS_B_PHASES in scripts/nightly.sh.
PHASE_ORDER = [
    # ── PASS A — fleet-health critical; promotes the serving snapshot on completion (~03:30 ET) ──
    "pipeline_mirror",   # slim mirror from pipeline-supabase
    "inbox_loader",      # drain /root/warehouse-inbox upsert batches (Gates 1b/2a)
    "instantly",         # workspaces, campaigns, accounts, tags, lead membership (replies split out below)
    "account_census",    # promote live /accounts poll parquet -> core.account_census (live truth)
    "portal_core",       # [2026-07-14] core.sending_account + core.account_first_cold_send — split out of
                         #   'canonical' because they read ONLY account_census / raw instantly / pipeline
                         #   (never reply/domain/dns), so they can run early. These + account_census +
                         #   account_tags are what the Data Hub's fleet-health pages read.
    "account_tags_late", # per-inbox Instantly tag pull. Still the last phase of its pass so it can never
                         #   block the phases above; now bounded by WAREHOUSE_ACCOUNT_TAGS_DEADLINE_MIN
                         #   (graceful skip, last-good kept) so it can't delay PASS B either.
    # ── PASS B — everything else; compaction + the full promote at completion, as before ──────────
    "comms_mirror",      # comms-orchestration snapshot
    "sendivo",           # Sendivo SMS send-side (delivery metrics, campaigns, billing)
    "iskra",             # Iskra WhatsApp (messages/conversations/meetings/deals/numbers/stats)
    "outreachify",       # Outreachify Supabase snapshot
    "replies_late",      # [2026-07-14] instantly_replies (~90m) — split out of the 'instantly' phase so it
                         #   no longer sits IN FRONT of the fleet-health tables. Still runs before
                         #   'canonical', which is what reads core.reply.
    "close",             # Close CRM warm-call activity (BI/BOF layer, spec 16)
    "sheets",            # Domain Tech Sheet, blacklist sheet, partner feedback
    "otd_billing",       # parse OTD account statement -> core.otd_* + cost_ledger rate fix
    "im_bookings",       # nightly mirror of the bookings-portal im_bookings table (Scope A)
    "account_truth",     # snapshot from droplet account-truth duckdb
    "dns_sweep",         # MX/A/SPF/DKIM/DMARC/PTR + DNSBLs + redirects
    "canonical",         # rebuild canonical tables from raw (incl. core.reply, lead spine, conversions)
    # "intent" phase removed — LLM reply-intent classifier uses Anthropic API; needs explicit Sam go-ahead before enabling
    "iam_response_time", # IAM response latency per prospect reply (after canonical)
    "derived",           # materialize derived views (incl. lead_intel)
]
