"""Single source of truth for paths and sync window phase ordering."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# DuckDB file path. Overridable via env (e.g., local dev uses ./core.duckdb).
DEFAULT_DB_PATH = "/root/core/core.duckdb"
DB_PATH = Path(os.environ.get("CORE_DB_PATH", DEFAULT_DB_PATH))

# Backup location. Nightly cp lands here. Droplet only.
BACKUP_DIR = Path(os.environ.get("CORE_BACKUP_DIR", "/root/archive/mac-offload/core"))
BACKUP_RETENTION_DAYS = 14

# .env search order. First match wins for any given key, but credentials.py merges
# across all of them so per-workspace Instantly keys can live in .env.instantly while
# other keys live in .env.
ENV_FILE_CANDIDATES = [
    REPO_ROOT / ".env",
    Path("/Users/sam/Documents/Claude Code/Renaissance/.env"),
    Path("/Users/sam/Documents/Claude Code/Renaissance/.env.instantly"),
]

# Sync window phase order. Orchestrator runs phases in this order. Within a phase
# multiple ingests can register and they run sequentially in registration order.
PHASE_ORDER = [
    "pipeline_mirror",   # 03:30 — slim mirror from pipeline-supabase
    "comms_mirror",      # 03:45 — comms-orchestration snapshot
    "outreachify",       # 03:45 — Outreachify Supabase snapshot
    "instantly",         # 04:00 — workspaces, campaigns, accounts, tags, lead membership
    "sheets",            # 04:15 — Domain Tech Sheet, blacklist sheet
    "account_truth",     # 04:30 — snapshot from droplet account-truth duckdb
    "dns_sweep",         # 04:45 — MX/A/SPF/DKIM/DMARC/PTR + DNSBLs + redirects
    "canonical",         # 05:30 — rebuild canonical tables from raw
    "derived",           # 05:40 — materialize derived views
]
