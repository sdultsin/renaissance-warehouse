"""comms-orchestration source: credential/connection helpers.

Thin wrapper (mirrors sources/pipeline_supabase.py). The actual mirror logic
lives in ``entities/comms_mirror.py`` and uses DuckDB's ``postgres_scanner`` to
read directly from the comms-orchestration Postgres (Sendivo SMS + warm-call +
AIM). This module centralizes how we resolve the connection URL.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# The credential key in the .env / credential store.
# It is a postgresql:// pooler URL.
CREDENTIAL_KEY = "COMMS_SUPABASE_DB_URL"


def resolve_pg_url(credentials) -> str:
    """Return the comms-orchestration Postgres connection URL.

    ``credentials`` is the RunContext credentials helper (has ``require``).
    """
    return credentials.require(CREDENTIAL_KEY)
