"""Env loader with allowlist + per-workspace Instantly key extraction.

Never logs values. Never falls through to 1Password CLI in the routine path. If a
required key is missing, raise loudly — Sam fixes the env, retries.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import dotenv_values

from core.config import ENV_FILE_CANDIDATES


@dataclass
class Credentials:
    """Merged read-only view across all .env files, plus the live process environment.

    Lookup precedence (high → low):
      1. Process environment
      2. ENV_FILE_CANDIDATES, in order

    Process env wins so cron / wrappers can override per-run.
    """

    _values: dict[str, str] = field(default_factory=dict)

    def require(self, key: str) -> str:
        v = self._values.get(key) or os.environ.get(key)
        if not v:
            raise KeyError(f"Required credential not set: {key}")
        return v

    def optional(self, key: str) -> str | None:
        return self._values.get(key) or os.environ.get(key) or None

    def instantly_workspace_keys(self) -> dict[str, str]:
        """Returns {workspace_slug: api_key} from INSTANTLY_KEY_<SLUG> conventions.

        Excludes meta-keys (PERSONAL, SAM_TEST, WARM_LEADS, etc.) — those belong
        in CONSUMER_WORKSPACE_KEYS or similar separate vars when needed.
        """
        keys = {}
        excluded = {"INSTANTLY_KEY_PERSONAL", "INSTANTLY_KEY_SAM_TEST", "INSTANTLY_KEY_WARM_LEADS"}
        for k, v in self._values.items():
            if k.startswith("INSTANTLY_KEY_") and k not in excluded and v:
                slug = k.removeprefix("INSTANTLY_KEY_").lower().replace("_", "-")
                keys[slug] = v
        for k, v in os.environ.items():
            if k.startswith("INSTANTLY_KEY_") and k not in excluded and v and k.removeprefix("INSTANTLY_KEY_").lower().replace("_", "-") not in keys:
                slug = k.removeprefix("INSTANTLY_KEY_").lower().replace("_", "-")
                keys[slug] = v
        return keys


def load_credentials() -> Credentials:
    merged: dict[str, str] = {}
    for path in ENV_FILE_CANDIDATES:
        if not path.exists():
            continue
        values = dotenv_values(path)
        for k, v in values.items():
            if v is None:
                continue
            # Earlier files win — preserve precedence in ENV_FILE_CANDIDATES order.
            merged.setdefault(k, v)
    return Credentials(_values=merged)
