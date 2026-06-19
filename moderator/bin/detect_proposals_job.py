#!/usr/bin/env python3
"""Weekly rule-evolution detection job (BUILD-SPEC-v2 §8). Run by moderator-proposals.timer.

Runs the deterministic, keyless detector in-process on the box (no auth/HTTP needed) and writes
moderator.rule_proposal rows. The weekly human confirm happens separately via
`moderator_client.py proposals` (the only human touch in rule evolution; default per the governance
fork = whoever runs the warehouse session). Never auto-promotes.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import moderator_common as mc  # noqa: E402
import moderator_engine as eng  # noqa: E402


def main() -> int:
    try:
        out = eng.detect_proposals(window_days=7, min_count=3)
        mc.log_event("proposals_detect_job", **out)
        print(out)
        return 0
    except Exception as e:
        mc.log_event("proposals_detect_job_error", error=f"{type(e).__name__}: {e}")
        print(f"detect_proposals_job error: {e}", file=sys.stderr)
        return 0  # never let the weekly job hard-fail the timer


if __name__ == "__main__":
    sys.exit(main())
