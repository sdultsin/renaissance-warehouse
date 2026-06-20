#!/usr/bin/env python3
"""Tiny CLI Slack alert helper — posts argv[1] to the warehouse alert channel.

Reusable fail-loud primitive for nightly.sh steps that are plain shell pipelines
(no Python of their own) but must alert on failure — e.g. the campaign_data D1
publish. Uses the SAME credential path as warehouse_qa.py: SLACK_TOKEN +
SLACK_ALERT_CHANNEL, read from the process env first, then the repo .env.

Exit 0 if the post succeeded (ok:true), 1 otherwise. Never raises — a Slack
failure must not abort the nightly.

Usage:
    python scripts/alert_slack.py ":rotating_light: something broke"
    SLACK_ALERT_CHANNEL=C123 python scripts/alert_slack.py "message"
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def main(argv: list[str]) -> int:
    if len(argv) < 2 or not argv[1].strip():
        print("alert_slack: no message given", flush=True)
        return 1
    text = argv[1]

    env = load_env(ENV_PATH)
    token = os.environ.get("SLACK_TOKEN") or env.get("SLACK_TOKEN", "")
    cookie = os.environ.get("SLACK_COOKIE") or env.get("SLACK_COOKIE", "")
    channel = os.environ.get("SLACK_ALERT_CHANNEL") or env.get("SLACK_ALERT_CHANNEL", "")
    if not token or not channel:
        print("alert_slack: no SLACK_TOKEN/SLACK_ALERT_CHANNEL, skipping alert", flush=True)
        return 1

    body = json.dumps({"channel": channel, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            **({"Cookie": f"d={cookie}"} if cookie else {}),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            out = json.loads(resp.read().decode("utf-8"))
        if not out.get("ok"):
            print(f"alert_slack: slack error {out.get('error')}", flush=True)
            return 1
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"alert_slack: slack post failed: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
