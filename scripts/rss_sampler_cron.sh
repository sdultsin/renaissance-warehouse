#!/usr/bin/env bash
# Per-phase RSS profiler for the nightly window (monitoring only; no data path).
# Truncates the profile at launch (report windows to the last 18h anyway) + self-limits to 12h.
cd /root/renaissance-warehouse
: > /root/core/rss_profile.jsonl
RSS_SAMPLE_OUT=/root/core/rss_profile.jsonl RSS_SAMPLE_DURATION_MIN=720 RSS_SAMPLE_INTERVAL=3 \
  .venv/bin/python /root/core/rss_sampler.py sample
