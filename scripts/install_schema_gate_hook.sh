#!/usr/bin/env bash
# Point this repo's git at .githooks so the schema-gate pre-commit hook fires.
# Idempotent. Run once per clone/worktree (or add to onboarding).
#
# Phase 1: the hook is WARN-ONLY — it never blocks a commit. This only wires it up.
set -euo pipefail
REPO_DIR="$(git rev-parse --show-toplevel)"
cd "$REPO_DIR"
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit 2>/dev/null || true
echo "schema-gate: core.hooksPath -> .githooks (pre-commit active, WARN-ONLY)"
