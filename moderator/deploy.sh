#!/usr/bin/env bash
# deploy.sh — push the Schema Moderator Service code + vendored engine + unit to the droplet (scp).
# The warehouse repo is the source of truth; /opt/moderator on the droplet is NOT a git repo
# (mirrors serving-mcp/deploy.sh). The canonical phase-1 engine (core/schema_gate_lib.py) is
# RE-COPIED every deploy so the service can never drift from the repo.
#
# Does NOT create the secrets env file (/opt/moderator/moderator.env — see moderator/README.md)
# and does NOT enable the unit on first deploy; it restarts the service iff already enabled.
set -euo pipefail
HOST="${WAREHOUSE_HOST:-renaissance-worker}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

echo "==> ensure remote dirs"
ssh "$HOST" 'mkdir -p /opt/moderator/bin /opt/moderator/engine /opt/moderator/logs'

echo "==> scp service code -> $HOST:/opt/moderator/bin/"
scp -q "$HERE"/bin/*.py "$HOST":/opt/moderator/bin/

echo "==> scp vendored engine (canonical phase-1 lib, no drift) -> /opt/moderator/engine/"
scp -q "$REPO"/core/schema_gate_lib.py "$HOST":/opt/moderator/engine/

echo "==> scp systemd unit -> /etc/systemd/system/"
scp -q "$HERE"/systemd/schema-moderator.service "$HOST":/etc/systemd/system/

ssh "$HOST" 'set -e
  systemctl daemon-reload
  if systemctl is-enabled schema-moderator >/dev/null 2>&1; then
    systemctl restart schema-moderator
    echo "restarted schema-moderator"
  else
    echo "schema-moderator NOT enabled yet (first deploy)."
    echo "  -> create /opt/moderator/moderator.env (see moderator/README.md), then:"
    echo "     systemctl enable --now schema-moderator"
  fi
  echo "bin/:";    ls /opt/moderator/bin/
  echo "engine/:"; ls /opt/moderator/engine/
'
echo "==> done."
