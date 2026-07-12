#!/usr/bin/env bash
# READER FLIP for the warehouse query API (mcp-server :8899) — env-switched, reversible in one step.
# Staged 2026-07-12 (Lane B). The shim (/opt/duckdb/bin/mcp_server.py + common.py) is already
# MotherDuck-capable (WAREHOUSE_BACKEND=local|md, code-reviewed wf_c8484120-6ef, flipped once on
# 07-10 and rolled back). This script is the operational wrapper: it injects/removes the systemd
# drop-in, restarts the service, and VERIFIES the result — auto-rolling back a failed apply.
#
#   reader_flip.sh apply     -> serve md:<active color from /opt/duckdb/md_serving_db> (read-scoped token)
#   reader_flip.sh rollback  -> back to the local serving snapshot (seconds)
#   reader_flip.sh status    -> current backend + healthz
set -uo pipefail

DROPIN_DIR=/etc/systemd/system/mcp-server.service.d
DROPIN=$DROPIN_DIR/md-backend.conf
ENVF=/root/renaissance-warehouse/.env
HEALTH=http://localhost:8899/healthz

healthz() { curl -s --max-time 10 "$HEALTH" 2>/dev/null; }

verify() {  # $1 = "md" | "local"; polls up to ~30s
    for _ in $(seq 1 10); do
        sleep 3
        H=$(healthz)
        case "$1" in
            md)    echo "$H" | grep -q '"ok":true' && echo "$H" | grep -q '"snapshot_id":"md:' && { echo "$H"; return 0; } ;;
            local) echo "$H" | grep -q '"ok":true' && echo "$H" | grep -q '\.duckdb"'          && { echo "$H"; return 0; } ;;
        esac
    done
    echo "$H"; return 1
}

case "${1:-status}" in
  apply)
    RO=$(grep '^MOTHERDUCK_TOKEN_RO=' "$ENVF" | cut -d= -f2- | tr -d '"' | tr -d "'")
    [ -n "$RO" ] || { echo "FATAL: MOTHERDUCK_TOKEN_RO missing from $ENVF (read-scoped token is the flip prereq)"; exit 2; }
    PTR=$(cat /opt/duckdb/md_serving_db 2>/dev/null || echo '?')
    echo "flipping mcp-server to MotherDuck (active color: $PTR)"
    mkdir -p "$DROPIN_DIR"
    umask 077
    cat > "$DROPIN" <<EOF
# Reader flip: warehouse query API -> MotherDuck (env-switched shim; RUNBOOK-TUESDAY.md step c)
# Rollback: /root/md-migration/reader_flip.sh rollback
[Service]
Environment=WAREHOUSE_BACKEND=md
Environment=MOTHERDUCK_TOKEN_RO=$RO
EOF
    systemctl daemon-reload && systemctl restart mcp-server
    if verify md; then
        echo "FLIPPED: mcp-server serving MotherDuck (read-only token; snapshot_id above)."
        echo "Observe 1 business day. Rollback anytime: $0 rollback"
    else
        echo "VERIFY FAILED — auto-rolling back to local"
        rm -f "$DROPIN"; systemctl daemon-reload; systemctl restart mcp-server
        verify local >/dev/null && echo "rolled back to local OK" || echo "ROLLBACK VERIFY ALSO FAILED — check: systemctl status mcp-server"
        exit 2
    fi
    ;;
  rollback)
    rm -f "$DROPIN"
    rmdir "$DROPIN_DIR" 2>/dev/null || true
    systemctl daemon-reload && systemctl restart mcp-server
    if verify local; then echo "ROLLED BACK: mcp-server serving the local snapshot."; else echo "VERIFY FAILED — check: systemctl status mcp-server"; exit 2; fi
    ;;
  status)
    [ -f "$DROPIN" ] && echo "drop-in PRESENT ($DROPIN):" && sed 's/MOTHERDUCK_TOKEN_RO=.*/MOTHERDUCK_TOKEN_RO=<redacted>/' "$DROPIN" || echo "drop-in absent (backend=local unless set elsewhere)"
    echo "healthz: $(healthz)"
    ;;
  *) echo "usage: $0 apply|rollback|status"; exit 1 ;;
esac
