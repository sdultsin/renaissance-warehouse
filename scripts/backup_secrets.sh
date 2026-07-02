#!/usr/bin/env bash
# Encrypt ALL box secrets (env files, API keys, query-API tokens, SSH keys, rclone config) with
# age PUBLIC-KEY encryption and push the ciphertext off-box to Drive. Only the holder of the age
# PRIVATE key (Sam, in his password manager) can decrypt — so the bundle is safe even if the box
# OR the Drive is compromised. The box only ever needs the public key to encrypt.
#
# Restore: rclone copy the newest secrets-*.tar.age down, then:
#   age -d -i <identity.key> secrets-YYYY-MM-DD.tar.age | tar -xzf - -C /restore/root
#
# Cron (after the data backups):
#   0 8 * * * /root/renaissance-warehouse/scripts/backup_secrets.sh >> /root/renaissance-warehouse/logs/secrets-backup.log 2>&1

set -uo pipefail
PUBKEY_FILE="${SECRETS_AGE_PUBKEY:-/root/.config/secrets-backup/recipient.pub}"
REMOTE="${SECRETS_OFFBOX_REMOTE:-sdultsin@gmail.com:Renaissance/secrets-encrypted}"
RETENTION_DAYS="${SECRETS_RETENTION_DAYS:-30}"
log() { echo "$(date -u +%FT%TZ) $*"; }

exec 9>/tmp/secrets-backup.lock
flock -n 9 || { log "SKIP: locked"; exit 0; }
command -v age >/dev/null 2>&1 || { log "ERROR: age not installed"; exit 1; }
[[ -f "$PUBKEY_FILE" ]] || { log "ERROR: public key missing: $PUBKEY_FILE"; exit 1; }
RECIP="$(cat "$PUBKEY_FILE")"

TS="$(date -u +%Y-%m-%d)"
LIST="$(mktemp)"; OUT="/tmp/secrets-$TS.tar.age"
trap 'rm -f "$LIST" "$OUT"' EXIT

# Collect every secret/credential file (tiny text files), preserving absolute paths.
find /root -maxdepth 6 \( \
      -name "*.env" -o -name ".env" -o -name ".env.*" -o -name "*.env.*" \
   -o -iname "*api*key*" -o -name "*token*.txt" \
   -o -name "*.pem" -o -name "*.key" -o -name "*.pkey" \) -type f 2>/dev/null \
   | grep -ivE "venv|site-packages|node_modules|/\.git/" >> "$LIST"
[[ -d /root/.ssh ]] && find /root/.ssh -type f 2>/dev/null >> "$LIST"
[[ -f /root/.config/rclone/rclone.conf ]] && echo "/root/.config/rclone/rclone.conf" >> "$LIST"
sort -u -o "$LIST" "$LIST"

COUNT="$(wc -l < "$LIST")"
[[ "$COUNT" -gt 0 ]] || { log "ERROR: no secret files found"; exit 1; }

# tar -> age (public-key) -> ciphertext. Plaintext never written to disk; only the .age file is.
tar -czf - --files-from="$LIST" 2>/dev/null | age -r "$RECIP" -o "$OUT"
SIZE="$(stat -c%s "$OUT")"
log "encrypted $COUNT secret files -> $OUT ($SIZE bytes)"

rclone mkdir "$REMOTE" 2>/dev/null
rclone copy "$OUT" "$REMOTE" --retries 5 2>&1 | tail -2
rclone delete "$REMOTE" --min-age "${RETENTION_DAYS}d" 2>/dev/null || true
log "secrets backup pushed -> $REMOTE (retention ${RETENTION_DAYS}d, $COUNT files)"
