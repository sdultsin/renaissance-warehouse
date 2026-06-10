#!/bin/bash
# Self-monitor for the Sendivo blast-body reconcile BACKFILL (cron, every 15 min).
# - process alive  -> do nothing
# - marker present -> one-time completion ping to #cc-sam, then retire itself (marker.pinged)
# - dead + no marker -> relaunch from checkpoint + ping (capped at 12 restarts so a
#   genuinely broken run alerts loudly instead of silently churning forever)
# Deployed at /root/reconcile/watchdog.sh on renaissance-worker.
set -u
DIR=/root/reconcile
CK=$DIR/checkpoint.json
LOG=$DIR/backfill.log
PY=/root/renaissance-warehouse/.venv/bin/python
SCRIPT=/root/renaissance-warehouse/scripts/sendivo_body_reconcile.py
set -a; . $DIR/.env; set +a

slack() {
  curl -s -m 30 -X POST https://slack.com/api/chat.postMessage \
    -H "Authorization: Bearer $CC_SLACK_BOT_TOKEN" \
    --data-urlencode "channel=C0AR0EA21C1" --data-urlencode "text=$1" >/dev/null
}

# Completed?
if [ -f "$CK.done" ]; then
  if [ ! -f "$CK.done.pinged" ]; then
    INS=$($PY -c "import json;print(json.load(open('$CK')).get('inserted_total',0))" 2>/dev/null || echo "?")
    slack ":white_check_mark: Sendivo blast-body backfill COMPLETE — recovered rows matched: ${INS}. Running final requeue now."
    touch "$CK.done.pinged"
    # Final requeue so everything recovered lands in Close without waiting for the nightly.
    nohup $PY $SCRIPT --requeue --env-file $DIR/.env --checkpoint $CK >> $DIR/requeue.log 2>&1 &
  fi
  exit 0
fi

# Still running?
pgrep -f "sendivo_body_reconcile.py --backfill" >/dev/null && exit 0

# Dead without completion -> relaunch (capped).
N=$(cat $DIR/restarts 2>/dev/null || echo 0)
if [ "$N" -ge 12 ]; then
  if [ ! -f $DIR/gaveup ]; then
    slack ":rotating_light: Sendivo blast-body backfill has died ${N}x and hit the restart cap — needs a human look (renaissance-worker:/root/reconcile/backfill.log)."
    touch $DIR/gaveup
  fi
  exit 0
fi
echo $((N+1)) > $DIR/restarts
nohup $PY $SCRIPT --backfill --auto --min-day 2026-05-01 \
  --env-file $DIR/.env --checkpoint $CK --slack-done >> $LOG 2>&1 &
slack ":arrows_counterclockwise: Sendivo blast-body backfill was not running — watchdog relaunched it from checkpoint (restart $((N+1))/12)."
