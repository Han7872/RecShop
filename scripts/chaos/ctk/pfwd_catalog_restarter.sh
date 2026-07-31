#!/usr/bin/env bash
# Dedicated catalog PF restarter (3s cadence). The catalog port-forward dies
# when the catalog pod restarts (FAULT_DELAY_MS env-hook rollout, e.g. catlat
# combos DK14/DK15/DK18/T2/T4) — kubectl port-forward can't transparently
# survive a backing-pod restart. This revives it within ~3s so the runner's
# catalog_direct carrier poll observes the slow state (DEP witness).
# Run ONLY during catalog-latency (catlat) collection. Mirror of
# pfwd_inventory_restarter.sh (inventory 5013 -> catalog 5005). Pure additive:
# does NOT touch pfwd_watchdog.sh; the 3s cadence keeps 5005 healthy so the 45s
# watchdog's check almost always finds it up and stays hands-off.
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
LOG=/tmp/pfwd_catalog_restarter.log
echo "[cat-restarter $(date +%H:%M:%S)] start (3s cadence)" > "$LOG"
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://127.0.0.1:5005/health 2>/dev/null)
  if [ "$code" != "200" ]; then
    # free stale listener (if any) + restart
    pids=$(netstat -ano 2>/dev/null | grep -E "[:.]5005 " | grep -i LISTENING | awk '{print $NF}' | sort -u)
    for pid in $pids; do taskkill //F //PID "$pid" >/dev/null 2>&1; done
    sleep 1
    nohup kubectl port-forward svc/catalog 5005:5005 -n recweb-chaos --address=127.0.0.1 >>/tmp/pfwd_catalog.log 2>&1 &
    echo "[cat-restarter $(date +%H:%M:%S)] restarted (code=$code)" >> "$LOG"
    sleep 4  # give the new PF time to establish
  fi
  sleep 3
done
