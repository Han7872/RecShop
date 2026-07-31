#!/usr/bin/env bash
# Dedicated inventory PF restarter (3s cadence). The inventory port-forward dies
# when the inventory pod restarts (FAULT_DELAY env-hook rollout) — kubectl
# port-forward can't transparently survive a backing-pod restart. This revives
# it within ~3s so the runner's inventory_direct carrier poll observes the slow
# state. Run ONLY during dual07/dual11 (inventory-latency) collection.
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
LOG=/tmp/pfwd_inventory_restarter.log
echo "[inv-restarter $(date +%H:%M:%S)] start (3s cadence)" > "$LOG"
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://127.0.0.1:5013/health 2>/dev/null)
  if [ "$code" != "200" ]; then
    # free stale listener (if any) + restart
    pids=$(netstat -ano 2>/dev/null | grep -E "[:.]5013 " | grep -i LISTENING | awk '{print $NF}' | sort -u)
    for pid in $pids; do taskkill //F //PID "$pid" >/dev/null 2>&1; done
    sleep 1
    nohup kubectl port-forward svc/inventory 5013:5013 -n recweb-chaos --address=127.0.0.1 >>/tmp/pfwd_inventory.log 2>&1 &
    echo "[inv-restarter $(date +%H:%M:%S)] restarted (code=$code)" >> "$LOG"
    sleep 4  # give the new PF time to establish
  fi
  sleep 3
done
