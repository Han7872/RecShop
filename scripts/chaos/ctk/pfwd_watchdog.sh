#!/usr/bin/env bash
# Health-based port-forward watchdog (NON-churning). Every 45s, curl each of the
# 8 service ports; only if unhealthy, kill the dead listener on that port and
# restart exactly one kubectl port-forward. Differs from the supervisor: no busy
# loop — acts solely on real deaths. For the long overnight collection.
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
NS=recweb-chaos
declare -A SVCS=([5000]=backend [5004]=user [5005]=catalog [5009]=announcement [5011]=checkout [5014]=pricing [5017]=search)
# NOTE: inventory(5013) excluded — handled by pfwd_inventory_restarter.sh (tighter 3s
# cadence needed because inventory pod restarts on FAULT_DELAY rollout kill the PF).
WLOG=/tmp/pfwd_watchdog.log
echo "[watchdog $(date +%H:%M:%S)] start (health-check every 45s, restart-on-death only)" > "$WLOG"

while true; do
  for port in "${!SVCS[@]}"; do
    svc=${SVCS[$port]}
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 "http://127.0.0.1:$port/health" 2>/dev/null)
    if [ "$code" != "200" ]; then
      echo "[watchdog $(date +%H:%M:%S)] $svc:$port UNHEALTHY (code=$code) -> restarting" >> "$WLOG"
      # free the port (kill any listener, dead or stale)
      pids=$(netstat -ano 2>/dev/null | grep -E "[:.]$port " | grep -i LISTENING | awk '{print $NF}' | sort -u)
      for pid in $pids; do taskkill //F //PID "$pid" >/dev/null 2>&1; done
      sleep 2
      nohup kubectl port-forward "svc/$svc" "${port}:${port}" -n "$NS" --address=127.0.0.1 >/tmp/pfwd_${svc}.log 2>&1 &
      sleep 6
      code2=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 "http://127.0.0.1:$port/health" 2>/dev/null)
      echo "[watchdog $(date +%H:%M:%S)] $svc:$port restart -> code=$code2" >> "$WLOG"
    fi
  done
  sleep 15
done
