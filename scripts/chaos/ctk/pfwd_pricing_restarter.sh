#!/usr/bin/env bash
# Dedicated pricing PF restarter (3s cadence). Mirror of pfwd_catalog_restarter.sh
# (catalog 5005 -> pricing 5014). For the 三-01 pricing-纠缠 combo
# (pricing_cpu_x_catalog_latency_x_cfg_timeout, defect#3): pricing is BOTH the
# RES root (StressChaos cpu) AND the CFG carrier. Under cpu-stress + single-thread
# dev server + serial ~2s catalog wait, pricing /health can miss repeatedly
# (3x20s ~= 60s) -> pod restart -> the 5014 port-forward dies. That is fail-closed
# (restart = non-exempt churn -> control_plane_healthy False -> case rejected, not
# dirty), but to keep yield we revive the 5014 pf within ~3s so the runner's
# pricing carrier poll (CFG 504 victim + RES throttle-observed pod) stays alive.
# Run ONLY during 三-01 pricing-纠缠 collection. Pure additive: does NOT touch
# pfwd_watchdog.sh or pfwd_catalog_restarter.sh; the 3s cadence keeps 5014 healthy
# so the 45s watchdog's check almost always finds it up and stays hands-off.
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
LOG=/tmp/pfwd_pricing_restarter.log
echo "[pricing-restarter $(date +%H:%M:%S)] start (3s cadence)" > "$LOG"
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://127.0.0.1:5014/health 2>/dev/null)
  if [ "$code" != "200" ]; then
    # free stale listener (if any) + restart
    pids=$(netstat -ano 2>/dev/null | grep -E "[:.]5014 " | grep -i LISTENING | awk '{print $NF}' | sort -u)
    for pid in $pids; do taskkill //F //PID "$pid" >/dev/null 2>&1; done
    sleep 1
    nohup kubectl port-forward svc/pricing 5014:5014 -n recweb-chaos --address=127.0.0.1 >>/tmp/pfwd_pricing.log 2>&1 &
    echo "[pricing-restarter $(date +%H:%M:%S)] restarted (code=$code)" >> "$LOG"
    sleep 4  # give the new PF time to establish
  fi
  sleep 3
done
