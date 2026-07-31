#!/usr/bin/env bash
# 3s-cadence user:5004 port-forward restarter (dual08/dual16 podfail-user combos).
# user podfail 反复重启 user pod -> 5004 pf 死；45s watchdog 追不上 -> pre_fault 基线探 user 全失败 -> gate 挂。
# 本 restarter 3s 内复活 5004,保住 podfail 重启间隙的基线可用性。用完 kill。
# 注: user podfail 检测走 availability(restart_delta),非本 carrier;pf-curl 在 pause 期仍 200(probe10 routing quirk),不干扰检测。
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
LOG=/tmp/pfwd_user_restarter.log
echo "[user-restarter $(date +%H:%M:%S)] start (3s cadence, 5004->user)" > "$LOG"
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://127.0.0.1:5004/health 2>/dev/null)
  if [ "$code" != "200" ]; then
    pids=$(netstat -ano 2>/dev/null | grep -E "[:.]5004 " | grep -i LISTENING | awk '{print $NF}' | sort -u)
    for pid in $pids; do taskkill //F //PID "$pid" >/dev/null 2>&1; done
    sleep 1
    nohup kubectl port-forward svc/user 5004:5004 -n recweb-chaos --address=127.0.0.1 >>/tmp/pfwd_user.log 2>&1 &
    echo "[user-restarter $(date +%H:%M:%S)] restarted (code=$code)" >> "$LOG"
    sleep 4
  fi
  sleep 3
done
