#!/usr/bin/env bash
# Clean port-forward reset: kill ALL port-forwards + supervisor + watchdog, then
# start 8 fresh kubectl port-forwards + launch the health-based watchdog.
# Run AFTER the dry-run completes, BEFORE the collection.
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1 NO_PROXY='*'
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=recweb-chaos
declare -A SVCS=([5000]=backend [5004]=user [5005]=catalog [5009]=announcement [5011]=checkout [5013]=inventory [5014]=pricing [5017]=search)

echo "[reset] killing ALL port-forwards / supervisor / watchdog..."
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { \$_.CommandLine -like '*pfwd_supervisor*' -or \$_.CommandLine -like '*pfwd_watchdog*' -or \$_.CommandLine -like '*port-forward svc*' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue }" >/dev/null 2>&1
sleep 4

echo "[reset] starting 8 fresh port-forwards..."
for port in "${!SVCS[@]}"; do
  svc=${SVCS[$port]}
  nohup kubectl port-forward "svc/$svc" "${port}:${port}" -n "$NS" --address=127.0.0.1 >/tmp/pfwd_${svc}.log 2>&1 &
done
sleep 10

echo "[reset] launching watchdog..."
nohup bash "$ROOT/chaos/ctk/pfwd_watchdog.sh" >/tmp/pfwd_watchdog_nohup.log 2>&1 &
sleep 3

echo "[reset] health check:"
ok=0
for port in "${!SVCS[@]}"; do
  svc=${SVCS[$port]}
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 "http://127.0.0.1:$port/health" 2>/dev/null)
  [ "$code" = "200" ] && ok=$((ok+1)) || echo "  WARN $svc:$port=$code"
done
echo "[reset] $ok/8 healthy; watchdog running"
