#!/usr/bin/env bash
# =============================================================================
# m9_guard.sh — 「守护者的守护者」常驻守卫(M9 通宵采集自愈)
# =============================================================================
# ★为什么需要它(2026-07-11 血泪 ×2):
#   本次 Phase 1 的 4 次采集失败【全部】是同一个模式:PREP 在跑的过程中【静默退化】——
#     · catalog pod 滚动重启 → 5005 port-forward 断 → catalog_latency/runtime_exception/db_lock 全废
#     · user pod 滚动重启   → 5004 port-forward 断 → dual08/dual16 全废
#   驱动器不知情, 傻傻重试 3 次, 每次白烧 ~20 分钟 = 近 1 小时被吃掉。
#   restarter/watchdog 本身也可能死(它们没人守)。→ 本脚本守【守护者】。
#
# ★守什么:
#   1) kubectl proxy 8001  —— 面板 + cadvisor + kube-state 三路单点, 它一挂全盘皆输
#   2) pfwd_watchdog.sh    —— 守 7 个业务口(5000/5004/5005/5009/5011/5014/5017)
#   3) 4 个 restarter      —— catalog/user/inventory/pricing(pod 滚动重启后拉回 pf)
#   4) 端口实际探活        —— 进程活着不代表口通(kubectl port-forward 会假死)
#
# ★为什么不用 pgrep/pkill: git-bash 的 pgrep -f / pkill -f 【看不见 Windows 进程】(静默假阴性,
#   正是它害我起了两个并发驱动器)。本脚本一律用【端口探活】作真判据, 不信进程表。
#
# 用法(主循环 nohup 起, 全程常驻):
#   nohup bash scripts/chaos/ctk/m9_guard.sh > /tmp/m9_guard.log 2>&1 &
# =============================================================================
set -u
export MSYS_NO_PATHCONV=1
export NO_PROXY='*'
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export KUBECTL="${KUBECTL:-kubectl}"

CWD='${REPO_DIR}'
CTK="$CWD/scripts/chaos/ctk"
NS=recweb-chaos
INTERVAL=20              # 巡检间隔(秒)

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[m9_guard $(now)] $*"; }

# 端口探活(真判据; 进程活着≠口通)
port_ok() {
  local port="$1" path="${2:-/health}"
  local code
  code=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' \
         "http://127.0.0.1:${port}${path}" --max-time 3 2>/dev/null)
  [ "$code" = "200" ]
}

heal_proxy8001() {
  if port_ok 8001 "/api/v1/namespaces"; then return 0; fi
  say "★ proxy 8001 不通 → 重拉(面板+cadvisor+kube-state 三路都靠它)"
  nohup "$KUBECTL" proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*' \
    > /tmp/proxy8001.log 2>&1 &
  sleep 5
  port_ok 8001 "/api/v1/namespaces" && say "  proxy 8001 已恢复" || say "  ⚠ proxy 8001 仍不通!"
}

# =============================================================================
# ★★ 幂等重拉(2026-07-12 血泪:守卫自己成了故障源)
# -----------------------------------------------------------------------------
# 老实现的 bug:端口不通就【无条件再拉一个】restarter,从不检查已经有一个在跑。
#   而 pod_failure 期间 :5005 【本来就该不通 ~60s】(catalog pod 被换成 pause 镜像)
#   → 守卫在这 60s 里堆了 3 个 restarter → 下一轮 pod_failure 再堆…
#   实测堆到 **109 个 catalog restarter**(+user 6/inv 4/pricing 2/watchdog 2/guard 2)。
# 为什么致命:每个 restarter 的循环是「taskkill 掉 5005 的监听者 → 起新 port-forward」。
#   N 个 restarter 就在【互相杀对方的 port-forward】,死循环:
#     "bind: Only one usage of each socket address"
#     "error creating error stream for port 5005 -> 5005: Timeout"
#   → :5005 永远起不来 → 驱动器的 precase_health 被堵死 → 整夜采集停摆。
# ★铁律:restarter 自己就是 3s 自愈死循环,【永远只该有一个】。重拉前必须先确认没有。
#   判据用 pidfile + kill -0(不用 pgrep —— git-bash 看不见 Windows 进程)。
# =============================================================================
_alive() {   # $1 = pidfile
  [ -f "$1" ] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null
}

_spawn_once() {   # $1=name  $2=script  $3=logfile —— 已有存活实例则【什么都不做】
  local pidf="/tmp/m9_own_$1.pid"
  if _alive "$pidf"; then return 1; fi          # 已在跑 → 绝不再拉(这是本次修复的核心)
  nohup bash "$2" > "$3" 2>&1 &
  echo $! > "$pidf"
  say "  拉起 $1 (pid=$(cat "$pidf"))"
  return 0
}

# 业务口不通 → 确保对应 restarter 【有且仅有一个】在跑(它会自己把 port-forward 拉回来)
heal_restarters() {
  local any_down=0
  for spec in "5005:catalog" "5004:user" "5013:inventory" "5014:pricing"; do
    local port="${spec%%:*}" svc="${spec##*:}"
    if ! port_ok "$port"; then
      any_down=1
      # ★不再无脑重拉:只有当该 restarter 【确实没在跑】时才拉一个新的。
      #   它已经在跑而口还不通 = pod 真的死着(如 pod_failure 注入期)→ 等它自己恢复,别添乱。
      if _spawn_once "restarter_$svc" "$CTK/pfwd_${svc}_restarter.sh" "/tmp/pfwd_${svc}.log"; then
        say "★ ${svc}:${port} 不通 且无 restarter 在跑 → 已拉起一个"
      fi
    fi
  done
  # watchdog 覆盖的另外几个口(backend/announcement/checkout/search)——同样幂等
  for spec in "5000:backend" "5009:announcement" "5011:checkout" "5017:search"; do
    local port="${spec%%:*}" svc="${spec##*:}"
    if ! port_ok "$port"; then
      any_down=1
      if _spawn_once "watchdog" "$CTK/pfwd_watchdog.sh" "/tmp/pfwd_watchdog.log"; then
        say "★ ${svc}:${port} 不通 且无 watchdog 在跑 → 已拉起一个"
      fi
      break
    fi
  done
  return $any_down
}

# 开跑即确保 4 个 restarter + watchdog 各有一个(不依赖端口状态 —— 它们本就该常驻)
ensure_all_restarters() {
  for svc in catalog user inventory pricing; do
    _spawn_once "restarter_$svc" "$CTK/pfwd_${svc}_restarter.sh" "/tmp/pfwd_${svc}.log" || true
  done
  _spawn_once "watchdog" "$CTK/pfwd_watchdog.sh" "/tmp/pfwd_watchdog.log" || true
}

# Prometheus 真在抓 cadvisor(它挂了 = metric 通道全空, 但 case 照样"成功"→ 静默坏数据)
check_prom() {
  local up
  up=$(curl -s --noproxy '*' "http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22cadvisor%22%7D" \
       --max-time 5 2>/dev/null | grep -o '"value"' | head -1)
  [ -n "$up" ]
}

say "=============================================="
say "M9 守卫启动(巡检 ${INTERVAL}s)——守 proxy8001 / 4×restarter / watchdog / prometheus"
say "  判据 = 端口实际探活(不信进程表, git-bash 的 pgrep 看不见 Windows 进程)"
say "=============================================="

ensure_all_restarters      # ★常驻:4 restarter + watchdog 各起一个(幂等)

consec_prom_fail=0
while true; do
  heal_proxy8001
  heal_restarters || true
  if check_prom; then
    consec_prom_fail=0
  else
    consec_prom_fail=$((consec_prom_fail + 1))
    say "⚠ Prometheus 未在抓 cadvisor(连续 ${consec_prom_fail} 次)——metric 通道可能全空!"
    if [ "$consec_prom_fail" -ge 3 ]; then
      say "★★ Prometheus/cadvisor 抓取持续异常 → 检查 proxy8001 与 prometheus 容器!"
    fi
  fi
  sleep "$INTERVAL"
done
