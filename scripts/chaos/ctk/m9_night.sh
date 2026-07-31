#!/usr/bin/env bash
# =============================================================================
# m9_night.sh — M9 两段式采集(用户设计:先 28 类型逐验 → 人看过 → 再全量补 reps)
# =============================================================================
# ★为什么必须两段(而不是直接全量 140):
#   m9_drive.sh 是【类型优先】的(跑满某类型 5 rep 才换下一类型)。若直接 --reps 5 全量,
#   第 8 个 single 类型的 r1 要等 ~5 小时才打分, dual 第 16 个类型要等到天亮 —— 【类型级
#   问题会暴露得极晚】。先把 28 个类型的 r1 全采一遍(每个 r1 内联跑 BARO/RCD), 3-4 小时内
#   就能拿到【28 类型 × 方法 verdict 全表】, 人看一眼再决定要不要全量。这是用户 2026-07-11 的设计。
#
# ★PHASE 1 (--phase1): 28 类型 × r1 = 28 case (~3.5-4h)
#     产物: logs/m9/m9_verdict.jsonl (28 行) = 类型 × BARO/RCD × MRCBench 四族
#     ★人工检查点★ —— 看完再起 phase2
# ★PHASE 2 (--phase2): 28 类型 × r2..r5 = 112 case (~13h, 通宵)
#     r1 已在 phase1 采好且计入(--from-rep 2), 不重采。
#
# 用法(主循环 nohup 起, ★不要 run_in_background):
#   nohup bash scripts/chaos/ctk/m9_night.sh --phase1 > /tmp/m9_p1.log 2>&1 &
#   (看完 verdict 后)
#   nohup bash scripts/chaos/ctk/m9_night.sh --phase2 > /tmp/m9_p2.log 2>&1 &
#
# ★铁律: 绝不并发。m9_drive.sh 自带单实例锁(logs/m9/m9_drive.lock) —— 2026-07-11 血泪:
#   pkill -f 在 Windows/git-bash 下【静默失败】, 老驱动器没死 + 新链又起 = 两个 runner 同打
#   一个集群 = netem/CRD/env-hook 互相踩 = 数据全废。锁是硬防线, 不依赖 pkill。
# =============================================================================
set -u

CWD='${REPO_DIR}'
CTK="$CWD/scripts/chaos/ctk"
LOGDIR="$CWD/logs/m9"
mkdir -p "$LOGDIR"

PHASE=""
case "${1:-}" in
  --phase1) PHASE=1 ;;
  --phase2) PHASE=2 ;;
  *) echo "用法: $0 --phase1 | --phase2" >&2; exit 64 ;;
esac

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[m9_night $(now)] $*"; }

# =============================================================================
# ★★ PREP + 常驻守卫(2026-07-11 血泪:Phase 1 的【全部 5 次采集失败】都是 PREP 静默退化)
# -----------------------------------------------------------------------------
#   失败模式:pod 滚动重启 → 该服务的 port-forward 断 → 载体 ok=0/N + p95≈2050ms
#            → gate FAIL → 驱动器傻傻重试 3 次 → 每次白烧 ~20 分钟(共吃掉近 1 小时)。
#     · catalog pod 重启 → 5005 断 → catalog_latency / runtime_exception / db_lock 全废
#     · user    pod 重启 → 5004 断 → dual08 / dual16 全废
#   闭环 = 三层:
#     ① 本函数:开跑前把 PREP 全部拉起(proxy8001 + 4 restarter + watchdog + m9_guard)
#     ② m9_guard.sh:20s 巡检、端口探活、断了自动重拉(守护"守护者")
#     ③ m9_drive.sh 的 precase_health():每 case 开跑前验一遍,不绿就等守卫自愈,绝不硬上
#   ★判据一律【端口探活】,绝不信进程表 —— git-bash 的 pgrep/pkill 看不见 Windows 进程
#     (静默假阴性;正是这个坑害我起了两个并发驱动器,差点毁掉整夜数据)。
# =============================================================================
ensure_prep() {
  export MSYS_NO_PATHCONV=1 NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
  export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
  export KUBECTL="${KUBECTL:-kubectl}"

  _ok()  { [ "$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$1" --max-time 3 2>/dev/null)" = "200" ]; }
  local PROXY_URL="http://127.0.0.1:8001/api/v1/namespaces"

  # 1) kubectl proxy 8001(面板 + cadvisor + kube-state 三路单点)
  if _ok "$PROXY_URL"; then say "PREP: proxy8001 已在"; else
    say "PREP: 拉起 kubectl proxy 8001"
    nohup "$KUBECTL" proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*' > /tmp/proxy8001.log 2>&1 &
    sleep 5
  fi
  # 2) 业务 port-forward(pfwd_start 一次性拉起;watchdog + 4 restarter 常驻守)
  _ok "http://127.0.0.1:5005/health" || { say "PREP: 拉起 pfwd_start"; nohup bash "$CTK/pfwd_start.sh" > /tmp/pfwd.log 2>&1 & sleep 12; }
  for svc in catalog user inventory pricing; do
    [ -f "$CTK/pfwd_${svc}_restarter.sh" ] && nohup bash "$CTK/pfwd_${svc}_restarter.sh" > "/tmp/pfwd_${svc}.log" 2>&1 &
  done
  nohup bash "$CTK/pfwd_watchdog.sh" > /tmp/pfwd_watchdog.log 2>&1 &
  say "PREP: 4 restarter + watchdog 已拉起"
  # 3) ★常驻守卫(守护"守护者";没它则 precase_health 会一直等一个永不到来的自愈)
  nohup bash "$CTK/m9_guard.sh" > /tmp/m9_guard.log 2>&1 &
  say "PREP: m9_guard 常驻守卫已拉起(20s 巡检 / 端口探活 / 自动重拉)"
  sleep 10

  # 4) 验收:全绿才放行(不绿就 fail-fast,绝不带病开跑整夜)
  local bad=""
  _ok "$PROXY_URL" || bad="$bad proxy8001"
  for p in 5005 5004 5013 5014 5009 5011 5000 5017; do
    _ok "http://127.0.0.1:$p/health" || bad="$bad :$p"
  done
  if [ -n "$bad" ]; then
    say "★★ FATAL: PREP 验收不通过(不绿:$bad)—— 中止,绝不带病开跑整夜采集"
    exit 70
  fi
  say "PREP 验收通过:proxy8001 + 8 个载体口全绿,守卫在跑"
}

ensure_prep     # ★开跑前:PREP 全拉起 + 常驻守卫 + 全绿验收(不绿 fail-fast)

if [ "$PHASE" -eq 1 ]; then
  say "=========================================================="
  say "PHASE 1: 28 类型 × r1 逐验(single 8 + dual 16 + triple 4)"
  say "  每个 r1 采完立刻内联跑 BARO+RCD → m9_verdict.jsonl"
  say "  空排名的类型会记账(phase2 前由人判读)"
  say "=========================================================="
  DRIVE_ARGS="--only-r1"
else
  say "=========================================================="
  say "PHASE 2: 28 类型 × r2..r5 = 112 case(r1 已在 phase1 采好, 不重采)"
  say "=========================================================="
  DRIVE_ARGS="--from-rep 2 --reps 5"
fi

for arity in single dual triple; do
  say ">>> $arity 档开始"
  t0=$(date +%s)
  bash "$CTK/m9_drive.sh" --arity "$arity" $DRIVE_ARGS 2>&1 | tee -a "$LOGDIR/m9_p${PHASE}_${arity}.log"
  rc=${PIPESTATUS[0]}
  t1=$(date +%s)
  say "<<< $arity 档结束 rc=$rc 用时 $(( (t1-t0)/60 )) 分钟"
  # 驱动器内部 fail-soft(单 case 失败只记账不杀批)。rc=75 = 撞锁(有别的驱动器在跑)→ 必须中止。
  if [ "$rc" -eq 75 ]; then
    say "★★ FATAL: 撞到 m9_drive 单实例锁 —— 有另一个驱动器在跑!中止(绝不并发)。"
    exit 75
  fi
  [ "$rc" -ne 0 ] && say "⚠ $arity 档驱动器返回 rc=$rc → 记账, 继续下一档"
  sleep 20
done

say "=========================================================="
say "★ PHASE $PHASE 结束。汇总:"
if [ -f "$LOGDIR/m9_ledger.tsv" ]; then
  say "  PASS   = $(grep -cP '\tPASS\t' "$LOGDIR/m9_ledger.tsv" 2>/dev/null || echo 0)"
  say "  GIVEUP = $(grep -cP '\tGIVEUP\t' "$LOGDIR/m9_ledger.tsv" 2>/dev/null || echo 0)"
fi
[ -f "$LOGDIR/m9_verdict.jsonl" ] && say "  方法 verdict 行数 = $(wc -l < "$LOGDIR/m9_verdict.jsonl")"
say "  隔离区(勿打包) = $CWD/(native trees) _dev_m9/"
if [ "$PHASE" -eq 1 ]; then
  say "  ★下一步 = 人判读 m9_verdict.jsonl(28 类型 × BARO/RCD × MRCBench)→ 确认后起 --phase2"
else
  say "  ★下一步 = 打包(--out 写死 *_dense_20260712, 老交付区只读)→ QC → 用户发上游"
fi
say "=========================================================="
