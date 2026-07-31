#!/usr/bin/env bash
# =============================================================================
# m9_drive.sh — M9 密度重采【顺序批驱动器】(黑板 TASK-K8S-M9 §1.2 裁定 B3 的 (g))
# =============================================================================
# WHY 存在:collect-{single,dual,triple}.sh 是【append-only 命令日志】,不是批脚本
#   (自述 L13-14「本骨架现在不整体可跑」)——每行都是 `nohup ... &` 立即后台返回。
#   整跑 = 40~80 个 runner 并发齐发互相踩(同一集群/同一 catalog/同一 netem)→ 整夜数据全废。
#   本脚本 = 真正的【顺序】驱动器:一个 case 前台跑完、验完、记完账,才起下一个。
#
# 每 case 三项核验(与 M8 一致,不新造判据):
#   1) runner 退出码 0(CHECKSUM 闸在 runner 自含 L78-84 → 非 0 即脏)
#   2) verify_dual.py  <case>            (gate ready_for_release + zero_drift + 18 字段 trace
#                                          + 6 K8s 标签 + 所有 root pod ∈ during_fault)
#   3) instance_check.py <case> <svc>    (对 GT 里出现的 rolling 根 catalog/inventory/pricing 逐个查)
#   三项全过 = PASS → 向 collect-<arity>-dense.sh append 一行【完整可跑命令】(沿用 M8 台账惯例)。
#   失败 → 重试(cap ≤2 次)→ 仍失败:记账 + 继续下一个 case(★绝不杀整批)。
#
# ★每类型 r1 采完立刻内联跑 m9_score.py(BARO+RCD,秒级):
#   - 断言【非空排名】= "密度到底修没修好" 的判据;顺带算 MRCBench 四族指标 → m9_verdict.jsonl。
#   - r1 空排名 → 跳过该类型剩余 reps,记账,继续下一个类型(不杀整夜、不浪费 4 个 rep 采废数据)。
#
# 用法(整个驱动器由主循环 nohup 起,★不要 run_in_background):
#   nohup bash scripts/chaos/ctk/m9_drive.sh --arity single --reps 5 > /tmp/m9_single.log 2>&1 &
#   nohup bash scripts/chaos/ctk/m9_drive.sh --arity dual --only-r1 > /tmp/m9_dual_r1.log 2>&1 &   # Phase 1 逐类型验证
#   bash scripts/chaos/ctk/m9_drive.sh --arity single --types net_delay_single,db_lock_single --reps 2
#   bash scripts/chaos/ctk/m9_drive.sh --arity dual --dry-run          # 只打印将要跑的命令
#
# 产物:
#   数据   (native trees) <arity>_dense/_<key>_reps_v19/<case_dir>/     (新树,老 140 一字节不碰)
#   台账   scripts/chaos/ctk/collect-<arity>-dense.sh                       (PASS 命令行 append)
#   verdict logs/m9/m9_verdict.jsonl                                        (每类型 r1 的方法判据)
#   记账   logs/m9/m9_ledger.tsv + logs/m9/<case_id>.log                    (每 case runner 全量输出)
# -----------------------------------------------------------------------------
# ★★ 环境铁律(照抄 collect-single.sh 头部;PREP 必须已就绪,本脚本 fail-fast 检查)★★
#   conda python / NO_PROXY='*' / NACOS_ENABLED=false / KUBECTL 必 export(bare kubectl 不在 PATH)
#   PREP(本脚本【不】代劳,起脚本前先跑,见 M8 runbook §1.5-A):
#     1) kubectl proxy 8001 (Prometheus 靠它抓 cAdvisor+kube-state;★M9 面板也走它)
#     2) pfwd_start.sh(business port-forward 守护) + pfwd_catalog_restarter.sh(全程)
#     3) OTel 栈 + 25 服务 + Chaos Mesh 在跑;CHECKSUM 基线核对
#   本脚本【会】按类型自动起:inventory restarter(需 inventory_direct 的组)。
#   ★不改任何 gate 判据 / 载体 / 注入逻辑 / GT —— flag 逐字抄自 collect-*.sh 的 append 行,
#     single 档【额外】只加 --wide-metrics(scope-only 开关,不换门);dual/triple 本就 --deep,不加。
# =============================================================================
set -u -o pipefail

export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1
export NO_PROXY='*'
export NACOS_ENABLED=false
export PYTHONIOENCODING=utf-8
export KUBECTL="${KUBECTL:-kubectl}"

PY='python3'
CWD='${REPO_DIR}'
CTK="$CWD/scripts/chaos/ctk"
RUNNER="$CTK/chaos_k8s_runner.py"
NS=recweb-chaos
UT='0870e257-6cd0-4fe4-b815-0a9da6b25d41'
CDB='http://127.0.0.1:5005'
IDB='http://127.0.0.1:5013'
ITEM='0071341196'
DENSE_ROOT="$CWD/output/k8s_pilot"      # 新树: <DENSE_ROOT>/<arity>_dense/_<key>_reps_v19
LOGDIR="$CWD/logs/m9"
VERDICT="$LOGDIR/m9_verdict.jsonl"
LEDGER="$LOGDIR/m9_ledger.tsv"
RETRY_CAP=2                                # 失败重试次数上限(总尝试 = 1 + RETRY_CAP)
HOSTCPU_COOLDOWN=60                        # host_cpu 类 case 间等 VM drain(秒)

ARITY=""; REPS=5; ONLY_R1=0; TYPES=""; DRYRUN=0; FROM_REP=1
while [ $# -gt 0 ]; do
  case "$1" in
    --arity)    ARITY="$2"; shift 2 ;;
    --reps)     REPS="$2"; shift 2 ;;
    --only-r1)  ONLY_R1=1; shift ;;
    --from-rep) FROM_REP="$2"; shift 2 ;;   # ★两段式:先 --only-r1 验 28 类型, 再 --from-rep 2 补 r2-r5
    --types)    TYPES="$2"; shift 2 ;;
    --dry-run)  DRYRUN=1; shift ;;
    -h|--help)  sed -n '1,50p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 64 ;;
  esac
done
[ -n "$ARITY" ] || { echo "FATAL: --arity {single,dual,triple} 必填" >&2; exit 64; }
case "$ARITY" in single|dual|triple) ;; *) echo "FATAL: bad --arity $ARITY" >&2; exit 64 ;; esac
[ "$ONLY_R1" -eq 1 ] && REPS=1
[ "$FROM_REP" -le "$REPS" ] || { echo "FATAL: --from-rep($FROM_REP) > --reps($REPS)" >&2; exit 64; }

mkdir -p "$LOGDIR"

# =============================================================================
# ★★ 单实例互斥锁(2026-07-11 血泪:两个驱动器并发 → 两个 runner 同时打同一集群
#     → netem/CRD/env-hook 互相踩 → 数据全废)。
#   起因: git bash 的 `pkill -f m9_drive.sh` 对 Windows 进程【不可靠】(静默失败),
#   老驱动器没死, 新链又起 → 并发。→ 这里用锁硬性杜绝, 不依赖 pkill。
#   dry-run 不占锁(纯打印)。
# =============================================================================
LOCK="$LOGDIR/m9_drive.lock"
if [ "$DRYRUN" -eq 0 ]; then
  if [ -e "$LOCK" ]; then
    old_pid=$(cat "$LOCK" 2>/dev/null || echo "?")
    # 锁在但进程已死 = 上次被硬杀留下的陈旧锁 → 可安全接管
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "FATAL: 已有 m9_drive 在跑(pid=$old_pid, 锁=$LOCK)。" >&2
      echo "       ★绝不允许并发(同集群 → 互相踩 → 数据全废)。" >&2
      echo "       要接管请先确认那个进程真死了, 再删锁: rm -f '$LOCK'" >&2
      exit 75
    fi
    echo "[m9_drive] 发现陈旧锁(pid=$old_pid 已死) → 接管" >&2
    rm -f "$LOCK"
  fi
  echo "$$" > "$LOCK"
  # 正常/异常退出都释放锁(被 SIGKILL 硬杀时留陈旧锁, 由上面的 kill -0 检测接管)
  trap 'rm -f "$LOCK"' EXIT INT TERM
fi

# =============================================================================
# 类型表(28 = single 8 + dual 16 + triple 4)。★flag 逐字抄自 collect-*.sh 的 append 行。
#   字段: arity | key | fault | case_prefix | reps_dir | prep | args
#     prep: '-' 无 | 'inv' 需 inventory restarter | 'rollout' 入组前 rollout restart user+catalog
#           | 'hostcpu' case 间等 VM drain
#     args: 除 --case-id/--fault/--out-dir 外的全部 flag(字面量,可直接 copy-paste 跑)
#   顺序 = M8 采集实施计划的分批顺序(干净组先,podfail/cpu 后)。
# =============================================================================
read -r -d '' TYPE_TABLE <<'TBL' || true
single|net_delay_single|net_delay_single|net_delay_single|net_delay|-|--item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|net_loss_single|net_loss_single|net_loss_single|net_loss|-|--item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|catalog_latency_single|catalog_latency_single|catalog_latency_single|catalog_latency|-|--catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|runtime_exception_single|runtime_exception_single|runtime_exception_single|runtime_exception|-|--catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|db_lock_single|db_lock_single|db_lock_single|db_lock|-|--carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|pod_failure_single|pod_failure_single|pod_failure_single|pod_failure|-|--item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|service_cpu_single|service_cpu_single|service_cpu_single|service_cpu|-|--item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
single|host_cpu_single|host_cpu_single|host_cpu_single|host_cpu|hostcpu|--carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual01|dual_timeout_retry|dual01_uni|dual01|-|--item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual02|net_delay_x_net_loss|dual02_uni|dual02|-|--item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual05|net_delay_x_cfg_connect|dual05_uni|dual05|-|--deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier
dual|dual10|db_lock_x_netdelay|dual10_uni|dual10|-|--carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual12|catalog_latency_x_cfg_timeout|dual12_uni|dual12|-|--deep --carriers s_dk14_catlat_cfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual13|catalog_latency_x_net_loss|dual13_uni|dual13|-|--deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual11|inv_latency_x_runtime_exc|dual11_uni|dual11|inv|--deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual07|net_delay_x_inv_latency|dual07_uni|dual07|inv|--deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier
dual|dual04|dual_podfail_staggered|dual04_uni|dual04|rollout|--deep --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual08|net_delay_x_podfail|dual08_uni|dual08|rollout|--carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual16|pod_failure_x_net_delay|dual16_uni|dual16|rollout|--carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --wide-metrics
dual|dual09|sasrec_cpu_x_catalog_netdelay|dual09_uni|dual09|-|--deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual14|net_delay_x_svc_cpu|dual14_uni|dual14|-|--deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual15|catalog_latency_x_svc_cpu|dual15_uni|dual15|-|--deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual03|host_cpu_x_svccpu|dual03_uni|dual03|hostcpu|--deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier
dual|dual06|host_cpu_x_cfg_timeout|dual06_uni|dual06|hostcpu|--carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics
triple|triple01|pricing_cpu_x_catalog_latency_x_cfg_timeout|triple01|triple01|-|--deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier
triple|t1|inv_latency_x_cfg_timeout_x_retry|t1|t1|inv|--deep --carriers s_t1_inv_cfg_retry --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier
triple|t3|net_delay_x_net_loss_x_db_lock|t3|t3|-|--deep --carriers s2_dblock_combo --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier
triple|t4|pod_failure_x_catalog_latency_x_cfg_timeout|t4|t4|-|--deep --carriers s_triple_lif_dep_cfg --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier
TBL

# =============================================================================
# helpers
# =============================================================================
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[m9_drive $(now)] $*"; }

ledger() {  # key rep case_id status detail
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$(now)" "$1" "$2" "$3" "$4" "$5" >> "$LEDGER"
}

preflight() {
  local bad=0
  [ -x "$KUBECTL" ] || { say "FATAL: KUBECTL 不存在: $KUBECTL"; bad=1; }
  # kubectl proxy 8001 —— panel + cadvisor + kube-state 三路单点(§1.2 MAJOR)
  if ! curl -s --noproxy '*' --max-time 5 "http://127.0.0.1:8001/api" > /dev/null; then
    say "FATAL: kubectl proxy 8001 不通(PREP 未做?) —— panel/cadvisor/kube-state 全靠它"; bad=1
  fi
  # Prometheus 活着 + 真在抓 cadvisor
  if ! curl -s --noproxy '*' --max-time 5 "http://127.0.0.1:9090/api/v1/query?query=up" | grep -q cadvisor; then
    say "FATAL: Prometheus 未抓到 cadvisor(proxy8001 / prom 容器?)"; bad=1
  fi
  [ -f "$RUNNER" ] || { say "FATAL: runner 不存在 $RUNNER"; bad=1; }
  [ "$bad" -eq 0 ] || exit 70
  say "preflight OK (kubectl / proxy8001 / prometheus+cadvisor / runner)"
}

start_inv_restarter() {
  local pidf=/tmp/m9_inv_restarter.pid
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf" 2>/dev/null)" 2>/dev/null; then
    say "inventory restarter 已在跑"; return 0
  fi
  say "起 inventory restarter(pfwd_inventory_restarter.sh)"
  nohup bash "$CTK/pfwd_inventory_restarter.sh" > /tmp/m9_inv_restarter.log 2>&1 &
  echo $! > "$pidf"
  sleep 3
}

rollout_prep() {   # dual04/08/16: 入组前 rollout restart user + catalog 并等 Ready(M8 计划 D2)
  say "rollout prep: restart deploy/user + deploy/catalog @ $NS"
  "$KUBECTL" rollout restart deploy/user -n "$NS"    || say "WARN rollout restart user 失败"
  "$KUBECTL" rollout restart deploy/catalog -n "$NS" || say "WARN rollout restart catalog 失败"
  "$KUBECTL" rollout status deploy/user -n "$NS" --timeout=180s    || say "WARN user 未 Ready"
  "$KUBECTL" rollout status deploy/catalog -n "$NS" --timeout=180s || say "WARN catalog 未 Ready"
  sleep 10
}

# =============================================================================
# ★★ podfail 族【per-rep】pod 重置(2026-07-12 实测捕获;本次补丁的核心)
# -----------------------------------------------------------------------------
# 机理(为什么 podfail 多 rep 连采必挂):
#   Chaos Mesh 的 PodChaos action=pod-failure = 把【同一个 pod】的容器镜像就地换成 pause
#   (pod 不重建, runner L185/L202 注: inject+1 / recover+1 → restartCount +2)。
#   kubelet 每次看到 "Container definition changed, will be restarted" 就 restartCount++,
#   而 kubelet 的容器重启退避是【按 restartCount 指数增长】的: 10s → 20s → 40s → … → 封顶 5m。
#   rep 一个接一个连采, restartCount 在【同一个 pod】上累积 → 退避越滚越大 →
#   某个 rep 的 recover 之后 pod 在 post_recovery 窗内爬不起来 → gate recovery_confirmed
#   看到 post_recovery ok_ratio=0.0 → 整个 case 报废; 最坏 catalog 进 CrashLoopBackOff(back-off 5m0s),
#   把后面所有 case 全堵死(2026-07-11/12 实测发生过, 主循环手删 pod 才恢复)。
#   实测账: pod_failure_single 只拿到 1/4 rep(prep='-' 从来没重置过); dual04(prep=rollout, 只在类型开头
#   重置一次)1/4; dual08/dual16 4/4 全过 —— 只因它们 stage 长(140/200s), 退避有时间自然衰减。
#
# 修法: rollout restart = 【建全新 pod】= restartCount 归零 = 退避清零。
#   原 rollout_prep() 只在【每个类型开头跑一次】→ 4 个 rep 下来退避照样攒满。
#   本函数把它降到【每个 rep(每次 attempt)之前】跑一次, 且只对 podfail 族跑(其它类型 no-op)。
#
# ★★ 必须等【旧 pod 完全消失】才能开跑(主循环 2026-07-12 亲自踩过的坑):
#   runner 的 restart_before/after = `kubectl get pod -l app=<X>` 所有 pod 的 restartCount 之【和】
#   (runner L2840 _pod_restart_sum)。若旧 pod 还在 Terminating、新 pod 已 Ready, 就会:
#     restart_before = 15(旧 pod, 采样早) / restart_after = 3(新 pod, 采样晚) → restart_delta = -12
#     → gate 'pod_failure_window_validity' 判 chaos_induced_restart=false → 一个【好 case 被误杀】。
#   故重置收敛判据 = (pod 数 == deploy 期望 replicas) AND (全部 pod restartCount 之和 == 0)。
#
# 打到哪个 pod(★逐条查 chaos_k8s_runner.py 实证, 不照记忆):
#   pod_failure_single → catalog   (runner L13026 inject_pod_failure(...) 用默认 app_label="catalog", L2955-2957)
#   dual04 dual_podfail_staggered → catalog + user
#                                  (L13092 F1 catalog 默认 / L13415 F2 target="user" app_label="user")
#   dual08 net_delay_x_podfail    → user ONLY (L13502 F2 target="user"; F1 是 catalog-gw 的 netem, 不 podfail)
#   dual16 pod_failure_x_net_delay→ catalog   (L13531 inject_pod_failure(..., hit_timeout=60) 默认 catalog;
#                                              F1 netem 打 catalog-gw, 不 podfail)
#   t4 pod_failure_x_catalog_latency_x_cfg_timeout → catalog (L13575 默认 catalog)
#   (config 侧交叉印证: runner L10420 f1_target=catalog/f2_target=user; L10538 f2_target=user; L10548 f2_target=catalog)
# =============================================================================
RESET_ROLLOUT_TIMEOUT=180     # 等新 pod Ready 的上限(秒)
RESET_DRAIN_TIMEOUT=120       # 等旧 pod Terminating 彻底消失 + restartCount 归零 的上限(秒)
RESET_DRAIN_POLL=5

podfail_targets() {   # $1 = 类型 key → 打印该类型 pod-failure 打到的 deployment(空 = 非 podfail 族 → 本补丁完全不介入)
  case "$1" in
    pod_failure_single) echo "catalog" ;;
    dual04)             echo "catalog user" ;;
    dual08)             echo "user" ;;
    dual16)             echo "catalog" ;;
    t4)                 echo "catalog" ;;
    *)                  echo "" ;;
  esac
}

retry_cap_for() {     # $1 = 类型 key → 该类型的重试 cap(非 podfail 族 = 原 RETRY_CAP, 行为字节级不变)
  case "$1" in
    # podfail 族天然 flaky(pod-failure 咬中 poll / netem 掩盖窗对齐都有时序性)。
    # ★但【不】提到 5:退避的根因已被 per-rep 重置消掉, 重置后还挂多半不是退避, 是别的病;
    #   而 t4 单 case ~20 分钟(stage 250s + 两次 catalog rollout), 5 次尝试 = 100 分钟只为 1 个 rep,
    #   会把整夜预算烧光。折中: 短 case 的 podfail 族 cap=3(总 4 次), t4 维持 cap=2(总 3 次)。
    pod_failure_single|dual04|dual08|dual16) echo 3 ;;
    *) echo "$RETRY_CAP" ;;
  esac
}

# ★审查 R3 补:podfail 族靠 pf-restarter(3s cadence)在 rollout 后复活 5005/5004,
#   否则 rollout → pf 死 → precase_health(含 5004/5005)只能等 45s watchdog → 逼近 HEALTH_WAIT_MAX=90s
#   → HEALTH_FAIL 吃掉一次 attempt → continue → 又 rollout 一次 → 自我强化的活锁。
#   驱动器【自己】把它们拉起来(镜像 start_inv_restarter;判据用端口探活+pid,不信 pgrep)。
start_pf_restarter() {   # $1 = svc(catalog|user)
  local svc="$1" port sh pidf
  case "$svc" in
    catalog) port=5005 ;;
    user)    port=5004 ;;
    *) return 0 ;;
  esac
  sh="$CTK/pfwd_${svc}_restarter.sh"; pidf="/tmp/m9_${svc}_restarter.pid"
  [ -f "$sh" ] || { say "WARN 缺 $sh(pf 自愈只能靠 45s watchdog)"; return 1; }
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf" 2>/dev/null)" 2>/dev/null; then return 0; fi
  say "起 ${svc} pf restarter(pfwd_${svc}_restarter.sh, :$port, 3s cadence)"
  nohup bash "$sh" > "/tmp/m9_${svc}_restarter.log" 2>&1 &
  echo $! > "$pidf"
  sleep 2
}

# ★审查 R3 补:t4 的 patch-OUT livenessProbe 是改【deployment 模板】(runner L13239-13242
#   `patch deploy catalog --type json remove /spec/template/spec/containers/0/livenessProbe`)。
#   ⇒ rollout restart 只是加 annotation 重建 pod,新 pod 照样【没有 liveness】—— 重置【不】还原 liveness。
#   若上个 t4 case 崩在 finally 之前,catalog 会整夜失去 self-heal,而 pod_failure_single/dual04/dual16
#   的 pod-failure 动力学(kubelet 抢重启 vs 不抢)与已采的 100 case 就【不可比】了。
#   → 非-t4 的 podfail 类型开跑前显式断言 liveness 在;不在就大声 WARN(留给早上人工 kubectl apply 还原)。
assert_catalog_liveness() {   # $1 = key
  [ "$1" = "t4" ] && return 0
  local lv
  lv=$("$KUBECTL" get deploy catalog -n "$NS" \
        -o 'jsonpath={.spec.template.spec.containers[0].livenessProbe}' 2>/dev/null)
  [ -n "$lv" ] && return 0
  say "★★WARN catalog deploy 【没有 livenessProbe】(上个 t4 case 的 patch-IN 没跑到?)"
  say "★★     rollout restart 【不会】还原它(它被 patch 出了 deployment 模板)。"
  say "★★     $1 的 pod-failure 动力学将与已采 case 不可比 → 请 kubectl apply -f k8s/pilot/10-catalog.yaml 还原后再采。"
  return 1
}

pod_reset_deploy() {  # $1 = deployment 名(与 app label 同名) → 新 pod Ready + 旧 pod 消失 + restartCount==0
  local d="$1" waited=0 want npods rsum
  want=$("$KUBECTL" get deploy "$d" -n "$NS" -o 'jsonpath={.spec.replicas}' 2>/dev/null)
  [ -n "$want" ] || want=1
  # ★审查 R3 补:先看一眼——已经是【全新 pod(restart_sum=0)+ 无残留旧 pod】就【不重置】。
  #   (a) 省掉 rep1(紧跟 rollout_prep)和 health-fail 重试路径上的白烧 rollout;
  #   (b) 断掉"rollout→断 pf→健康闸失败→continue→又 rollout"的活锁:第二次 attempt 时 pod 已干净 → 跳过。
  npods=$("$KUBECTL" get pods -n "$NS" -l "app=$d" --no-headers 2>/dev/null | grep -c . || true)
  rsum=$("$KUBECTL" get pods -n "$NS" -l "app=$d" \
           -o 'jsonpath={.items[*].status.containerStatuses[*].restartCount}' 2>/dev/null \
         | tr ' ' '\n' | awk '{s += $1} END {print s + 0}')
  if [ "${npods:-0}" -eq "$want" ] && [ "${rsum:-1}" -eq 0 ]; then
    say "podfail 重置 SKIP: $d 已是全新 pod(pods=$npods restart_sum=0)→ 无退避可清, 不动它"
    return 0
  fi
  say "podfail 重置: rollout restart deploy/$d(当前 pods=$npods restart_sum=$rsum → 建全新 pod → restartCount 归零 → 清 kubelet 指数退避)"
  "$KUBECTL" rollout restart "deploy/$d" -n "$NS" || { say "WARN rollout restart $d 失败"; return 1; }
  "$KUBECTL" rollout status "deploy/$d" -n "$NS" --timeout=${RESET_ROLLOUT_TIMEOUT}s \
    || say "WARN $d rollout status 超时(${RESET_ROLLOUT_TIMEOUT}s)"
  # ★等旧 pod 彻底消失 + restartCount 归零(否则 runner 的 restart_delta 会算成负数 → 好 case 被误杀)
  while [ "$waited" -lt "$RESET_DRAIN_TIMEOUT" ]; do
    npods=$("$KUBECTL" get pods -n "$NS" -l "app=$d" --no-headers 2>/dev/null | grep -c . || true)
    rsum=$("$KUBECTL" get pods -n "$NS" -l "app=$d" \
             -o 'jsonpath={.items[*].status.containerStatuses[*].restartCount}' 2>/dev/null \
           | tr ' ' '\n' | awk '{s += $1} END {print s + 0}')
    if [ "${npods:-0}" -eq "$want" ] && [ "${rsum:-1}" -eq 0 ]; then
      say "podfail 重置 OK: $d → pods=$npods(期望 $want) restart_sum=0 (等了 ${waited}s)"
      return 0
    fi
    sleep "$RESET_DRAIN_POLL"; waited=$((waited + RESET_DRAIN_POLL))
  done
  say "WARN podfail 重置未收敛: $d pods=${npods:-?}(期望 $want) restart_sum=${rsum:-?} 等了 ${waited}s → 继续(健康闸兜底)"
  return 1
}

pre_rep_reset() {     # $1 = 类型 key —— ★每个 rep(每次 attempt)开跑【之前】调; 非 podfail 族 = 立即 return(no-op)
  local d targets did=0
  targets=$(podfail_targets "$1")
  [ -n "$targets" ] || return 0
  assert_catalog_liveness "$1" || true      # WARN-only(不拦采集;拦了整夜停摆更糟)
  # ★先把 pf-restarter 拉起来【再】rollout —— 顺序不能反(先 rollout 再起守护 = 5005/5004 空窗 → 健康闸干等)
  for d in $targets; do start_pf_restarter "$d"; done
  for d in $targets; do
    pod_reset_deploy "$d" || did=1
  done
  # rollout 会打断 catalog(5005)/user(5004)的 port-forward → 给守护(3s cadence)时间追上新 pod;
  # 之后紧跟的 precase_health 会真探活, 不绿会继续等守卫自愈。
  sleep 8
  # ★审查 R3 补:全新 pod = 冷进程(连接池/ORM/首请求惩罚)。baseline 窗紧跟其后,冷 p95 会抬高 baseline,
  #   而 dual08/dual16 的 NET arm 判据是 during/baseline 的位移/比值 → 冷 baseline 会【压低】信号。
  #   开跑前先热身(10 次 /health + 若干真业务读),把首请求惩罚打掉。
  for d in $targets; do
    case "$d" in
      catalog) for _i in 1 2 3 4 5 6 7 8 9 10; do
                 curl -s --noproxy '*' -o /dev/null --max-time 3 "$CDB/health" 2>/dev/null
                 curl -s --noproxy '*' -o /dev/null --max-time 5 "$CDB/api/items/$ITEM" 2>/dev/null   # = runner L12690 catalog_direct 载体同一路径(打热 DB 池/ORM)
               done ;;
      user)    for _i in 1 2 3 4 5 6 7 8 9 10; do
                 curl -s --noproxy '*' -o /dev/null --max-time 3 "http://127.0.0.1:5004/health" 2>/dev/null
               done ;;
    esac
  done
  say "podfail 重置完毕(targets=$targets, 已热身)"
}

# =============================================================================
# ★★ per-case 健康闸(2026-07-11 血泪总结)
# -----------------------------------------------------------------------------
# 本次 Phase 1 的 4 次采集失败【全部】是同一模式:PREP 在跑的过程中【静默退化】——
#   · catalog pod 滚动重启 → 5005 port-forward 断 → catalog_latency/runtime_exception/db_lock 全废
#   · user   pod 滚动重启 → 5004 port-forward 断 → dual08/dual16 全废
# 症状: 载体 ok=0/N + p95≈2050ms(连不上, 超时)→ gate 判 FAIL → 重试 3 次 → 每次白烧 ~20 分钟。
# 修法: 每个 case 开跑【之前】验一遍环境; 不绿就等 m9_guard.sh 自愈(它 20s 巡检一次), 别硬上。
#
# ★判据一律用【端口实际探活】, 绝不信进程表 —— git-bash 的 pgrep/pkill 看不见 Windows 进程
#   (静默假阴性; 正是这个坑害我起了两个并发驱动器, 差点毁掉整夜数据)。
# =============================================================================
HEALTH_PORTS="5005 5004 5013 5014 5009 5011 5000 5017"   # 全部载体口(carrier 用得到的)
HEALTH_WAIT_MAX=90        # 不绿时最多等守卫自愈多少秒
HEALTH_POLL=10

_port_ok() {
  local code
  code=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' \
         "http://127.0.0.1:$1/health" --max-time 3 2>/dev/null)
  [ "$code" = "200" ]
}
_proxy_ok() {
  local code
  code=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' \
         "http://127.0.0.1:8001/api/v1/namespaces" --max-time 4 2>/dev/null)
  [ "$code" = "200" ]
}
_prom_scraping_ok() {   # Prometheus 真在抓 cadvisor —— 它挂了 = metric 通道全空, 但 case 照样"成功"(静默坏数据)
  curl -s --noproxy '*' --max-time 5 \
    "http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22cadvisor%22%7D" 2>/dev/null \
    | grep -q '"value"'
}

precase_health() {
  local waited=0 down
  while : ; do
    down=""
    _proxy_ok            || down="$down proxy8001"
    _prom_scraping_ok    || down="$down prometheus/cadvisor"
    for p in $HEALTH_PORTS; do
      _port_ok "$p" || down="$down :$p"
    done
    [ -z "$down" ] && return 0
    if [ "$waited" -ge "$HEALTH_WAIT_MAX" ]; then
      say "★健康闸 FAIL(等了 ${waited}s 仍不绿):$down —— 守卫 m9_guard.sh 是否在跑?"
      return 1
    fi
    say "健康闸: 不绿 ($down) → 等守卫自愈… ${waited}/${HEALTH_WAIT_MAX}s"
    sleep "$HEALTH_POLL"
    waited=$((waited + HEALTH_POLL))
  done
}

per_type_prep() {  # $1 = prep token
  case "$1" in
    inv)     start_inv_restarter ;;
    rollout) rollout_prep ;;
    hostcpu) : ;;    # cooldown 在 case 之间(post_case_cooldown)
    *)       : ;;
  esac
}

post_case_cooldown() {  # $1 = prep token
  if [ "$1" = "hostcpu" ]; then
    say "host_cpu 类:等 VM drain ${HOSTCPU_COOLDOWN}s"
    sleep "$HOSTCPU_COOLDOWN"
  else
    sleep 5
  fi
}

mtime_of() { stat -c %Y "$1" 2>/dev/null || echo 0; }

newest_case_dir() {  # $1=out_dir  $2=epoch_start → 打印本次 run 新产出的 case 目录(带 metadata.json)
  local out="$1" start="$2" d mt best="" best_mt=0
  for d in "$out"/*/; do
    [ -d "$d" ] || continue
    [ -f "${d}metadata.json" ] || continue
    mt=$(mtime_of "${d}metadata.json")
    if [ "$mt" -ge "$start" ] && [ "$mt" -gt "$best_mt" ]; then
      best="${d%/}"; best_mt="$mt"
    fi
  done
  [ -n "$best" ] && echo "$best"
}

gt_rolling_roots() {  # $1=case_dir → 打印 GT 里出现的、需要 instance 校验的根
  # ★★ 2026-07-13 必须包含 catalog-gw(NET 类 GT 更正的连带修复)
  #   血泪:本白名单原为 ("catalog","inventory","pricing")。NET 类故障的 GT 从 catalog 更正为
  #   catalog-gw 之后,这 15 个 case 的 root_cause_services 里【不再有 catalog】
  #   → 本函数返回【空串】→ 调用方驱动 instance_check 的 for 循环【一次都不执行】→ inst_ok=1。
  #   ★这是最阴险的一类 bug:校验器不报 FAIL,它【根本没跑】,而台账上照样写 PASS。
  #   (更正前它也是错的 —— 那时它拿 root=catalog 去校验 catalog 的 pod,而真正被注入的是 catalog-gw。)
  #   catalog-gw 的 pod 不滚动,把它加进来无害;instance_check.py 的 else 分支
  #   `p.startswith(root_svc + "-")` 传 catalog-gw 进去即正确匹配 `catalog-gw-*`,无需改它。
  #
  # ★★ 2026-07-13 M9-R 追加 6 个 retarget 根(order/cart/search/review-query/backend/checkout)
  #   —— 这【不是】因为它们的 pod 会滚(StressChaos 在容器内起 stress-ng 不 rollout;
  #   PodChaos pod-failure 是同 pod 原地 pause-swap 重启, pod 名也不变)。加它们的唯一理由是:
  #   **本函数是 instance_check 的【驱动器】,不在名单里 = 校验器根本不跑 = 台账假 PASS**(即上面那条血泪本身)。
  #   --target-service 采出来的 case,GT root_cause_services 里是 order/cart/…,不加就必然复现同一个坑。
  #   instance_check.py 的 else 分支 `p.startswith(root_svc+"-")` 对这 6 个全部正确:
  #   pod 名 = deploy 名前缀(order-*/cart-*/search-*/review-query-*/backend-*/checkout-*),
  #   且无前缀嵌套歧义(review-query-* 不会被 review-* 误吞 —— 因为传进去的是 "review-query" 全名;
  #   ★注意 backend 的 pod 是 backend-*, 但它的 label 是 app=backend_api —— instance_check 只看 pod 名, 故正确)。
  "$PY" - "$1" <<'PYEOF'
import json, os, sys
try:
    g = json.load(open(os.path.join(sys.argv[1], "groundtruth.json"), encoding="utf-8-sig"))
    roots = [str(x) for x in (g.get("root_cause_services") or [])]
    known = ("catalog", "catalog-gw", "inventory", "pricing",
             "order", "cart", "search", "review-query", "backend", "checkout")   # ★M9-R retarget 6 根
    print(" ".join(s for s in known if s in roots))
except Exception:
    print("")
PYEOF
}

dense_log_header() {  # $1=arity 台账文件不存在则写头
  local f="$CTK/collect-$1-dense.sh"
  [ -f "$f" ] && return 0
  cat > "$f" <<HDR
#!/usr/bin/env bash
# =============================================================================
# collect-$1-dense.sh — M9【密度重采】$1 档 可复现命令日志(由 m9_drive.sh 自动 append)
# =============================================================================
# PROVENANCE:
#   harness  = M9 density build(Prometheus scrape 2s〔cadvisor/kube-state 5s〕+ prom_range 逐点 emit
#              + 统一探针面板 probe-panel〔11 目标, 扇出型只读端点, 走 kubectl proxy 8001〕
#              + db_lock 锁指标窗内真采样 + single 档 --wide-metrics)
#   数据树   (native trees) $1_dense/_<key>_reps_v19/
#   老 140 与 collect-$1.sh 只读不动;本档 = 并存新版本。
#   每行 = 一个【三项 PASS】(runner exit0 + verify_dual + instance_check)的 case 的完整可跑命令。
#   ★这些行沿用 M8 台账形状(nohup ... &)= 复现命令记录;整文件不是批脚本,
#     顺序重跑请用: bash scripts/chaos/ctk/m9_drive.sh --arity $1 --reps 5
# -----------------------------------------------------------------------------
export PATH="/c/Program Files/Docker/Docker/resources/bin:\$PATH"
export MSYS_NO_PATHCONV=1
export NO_PROXY='*'
export NACOS_ENABLED=false
export PYTHONIOENCODING=utf-8
export KUBECTL='kubectl'
PY='python3'
RUNNER='${REPO_DIR}/scripts/chaos/ctk/chaos_k8s_runner.py'

# --- verified cases (appended by m9_drive.sh) ---
HDR
  say "新建台账 $f"
}

# =============================================================================
# 主循环
# =============================================================================
preflight
[ "$DRYRUN" -eq 1 ] || dense_log_header "$ARITY"    # --dry-run 不落任何文件
DENSE_SH="$CTK/collect-$ARITY-dense.sh"

TOTAL_PASS=0; TOTAL_FAIL=0; SKIPPED_TYPES=""; METHOD_ERR_TYPES=""

# ★先把类型表读进数组,再循环 —— 不能 `while read ... done <<< "$TABLE"`:
#   循环体里的 runner/kubectl/tee 会从同一个 here-string 抢 stdin,把表吃掉(经典坑)。
TYPE_LINES=()
while IFS= read -r _l; do
  [ -n "$_l" ] && TYPE_LINES+=("$_l")
done <<< "$TYPE_TABLE"

for _line in "${TYPE_LINES[@]}"; do
  IFS='|' read -r t_arity key fault prefix repsdir prep args <<< "$_line"
  [ -n "${t_arity:-}" ] || continue
  [ "$t_arity" = "$ARITY" ] || continue
  if [ -n "$TYPES" ]; then
    case ",$TYPES," in *",$key,"*) ;; *) continue ;; esac
  fi

  OUT="$DENSE_ROOT/${ARITY}_dense/_${repsdir}_reps_v19"
  say "=========== 类型 $key (fault=$fault, prep=$prep) → $OUT [rep $FROM_REP..$REPS]"
  if [ "$DRYRUN" -eq 1 ]; then
    for rep in $(seq "$FROM_REP" "$REPS"); do
      echo "nohup \"\$PY\" \"\$RUNNER\" --case-id ${prefix}_r${rep} --fault $fault $args --out-dir \"$OUT\" &"
    done
    continue
  fi

  mkdir -p "$OUT"
  per_type_prep "$prep"

  for rep in $(seq "$FROM_REP" "$REPS"); do
    case_id="${prefix}_r${rep}"
    caselog="$LOGDIR/${case_id}.log"
    attempt=0; ok=0; case_dir=""
    cap=$(retry_cap_for "$key")      # 非 podfail 族 = $RETRY_CAP(行为不变); podfail 短 case = 3
    while [ "$attempt" -le "$cap" ]; do
      attempt=$((attempt + 1))
      # ★★ podfail 族 per-rep 重置(2026-07-12 实测):pod-failure 就地换 pause 镜像 → restartCount 累积
      #   → kubelet 指数退避(10s→…→5m)→ 某个 rep 起不来 → recovery_confirmed FAIL / CrashLoopBackOff 堵死整批。
      #   rollout restart 建新 pod = restartCount 归零 = 退避清零;★必须等旧 pod 消失(否则 restart_delta 变负 → 好 case 被误杀)。
      #   非 podfail 类型 = no-op(一字不变)。放在健康闸【之前】:rollout 会断 5005/5004 的 pf, 由健康闸+守卫接住。
      pre_rep_reset "$key"
      # ★★ per-case 健康闸(2026-07-11 血泪):本次 Phase 1 的 4 次失败【全部】是
      #   "PREP 跑着跑着静默退化"——pod 滚动重启 → port-forward 断 → 载体 ok=0/N →
      #   gate FAIL → 傻傻重试 3 次, 每次白烧 ~20 分钟。开跑前先验, 不绿就等守卫自愈, 别硬上。
      if ! precase_health; then
        say "$case_id ★健康闸未过 → 跳过本次尝试(不浪费一个完整 case 的时间)"
        ledger "$key" "$case_id" "HEALTH_FAIL" "precase_health 未过 attempt=$attempt" ""
        sleep 30
        continue
      fi
      t0=$(date +%s)
      say "--- $case_id 尝试 $attempt/$((cap + 1))"
      # shellcheck disable=SC2086
      # ★ < /dev/null:runner 绝不从驱动器 stdin 读(否则会吃掉外层数据)
      "$PY" "$RUNNER" --case-id "$case_id" --fault "$fault" $args --out-dir "$OUT" \
        < /dev/null 2>&1 | tee -a "$caselog"
      rc=${PIPESTATUS[0]}
      if [ "$rc" -ne 0 ]; then
        say "$case_id runner exit=$rc (CHECKSUM/gate 闸在 runner 自含) → 重试"
        ledger "$key" "$case_id" "FAIL" "runner_exit=$rc attempt=$attempt" ""
        sleep 20; continue
      fi
      case_dir=$(newest_case_dir "$OUT" "$t0")
      if [ -z "$case_dir" ]; then
        say "$case_id runner exit=0 但找不到新 case 目录 → 重试"
        ledger "$key" "$case_id" "FAIL" "no_case_dir attempt=$attempt" ""
        sleep 10; continue
      fi
      # 核 2: verify_dual(gate + checksum zero_drift + trace 18 字段 + 6 标签 + root pod ∈ during)
      # ★必须看 PIPESTATUS[0]:`if ! cmd | tee` 判的是 tee 的退出码(恒 0)—— 会把 FAIL 当 PASS。
      "$PY" "$CTK/verify_dual.py" "$case_dir" < /dev/null 2>&1 | tee -a "$caselog"
      vrc=${PIPESTATUS[0]}
      if [ "$vrc" -ne 0 ]; then
        say "$case_id verify_dual FAIL(exit=$vrc) → 重试"
        ledger "$key" "$case_id" "FAIL" "verify_dual attempt=$attempt" "$case_dir"
        sleep 10; continue
      fi
      # 核 3: instance_check(GT 里出现的 rolling 根 catalog/inventory/pricing 逐个查)
      inst_ok=1
      for rsvc in $(gt_rolling_roots "$case_dir"); do
        "$PY" "$CTK/instance_check.py" "$case_dir" "$rsvc" < /dev/null 2>&1 | tee -a "$caselog"
        [ "${PIPESTATUS[0]}" -eq 0 ] || inst_ok=0
      done
      if [ "$inst_ok" -ne 1 ]; then
        say "$case_id instance_check FAIL → 重试"
        ledger "$key" "$case_id" "FAIL" "instance_check attempt=$attempt" "$case_dir"
        sleep 10; continue
      fi
      ok=1; break
    done

    if [ "$ok" -ne 1 ]; then
      TOTAL_FAIL=$((TOTAL_FAIL + 1))
      # ★隔离(审查 R2 MAJOR):runner 无条件写 case_dir,verify/instance FAIL 的 case 仍是一个完整目录;
      #   而 package_for_delivery.find_cases 只 glob **/metadata.json、【无 ready_for_release 过滤】
      #   → 不隔离就会被打包发给上游(凌晨 5 点没人会记得手动挪)。移出打包扫描树。
      if [ -n "${case_dir:-}" ] && [ -d "$case_dir" ]; then
        mkdir -p "$CWD/(native trees) _dev_m9"
        q="$CWD/(native trees) _dev_m9/${key}_$(basename "$case_dir")_$(date +%s)"
        mv "$case_dir" "$q" 2>/dev/null && say "$case_id ★已隔离到 $q(移出打包扫描树)"
        ledger "$key" "$case_id" "GIVEUP" "retries_exhausted; quarantined" "$q"
      else
        ledger "$key" "$case_id" "GIVEUP" "retries_exhausted; no case_dir" "${case_dir:-}"
      fi
      say "$case_id ★放弃(重试 cap 用尽)→ 已隔离+记账,继续下一个 case"
      post_case_cooldown "$prep"
      continue
    fi

    TOTAL_PASS=$((TOTAL_PASS + 1))
    say "$case_id PASS (runner+verify_dual+instance_check) → $case_dir"
    ledger "$key" "$case_id" "PASS" "attempts=$attempt" "$case_dir"
    # 台账 append(沿用 M8 惯例:完整可跑命令 + 注释)
    printf 'nohup "$PY" "$RUNNER" --case-id %s --fault %s %s --out-dir "%s" &   # rep %s/%s | verify_dual PASS | instance_check PASS | checksum净 | %s\n' \
      "$case_id" "$fault" "$args" "$OUT" "$rep" "$REPS" "$(now)" >> "$DENSE_SH"

    # ★r1 内联方法检查(BARO+RCD,秒级):非空排名 = 密度修好没有的判据 + MRCBench 四族指标
    if [ "$rep" -eq 1 ]; then
      say "$key r1 → m9_score.py(BARO+RCD, MRCBench)"
      "$PY" "$CTK/m9_score.py" "$case_dir" --type "$key" --out "$VERDICT" < /dev/null 2>&1 | tee -a "$caselog"
      sc=${PIPESTATUS[0]}     # ★同上:必须取 PIPESTATUS,不能判 tee
      # ★退出码必须分流(审查 R2 MAJOR):打分器的 bug 绝不能吃掉今晚采不回来的数据。
      #   exit 0 = 两法非空排名 → 密度修好了,照常采完 5 reps。
      #   exit 3 = 真·空排名 → 该类型密度没修好,跳过剩余 reps(省 4 个 rep 的时间),留早上处理。
      #   exit 4 = 方法基础设施异常/无数据(_cl_patched 炸 / RCD 崩 / adapter 出错)→ 与采集质量无关,
      #            ★必须继续采完该类型剩余 reps★(否则一个打分器 bug 会让整夜 140 缩水成 28)。
      if [ "$sc" -eq 0 ]; then
        say "$key r1 方法检查 PASS(两法非空排名);verdict → $VERDICT"
        ledger "$key" "$case_id" "METHOD_OK" "baro+rcd non-empty" "$case_dir"
      elif [ "$sc" -eq 3 ]; then
        say "$key r1 ★空排名(exit=3 密度未修好)→ 跳过本类型剩余 reps,继续下一个类型"
        ledger "$key" "$case_id" "METHOD_EMPTY" "m9_score_exit=3 empty_ranking" "$case_dir"
        SKIPPED_TYPES="$SKIPPED_TYPES $key"
        post_case_cooldown "$prep"
        break
      else
        say "$key r1 ★方法侧异常(exit=$sc,非空排名问题)→ 记账,★照常继续采完本类型剩余 reps"
        ledger "$key" "$case_id" "METHOD_ERROR" "m9_score_exit=$sc (infra/no-data; 采集继续)" "$case_dir"
        METHOD_ERR_TYPES="$METHOD_ERR_TYPES $key"
      fi
    fi
    post_case_cooldown "$prep"
  done
done

say "=========== 驱动器结束: PASS=$TOTAL_PASS FAIL/GIVEUP=$TOTAL_FAIL"
[ -n "$SKIPPED_TYPES" ]    && say "★空排名被跳过的类型(密度未修好,留早上处理):$SKIPPED_TYPES"
[ -n "$METHOD_ERR_TYPES" ] && say "★方法侧异常的类型(采集已照常完成,仅打分待补,留早上处理):$METHOD_ERR_TYPES"
say "台账 $DENSE_SH | verdict $VERDICT | 记账 $LEDGER"
say "隔离区(GIVEUP case,勿打包): $CWD/(native trees) _dev_m9/"
exit 0
