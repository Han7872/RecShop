#!/usr/bin/env bash
# =============================================================================
# m9r_drive.sh — 【根因摊开】采集驱动器(M9-retarget)
# =============================================================================
# WHY:数据集 GT 极度偏斜(catalog 系占 93%)→ 一个【什么都不看、只按频次猜】的常量榜单
#      Hit@1 = 0.643、Hit@5 = 1.000,三个已发表方法(BARO/RCD/Eadro)一个都打不过它。
#      ⇒ 作为 benchmark,聚合指标【没有区分度】。这是当前数据集最大的短板。
#
# 做什么:把两个【Chaos Mesh CRD 类】单根因故障打到【从未当过根因】的服务上。
#   service_cpu (StressChaos):order · cart · review-query · backend · checkout   —— 5 个
#   pod_failure (PodChaos)   :order · cart · review-query · backend · checkout · search —— 6 个
#   11 组合 × 5 rep = 55 个新 single case
#
# ★ search × service_cpu 【已砍】—— 活集群实测 p95_ratio = 0.99(门槛 1.8×)。
#   它的 1.7s baseline 是 MySQL LIKE 全表扫,压 CPU 动不了 I/O 等待。
#   救援全败(workers=8 → 1.34× / enrich=0 + workers=8 → 1.26×)。别浪费一夜。
#
# 预期:常量先验 0.643 → ~0.46;根因服务种类 8 → 14。
#
# ★纯增量:已交付的 140 个 case 【一个字节不碰】。新树 single_spread/。
#
# 每 case 三项核验(与 M9 一致,不新造判据):
#   1) runner 退出码 0(CHECKSUM 闸在 runner 自含 → 非 0 即脏)
#   2) verify_dual.py  <case>
#   3) instance_check.py <case> <target_svc>
#   三项全过 = PASS → append 一行完整可跑命令到台账。
#   失败 → 重试(cap 2)→ 仍失败:记账 + 继续(绝不杀整批)。
#
# 用法(主循环 nohup 起,★不要 run_in_background):
#   nohup bash scripts/chaos/ctk/m9r_drive.sh > /tmp/m9r.log 2>&1 &
#   bash scripts/chaos/ctk/m9r_drive.sh --dry-run          # 只打印
#   bash scripts/chaos/ctk/m9r_drive.sh --types svccpu_order --reps 1
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
ITEM='0071341196'
OUTROOT="$CWD/(native trees) single_spread"     # ★新树,老 140 一字节不碰
LOGDIR="$CWD/logs/m9r"
LEDGER="$LOGDIR/m9r_ledger.tsv"
TALLY="$CTK/collect-single-spread.sh"
RETRY_CAP=2

REPS=5; TYPES=""; DRYRUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --reps)    REPS="$2"; shift 2 ;;
    --types)   TYPES="$2"; shift 2 ;;
    --dry-run) DRYRUN=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 64 ;;
  esac
done
mkdir -p "$LOGDIR"

# ---- 单实例锁(血泪:两个驱动器并发 → 同集群互相踩 → 数据全废)----
LOCK="$LOGDIR/m9r_drive.lock"
if [ "$DRYRUN" -eq 0 ]; then
  if [ -e "$LOCK" ]; then
    old=$(cat "$LOCK" 2>/dev/null || echo "?")
    if kill -0 "$old" 2>/dev/null; then
      echo "FATAL: 已有 m9r_drive 在跑(pid=$old)。绝不允许并发。" >&2; exit 75
    fi
    echo "[m9r] 陈旧锁(pid=$old 已死)→ 接管" >&2; rm -f "$LOCK"
  fi
  echo "$$" > "$LOCK"
  trap 'rm -f "$LOCK"' EXIT INT TERM
fi

# ---- 类型表:key | fault | target_svc ----
#   ★search 只做 pod_failure(service_cpu 实测 0.99× 过不了门,已砍)
read -r -d '' TABLE <<'TBL' || true
svccpu_order|service_cpu_single|order
svccpu_cart|service_cpu_single|cart
svccpu_review-query|service_cpu_single|review-query
svccpu_backend|service_cpu_single|backend
svccpu_checkout|service_cpu_single|checkout
podfail_order|pod_failure_single|order
podfail_cart|pod_failure_single|cart
podfail_review-query|pod_failure_single|review-query
podfail_backend|pod_failure_single|backend
podfail_checkout|pod_failure_single|checkout
podfail_search|pod_failure_single|search
TBL

ARGS="--item $ITEM --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics"

# 台账头
if [ ! -f "$TALLY" ] && [ "$DRYRUN" -eq 0 ]; then
  cat > "$TALLY" <<HDR
#!/usr/bin/env bash
# collect-single-spread.sh — 【根因摊开】55 case 可复现命令日志(由 m9r_drive.sh 自动 append)
# 前置(PREP):kubectl proxy 8001 · OTel 栈 · 25 服务 · Chaos Mesh · CHECKSUM 基线
# 每行 = 一个 PASS 的 case(三项核验全过);行尾注释含 rep / 核验结论 / 时间戳。
HDR
fi

say() { echo "[m9r $(date -u +%H:%M:%SZ)] $*"; }

# =============================================================================
# ★★ pre_rep_reset —— pod_failure 族【每个 rep 前】必须等目标 pod 完全恢复
# -----------------------------------------------------------------------------
# 血泪(M9 踩过,本脚本首版漏了移植 → podfail_order_r5 / podfail_cart_r4 当场复现):
#   PodChaos 把容器换成 pause 镜像 → 恢复后 kubelet 有【指数退避】。
#   上一个 rep 的 pod 还没爬起来,下一个 rep 就开采:
#     - runner 的 fail-closed 载体预检会拦住(注入前载体 503)→ FATAL → GIVEUP
#       ★这个预检【是对的】:带病开采会让 pod_failure 的门(error_ratio>=0.8)
#         对着一个【本来就不通】的载体判 PASS = 产出一个根本没注进去的假 case。
#     - 或者更阴险:runner 同时读到新旧两个 pod → 算出【负的 restart_delta】→ 好 case 被判死。
# 修:每个 pod_failure rep 前,等到 (pods==1 && 该 pod Ready) 且载体探得通,才放行。
# =============================================================================
# ★★ 第二版(2026-07-13 08:40,podfail_review-query_r5 打脸后):
#   第一版只【等 Ready + 载体 200】—— 不够。
#   实测:review-query 的 restartCount 累积到 18 之后,kubelet 退避拉得太长,
#         PodChaos 这一轮【根本没完成一次重启周期】→ restart_delta=0
#         → gate 判 chaos_induced=False → 好端端一个 case 被判死。
#   ⇒ 必须【删掉 pod 让 Deployment 重建】,把 restartCount 归零(这才是 M9 那个修复的全貌)。
pre_rep_reset() {   # $1 = target service
  local svc="$1" tries=0 lbl="${APP_LABEL[$svc]}"
  local url="http://127.0.0.1:8001/api/v1/namespaces/$NS/services/${svc}:${SVC_PORT[$svc]}/proxy/${SVC_PATH[$svc]}"

  # ① restartCount > 0 → 删 pod,让 Deployment 重建一个【全新的、计数为 0 的】
  local rs
  rs=$("$KUBECTL" get pods -n "$NS" -l "app=$lbl" \
       -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' 2>/dev/null \
       | awk '{s+=$1} END{print s+0}')
  if [ "${rs:-0}" -gt 0 ]; then
    say "  [reset] $svc restartCount=$rs > 0 → 删 pod 重建(清零 kubelet 退避)"
    "$KUBECTL" delete pod -n "$NS" -l "app=$lbl" --wait=false >/dev/null 2>&1
    sleep 8
  fi

  # ② 等到:恰好 1 个 pod · Ready · restartCount==0 · 载体探得通
  while [ "$tries" -lt 72 ]; do
    local pods ready code
    pods=$("$KUBECTL" get pods -n "$NS" -l "app=$lbl" --no-headers 2>/dev/null | wc -l)
    ready=$("$KUBECTL" get pods -n "$NS" -l "app=$lbl" \
            -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null)
    rs=$("$KUBECTL" get pods -n "$NS" -l "app=$lbl" \
         -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' 2>/dev/null \
         | awk '{s+=$1} END{print s+0}')
    code=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$url" --max-time 5 2>/dev/null)
    if [ "$pods" = "1" ] && [ "$ready" = "true" ] && [ "${rs:-1}" = "0" ] && [ "$code" = "200" ]; then
      [ "$tries" -gt 0 ] && say "  [reset] $svc 就绪(等了 $((tries*5))s,restartCount=0)"
      return 0
    fi
    tries=$((tries+1)); sleep 5
  done
  say "  [reset] ⚠ $svc 等了 360s 仍未就绪(pods=$pods ready=$ready restarts=$rs code=$code)—— 仍开采,让 runner 预检决定"
  return 1
}

# 6 个目标服务的端口 / 探针路径 / app 标签(★backend 的 app 标签是 backend_api,不是 backend)
declare -A SVC_PORT=( [order]=5010 [cart]=5006 [review-query]=5018 [backend]=5000 [checkout]=5011 [search]=5017 )
declare -A SVC_PATH=( [order]="api/orders?user_token=user_demo_001&per_page=5" [cart]="api/cart/count?user_token=user_demo_001" \
                      [review-query]="api/reviews?item_id=$ITEM&per_page=5" [backend]="api/stats/model" \
                      [checkout]="api/checkout/preview?user_token=user_demo_001" [search]="api/search?q=phone&per_page=5" )
declare -A APP_LABEL=( [order]=order [cart]=cart [review-query]=review-query [backend]=backend_api [checkout]=checkout [search]=search )

pass=0; fail=0; skip=0

say "=============================================="
say "根因摊开采集:11 组合 × ${REPS} rep = $((11*REPS)) case"
say "  service_cpu × 5 服务(search 已砍:实测 0.99× 过不了门)"
say "  pod_failure × 6 服务"
say "  输出 → $OUTROOT   (已交付的 140 个一字节不碰)"
say "=============================================="

while IFS='|' read -r key fault svc; do
  [ -n "$key" ] || continue
  if [ -n "$TYPES" ] && ! echo ",$TYPES," | grep -q ",$key,"; then continue; fi

  outdir="$OUTROOT/_${key}_reps_v19"
  for rep in $(seq 1 "$REPS"); do
    cid="${key}_r${rep}"
    cdir="$outdir/$cid"

    if [ -d "$cdir" ] && [ -f "$cdir/groundtruth.json" ]; then
      say "SKIP  $cid (已存在)"; skip=$((skip+1)); continue
    fi

    cmd="\"\$PY\" \"\$RUNNER\" --case-id $cid --fault $fault --target-service $svc $ARGS --out-dir \"$outdir\""
    if [ "$DRYRUN" -eq 1 ]; then echo "  $cmd"; continue; fi

    ok=0
    for try in $(seq 0 "$RETRY_CAP"); do
      [ "$try" -gt 0 ] && say "  重试 $try/$RETRY_CAP: $cid"
      # ★pod_failure 族:每次尝试前都等目标 pod 完全恢复(见 pre_rep_reset 的长注释)
      [ "$fault" = "pod_failure_single" ] && pre_rep_reset "$svc"
      rm -rf "$cdir"
      "$PY" "$RUNNER" --case-id "$cid" --fault "$fault" --target-service "$svc" \
        --item "$ITEM" --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
        --keep-carrier --wide-metrics --out-dir "$outdir" > "$LOGDIR/$cid.log" 2>&1
      rc=$?
      [ "$rc" -ne 0 ] && { say "  FAIL $cid runner rc=$rc"; continue; }
      "$PY" "$CTK/verify_dual.py" "$cdir" > "$LOGDIR/$cid.verify" 2>&1 || { say "  FAIL $cid verify_dual"; continue; }
      "$PY" "$CTK/instance_check.py" "$cdir" "$svc" >> "$LOGDIR/$cid.verify" 2>&1 || { say "  FAIL $cid instance_check($svc)"; continue; }
      ok=1; break
    done

    if [ "$ok" -eq 1 ]; then
      ratio=$(grep -o "p95_ratio=[0-9.]*" "$LOGDIR/$cid.log" | tail -1)
      say "PASS  $cid  ($svc / $fault)  $ratio"
      printf '%s\t%s\t%s\t%s\tPASS\t%s\n' "$(date -u +%FT%TZ)" "$cid" "$svc" "$fault" "$ratio" >> "$LEDGER"
      echo "nohup \"\$PY\" \"\$RUNNER\" --case-id $cid --fault $fault --target-service $svc $ARGS --out-dir \"$outdir\" &   # rep $rep/$REPS | verify_dual PASS | instance_check PASS | checksum净 | $(date -u +%FT%TZ)" >> "$TALLY"
      pass=$((pass+1))
    else
      say "GIVEUP $cid  (试了 $((RETRY_CAP+1)) 次)"
      printf '%s\t%s\t%s\t%s\tGIVEUP\t-\n' "$(date -u +%FT%TZ)" "$cid" "$svc" "$fault" >> "$LEDGER"
      fail=$((fail+1))
    fi
  done
done <<< "$TABLE"

say "=============================================="
say "完成:PASS=$pass  GIVEUP=$fail  SKIP=$skip"
say "=============================================="
echo "M9R-DONE pass=$pass giveup=$fail skip=$skip"
