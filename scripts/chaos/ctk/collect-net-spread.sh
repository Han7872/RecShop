#!/usr/bin/env bash
# M10 net-spread 冒烟:net_delay@80 / net_loss@10 × 6 服务 × r1 = 12 case(串行)。
# ★每条带死 80/16/10(防忘传走默认 500/50/60 顶穿 —— runner 守卫只管乱改不管忘传)。
# ★这是 runner net retarget(f35744d)的【端到端真测】——之前只 dry-run 过,第1条即真验。
# 顺序:delay order(30s,最快出 + 验 retarget + 超时关注)→ loss backend(90s,边缘风险)
#       → delay 其余 → loss 其余。
# 用法: nohup bash collect-net-spread.sh > /tmp/m10_smoke.log 2>&1 &
set -uo pipefail
export NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"   # kubectl.exe 在此(git-bash 默认看不见)
export KUBECTL="kubectl"
PY="python3"
RUNNER="scripts/chaos/ctk/chaos_k8s_runner.py"
OUT="(native trees) net_spread"
COMMON="--item 0071341196 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics"

run() {  # cid fault svc extra stage
  local cid=$1 fault=$2 svc=$3 extra=$4 stage=$5 key=${1%_r1}
  echo "############################################################"
  echo "### $(date '+%H:%M:%S')  $cid  ($fault @ $svc)"
  echo "############################################################"
  "$PY" "$RUNNER" --case-id "$cid" --fault "$fault" --target-service "$svc" \
    $extra --stage-seconds $stage $COMMON --out-dir "$OUT/_${key}_reps_v19"
  echo "### $cid  exit=$?  @ $(date '+%H:%M:%S')"
  echo
}

run netdelay_order_r1        net_delay_single order        "--net-delay-ms 80 --net-jitter-ms 16" 30
run netloss_backend_r1       net_loss_single  backend      "--net-loss-pct 10"                    90
run netdelay_backend_r1      net_delay_single backend      "--net-delay-ms 80 --net-jitter-ms 16" 30
run netdelay_cart_r1         net_delay_single cart         "--net-delay-ms 80 --net-jitter-ms 16" 30
run netdelay_review-query_r1 net_delay_single review-query "--net-delay-ms 80 --net-jitter-ms 16" 30
run netdelay_checkout_r1     net_delay_single checkout     "--net-delay-ms 80 --net-jitter-ms 16" 30
run netdelay_search_r1       net_delay_single search       "--net-delay-ms 80 --net-jitter-ms 16" 30
run netloss_order_r1         net_loss_single  order        "--net-loss-pct 10"                    90
run netloss_cart_r1          net_loss_single  cart         "--net-loss-pct 10"                    90
run netloss_review-query_r1  net_loss_single  review-query "--net-loss-pct 10"                    90
run netloss_checkout_r1      net_loss_single  checkout     "--net-loss-pct 10"                    90
run netloss_search_r1        net_loss_single  search       "--net-loss-pct 10"                    90

echo "============================================================"
echo "ALL DONE @ $(date '+%H:%M:%S')"
