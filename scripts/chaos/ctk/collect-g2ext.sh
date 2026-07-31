#!/usr/bin/env bash
# =============================================================================
# collect-g2ext.sh — G2ext 多根因扩充批次【可复现一键采集器】(双-17..21 + 三-05..08, 45 case)
# =============================================================================
# 与 collect-{dual,triple}-ext.sh 的分工:
#   - collect-*-ext.sh = 命令【日志/台账】(append-only, 记"每 rep 用了什么命令 + 踩坑史"), 供人工溯源。
#   - 本脚本         = 无人值守【orchestrator】(PID 绑定 + MAXWAIT + 三项 PASS 核 + 清脏重试≤3 +
#                      双-17 inventory 特殊前置), 供一条命令复跑整批。
#
# ★冻结 runner 版本 = commit f83fe5b(Phase C invlat 时序修复后; selftest 167/167)。
#   复现须 git checkout 该 commit 的 chaos_k8s_runner.py, 否则 GT schema 可能不一致。
# 设计权威 = (project docs)/{dual,triple}-root-catalog.md §扩充批次
# 采集黑板 = (project docs)/archive/TASK-K8S-G2ext-multiroot-expand.md
#
# 用法:
#   bash scripts/chaos/ctk/collect-g2ext.sh [phaseA|phaseB|phaseC|all]   (默认 all)
#   前置守护须【先】手工起(见 §preflight 打印的清单); 本脚本只做只读核查不代起(避免与已起的抢)。
#
# 环境铁律(archive/TASK-K8S-M8-overnight-recollect §1.5-A):
#   NACOS_ENABLED=false / NO_PROXY='*' / kubectl 在 PATH / proxy8001 / pfwd 守护 / CHECKSUM 闸(runner 自含)。
# =============================================================================
set -u
cd /d/AIProjects/RecWeb2 || { echo "FATAL: 需在 ${REPO_DIR} 下运行"; exit 1; }

export KUBECTL='kubectl'
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
PY=python3
RUN=scripts/chaos/ctk/chaos_k8s_runner.py
UT=0870e257-6cd0-4fe4-b815-0a9da6b25d41
PHASE="${1:-all}"
FAILDIR=(native trees) _g2ext_smoke_failed
mkdir -p "$FAILDIR"

# --------------------------------------------------------------------------
# preflight: 只读核查(不代起守护, 缺则 FATAL 让操作员按清单起)
# --------------------------------------------------------------------------
preflight() {
  local ok=1
  echo "=== preflight($PHASE) ==="
  # 1. 集群 + 无残留 CRD
  "$KUBECTL" get nodes >/dev/null 2>&1 || { echo "  [FATAL] K8S API 不可达(起 Docker Desktop)"; ok=0; }
  local crd=$("$KUBECTL" get networkchaos,podchaos,stresschaos -n recweb-chaos --no-headers 2>/dev/null | wc -l)
  [ "$crd" = "0" ] || { echo "  [WARN] 残留 CRD $crd 个 → 清理"; "$KUBECTL" delete networkchaos,podchaos,stresschaos --all -n recweb-chaos --ignore-not-found >/dev/null 2>&1; }
  # 2. proxy8001(checkout/cart/order/review-query/rec-agent 载体走它)
  curl -s --noproxy '*' -o /dev/null --max-time 4 "http://127.0.0.1:8001/api/v1/namespaces/recweb-chaos/pods?limit=1" \
    && echo "  [OK] proxy8001" || { echo "  [FATAL] proxy8001 未起: nohup \"\$KUBECTL\" proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*' &"; ok=0; }
  # 3. pfwd 守护(pricing 5014/backend 5000/user 5004/catalog 5005 直连载体)
  for p in 5000 5004 5005 5014; do
    curl -s --noproxy '*' -o /dev/null --max-time 3 "http://127.0.0.1:$p/health" || { echo "  [FATAL] :$p 不通 → 起 pfwd_start.sh"; ok=0; }
  done
  [ "$ok" = "1" ] && echo "  [OK] business pfwd (5000/5004/5005/5014)"
  # 4. catalog restarter(所有含 catalog podfail/CPU 组; 三-05/07/08 + 三-06 也稳)
  echo "  [守护清单] 需已起: pfwd_start.sh + pfwd_catalog_restarter.sh"
  # 5. rec-agent deepseek secret(双-20/三-07 recommend 证据载体; 缺则 recovery_confirmed 挂)
  if [ "$PHASE" = "all" ] || [ "$PHASE" = "phaseB" ] || [ "$PHASE" = "phaseC" ]; then
    local ds=$("$KUBECTL" get deploy rec-agent -n recweb-chaos -o jsonpath='{.spec.template.spec.containers[0].env[*].name}' 2>/dev/null | tr ' ' '\n' | grep -c DEEPSEEK)
    [ "${ds:-0}" -ge 1 ] && echo "  [OK] rec-agent deepseek-env 已挂" \
      || { echo "  [FATAL] rec-agent 缺 deepseek-env(双-20/三-07 需): \"\$KUBECTL\" set env deploy/rec-agent --from=secret/deepseek-env -n recweb-chaos"; ok=0; }
  fi
  # 6. inventory restarter(双-17; rollout 杀 5013 PF)
  if [ "$PHASE" = "all" ] || [ "$PHASE" = "phaseC" ]; then
    echo "  [守护清单] 双-17 另需: nohup bash scripts/chaos/ctk/pfwd_inventory_restarter.sh &"
  fi
  # 7. CHECKSUM 基线(runner 自含闸, 这里只提示)
  echo "  [铁律] items=3849590678 / inventory=3935678504 (runner CHECKSUM 闸 fail-closed 自核)"
  [ "$ok" = "1" ] || { echo "=== preflight FAILED, 按上方 FATAL 起前置后重跑 ==="; exit 2; }
  echo "=== preflight OK ==="
}

clean_cluster() {
  "$KUBECTL" delete networkchaos,podchaos,stresschaos --all -n recweb-chaos --ignore-not-found >/dev/null 2>&1
  sleep 10
}

# 双-17 专用: unset inv env + 等 rollout settle + 核基线<1s(残留污染 base, commit f83fe5b 教训)
inv_clean_wait() {
  "$KUBECTL" set env deploy/inventory FAULT_DELAY_MS- -n recweb-chaos >/dev/null 2>&1
  "$KUBECTL" rollout status deploy/inventory -n recweb-chaos --timeout=150s >/dev/null 2>&1
  local w=0
  while [ "$w" -lt 150 ]; do
    local t=$(curl -s --noproxy '*' -o /dev/null -w '%{time_total}' --max-time 5 "http://127.0.0.1:5013/api/inventory/0071341196" 2>/dev/null)
    [ "$(echo "${t:-9}" | cut -d. -f1)" = "0" ] && return 0
    sleep 5; w=$((w+5))
  done
  echo "  [WARN] inventory baseline still slow after 150s(双-17 base 可能污染)"
}

# 三项 PASS 核: ready_for_release + checksum zero_drift + contract valid + gt/summary 存在
check_pass() {
  "$PY" - "$1" <<'PYEOF'
import json, sys, os
base = sys.argv[1]
try:
    md = json.load(open(os.path.join(base,'metadata.json'), encoding='utf-8'))
    ok = (md.get('ready_for_release') is True
          and md.get('checksum_guard',{}).get('zero_drift') is True
          and md.get('root_metric_contract',{}).get('valid') is True
          and os.path.exists(os.path.join(base,'groundtruth.json'))
          and os.path.exists(os.path.join(base,'summary.md')))
    sys.exit(0 if ok else 1)
except Exception:
    sys.exit(1)
PYEOF
}

# run_rep <case_id> <fault> <outdir> <stage> <is_inv:inv|no>
run_rep() {
  local cid="$1" fault="$2" outdir="$3" stage="$4" is_inv="$5"
  local casedir="(native trees) $outdir/$cid"
  local mjson="$casedir/metadata.json"
  local attempt=1
  while [ "$attempt" -le 3 ]; do
    [ "$is_inv" = "inv" ] && inv_clean_wait
    echo "[$(date +%H:%M:%S)] LAUNCH $cid attempt $attempt (stage=$stage)"
    rm -rf "$casedir" 2>/dev/null
    nohup "$PY" "$RUN" --case-id "$cid" --fault "$fault" --deep --user-token "$UT" \
      --stage-seconds "$stage" --poll 2.0 --keep-carrier \
      --out-dir "${REPO_DIR}/(native trees) $outdir" > "/tmp/rep_$cid.log" 2>&1 &
    local pid=$! waited=0
    while kill -0 "$pid" 2>/dev/null && [ ! -f "$mjson" ] && [ "$waited" -lt 2100 ]; do
      sleep 20; waited=$((waited+20))
    done
    if [ -f "$mjson" ]; then
      wait "$pid" 2>/dev/null
      if check_pass "$casedir"; then
        echo "[$(date +%H:%M:%S)] PASS $cid (${waited}s, attempt $attempt)"
        clean_cluster
        [ "$is_inv" = "inv" ] && "$KUBECTL" set env deploy/inventory FAULT_DELAY_MS- -n recweb-chaos >/dev/null 2>&1
        return 0
      else
        echo "[$(date +%H:%M:%S)] GATE-FAIL $cid attempt $attempt"
      fi
    elif kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null; echo "[$(date +%H:%M:%S)] TIMEOUT $cid attempt $attempt"
    else
      echo "[$(date +%H:%M:%S)] ABORT $cid attempt $attempt; log tail:"; tail -8 "/tmp/rep_$cid.log"
    fi
    clean_cluster
    "$KUBECTL" set env deploy/catalog FAULT_DELAY_MS- FAULT_RAISE- -n recweb-chaos >/dev/null 2>&1
    "$KUBECTL" set env deploy/inventory FAULT_DELAY_MS- FAULT_RAISE- -n recweb-chaos >/dev/null 2>&1
    attempt=$((attempt+1)); sleep 20
  done
  echo "[$(date +%H:%M:%S)] SKIP $cid after 3 attempts (morning-report flag)"
  mv "$casedir" "$FAILDIR/${cid}_final_fail" 2>/dev/null
  return 1
}

# --------------------------------------------------------------------------
# Phase 定义(组序交替=CPU flake 风险分散; is_inv=inv 触发 inventory 特殊前置)
# --------------------------------------------------------------------------
run_phaseA() {   # 双-18/19/21 + 三-05/08, stage 240, 无 inventory 腿
  for r in 1 2 3 4 5; do
    run_rep "cart_order_cpu_r$r"          cart_cpu_x_order_cpu                          dual_ext   240 no
    run_rep "search_rq_r$r"               search_podfail_x_reviewquery_cpu              dual_ext   240 no
    run_rep "user_backend_r$r"            user_podfail_x_backend_cpu                    dual_ext   240 no
    run_rep "checkout_cart_pricing_r$r"   checkout_podfail_x_cart_cpu_x_pricing_cpu     triple_ext 240 no
    run_rep "order_rq_catalog_r$r"        order_podfail_x_reviewquery_cpu_x_catalog_cpu triple_ext 240 no
  done
}
run_phaseB() {   # 双-20(rec-agent 五件套) + 三-06, stage 240, 需 deepseek secret
  for r in 1 2 3 4 5; do
    run_rep "recagent_backend_r$r"     recagent_cpu_x_backend_cpu             dual_ext   240 no
    run_rep "backend_sasrec_gwnet_r$r" backend_cpu_x_sasrec_cpu_x_gw_netdelay triple_ext 240 no
  done
}
run_phaseC() {   # 双-17(invlat, stage 300, inventory 前置) + 三-07(三机制, stage 240)
  for r in 1 2 3 4 5; do
    run_rep "checkout_inv_r$r"            checkout_podfail_x_inv_latency                   dual_ext   300 inv
    run_rep "recagent_sasrec_catalog_r$r" recagent_netdelay_x_sasrec_cpu_x_catalog_podfail triple_ext 240 no
  done
}

preflight
case "$PHASE" in
  phaseA) run_phaseA ;;
  phaseB) run_phaseB ;;
  phaseC) run_phaseC ;;
  all)    run_phaseA; run_phaseB; run_phaseC ;;
  *) echo "用法: $0 [phaseA|phaseB|phaseC|all]"; exit 1 ;;
esac
echo "G2EXT_COLLECT_DONE ($PHASE)"
echo "★采后核: python -c 'REGISTRY 校验' + CHECKSUM 复核 + build_full_delivery.py TRAD 表加两行"
