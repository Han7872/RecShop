#!/usr/bin/env bash
# G1 放量: 3类×5 剩余 12 发(r1 已成). 每发 PID+硬上限, 采后三项核 + agent_spans 回收 + 重试≤2.
set -uo pipefail
REPO_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$REPO_DIR"
export NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
export KUBECTL="kubectl"
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
PY="python3"
RUNNER="scripts/chaos/ctk/chaos_k8s_runner.py"
OUT="${REPO_DIR}/(native trees) single_recagent"
SEQ="B000PGJ7SA,B000HKMM4A,B00F0RD86G,B01C2O7YNC"
COMMON="--target-service rec-agent --item 0071341196 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics --recagent-seq $SEQ --recagent-top-k 5 --recagent-recommend-timeout 150"
LEDGER="$OUT/g1_bulk_ledger.tsv"; echo -e "case\tstatus\twaited\troot\taffected\tspans" > "$LEDGER"

recover_spans() {  # case_dir
  # ★MSYS 路径改写坑(黑板 spike 坑2 的同一条, 这里栽了第二次): Git Bash 会把 kubectl 参数里的
  #   /agentfault-data/... 改写成 C:/Program Files/Git/agentfault-data/... → cat 必失败。
  #   必须 MSYS2_ARG_CONV_EXCL='*' 关掉转换; 且【不能吞 stderr】, 失败要吼出来(之前 2>/dev/null
  #   把错误吞掉 + 紧跟的 truncate 却成功 → span 被清空且没存下, 10 个 case 的 agent 层永久丢失)。
  local cdir="$1"; mkdir -p "$cdir/raw/agent_spans"
  local out="$cdir/raw/agent_spans/spans.jsonl"
  # ★pod_failure 会杀/换容器: 采集刚结束时 exec 会 "container not found" → 先等 Ready 再收, 并重试。
  "$KUBECTL" wait --for=condition=Ready pod -l app=recommendation_agent -n recweb-chaos --timeout=150s >/dev/null 2>&1
  local rc=1 n=0 t
  for t in 1 2 3; do
    MSYS2_ARG_CONV_EXCL='*' "$KUBECTL" exec -n recweb-chaos deploy/rec-agent -- cat /agentfault-data/spans.jsonl > "$out" 2>/tmp/recover_err.txt
    rc=$?; n=$(wc -l < "$out" 2>/dev/null || echo 0)
    [ "$rc" -eq 0 ] && [ "$n" -gt 0 ] && break
    echo "  [span-retry $t] rc=$rc n=$n $(head -c 90 /tmp/recover_err.txt 2>/dev/null)"
    sleep 15
  done
  if [ "$rc" -ne 0 ] || [ "$n" -eq 0 ]; then
    echo "  [SPAN-FAIL] $(basename "$cdir") rc=$rc lines=$n -- 不截断 pod 侧, 保住数据"
    return 1
  fi
  MSYS2_ARG_CONV_EXCL='*' "$KUBECTL" exec -n recweb-chaos deploy/rec-agent -- sh -c 'true > /agentfault-data/spans.jsonl'
  return 0
}

one() {  # cid fault extra stage subdir
  local cid=$1 fault=$2 extra=$3 stage=$4 sub=$5 try=0
  local dir="$OUT/$sub"; local cdir="$dir/$cid"; local MJSON="$cdir/metadata.json"
  while [ "$try" -lt 3 ]; do
    try=$((try+1)); rm -rf "$cdir"
    echo "### $(date '+%H:%M:%S') $cid try=$try ($fault)"
    nohup "$PY" "$RUNNER" --case-id "$cid" --fault "$fault" $extra --stage-seconds "$stage" $COMMON --out-dir "$dir" > "/tmp/g1_${cid}.log" 2>&1 &
    local pid=$!; local w=0; local max=1500
    while kill -0 "$pid" 2>/dev/null && [ ! -f "$MJSON" ] && [ "$w" -lt "$max" ]; do sleep 20; w=$((w+20)); done
    if [ -f "$MJSON" ]; then
      wait "$pid" 2>/dev/null
      recover_spans "$cdir"
      local vr=$(NO_PROXY='*' "$PY" scripts/chaos/ctk/verify_dual.py "$cdir" 2>&1 | grep -o "VERIFY=[A-Z]*" | head -1)
      local ic=$(NO_PROXY='*' "$PY" scripts/chaos/ctk/instance_check.py "$cdir" rec-agent 2>&1 | grep -o "OK\|MISMATCH\|FAIL" | head -1)
      local info=$("$PY" -c "import json;g=json.load(open(r'$cdir/groundtruth.json',encoding='utf-8'));print(g.get('root_cause_services'),'|',g.get('affected_services'))" 2>/dev/null)
      local sp=$(wc -l < "$cdir/raw/agent_spans/spans.jsonl" 2>/dev/null || echo 0)
      if [ "$vr" = "VERIFY=PASS" ] && [ "$ic" = "OK" ]; then
        echo "  [PASS] $cid $vr $ic | $info | spans=$sp"
        echo -e "$cid\tPASS\t${w}s\t$info\t$sp" >> "$LEDGER"; return 0
      else
        echo "  [gate-FAIL] $cid $vr $ic (try=$try) → 重跑"
      fi
    elif kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null; echo "  [TIMEOUT] $cid try=$try"
    else echo "  [ABORT] $cid try=$try"; tail -6 "/tmp/g1_${cid}.log" | cut -c1-140; fi
  done
  echo "  [GIVEUP] $cid 3 次未成"; echo -e "$cid\tGIVEUP\t-\t-\t-" >> "$LEDGER"; return 1
}

# svccpu r2-5
for i in 2 3 4 5; do one "svccpu_recagent_r$i" service_cpu_single "" 240 "_svccpu_recagent_reps_v20"; done
# netdelay r2-5 (450ms 定档)
for i in 2 3 4 5; do one "netdelay_recagent_r$i" net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120 "_netdelay_recagent_reps_v20"; done
# podfail r2-5
for i in 3 4 5; do one "podfail_recagent_r$i" pod_failure_single "" 30 "_podfail_recagent_reps_v20"; done   # r2 已补收

echo "### G1 放量完成 $(date '+%H:%M:%S')"
echo "=== 台账 ==="; cat "$LEDGER"
