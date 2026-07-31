#!/usr/bin/env bash
# collect-single-recagent.sh — ★G1 rec-agent 传统故障补采(4 fault × 5 reps ≈ 20 case)可复现命令日志
# 任务书: (project docs)/archive/TASK-K8S-G1-recagent-traditional.md(设计定案+改动清单)
# 骨架照 collect-single-spread.sh / collect-net-spread.sh;本文件初始只放 rep1 当 smoke 行,
# rep2-5 注释占位 —— ★smoke 三核(注入生效/agent_spans 回收/成本)全过后再放开。
#
# ============================== §前置(PREP) ==============================
# 0. 环境照 (project docs)/TASK-K8S-M8-overnight-recollect.md §1.5-A:
#    conda env recweb2 · kubectl proxy 8001 在跑(panel+cadvisor+kube-state 三路单点) ·
#    Docker OTel 栈 · 25 服务全 Ready(含 rec-agent) · Chaos Mesh 就绪 · CHECKSUM 基线核过 ·
#    pfwd 守护(pfwd_start.sh 等)按 runbook。
# 1. ★G1 变体镜像补丁(本任务新增;必须 PowerShell —— Git Bash 的 MSYS 会把 `=/路径` 参数改写成
#    Windows 路径灌进 Linux 容器,spike 2026-07-19 实证踩坑):
#      powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_observe.ps1
#    (切 recweb-rec-agent:agentfault 变体镜像 + AGENTFAULT_INSTRUMENT=1 + AGENTFAULT_OBSERVE=1
#     [observe-only,不注任何故障 env] + SPAN_FILE/AGENTFAULT_LEDGER 落 emptyDir 卷 + 引用
#     deepseek-env secret[spike 遗产,已在 ns]。镜像若未 build 先按 Dockerfile.agentfault 头注 build。)
# 2. 每 case 结束后回收 agent 层轨迹(轨迹事后补不回 —— 上游 07-10 教训):
#    ★★必须带 MSYS2_ARG_CONV_EXCL='*' —— 否则 Git Bash 的 MSYS 会把 /agentfault-data/... 改写成
#      C:/Program Files/Git/agentfault-data/... , cat 必失败(2026-07-22 实证: 静默丢掉 10 个 case 的轨迹):
#        kubectl wait --for=condition=Ready pod -l app=recommendation_agent -n recweb-chaos --timeout=150s
#        MSYS2_ARG_CONV_EXCL='*' kubectl exec -n recweb-chaos deploy/rec-agent -- \
#          cat /agentfault-data/spans.jsonl > <case_dir>/raw/agent_spans/spans.jsonl
#    ★拷完【先核非空再 truncate】: 拷贝失败却截断 = 数据永久丢失, 而 runner 仍报 VERIFY=PASS
#      (VERIFY 只管 metrics/GT, 不管副产物)。成熟实现见 scripts/chaos/ctk/g1_bulk_collect.sh 的 recover_spans()。
#    ⚠ pod_failure 案: 容器被杀, 采完立刻 exec 会 container not found → 必须先 wait Ready(已含在上)。
# 3. 采完全部 case 后还原现场:
#      powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1
#
# ============================== §参数口径(保守初值,smoke 后调) ==============================
# - recommend 业务探针 ~50s/发(4-agent+DeepSeek) → 每 stage 样本数 ≈ stage_seconds/50。
#   svccpu 给 240s/stage(每 stage ~4-5 个 recommend 样本 + health 载体 ~120 个 gate 样本);
#   podfail 给 30s/stage(照 single-spread 先例;rec-agent livenessProbe period20s×fail3=60s,
#     during 窗拉长会触发 kubelet 额外重启破 restart_delta∈{1,2} 门 → 先保守 30s,smoke 看 churn);
#   netdelay 给 120s/stage;netloss 给 90s/stage(照 net-spread 先例,TCP 重传拖慢探测)。
# - ★netem 强度:net-spread 用的 80ms 是打在【DB 型服务】上(每请求多次 MySQL RTT 累积过 800ms 门);
#   rec-agent 的 gate 载体 /recommend/health 是轻端点(无 DB egress,仅响应包过 netem)→ 80ms 预计
#   凑不齐 single_sli_gate 的【绝对 p95 位移>=800ms】→ 实测 300ms 位移仅 622ms 未过, **定档 450ms/抖动 90ms**(位移 1029ms)。
#   net_loss 已砍(见下方 ③): 与 median 位移门本质不兼容。
# - ★netem 注入面在 rec-agent pod 上于 2026-07-22 **首验通过**(tc qdisc 挂上 + 门信号生效)。
# - recommend 探针经 kubectl proxy 8001;⚠ apiserver service-proxy 对 ~60s 长请求可能有上限(未验证):
#   smoke 若见 recommend 恒 error/连接被掐 → 起 rec-agent port-forward 并给每行加
#   --recagent-recommend-url http://127.0.0.1:5001/recommend。
# - 成本:~(3 stage × stage_seconds/50) 次 DeepSeek 驱动/case;svccpu 240s ≈ ~14 发/case。
#   用户拍板"烧钱不设限",但 smoke 后先估一发单 case 成本再放量(G1 黑板执行顺序)。
#
# 用法(串行亲驱,主循环 Monitor 盯;长采集不 run_in_background 撒手):
#   nohup bash scripts/chaos/ctk/collect-single-recagent.sh > /tmp/g1_recagent.log 2>&1 &
set -uo pipefail
export NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"   # kubectl.exe 在此(git-bash 默认看不见)
export KUBECTL="kubectl"
PY="python3"
RUNNER="scripts/chaos/ctk/chaos_k8s_runner.py"
OUT="${REPO_DIR}/(native trees) single_recagent"
# ★防泄漏:item_sequence 全 case/全 stage 恒同(= runner 默认值 = carrier_pool.json seq_id 0,真标题/词表内)
SEQ="B000PGJ7SA,B000HKMM4A,B00F0RD86G,B01C2O7YNC"
COMMON="--item 0071341196 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --wide-metrics --recagent-seq $SEQ --recagent-top-k 5 --recagent-recommend-timeout 150"

run() {  # cid fault extra stage
  local cid=$1 fault=$2 extra=$3 stage=$4
  echo "############################################################"
  echo "### $(date '+%H:%M:%S')  $cid  ($fault @ rec-agent)"
  echo "############################################################"
  "$PY" "$RUNNER" --case-id "$cid" --fault "$fault" --target-service rec-agent \
    $extra --stage-seconds "$stage" $COMMON --out-dir "$OUT/_${fault_dir}_recagent_reps_v20"
  echo "### $cid  exit=$?  @ $(date '+%H:%M:%S')"
  echo
}

# ---------- ① service_cpu smoke(G1 执行顺序第一发:核注入生效/对比度 + agent_spans 回收 + 成本估) ----------
fault_dir=svccpu
run svccpu_recagent_r1 service_cpu_single "" 240
# ★smoke 核验点:gate=recagent_health p95 ratio>=1.8(RES 慢非失败) · cfs_throttle 旁证指向 rec-agent pod ·
#   traffic_stats_by_carrier.recagent_recommend 有样本且 during 时延可读(DeepSeek 方差 vs CPU 信号=最大未知数) ·
#   /agentfault-data/spans.jsonl 非空可回收 · 单 case DeepSeek 花费可接受 → 全过再放开 rep2-5。
run svccpu_recagent_r2 service_cpu_single "" 240
run svccpu_recagent_r3 service_cpu_single "" 240
run svccpu_recagent_r4 service_cpu_single "" 240
run svccpu_recagent_r5 service_cpu_single "" 240

# ---------- ② netem delay smoke(注入面未验证:先核 tc qdisc 挂上 + p95 位移>=800ms 门过) ----------
fault_dir=netdelay
run netdelay_recagent_r1 net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120
run netdelay_recagent_r2 net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120
run netdelay_recagent_r3 net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120
run netdelay_recagent_r4 net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120
run netdelay_recagent_r5 net_delay_single "--net-delay-ms 450 --net-jitter-ms 90" 120

# ---------- ③ netem loss —— 【2026-07-22 smoke 砍掉,不采】----------
# 实测 net_loss_single @10% carrier_med=24.5ms(基线水平)未过 isolation_gate target_hit(位移>=800ms 绝对门)。
# 根因本质不兼容:isolation_gate 只有 median 位移门(无 loss 特判); 丢包不产生延迟位移(成功请求延迟不变、
# 只是失败率上升), health 单跳快端点上 median 要到 >50% 丢包才动 —— 而 >50% 丢包语义已与 service_unavailable
# 重叠(不再是干净的 network_loss)。→ 照黑板风险预案砍 loss。net 类保留 delay 一种代表网络模态足够
# (single_spread 先例也只采 podfail+svccpu 两类)。故 G1 = 3 类 × 5 = 15 case。
# fault_dir=netloss
# run netloss_recagent_r1 net_loss_single "--net-loss-pct 10" 90   # 砍

# ---------- ④ pod_failure(放最后:pause-swap 期间 exec 不可用,台账回收时序最麻烦;
#             ⚠ during 窗勿拉长 —— rec-agent liveness 60s 剔除窗,restart_delta 门要 ∈{1,2}) ----------
fault_dir=podfail
run podfail_recagent_r1 pod_failure_single "" 30
# ★smoke 核验点:gate error_ratio>=0.8 · restart_delta∈{1,2}(churn_pods 空) · 恢复后 agent_spans 仍可回收
#   (pod in-place 重启 emptyDir 不丢;若见 pod 重建则该 case 台账已丢,如实标注勿补采顶替)。
run podfail_recagent_r2 pod_failure_single "" 30
run podfail_recagent_r3 pod_failure_single "" 30
run podfail_recagent_r4 pod_failure_single "" 30
run podfail_recagent_r5 pod_failure_single "" 30

# ============================== PROVENANCE(采完逐项填实) ==============================
# collector        : (填:主循环会话/日期)
# runner commit    : (填:git rev-parse HEAD @ 采集时)
# 变体镜像          : recweb-rec-agent:agentfault(Dockerfile.agentfault;build 日期/镜像 ID 填此)
# observe-only env : AGENTFAULT_INSTRUMENT=1 + AGENTFAULT_OBSERVE=1(零注入;install_observer 强制非流式 ——
#                    与 v2 数据集 faulted 采集同口径,provenance 如实记"批内统一非流式")
# 模型口径          : **deepseek-chat**(参数名)。官方新名 deepseek-v4-flash 是同一模型, 但该参数名默认开
#                    thinking 且【拒绝 tool_choice】(400 Thinking mode does not support this tool_choice),
#                    agent 流水线 Synthesizer 强制 tool_call 必炸 → 集群 secret 与 .env 均用 deepseek-chat。
# GT 口径          : pod_failure_single 的 fault_type 按 per_service_canon 出 service_unavailable
#                    (勿按目录名当类型 —— G2 教训);netem retarget 语义 = rec-agent pod 整个 egress
#                    (节点级,含对 sasrec/DeepSeek 的出向包),scope=rec_agent_pod_egress 如实入 GT。
# 面板口径          : ★本树起 probe-panel 为 12 目标(11 DEEP + rec-agent /recommend/health,G1 变更);
#                    与历史 140/195-case 树(11 目标)features 列集不同,不跨树拼视图。
# gate 口径         : 判据仅用 recagent_health 载体(single_sli_gate 按 carrier_name 过滤);
#                    recagent_recommend POST 为业务证据通道不进判据(metadata.config 有溯源键)。
# netem 验证声明    : rec-agent pod netem 注入面于 2026-07-22 首验通过。300ms 时 carrier 位移仅 622ms
#                    未过 isolation_gate 绝对门(>=800ms) → **定档 450ms/抖动 90ms**(实测位移 1029ms,
#                    target_hit=True carrier_med 908-946ms vs thr 824ms)。
# net_loss 砍除说明  : @10% carrier_med=24.5ms 未过门。isolation_gate 只有 median 位移门(无 loss 特判),
#                    丢包不动 median(除非 >50% → 语义并入 service_unavailable) → 与 health 轻端点本质不兼容,
#                    照黑板风险预案砍除。net 类留 delay 一种代表(single_spread 先例亦只 2 类)。
# 复现须知          : 前置 §PREP 全做;强度/时长若 smoke 后调整,以本文件放量行的最终参数为准。

# ============================================================================
# 实际执行结果(2026-07-22 收工) —— 本节为事后如实回填
# ----------------------------------------------------------------------------
# 15/15 全 PASS(三档各 5 reps):
#   service_cpu_saturation 5 | network_delay(450ms) 5 | service_unavailable(pod_failure_single) 5
#   每 case 三项核: VERIFY=PASS(3-part + 根实例 ∈ during_fault) · instance_check OK ·
#   GT root/affected 均 ['rec-agent'] · agent_spans 非空(svccpu 3459-4001 / netdelay 1662-4227 /
#   podfail 321-1259; podfail 数小属正常: 30s 档 + pod 被杀, 可采调用本就少)。
#   逐 case 台账: (native trees) single_recagent/g1_bulk_ledger.tsv
#   放量编排器(带重试/回收/台账): scripts/chaos/ctk/g1_bulk_collect.sh
#
# ★采集期间修掉的 2 个 runner bug(都靠 smoke 抓到, 直接放量会整树污染):
#   1. --wide-metrics 的【替换语义】把 svc_pods 换成 DEEP_TOPOLOGY 12 服务固定集(不含 rec-agent)
#      → cadvisor/kube-state/OTel 无 rec-agent 序列 → 根 pod 不在 during 遥测 → verify FAIL。
#      修 commit a847607(is_recagent 时把 rec-agent 加回采集拓扑)。
#   2. write_case 的 isolation 降级逻辑(catalog-gw netem 专用探针)对 rec-agent 恒 False,
#      误把 GT.affected_services 写成全体 catalog callers。修 commit f0438f1(rec-agent 叶子服务,
#      affected 恒 = 自身)。
#
# ★采集脚本自身的 1 个坑(教训, 写进 playbook):
#   agent_spans 回收用 kubectl exec ... cat /agentfault-data/spans.jsonl, 在 Git Bash 下被 MSYS
#   改写成 C:/Program Files/Git/agentfault-data/... → cat 必失败; 而配对的 truncate 用 sh -c '...'
#   单引号包路径【不被改写】、执行成功 → 拷贝失败但源文件被清空 = 数据永久丢失, 且 runner 仍报 PASS
#   (VERIFY 只管 metrics/GT, 不管副产物)。修法三件: MSYS2_ARG_CONV_EXCL='*' + 不吞 stderr +
#   失败绝不 truncate; 另加等容器 Ready + 重试(pod_failure 会杀容器, 采完立刻 exec 会 container not found)。
# ============================================================================
