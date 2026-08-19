#!/usr/bin/env bash
# =============================================================================
# collect-single.sh — 单根因(single-root)正式重采 可复现命令日志 SKELETON
# =============================================================================
# 用途 / What this is:
#   本文件是 SINGLE 一档的【可复现重采命令日志】。正式重采期间，主循环每成功采集
#   一个 case（gate PASS + CHECKSUM 净）就在文末 append 一行【完整可跑命令】。
#   采完后，本脚本即成为该档【完整、可复现的命令清单】。
#   重采动机 = 2026-07-07 labels enrichment(pod/namespace/container/node) +
#   18 字段 trace + adapter 交付对齐 → 旧数据 pre-enrichment，需带全套标签重采。
#   规模：8 single-root faults × 5 rep ≈ 40 case。
#
#   ★这是 DOCUMENTATION / LOG 骨架：下方【模板命令】给确切 flag（copy-paste 正确），
#     但逐 case 命令由采集时 append，本骨架现在不整体可跑。
# -----------------------------------------------------------------------------
# ★★ 环境铁律 (项目约定) ★★
#   conda python = python3（必须此解释器）
#   NO_PROXY='*'         — 全程绕 Clash，重启服务也要带
#   NACOS_ENABLED=false  — 否则 env 重定向被 Nacos 旁路
#   PYTHONIOENCODING=utf-8 — Windows 控制台中文/emoji 不崩
#   KUBECTL 必 export    — runner L65 os.environ.get("KUBECTL","kubectl")；bare kubectl
#                          不在 PATH → FileNotFoundError FATAL。必指到 .exe 全路径。
#   Windows C:/ / D:/ 路径；service→service URL 用 127.0.0.1 不用 localhost。
#   长 fault-injection 用 nohup 【不用 run_in_background】——后者被 harness SIGKILL
#     reaped → Python finally 不跑 → 集群留脏（本 session 血泪）。
#   CHECKSUM 铁律：items=3849590678 / inventory=3935678504，逐 case fail-closed，
#     【绝不 --skip-checksum】。db_lock：LOCK 前 pre / UNLOCK+确认释放后 post，绝不锁期间核。
# -----------------------------------------------------------------------------
# ★ 前置顺序（关键，来自 round-2 audit 修正）★
#   ★★【勿】在重采时 apply 25 manifests(kubectl apply -f k8s/pilot/) — 复用已在跑的环境。★★
#       WHY: metrics_v2 的 pod/namespace/node/container 标签来自 prom_k8s_meta kube_pod_info
#       【side-query】(runner L1814/L1876)，与 25 manifest 的 env 无关(OTel app 指标 histogram_quantile
#       /sum 已把 pod 标签聚合掉)。故 apply 对数据【零收益】，反而: 全量 rollout(含 sasrec 9.2GB pickle
#       readiness 90-120s) + 抹掉已 stage 的环境(pricing 路由 / catalog 存活 / restarter) + 新 pod churn
#       可能踩 control_plane_healthy 门。(catalog manifest 已 apply 的无害，留着即可。)
#   (a) 复用现有运行环境(全服务已带 labels enrichment; 若某服务确未起再单独 apply 该一个)。
#   (b) 只做 per-combo staged 环境（载体 restarter / stressor / env hook）。
#   (c) per-fault 前置见下每个 fault 的注释。
#   ★★全局 PREP(★本骨架=append-only 命令日志,非独立 recipe——完整见黑板
#      (project docs)/TASK-K8S-M8-overnight-recollect.md §1.5-A,必配读):
#     1) proxy8001(★Prometheus 经 host.docker.internal:8001 抓 cAdvisor+kube-state;没它→cpu门死+6标签verify全FAIL):
#        nohup "$KUBECTL" proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*' > /tmp/proxy8001.log 2>&1 & echo $! > /tmp/proxy8001.pid
#        验: curl -s "http://localhost:9090/api/v1/query?query=up" | grep -q cadvisor
#     2) business port-forward 守护 = pfwd_start.sh(★勿用 pfwd_supervisor.sh):nohup bash scripts/chaos/ctk/pfwd_start.sh > /tmp/pfwd.log 2>&1 &
#     3) catalog restarter 全程;host_cpu → runner 自管 stressor;db_lock → runner 自管 DbContentionInjector。CHECKSUM 基线核对。
# =============================================================================
# ★ 采集实施计划 v1 (2026-07-08) — 顺序/flaky（全文见黑板
#   (project docs)/TASK-K8S-M8-overnight-recollect.md §「dual/single 采集实施计划 v1」）
# -----------------------------------------------------------------------------
#   ★single 在 dual 之后【连续】跑（★上游要的交付项，采完同样打包交用户发上游，非内部）；single 不触发 user/sasrec 根（dual 独有）。
#   顺序 = S1 干净 → S2 podfail/cpu：
#     S1 (5×5): net_delay·net_loss·catalog_latency(★env-hook rolling,verify catalog pod)·
#        runtime_exception(★env-hook rolling+首采 0 独立数据)·db_lock(LOCK-pre/UNLOCK-post)
#     S2 (3×5): pod_failure(catalog in-place)·service_cpu·host_cpu(★case 间等 VM drain)
#   ★每 fault 一律 fresh 5 rep 进 _v18（单-07 首采/单-08 补 rep 是 pre-_v18 旧状态,勿减）。
#   flaky: retry ≤10/rep,弱 rep fail-closed 正确；service_cpu/host_cpu(相对 1.8x)预留多跑凑 5。
#   ★instance-label bug 已修(4e2271f+f3979ff)+ code-verified；每 case verify_dual.py 强制
#     root pod∈during_fault（MANDATORY）。single env-hook rolling 仅 single-07/08(catalog)。
# =============================================================================

export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
export MSYS_NO_PATHCONV=1
export NO_PROXY='*'
export NACOS_ENABLED=false
export PYTHONIOENCODING=utf-8
export KUBECTL='kubectl'

PY='python3'
CWD='${REPO_DIR}'
RUNNER="$CWD/scripts/chaos/ctk/chaos_k8s_runner.py"
NS=recweb-chaos
USER_TOKEN='0870e257-6cd0-4fe4-b815-0a9da6b25d41'
OUT='${REPO_DIR}/(native trees) single'   # 重采入 _v18 子目录（见模板 --out-dir）
# cd "$CWD" 后再跑（k8s/pilot yaml 路径 repo-root-relative；本骨架模板用绝对 --out-dir）

# -----------------------------------------------------------------------------
# 8 single-root faults（chaos_k8s_runner.py SINGLE_ROOT_FAULTS, L143）
# -----------------------------------------------------------------------------
SINGLE_FAULTS=(
  net_delay_single          # 单-01 NET-01  netem delay 500ms/jitter50 → pricing p95 位移
  net_loss_single           # 单-02 NET-03  netem loss 60% → pricing p95+error
  pod_failure_single        # 单-03 LIF-02  PodChaos pod-failure → pricing 404/502 error-burst
  service_cpu_single        # 单-04 RES-03  StressChaos cpu w2 (cgroup 限) → pricing 相对 p95≥1.8×
  host_cpu_single           # 单-05 RES-03(host) stressor deploy cpu w12 撑爆 VM → 多受害者
  db_lock_single            # 单-06 DEP-02(proxy) app-side LOCK TABLES items WRITE → items-reader error-burst
  runtime_exception_single  # 单-07 RUN-03  env FAULT_RAISE=1 → catalog before_request 500  ★需首采(0 独立数据)
  catalog_latency_single    # 单-08 DEP-01  env FAULT_DELAY_MS=2000 → catalog slow-200 p95 位移  ★需补 4 rep(仅 1 standalone)
)
# 采集状态：单-01..06 已有多 rep(重采带 labels+18trace)；
#   单-07 runtime_exception = ★needs-first-collect（主循环刚 live-verify PASS，从未单跑）；
#   单-08 catalog_latency  = ★needs 4 more reps（仅 catlat_single_01 一个 standalone）。

# =============================================================================
# 逐 fault 前置注释 + 模板命令
#   通用参数（照各 reps metadata.config 核实）：poll=2.0 · f2-offset=14 · f2-dur=31 ·
#   stage=30（db_lock=32）· item=0071341196 · gate=standard single_sli_gate（非 --deep）。
#   ★重采仅改 --out-dir 到 _v18 + 确保 KUBECTL export；数值 flag 保持不变。
# =============================================================================

# --- 单-01 net_delay_single ---------------------------------------------------
# 前置：pricing 载体(scale 1) + catalog restarter(5014/5005；netem 不杀 pf，但对齐通用前置)。
#       注入 = chaos-net-catalog-delay.yaml (NetworkChaos delay 500ms/jitter50)，无 --net-delay-ms CLI。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id net_delay_single_r1 --fault net_delay_single \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_net_delay_reps_v18" &

# --- 单-02 net_loss_single ----------------------------------------------------
# 前置：同 单-01 pricing 载体 + restarter。注入 = NET_LOSS_YAML(60% loss；net_loss_pct=60)。
#       ⚠ netem 丢包不进 cAdvisor packets_dropped(0.0@60%) → gate 用 app 层签名(p95+error)。
# 模板：
#   ★stage 90 不是 30!(60% 丢包近超时,stage30 during_fault 只 4 snapshot < gate 最小门≈5-6;stage90 够。2026-07-09 采集实定)
#   nohup "$PY" "$RUNNER" --case-id net_loss_single_r1 --fault net_loss_single \
#     --item 0071341196 --stage-seconds 90 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_net_loss_reps_v18" &

# --- 单-03 pod_failure_single -------------------------------------------------
# 前置：pricing 载体 + catalog restarter(pod-failure=pause 换像，catalog rollout 后 restarter 拉回 pf)。
#       注入 = PodChaos pod-failure；窗有效=restart_delta∈{1,2}。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id pod_failure_single_r1 --fault pod_failure_single \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_pod_failure_reps_v18" &

# --- 单-04 service_cpu_single -------------------------------------------------
# 前置：pricing 载体 + catalog restarter(stress rollout 杀 pf → restarter 拉回)。
#       注入 = StressChaos cpu w2/load100 (catalog cgroup 限)；相对 p95≥1.8×。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id service_cpu_single_r1 --fault service_cpu_single \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_service_cpu_reps_v18" &

# --- 单-05 host_cpu_single ----------------------------------------------------
# 前置：★stressor Deployment scale-up(无 cpu limit) + StressChaos cpu w12 溢出整 VM。
#       多载体 s1_hostcpu(pricing+user，disjoint 见共因)。走 common_cause_gate。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id host_cpu_single_r1 --fault host_cpu_single \
#     --carriers s1_hostcpu --user-token "$USER_TOKEN" \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_host_cpu_reps_v18" &

# --- 单-06 db_lock_single -----------------------------------------------------
# 前置：★app-side DbContentionInjector(LOCK TABLES items WRITE，非 Chaos Mesh) +
#       多载体 s2_dblock(catalog_direct + search items-reader + disjoint user) +
#       ★CHECKSUM 三验铁律(LOCK 前 pre / UNLOCK+确认释放后 post，绝不锁期间核)。
#       stage=32(略长容 hold 12s + gap 2s)。走 common_cause_gate_db(error-burst)。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id db_lock_single_r1 --fault db_lock_single \
#     --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 \
#     --user-token "$USER_TOKEN" --item 0071341196 --stage-seconds 32 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_db_lock_reps_v18" &

# --- 单-07 runtime_exception_single  ★needs-first-collect ---------------------
# 前置：catalog before_request env hook FAULT_RAISE=1(runner set env deploy/catalog) +
#       catalog_direct 载体直连观测原始 500(pricing 会 remap 500→404 掩码，故不用 pricing)。
#       零 DB 访问(raise 早于任何 DML) → CHECKSUM 平凡安全。走 single_sli_gate(pod_failure error-dominant 分支)。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r1 --fault runtime_exception_single \
#     --catalog-direct-base http://127.0.0.1:5005 \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_runtime_exception_reps_v18" &

# --- 单-08 catalog_latency_single  ★needs 4 more reps -------------------------
# 前置：catalog before_request env hook FAULT_DELAY_MS(runner 复用已有 hook，零源码改) +
#       catalog_direct 载体直连(GET /api/items 纯 SELECT 只读，绕 catalog-gw)观测原始延迟态。
#       慢非失败(200 OK，p95 位移~2000ms)。走 single_sli_gate(catlat 分支)。--cat-delay-ms=2000。
# 模板：
#   nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r1 --fault catalog_latency_single \
#     --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier \
#     --out-dir "$OUT/_catalog_latency_reps_v18" &

# =============================================================================
# Append-line 格式（采集时主循环每 PASS 一个 case 追加一行）：
#   <完整可跑命令>   # rep N/5 | gate PASS | checksum净 | <YYYY-MM-DDTHH:MMZ>
# 例（EXAMPLE — 采集时替换为真命令+真时戳，勿直接跑）：
# nohup "$PY" "$RUNNER" --case-id net_delay_single_r1 --fault net_delay_single --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | 2026-07-08T02:14Z   <<EXAMPLE>>
# =============================================================================

# --- verified cases (appended during collection) ---
nohup "$PY" "$RUNNER" --case-id net_delay_single_r1 --fault net_delay_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T23:21Z
nohup "$PY" "$RUNNER" --case-id net_delay_single_r2 --fault net_delay_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T23:25Z
nohup "$PY" "$RUNNER" --case-id net_delay_single_r3 --fault net_delay_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T23:29Z
nohup "$PY" "$RUNNER" --case-id net_delay_single_r4 --fault net_delay_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T23:31Z
nohup "$PY" "$RUNNER" --case-id net_delay_single_r5 --fault net_delay_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_delay_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T23:33Z
nohup "$PY" "$RUNNER" --case-id net_loss_single_r1 --fault net_loss_single  --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_loss_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T23:54Z(pre-existing)
nohup "$PY" "$RUNNER" --case-id net_loss_single_r2 --fault net_loss_single  --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_loss_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T23:59Z
nohup "$PY" "$RUNNER" --case-id net_loss_single_r3 --fault net_loss_single  --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_loss_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T00:05Z
nohup "$PY" "$RUNNER" --case-id net_loss_single_r4 --fault net_loss_single  --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_loss_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T00:10Z
nohup "$PY" "$RUNNER" --case-id net_loss_single_r5 --fault net_loss_single  --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_net_loss_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T00:15Z
nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r1 --fault catalog_latency_single --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_catalog_latency_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T00:19Z
nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r2 --fault catalog_latency_single --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_catalog_latency_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T00:23Z
nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r3 --fault catalog_latency_single --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_catalog_latency_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T00:26Z
nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r4 --fault catalog_latency_single --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_catalog_latency_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T00:30Z
nohup "$PY" "$RUNNER" --case-id catalog_latency_single_r5 --fault catalog_latency_single --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_catalog_latency_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T00:33Z
nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r1 --fault runtime_exception_single --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_runtime_exception_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T00:37Z
nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r2 --fault runtime_exception_single --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_runtime_exception_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T00:41Z
nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r3 --fault runtime_exception_single --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_runtime_exception_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T00:44Z
nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r4 --fault runtime_exception_single --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_runtime_exception_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T00:48Z
nohup "$PY" "$RUNNER" --case-id runtime_exception_single_r5 --fault runtime_exception_single --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_runtime_exception_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T00:51Z
nohup "$PY" "$RUNNER" --case-id db_lock_single_r1 --fault db_lock_single --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_db_lock_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T01:02Z
nohup "$PY" "$RUNNER" --case-id db_lock_single_r2 --fault db_lock_single --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_db_lock_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T01:04Z
nohup "$PY" "$RUNNER" --case-id db_lock_single_r3 --fault db_lock_single --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_db_lock_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T01:06Z
nohup "$PY" "$RUNNER" --case-id db_lock_single_r4 --fault db_lock_single --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_db_lock_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T01:08Z
nohup "$PY" "$RUNNER" --case-id db_lock_single_r5 --fault db_lock_single --carriers s2_dblock --catalog-direct-base http://127.0.0.1:5005 --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_db_lock_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T01:10Z
nohup "$PY" "$RUNNER" --case-id pod_failure_single_r1 --fault pod_failure_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_pod_failure_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T01:13Z
nohup "$PY" "$RUNNER" --case-id pod_failure_single_r2 --fault pod_failure_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_pod_failure_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T01:16Z
nohup "$PY" "$RUNNER" --case-id pod_failure_single_r3 --fault pod_failure_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_pod_failure_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T01:19Z
nohup "$PY" "$RUNNER" --case-id pod_failure_single_r4 --fault pod_failure_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_pod_failure_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T01:22Z
nohup "$PY" "$RUNNER" --case-id pod_failure_single_r5 --fault pod_failure_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_pod_failure_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 采 2026-07-09T01:31Z → ★重采 2026-07-09T04:54Z(原 rep pre_fault 基线被上一 rep pod-kill 污染: pre error_ratio=1.0==during; 净基线重采后 pre=0.0/during=1.0)
nohup "$PY" "$RUNNER" --case-id service_cpu_single_r1 --fault service_cpu_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_service_cpu_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T01:33Z
nohup "$PY" "$RUNNER" --case-id service_cpu_single_r2 --fault service_cpu_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_service_cpu_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T01:35Z
nohup "$PY" "$RUNNER" --case-id service_cpu_single_r3 --fault service_cpu_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_service_cpu_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T01:36Z
nohup "$PY" "$RUNNER" --case-id service_cpu_single_r4 --fault service_cpu_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_service_cpu_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T01:38Z
nohup "$PY" "$RUNNER" --case-id service_cpu_single_r5 --fault service_cpu_single  --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_service_cpu_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T01:40Z
nohup "$PY" "$RUNNER" --case-id host_cpu_single_r1 --fault host_cpu_single --carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_host_cpu_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-09T01:42Z
nohup "$PY" "$RUNNER" --case-id host_cpu_single_r2 --fault host_cpu_single --carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_host_cpu_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-09T01:46Z
nohup "$PY" "$RUNNER" --case-id host_cpu_single_r3 --fault host_cpu_single --carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_host_cpu_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-09T01:49Z
nohup "$PY" "$RUNNER" --case-id host_cpu_single_r4 --fault host_cpu_single --carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_host_cpu_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-09T01:52Z
nohup "$PY" "$RUNNER" --case-id host_cpu_single_r5 --fault host_cpu_single --carriers s1_hostcpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_host_cpu_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-09T01:56Z

# =============================================================================
# ★★ PROVENANCE (2026-07-09 通宵重采 → single 40/40 全采完)
# =============================================================================
#   - 全 8 fault × 5 rep = 40 case,2026-07-09 fresh 采完(全 _v18,pre-_v18 旧状态弃)。
#   - runner = 【instance-fixed】(4e2271f + f3979ff);验证器 = 【off-graph-fixed verify_dual】。
#   - 每 case 验: verify_dual.py PASS(3-part + root∈during_fault;off-graph 纯根 db_lock/host_cpu 无 pod,rci 非空即可)。全程 CHECKSUM 基线零漂移。
#   ★★复现须知(否则采不出 / 误判):
#     ① ★net_loss_single 用 **stage 90 不是 30**(60% 丢包近超时,stage30 during_fault 只 4 snapshot < gate 最小门≈5-6;stage90 够)。append 行已记 --stage-seconds 90,照 append 跑即对。
#     ② db_lock_single(根=mysql:items)/ host_cpu_single(根=host)是 **off-graph 纯根**,须用 off-graph-fixed verify_dual(否则误判 'no real root pods';fix 见 verify_dual.py,commit 4069bc0)。
#     ③ 环境前置见头部;db_lock LOCK-pre/UNLOCK-post checksum 铁律;host_cpu case 间等 VM drain。
