#!/usr/bin/env bash
# =============================================================================
# collect-triple.sh — 三根因(triple-root)正式重采 可复现命令日志 SKELETON
# =============================================================================
# 用途 / What this is:
#   本文件是 TRIPLE 一档的【可复现重采命令日志】。正式重采期间，主循环每成功采集
#   一个 case（gate PASS + CHECKSUM 净 + 18 字段 trace）就在文末 append 一行完整可跑命令。
#   采完后，本脚本即成为该档【完整、可复现的命令清单】。
#   重采动机 = 2026-07-07 labels enrichment + 18 字段 trace(commit 1767472) + adapter 交付对齐。
#   规模：4 可建 triple combo × 5 rep = 20 case（三-01/T1/T3/T4，全 reps 已完结待 18 字段重采）。
#
#   ★参数权威 = (project docs)/archive/roadmap-multi-root/TRIPLE-DELIVERY-PLAN-2026-07-07.md §3 Phase 5（v4）+ 各 reps metadata.config /
#     injection.json 反算。★这是 DOCUMENTATION / LOG 骨架：模板命令给确切 flag，逐 case 采集时 append。
# -----------------------------------------------------------------------------
# ★★ 环境铁律 (项目约定) ★★
#   conda python = python3
#   NO_PROXY='*' · NACOS_ENABLED=false · PYTHONIOENCODING=utf-8
#   ★KUBECTL 必 export（runner L65 os.environ.get("KUBECTL","kubectl")；bare kubectl
#     不在 PATH → FileNotFoundError FATAL）。
#   Windows C:/ D:/ 路径；service→service URL 用 127.0.0.1。
#   ★长 rep 用 nohup【不用 run_in_background】——T4 ~14min > 10min 前台窗，
#     run_in_background 被 harness SIGKILL reaped → Python finally 不跑 → 集群留脏(本 session 血泪)。
#   CHECKSUM 铁律：items=3849590678 / inventory=3935678504，逐 case fail-closed，绝不 --skip-checksum。
# -----------------------------------------------------------------------------
# ★ 前置顺序（关键，来自 round-2 audit 修正 + plan Phase 5）★
#   ★★【勿】重采时 apply 25 manifests(kubectl apply -f k8s/pilot/) — 复用已在跑的环境。★★
#       WHY: metrics_v2 的 pod/namespace/node/container 标签来自 prom_k8s_meta kube_pod_info side-query
#       (runner L1814/L1876)，与 25 manifest env 无关(OTel app 指标 sum/histogram_quantile 已聚合掉 pod 标签)。
#       apply 对数据零收益，反而 = 全量 rollout(含 sasrec 9.2GB 90-120s)+抹已 stage 环境+新 pod churn 踩 cp_healthy 门。
#       (catalog manifest 已 apply 的无害，留着。)
#   (a) 复用现有运行环境(已带 labels enrichment)。
#   (b) 只做 per-combo staged 环境(pricing scale+route / T4 catalog liveness[runner 自管] / restarter)。
#   (c) per-combo 前置见下每组注释。
#   全局：proxy8001(--address=0.0.0.0 LAN 暴露，采完 kill) + catalog/pricing restarter
#         (业务 pf 5004/5005/5014) + CHECKSUM 基线。
#   一次环境采完 4 combo × 5 rep = 20 rep 全 18 字段，按 combo 切(env per-combo 前置)。
#   每 rep 验：gate PASS + CHECKSUM 净 + 18 字段 trace
#     (process_id / process_tags[真 telemetry.sdk.*] / references[flat=[]] / collector_query_service present)。
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
UT='0870e257-6cd0-4fe4-b815-0a9da6b25d41'    # --user-token
CDB='http://127.0.0.1:5005'                  # --catalog-direct-base

# -----------------------------------------------------------------------------
# 4 triple combos（chaos_k8s_runner.py TRIPLE_ROOT_FAULTS, L156）
# -----------------------------------------------------------------------------
TRIPLE_FAULTS=(
  # 我方号 | fault (--fault)                                | 上游号 | stage | ~min/rep
  "三-01 | pricing_cpu_x_catalog_latency_x_cfg_timeout      | 三-07 | 120 | 7.5"   # RES pricing-cpu ∥ DEP catlat + CFG read_timeout；首个真空间可分
  "T1    | inv_latency_x_cfg_timeout_x_retry                | 三-01 | 120 | 7.5"   # DEP inv-latency + CFG timeout + CFG retry×2；mixed_class 无 resource 根
  "T3    | net_delay_x_net_loss_x_db_lock                   | 三-03 | 150 | 8"     # NET delay + NET loss(30%) + DEP db_lock；首个真 9-路 gate
  "T4    | pod_failure_x_catalog_latency_x_cfg_timeout      | 三-04 | 250 | 14"    # DEP catlat + CFG timeout + LIF pod_failure；最难 rollout barrier
)
# 全 4 combo reps 已完结(三-01/T1/T3/T4 各 5/5/5/4 PASS)；本次 = 统一重采带 18 字段 trace。
# 全 20 rep + env cycling ≈ ~3.5h（宜专做，主循环 nohup 亲驱）。

# =============================================================================
# 逐 combo 前置 + 模板命令（★权威 = plan v4 §3 Phase 5；数值 flag 逐字保留）
#   逐 rep 改 --case-id + --out-dir(各 _v18)；5 rep/combo。
# =============================================================================

# --- 三-01 pricing_cpu_x_catalog_latency_x_cfg_timeout (stage120,~7.5min/rep) -
# 前置：pricing scale1 + pricing 路由 catalog-gw + catalog/pricing restarter + chaos-stress-pricing-cpu.yaml。
#       catalog liveness 保持 IN。RES 从 catalog CPU 挪到 pricing pod CPU → 空间可分(2 不同 cgroup)。
#   nohup "$PY" "$RUNNER" --case-id triple01_r1 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep \
#     --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" \
#     --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 \
#     --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier \
#     --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &

# --- T1 inv_latency_x_cfg_timeout_x_retry (stage120,~7.5min/rep) -------------
# 前置：+ inventory restarter(5013) + runner 自 apply_catalog_bad(11b-catalog-bad)。
#       无 --catalog-direct-base；catalog liveness IN。inv env FAULT_DELAY_MS via --inv-delay-ms 2000。
#   nohup "$PY" "$RUNNER" --case-id t1_r1 --fault inv_latency_x_cfg_timeout_x_retry --deep \
#     --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 \
#     --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier \
#     --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &

# --- T3 net_delay_x_net_loss_x_db_lock (stage150,~8min/rep) ------------------
# 前置：+ s2_dblock 载体 + chaos-net-catalog-loss-t3.yaml(loss 30%)。catalog liveness IN。
#       f3=db_lock mid_action。
#   ★ db_lock CHECKSUM 铁律(同 single/dual 的 db_lock 案)：f3=LOCK TABLES items WRITE。
#     checksum_pre 在 LOCK 前 / checksum_post 在 UNLOCK+确认释放后(recover_db_lock 阻塞 confirm)，
#     绝不在锁期间核 checksum(否则读被阻塞 false-fail)。gate 已内建 checksum_zero_drift fail-closed。
#   ★ --f3-offset-seconds 50 / --f3-duration-seconds 30 = VERBATIM，NOT in metadata.config
#     (照 injection.json 反算；勿用 runner default f3-offset=24/f3-duration=12)。
#   ★ F-A 修：【不传】--net-loss-yaml —— 默认 --loss-pct 30 已让 runner 用绝对路径 NET_LOSS_T3_YAML
#     (runner L167 + L12537-40 解析)；传 cwd-相对 k8s/pilot/... 反而是唯一 cwd 依赖坑(对齐 runbook §3 fix#2)。
#   nohup "$PY" "$RUNNER" --case-id t3_r1 --fault net_delay_x_net_loss_x_db_lock --deep \
#     --carriers s2_dblock_combo --catalog-direct-base "$CDB" \
#     --user-token "$UT" --item 0071341196 \
#     --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 \
#     --f3-offset-seconds 50 --f3-duration-seconds 30 \
#     --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &

# --- T4 pod_failure_x_catalog_latency_x_cfg_timeout (stage250,~14min/rep) ----
# 前置：★catalog livenessProbe 现由 RUNNER 自管(is_t4+deep inject 前 patch-OUT / finally patch-IN 还原,
#       CATALOG_LIVENESS_PROBE 单一真相源) → 【无需手工 kubectl patch】。pod-down ~70s>60s liveness-kill
#       否则 kubelet 抢重启 PodChaos。cat env FAULT_DELAY_MS via --cat-delay-ms 2000。
#   ★ --cfg-carve-seconds 20 / --f3-dwell-seconds 60 = VERBATIM 校准值，NOT in metadata.config
#     (本 session 验；勿用 runner default cfg-carve=45/f3-dwell=40)。
#   nohup "$PY" "$RUNNER" --case-id t4_r1 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep \
#     --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" \
#     --user-token "$UT" --item 0071341196 \
#     --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 \
#     --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier \
#     --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &
#   ★T4 catalog liveness 由 runner 自管(patch-OUT@inject / restore@finally) — 无需手工前后 patch。

# =============================================================================
# Append-line 格式（采集时主循环每 PASS 一个 case 追加一行）：
#   <完整可跑命令>   # rep N/5 | gate PASS | checksum净 | <YYYY-MM-DDTHH:MMZ>
# 例（EXAMPLE — 采集时替换为真命令+真时戳，勿直接跑）：
# nohup "$PY" "$RUNNER" --case-id triple01_r1 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | 2026-07-08T05:12Z   <<EXAMPLE>>
# =============================================================================

# --- verified cases (appended during collection) ---
nohup "$PY" "$RUNNER" --case-id triple01_r1 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | 2026-07-07T19:18Z
nohup "$PY" "$RUNNER" --case-id triple01_r2 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 2/5 | gate PASS | checksum净 | 2026-07-07T19:29Z
nohup "$PY" "$RUNNER" --case-id triple01_r3 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 3/5 | gate PASS | checksum净 | 2026-07-07T19:38Z
nohup "$PY" "$RUNNER" --case-id triple01_r4 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 4/5 | gate PASS | checksum净 | 2026-07-07T19:47Z
nohup "$PY" "$RUNNER" --case-id triple01_r5 --fault pricing_cpu_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple01_pricing_cat_cfg --catalog-direct-base "$CDB" --user-token "$UT" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _triple01_reps_v18" &   # rep 5/5 | gate PASS | checksum净 | 2026-07-07T19:56Z
nohup "$PY" "$RUNNER" --case-id t1_r1 --fault inv_latency_x_cfg_timeout_x_retry --deep --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | 2026-07-07T20:06Z
nohup "$PY" "$RUNNER" --case-id t1_r2 --fault inv_latency_x_cfg_timeout_x_retry --deep --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &   # rep 2/5 | gate PASS | checksum净 | 2026-07-07T20:15Z
nohup "$PY" "$RUNNER" --case-id t1_r3 --fault inv_latency_x_cfg_timeout_x_retry --deep --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &   # rep 3/5 | gate PASS | checksum净 | 2026-07-07T20:24Z
nohup "$PY" "$RUNNER" --case-id t1_r4 --fault inv_latency_x_cfg_timeout_x_retry --deep --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &   # rep 4/5 | gate PASS | checksum净 | 2026-07-07T20:34Z
nohup "$PY" "$RUNNER" --case-id t1_r5 --fault inv_latency_x_cfg_timeout_x_retry --deep --carriers s_t1_inv_cfg_retry --user-token "$UT" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 120 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 40 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t1_reps_v18" &   # rep 5/5 | gate PASS | checksum净 | 2026-07-07T20:43Z
nohup "$PY" "$RUNNER" --case-id t3_r1 --fault net_delay_x_net_loss_x_db_lock --deep --carriers s2_dblock_combo --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &   # rep 1/5 | gate PASS | checksum净(db_lock post==base) | 2026-07-07T20:53Z
nohup "$PY" "$RUNNER" --case-id t3_r2 --fault net_delay_x_net_loss_x_db_lock --deep --carriers s2_dblock_combo --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &   # rep 2/5 | gate PASS | checksum净 | 2026-07-07T21:02Z
nohup "$PY" "$RUNNER" --case-id t3_r3 --fault net_delay_x_net_loss_x_db_lock --deep --carriers s2_dblock_combo --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &   # rep 3/5 | gate PASS | checksum净 | 2026-07-07T21:10Z
nohup "$PY" "$RUNNER" --case-id t3_r4 --fault net_delay_x_net_loss_x_db_lock --deep --carriers s2_dblock_combo --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &   # rep 4/5 | gate PASS | checksum净 | 2026-07-07T21:19Z
nohup "$PY" "$RUNNER" --case-id t3_r5 --fault net_delay_x_net_loss_x_db_lock --deep --carriers s2_dblock_combo --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --stage-seconds 150 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 90 --f3-offset-seconds 50 --f3-duration-seconds 30 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t3_reps_v18" &   # rep 5/5 | gate PASS | checksum净 | 2026-07-07T21:28Z
nohup "$PY" "$RUNNER" --case-id t4_r1 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | liveness restored | 2026-07-07T21:37Z
nohup "$PY" "$RUNNER" --case-id t4_r2 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &   # rep 2/5 | gate PASS | checksum净 | liveness restored | 2026-07-07T21:53Z
nohup "$PY" "$RUNNER" --case-id t4_r3 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &   # rep 3/5 | gate PASS | checksum净 | liveness restored | 2026-07-07T22:09Z
nohup "$PY" "$RUNNER" --case-id t4_r4 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &   # rep 4/5 | gate PASS | checksum净 | liveness restored | 2026-07-07T22:24Z
nohup "$PY" "$RUNNER" --case-id t4_r5 --fault pod_failure_x_catalog_latency_x_cfg_timeout --deep --carriers s_triple_lif_dep_cfg --catalog-direct-base "$CDB" --user-token "$UT" --item 0071341196 --cat-delay-ms 2000 --poll 2.0 --stage-seconds 250 --f2-offset-seconds 25 --cfg-carve-seconds 20 --f3-dwell-seconds 60 --keep-carrier --out-dir "${REPO_DIR}/(native trees) _t4_reps_v18" &   # rep 5/5 | gate PASS | checksum净 | liveness restored | 2026-07-07T22:40Z

# =============================================================================
# ★PROVENANCE (2026-07-08 instance-label fix + 重采):
#   上方 20 行命令是权威可复现日志。命令行本身【不变】—— 但正确性依赖 runner 版本:
#   - T3 r1-r5(5 case): 昨晚原采,instance-label bug 不影响(net/off-graph 根不 rollout),不重采。
#   - 三-01/T1/T4 r1-r5(15 case): 昨晚原采【中 instance-label bug】(root_cause_instances 钉 post-recovery pod);
#     用【修复后 runner】(_pod_from_during_fault/_root_cause_pod;见 chaos_k8s_runner.py + 本次 commit)
#     以【同样命令】重采 → root instance 现 ∈ during_fault 窗。每 case 验: verify_dual.py(3-part + 所有 root pod∈during_fault) +
#     instance_check.py(pinned pod ∈ during_fault)双 PASS。〔注: verify_dual.py 是本仓实存验证器; 早期草名 verify_case.py 已不存在,勿引。〕
#   ★复现须知: 跑这些命令前确保 runner 含 instance-label fix,否则 instance 标签会退回 post-recovery bug。
# =============================================================================
