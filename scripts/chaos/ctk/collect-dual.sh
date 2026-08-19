#!/usr/bin/env bash
# =============================================================================
# collect-dual.sh — 双根因(dual-root)正式重采 可复现命令日志 SKELETON
# =============================================================================
# 用途 / What this is:
#   本文件是 DUAL 一档的【可复现重采命令日志】。正式重采期间，主循环每成功采集
#   一个 case（gate PASS + CHECKSUM 净）就在文末 append 一行【完整可跑命令】。
#   采完后，本脚本即成为该档【完整、可复现的命令清单】。
#   重采动机 = 2026-07-07 labels enrichment + 18 字段 trace + adapter 交付对齐。
#   规模：16 dual combo × 5 rep = 80 case（M7 `dual-collect-v1` 终态即此 16 组）。
#
#   ★参数权威 = 各 combo 已采 reps 的 metadata.config +
#     (native trees) dual_v2/SUMMARY.md「16 combo × stage × --deep × carrier」as-collected 表。
#     ★这是 DOCUMENTATION / LOG 骨架：模板命令给确切 flag，逐 case 命令采集时 append。
# -----------------------------------------------------------------------------
# ★★ 环境铁律 (项目约定) ★★
#   conda python = python3
#   NO_PROXY='*' · NACOS_ENABLED=false · PYTHONIOENCODING=utf-8
#   KUBECTL 必 export（runner L65 默认 bare kubectl 不在 PATH → FATAL）
#   Windows C:/ D:/ 路径；service→service URL 用 127.0.0.1。
#   长 fault-injection 用 nohup【不用 run_in_background】(被 SIGKILL reaped → 集群留脏)。
#   CHECKSUM 铁律：items=3849590678 / inventory=3935678504，逐 case fail-closed，
#     绝不 --skip-checksum。双-10 db_lock：LOCK 前 pre / UNLOCK+确认释放后 post。
# -----------------------------------------------------------------------------
# ★ 前置顺序（关键，来自 round-2 audit 修正）★
#   ★★【勿】重采时 apply 25 manifests(kubectl apply -f k8s/pilot/) — 复用已在跑的环境。★★
#       WHY: metrics_v2 的 pod/namespace/node/container 标签来自 prom_k8s_meta kube_pod_info side-query
#       (runner L1814/L1876)，与 25 manifest env 无关(OTel app 指标 sum/histogram_quantile 已聚合掉 pod 标签)。
#       apply 对数据零收益，反而 = 全量 rollout(含 sasrec 9.2GB 90-120s)+抹已 stage 环境+新 pod churn 踩 cp_healthy 门。
#       (catalog manifest 已 apply 的无害，留着。)
#   (a) 复用现有运行环境(已带 labels enrichment)。
#   (b) 只做 per-combo staged 环境。
#   (c) per-combo 前置见下每组注释。
#   ★★全局 PREP(采集前一次性,照序;★本骨架=append-only 命令日志,非独立 recipe——完整含重试/清脏/切combo纪律见黑板
#      (project docs)/TASK-K8S-M8-overnight-recollect.md §1.5-A,必配读):
#     1) proxy8001(★Prometheus 经 host.docker.internal:8001 抓 cAdvisor+kube-state;没它→cpu门死+6标签verify全FAIL):
#        nohup "$KUBECTL" proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*' > /tmp/proxy8001.log 2>&1 & echo $! > /tmp/proxy8001.pid
#        验: curl -s "http://localhost:9090/api/v1/query?query=up" | grep -q cadvisor
#     2) business port-forward 守护 = pfwd_start.sh(其 watchdog 排除 5013,交 inventory restarter;★勿用 pfwd_supervisor.sh):
#        nohup bash scripts/chaos/ctk/pfwd_start.sh > /tmp/pfwd.log 2>&1 &
#     3) 载体 restarter: catalog 全程(pfwd_catalog_restarter.sh);dual07/11 加 inventory restarter;dual08 加 user restarter(见各 combo 前置)。
#     4) CHECKSUM 基线核对(items=3849590678/inventory=3935678504)。
#   ⚠ 双-07/11 inventory 载体：inventory pod 在 FAULT_DELAY rollout 重启会掉 pfwd →
#     需 pfwd_inventory_restarter.sh(3s 紧 loop 复活)维持 inventory_direct。
# =============================================================================
# ★ 采集实施计划 v1 (2026-07-08) — 顺序/分批/flaky（全文+bug状态见黑板
#   (project docs)/TASK-K8S-M8-overnight-recollect.md §「dual/single 采集实施计划 v1」）
# -----------------------------------------------------------------------------
#   ★采前 smoke: dual12_r1（catalog env-hook rolling，确定性 60s）→ verify_dual.py PASS
#     + catalog pod ∈ during_fault → 证 fixed-runner 在曾-bug 根类上正确+管线通；PASS 留作 r1，FAIL 停查。
#   顺序 = smoke → D1 干净走量 → D2 podfail → D3 cpu-stress：
#     D1 (8×5,无 variance/无 podfail-flake): dual01·02·05·10(db_lock)·12(补r2-5)·13·11·07
#        （inv-restarter 组 dual11·07 排末尾相邻，inventory restarter 起一次）
#     D2 (3 combo,★入前 kubectl rollout restart deploy/user deploy/catalog -n recweb-chaos + 等 Ready):
#        dual04(补 r3-5)·08·16
#     D3 (5×5,最高 variance,host_cpu case 间等 VM drain):
#        dual09(★首 rep 验 sasrec pod∈during_fault)·14·15·03·06
#   flaky: retry ≤10/rep,弱 rep fail-closed 正确（绝不松 gate 凑数）；dual04/09/03/06/14/15 预留多跑凑 5。
#   ★instance-label bug = 已修(4e2271f+f3979ff)+ code-verified；每 case verify_dual.py 强制
#     "所有 root pod ∈ during_fault"（MANDATORY 不可跳）。env-hook rolling 真-修前-bug 类 =
#     dual07/11(inv)·dual11/12/13/15(catalog)——这几组 verify 必核 inv/catalog pod∈during_fault。
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
IDB='http://127.0.0.1:5013'                  # --inventory-direct-base
OUT='${REPO_DIR}/(native trees) dual_v2'   # 重采入 _dualNN_reps_v18

# -----------------------------------------------------------------------------
# 16 dual combos — 真实 fault 名（读自 dual_v2/dualNN_uni_r1/metadata.json config.fault）
#   dualNN → fault（1:1 双-XX catalog 位）
# -----------------------------------------------------------------------------
DUAL_FAULTS=(
  # dualNN | fault                            | stage | deep | carrier
  "dual01 | dual_timeout_retry                |  60 | no  | 默认(pricing)"          # 双-01 CFG timeout×retry
  "dual02 | net_delay_x_net_loss              |  60 | no  | 默认(pricing)"          # 双-02 NET delay×loss
  "dual03 | host_cpu_x_svccpu                 |  30 | yes | s3_checkout_fanin"      # 双-03 RES host×svc CPU
  "dual04 | dual_podfail_staggered            |  48 | yes | 内部(internal)"         # 双-04 LIF podfail 错峰
  "dual05 | net_delay_x_cfg_connect           |  30 | yes | s3_checkout_fanin"      # 双-05 NET delay × CFG connect
  "dual06 | host_cpu_x_cfg_timeout            |  60 | no  | s1_hostcfg"             # 双-06 RES host CPU × CFG timeout
  "dual07 | net_delay_x_inv_latency           | 300 | yes | deep_dual_edge"         # 双-07 NET delay × DEP inv-latency
  "dual08 | net_delay_x_podfail               | 140 | no  | s_netpod_cross"         # 双-08 NET delay × LIF podfail
  "dual09 | sasrec_cpu_x_catalog_netdelay     |  60 | yes | s_dk12_sasrec_net"      # 双-09 RES sasrec CPU × NET catlat
  "dual10 | db_lock_x_netdelay                |  32 | no  | s2_dblock_combo"        # 双-10 DEP db-lock × NET delay
  "dual11 | inv_latency_x_runtime_exc         |  90 | yes | s_dk13_inv_run"         # 双-11 DEP inv-latency × RUN exception
  "dual12 | catalog_latency_x_cfg_timeout     |  60 | yes | s_dk14_catlat_cfg"      # 双-12 DEP catlat × CFG timeout
  "dual13 | catalog_latency_x_net_loss        | 300 | yes | s_dk15_catlat_loss"     # 双-13 DEP catlat × NET loss(85% DK15)
  "dual14 | net_delay_x_svc_cpu               |  90 | yes | s_dk17_netdelay_svccpu" # 双-14 NET delay × RES svc CPU
  "dual15 | catalog_latency_x_svc_cpu         |  90 | yes | s_dk18_catlat_svccpu"   # 双-15 DEP catlat × RES svc CPU(同服务 NO spatial control)
  "dual16 | pod_failure_x_net_delay           | 200 | no  | s_podfail_netdelay"     # 双-16 LIF podfail × NET delay(masking)
)

# =============================================================================
# 逐 combo 前置 + 模板命令
#   通用：--poll 2.0 · --item 0071341196 · --keep-carrier · deep 组带 --deep ·
#   net_delay(500/50)/net_loss(60/85%) 由 chaos yaml 固定(无 CLI)；cat/inv delay 有 CLI。
#   ★重采仅改 --out-dir 到 _dualNN_reps_v18 + 确保 KUBECTL export；数值 flag 不变。
# =============================================================================

# --- 双-01 dual_timeout_retry (stage60,no-deep,默认载体) ---------------------
# 前置：pricing 单载体(现状) + catalog restarter。CFG f1_timeout_short=1000/baseline=8000(内部,无CLI)。
#   nohup "$PY" "$RUNNER" --case-id dual01_uni_r1 --fault dual_timeout_retry \
#     --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &

# --- 双-02 net_delay_x_net_loss (stage60,no-deep,默认载体) -------------------
# 前置：pricing 载体 + catalog restarter。delay 500/50 + loss 60%(共享 NET_LOSS_YAML，均 yaml 固定)。
#   nohup "$PY" "$RUNNER" --case-id dual02_uni_r1 --fault net_delay_x_net_loss \
#     --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &

# --- 双-03 host_cpu_x_svccpu (stage30,deep,s3_checkout_fanin) ----------------
# 前置：stressor Deployment scale-up(host cpu w12,无limit) + StressChaos svc cpu w2(catalog cgroup) + s3 载体。
#   nohup "$PY" "$RUNNER" --case-id dual03_uni_r1 --fault host_cpu_x_svccpu --deep \
#     --carriers s3_checkout_fanin --user-token "$UT" \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &

# --- 双-04 dual_podfail_staggered (stage48,deep,内部载体) --------------------
# 前置：catalog restarter。两 PodChaos staggered(f1=catalog[0,0.5] / f2=user[0.5,1.0]，midpoint 24s)，无 --carriers。
#   nohup "$PY" "$RUNNER" --case-id dual04_uni_r1 --fault dual_podfail_staggered --deep \
#     --user-token "$UT" --item 0071341196 --stage-seconds 48 --poll 2.0 \
#     --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &

# --- 双-05 net_delay_x_cfg_connect (stage30,deep,s3_checkout_fanin) ----------
# 前置：s3 载体 + catalog-gw ov-net-f2.conf(connect_timeout 200ms 短) + net delay 500/50。★f2-offset=8/f2-dur=14(窄)。
#   nohup "$PY" "$RUNNER" --case-id dual05_uni_r1 --fault net_delay_x_cfg_connect --deep \
#     --carriers s3_checkout_fanin --user-token "$UT" \
#     --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 \
#     --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &

# --- 双-06 host_cpu_x_cfg_timeout (stage60,no-deep,s1_hostcfg) ---------------
# 前置：stressor host cpu w12 + catalog-gw read_timeout 20ms conf + s1_hostcfg 载体(pricing gw-path 见 cfg / catalog_direct 绕 gw 不见)。
#   nohup "$PY" "$RUNNER" --case-id dual06_uni_r1 --fault host_cpu_x_cfg_timeout \
#     --carriers s1_hostcfg --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &

# --- 双-07 net_delay_x_inv_latency (stage300,deep,deep_dual_edge) ------------
# 前置：★inventory restarter(pfwd_inventory_restarter.sh) + catalog/inventory direct 载体 + net delay 500/50。inv env FAULT_DELAY_MS=2000。
#   nohup "$PY" "$RUNNER" --case-id dual07_uni_r1 --fault net_delay_x_inv_latency --deep \
#     --carriers deep_dual_edge --user-token "$UT" \
#     --catalog-direct-base "$CDB" --inventory-direct-base "$IDB" --inv-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &

# --- 双-08 net_delay_x_podfail (stage140,no-deep,s_netpod_cross) -------------
# 前置：catalog restarter + s_netpod_cross 载体 + net delay 500/50 + PodChaos(f1=catalog-gw / f2=user，staggered midpoint 70s)。
#   ★★2026-07-09 采集补:s_netpod_cross **探 user 载体**,podfail 连击(dual04→dual08)把 user pod 打到 5004 pf 反复掉
#     → pre_fault 基线探 user 全失败 → gate 'not ready_for_release'。**须起 user restarter 保 5004**:
#     `nohup bash scripts/chaos/ctk/pfwd_user_restarter.sh > /tmp/pfwd_user.log 2>&1 & echo $! > /tmp/pfwd_user.pid`
#     (3s cadence,照 pfwd_catalog_restarter.sh 改 5005→5004/catalog→user)。D1→D2 rollout-restart 也掉 5004,dual08 需、dual04 靠 availability 不需。
#   nohup "$PY" "$RUNNER" --case-id dual08_uni_r1 --fault net_delay_x_podfail \
#     --carriers s_netpod_cross --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &

# --- 双-09 sasrec_cpu_x_catalog_netdelay (stage60,deep,s_dk12_sasrec_net) ----
# 前置：StressChaos sasrec cpu w8(≥6 超额压测) + catalog net delay 500/50 + s_dk12 载体。
#   ⚠ w8 饱和 run-to-run 波动(Chaos Mesh #4038)→ 可能多跑凑 5 valid。
#   nohup "$PY" "$RUNNER" --case-id dual09_uni_r1 --fault sasrec_cpu_x_catalog_netdelay --deep \
#     --carriers s_dk12_sasrec_net --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &

# --- 双-10 db_lock_x_netdelay (stage32,no-deep,s2_dblock_combo) --------------
# 前置：★app-side DbContentionInjector(LOCK TABLES items WRITE, hold 12s) + net delay 500/50 + s2_dblock_combo 载体。
#   ★CHECKSUM 三验铁律(LOCK 前 pre / UNLOCK+确认释放后 post，绝不锁期间核)。
#   nohup "$PY" "$RUNNER" --case-id dual10_uni_r1 --fault db_lock_x_netdelay \
#     --carriers s2_dblock_combo --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &

# --- 双-11 inv_latency_x_runtime_exc (stage90,deep,s_dk13_inv_run) -----------
# 前置：★inventory restarter + inv env FAULT_DELAY_MS=2000 + catalog env FAULT_RAISE(runtime 500) + catalog/inventory direct 载体。
#   nohup "$PY" "$RUNNER" --case-id dual11_uni_r1 --fault inv_latency_x_runtime_exc --deep \
#     --carriers s_dk13_inv_run --user-token "$UT" \
#     --catalog-direct-base "$CDB" --inventory-direct-base "$IDB" --inv-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &

# --- 双-12 catalog_latency_x_cfg_timeout (stage60,deep,s_dk14_catlat_cfg) ----
# 前置：catalog env FAULT_DELAY_MS(--cat-delay-ms 2000) + catalog-gw read_timeout 1000ms conf + s_dk14 载体。
#   nohup "$PY" "$RUNNER" --case-id dual12_uni_r1 --fault catalog_latency_x_cfg_timeout --deep \
#     --carriers s_dk14_catlat_cfg --user-token "$UT" --catalog-direct-base "$CDB" --cat-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &

# --- 双-13 catalog_latency_x_net_loss (stage300,deep,s_dk15_catlat_loss) -----
# 前置：catalog env FAULT_DELAY_MS(--cat-delay-ms 2000) + ★net loss 85%(chaos-net-catalog-loss-dk15.yaml，DK15 专用非共享60%) + s_dk15 载体。
#   nohup "$PY" "$RUNNER" --case-id dual13_uni_r1 --fault catalog_latency_x_net_loss --deep \
#     --carriers s_dk15_catlat_loss --user-token "$UT" --catalog-direct-base "$CDB" --cat-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &

# --- 双-14 net_delay_x_svc_cpu (stage90,deep,s_dk17_netdelay_svccpu) ---------
# 前置：net delay 500/50 + StressChaos svc cpu w2(catalog cgroup) + s_dk17 载体。
#   nohup "$PY" "$RUNNER" --case-id dual14_uni_r1 --fault net_delay_x_svc_cpu --deep \
#     --carriers s_dk17_netdelay_svccpu --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &

# --- 双-15 catalog_latency_x_svc_cpu (stage90,deep,s_dk18_catlat_svccpu) -----
# 前置：catalog env FAULT_DELAY_MS(--cat-delay-ms 2000) + StressChaos svc cpu w2 + s_dk18 载体。
#   ⚠ 两根【同服务(catalog)同模态(latency)】=NO spatial control → catalog_direct DOUBLE victim，可分靠 mechanism(intensity-gap + cfs_throttle marker)。
#   nohup "$PY" "$RUNNER" --case-id dual15_uni_r1 --fault catalog_latency_x_svc_cpu --deep \
#     --carriers s_dk18_catlat_svccpu --user-token "$UT" --catalog-direct-base "$CDB" --cat-delay-ms 2000 \
#     --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &

# --- 双-16 pod_failure_x_net_delay (stage200,no-deep,s_podfail_netdelay) -----
# 前置：catalog restarter + PodChaos(f1=catalog-gw / f2=catalog) + net delay 500/50 + s_podfail_netdelay 载体。★f2-offset=20(masking by design)。
#   nohup "$PY" "$RUNNER" --case-id dual16_uni_r1 --fault pod_failure_x_net_delay \
#     --carriers s_podfail_netdelay --user-token "$UT" --catalog-direct-base "$CDB" \
#     --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 \
#     --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &

# =============================================================================
# Append-line 格式（采集时主循环每 PASS 一个 case 追加一行）：
#   <完整可跑命令>   # rep N/5 | gate PASS | checksum净 | <YYYY-MM-DDTHH:MMZ>
# 例（EXAMPLE — 采集时替换为真命令+真时戳，勿直接跑）：
# nohup "$PY" "$RUNNER" --case-id dual07_uni_r1 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token "$UT" --catalog-direct-base "$CDB" --inventory-direct-base "$IDB" --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 1/5 | gate PASS | checksum净 | 2026-07-08T03:41Z   <<EXAMPLE>>
# =============================================================================

# --- verified cases (appended during collection) ---
nohup "$PY" "$RUNNER" --case-id dual04_uni_r1 --fault dual_podfail_staggered --deep --user-token "$UT" --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &   # rep 1/5 | PASS | checksum净 | 2026-07-08T07:33Z
nohup "$PY" "$RUNNER" --case-id dual04_uni_r2 --fault dual_podfail_staggered --deep --user-token "$UT" --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &   # rep 2/5 | PASS | 2026-07-08T07:38Z
nohup "$PY" "$RUNNER" --case-id dual12_uni_r1 --fault catalog_latency_x_cfg_timeout --deep --carriers s_dk14_catlat_cfg --user-token "$UT" --catalog-direct-base "$CDB" --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &   # rep 1/5 (采前 smoke, plan §B) | verify_dual PASS + instance_check catalog OK (catalog pod ∈ during_fault) | checksum净 | 2026-07-08T11:05Z
nohup "$PY" "$RUNNER" --case-id dual01_uni_r1 --fault dual_timeout_retry  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T12:05Z(pre-existing)
nohup "$PY" "$RUNNER" --case-id dual01_uni_r2 --fault dual_timeout_retry  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T12:09Z
nohup "$PY" "$RUNNER" --case-id dual01_uni_r3 --fault dual_timeout_retry  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T12:12Z
nohup "$PY" "$RUNNER" --case-id dual01_uni_r4 --fault dual_timeout_retry  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T12:16Z
nohup "$PY" "$RUNNER" --case-id dual01_uni_r5 --fault dual_timeout_retry  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual01_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T12:19Z
nohup "$PY" "$RUNNER" --case-id dual02_uni_r1 --fault net_delay_x_net_loss  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T12:23Z
nohup "$PY" "$RUNNER" --case-id dual02_uni_r2 --fault net_delay_x_net_loss  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T12:27Z
nohup "$PY" "$RUNNER" --case-id dual02_uni_r3 --fault net_delay_x_net_loss  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T12:30Z
nohup "$PY" "$RUNNER" --case-id dual02_uni_r4 --fault net_delay_x_net_loss  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T12:34Z
nohup "$PY" "$RUNNER" --case-id dual02_uni_r5 --fault net_delay_x_net_loss  --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual02_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T12:37Z
nohup "$PY" "$RUNNER" --case-id dual05_uni_r1 --fault net_delay_x_cfg_connect --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T12:40Z
nohup "$PY" "$RUNNER" --case-id dual05_uni_r2 --fault net_delay_x_cfg_connect --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T12:42Z
nohup "$PY" "$RUNNER" --case-id dual05_uni_r3 --fault net_delay_x_cfg_connect --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T12:45Z
nohup "$PY" "$RUNNER" --case-id dual05_uni_r4 --fault net_delay_x_cfg_connect --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T12:47Z
nohup "$PY" "$RUNNER" --case-id dual05_uni_r5 --fault net_delay_x_cfg_connect --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 8 --f2-duration-seconds 14 --keep-carrier --out-dir "$OUT/_dual05_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T12:49Z
nohup "$PY" "$RUNNER" --case-id dual10_uni_r1 --fault db_lock_x_netdelay --carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T12:52Z
nohup "$PY" "$RUNNER" --case-id dual10_uni_r2 --fault db_lock_x_netdelay --carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T12:54Z
nohup "$PY" "$RUNNER" --case-id dual10_uni_r3 --fault db_lock_x_netdelay --carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T12:56Z
nohup "$PY" "$RUNNER" --case-id dual10_uni_r4 --fault db_lock_x_netdelay --carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T12:58Z
nohup "$PY" "$RUNNER" --case-id dual10_uni_r5 --fault db_lock_x_netdelay --carriers s2_dblock_combo --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 32 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual10_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T13:00Z
nohup "$PY" "$RUNNER" --case-id dual13_uni_r1 --fault catalog_latency_x_net_loss --deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T13:17Z
nohup "$PY" "$RUNNER" --case-id dual13_uni_r2 --fault catalog_latency_x_net_loss --deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T13:33Z
nohup "$PY" "$RUNNER" --case-id dual13_uni_r3 --fault catalog_latency_x_net_loss --deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T13:50Z
nohup "$PY" "$RUNNER" --case-id dual13_uni_r4 --fault catalog_latency_x_net_loss --deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T14:07Z
nohup "$PY" "$RUNNER" --case-id dual13_uni_r5 --fault catalog_latency_x_net_loss --deep --carriers s_dk15_catlat_loss --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual13_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T14:23Z
nohup "$PY" "$RUNNER" --case-id dual11_uni_r1 --fault inv_latency_x_runtime_exc --deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T15:19Z
nohup "$PY" "$RUNNER" --case-id dual11_uni_r2 --fault inv_latency_x_runtime_exc --deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T15:27Z
nohup "$PY" "$RUNNER" --case-id dual11_uni_r3 --fault inv_latency_x_runtime_exc --deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T15:36Z
nohup "$PY" "$RUNNER" --case-id dual11_uni_r4 --fault inv_latency_x_runtime_exc --deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T15:44Z
nohup "$PY" "$RUNNER" --case-id dual11_uni_r5 --fault inv_latency_x_runtime_exc --deep --carriers s_dk13_inv_run --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual11_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T15:53Z
nohup "$PY" "$RUNNER" --case-id dual07_uni_r1 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T16:11Z
nohup "$PY" "$RUNNER" --case-id dual07_uni_r2 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T16:27Z
nohup "$PY" "$RUNNER" --case-id dual07_uni_r3 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T16:44Z
nohup "$PY" "$RUNNER" --case-id dual07_uni_r4 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T17:01Z
nohup "$PY" "$RUNNER" --case-id dual07_uni_r5 --fault net_delay_x_inv_latency --deep --carriers deep_dual_edge --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --inventory-direct-base http://127.0.0.1:5013 --inv-delay-ms 2000 --item 0071341196 --stage-seconds 300 --poll 2.0 --f2-offset-seconds 40 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual07_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T17:17Z
nohup "$PY" "$RUNNER" --case-id dual12_uni_r2 --fault catalog_latency_x_cfg_timeout --deep --carriers s_dk14_catlat_cfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T18:13Z
nohup "$PY" "$RUNNER" --case-id dual12_uni_r3 --fault catalog_latency_x_cfg_timeout --deep --carriers s_dk14_catlat_cfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T18:18Z
nohup "$PY" "$RUNNER" --case-id dual12_uni_r4 --fault catalog_latency_x_cfg_timeout --deep --carriers s_dk14_catlat_cfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T18:24Z
nohup "$PY" "$RUNNER" --case-id dual12_uni_r5 --fault catalog_latency_x_cfg_timeout --deep --carriers s_dk14_catlat_cfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual12_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T18:29Z
nohup "$PY" "$RUNNER" --case-id dual04_uni_r3 --fault dual_podfail_staggered --deep --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T18:40Z
nohup "$PY" "$RUNNER" --case-id dual04_uni_r4 --fault dual_podfail_staggered --deep --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T18:44Z
nohup "$PY" "$RUNNER" --case-id dual04_uni_r5 --fault dual_podfail_staggered --deep --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 48 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual04_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T18:47Z
nohup "$PY" "$RUNNER" --case-id dual08_uni_r1 --fault net_delay_x_podfail --carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T19:39Z
nohup "$PY" "$RUNNER" --case-id dual08_uni_r2 --fault net_delay_x_podfail --carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T19:48Z
nohup "$PY" "$RUNNER" --case-id dual08_uni_r3 --fault net_delay_x_podfail --carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T19:56Z
nohup "$PY" "$RUNNER" --case-id dual08_uni_r4 --fault net_delay_x_podfail --carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 采 2026-07-08T20:12Z → ★重采 2026-07-09T05:05Z(原 rep pre_fault netem bleed-over 污染: pre_p95=2045ms≈during 2059ms 无分离; 净基线重采后 pre=37ms/during=2064ms)
nohup "$PY" "$RUNNER" --case-id dual08_uni_r5 --fault net_delay_x_podfail --carriers s_netpod_cross --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 140 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual08_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T20:20Z
nohup "$PY" "$RUNNER" --case-id dual16_uni_r1 --fault pod_failure_x_net_delay --carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T20:31Z
nohup "$PY" "$RUNNER" --case-id dual16_uni_r2 --fault pod_failure_x_net_delay --carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T20:42Z
nohup "$PY" "$RUNNER" --case-id dual16_uni_r3 --fault pod_failure_x_net_delay --carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T20:53Z
nohup "$PY" "$RUNNER" --case-id dual16_uni_r4 --fault pod_failure_x_net_delay --carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T21:03Z
nohup "$PY" "$RUNNER" --case-id dual16_uni_r5 --fault pod_failure_x_net_delay --carriers s_podfail_netdelay --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 200 --poll 2.0 --f2-offset-seconds 20 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual16_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T21:14Z
nohup "$PY" "$RUNNER" --case-id dual09_uni_r1 --fault sasrec_cpu_x_catalog_netdelay --deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T21:18Z
nohup "$PY" "$RUNNER" --case-id dual09_uni_r2 --fault sasrec_cpu_x_catalog_netdelay --deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T21:22Z
nohup "$PY" "$RUNNER" --case-id dual09_uni_r3 --fault sasrec_cpu_x_catalog_netdelay --deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T21:25Z
nohup "$PY" "$RUNNER" --case-id dual09_uni_r4 --fault sasrec_cpu_x_catalog_netdelay --deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T21:29Z
nohup "$PY" "$RUNNER" --case-id dual09_uni_r5 --fault sasrec_cpu_x_catalog_netdelay --deep --carriers s_dk12_sasrec_net --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual09_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T21:33Z
nohup "$PY" "$RUNNER" --case-id dual14_uni_r1 --fault net_delay_x_svc_cpu --deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T21:39Z
nohup "$PY" "$RUNNER" --case-id dual14_uni_r2 --fault net_delay_x_svc_cpu --deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T21:44Z
nohup "$PY" "$RUNNER" --case-id dual14_uni_r3 --fault net_delay_x_svc_cpu --deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T21:49Z
nohup "$PY" "$RUNNER" --case-id dual14_uni_r4 --fault net_delay_x_svc_cpu --deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T21:54Z
nohup "$PY" "$RUNNER" --case-id dual14_uni_r5 --fault net_delay_x_svc_cpu --deep --carriers s_dk17_netdelay_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual14_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T21:59Z
nohup "$PY" "$RUNNER" --case-id dual15_uni_r1 --fault catalog_latency_x_svc_cpu --deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T22:07Z
nohup "$PY" "$RUNNER" --case-id dual15_uni_r2 --fault catalog_latency_x_svc_cpu --deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T22:14Z
nohup "$PY" "$RUNNER" --case-id dual15_uni_r3 --fault catalog_latency_x_svc_cpu --deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T22:21Z
nohup "$PY" "$RUNNER" --case-id dual15_uni_r4 --fault catalog_latency_x_svc_cpu --deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T22:27Z
nohup "$PY" "$RUNNER" --case-id dual15_uni_r5 --fault catalog_latency_x_svc_cpu --deep --carriers s_dk18_catlat_svccpu --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --cat-delay-ms 2000 --item 0071341196 --stage-seconds 90 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual15_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T22:34Z
nohup "$PY" "$RUNNER" --case-id dual03_uni_r1 --fault host_cpu_x_svccpu --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T22:37Z
nohup "$PY" "$RUNNER" --case-id dual03_uni_r2 --fault host_cpu_x_svccpu --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T22:41Z
nohup "$PY" "$RUNNER" --case-id dual03_uni_r3 --fault host_cpu_x_svccpu --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T22:44Z
nohup "$PY" "$RUNNER" --case-id dual03_uni_r4 --fault host_cpu_x_svccpu --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T22:48Z
nohup "$PY" "$RUNNER" --case-id dual03_uni_r5 --fault host_cpu_x_svccpu --deep --carriers s3_checkout_fanin --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --item 0071341196 --stage-seconds 30 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual03_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T22:52Z
nohup "$PY" "$RUNNER" --case-id dual06_uni_r1 --fault host_cpu_x_cfg_timeout --carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &   # rep 1/5 | verify_dual PASS | checksum净 | 2026-07-08T22:57Z
nohup "$PY" "$RUNNER" --case-id dual06_uni_r2 --fault host_cpu_x_cfg_timeout --carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &   # rep 2/5 | verify_dual PASS | checksum净 | 2026-07-08T23:02Z
nohup "$PY" "$RUNNER" --case-id dual06_uni_r3 --fault host_cpu_x_cfg_timeout --carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &   # rep 3/5 | verify_dual PASS | checksum净 | 2026-07-08T23:07Z
nohup "$PY" "$RUNNER" --case-id dual06_uni_r4 --fault host_cpu_x_cfg_timeout --carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &   # rep 4/5 | verify_dual PASS | checksum净 | 2026-07-08T23:12Z
nohup "$PY" "$RUNNER" --case-id dual06_uni_r5 --fault host_cpu_x_cfg_timeout --carriers s1_hostcfg --user-token 0870e257-6cd0-4fe4-b815-0a9da6b25d41 --catalog-direct-base http://127.0.0.1:5005 --item 0071341196 --stage-seconds 60 --poll 2.0 --f2-offset-seconds 14 --f2-duration-seconds 31 --keep-carrier --out-dir "$OUT/_dual06_reps_v18" &   # rep 5/5 | verify_dual PASS | checksum净 | 2026-07-08T23:16Z

# =============================================================================
# ★★ PROVENANCE (2026-07-08~09 通宵重采 → dual 80/80 全采完)
# =============================================================================
#   - 全 16 combo × 5 rep = 80 case,2026-07-08~09 一次采完(dual04 r1/r2 前采、r3-5 本批补;余全本批新采)。
#   - runner = 【instance-fixed】: commit 4e2271f(catalog/inv/pricing 根取 during_fault pod)+ f3979ff(user/sasrec 根)。
#   - 每 case 验: verify_dual.py PASS = 3-part(gate+checksum / 18字段 trace / 6 K8s 标签)+ 所有 real root pod ∈ during_fault。
#   - 质量: 几乎全 attempt-1;仅 dual08 r4 一次 flaky retry。全程 CHECKSUM items=3849590678/inventory=3935678504 零漂移。
#   - instance-fix 三根家族全验: catalog-rolling(dual12 采前 smoke)/ user-inplace(dual04)/ sasrec-inplace(dual09 root含 sasrec-…∈during)。
#   ★★复现须知(否则结果不一致):
#     ① runner 须含 instance-label fix(4e2271f + f3979ff),否则 rolling 根 instance 退回 post-recovery bug。
#     ② dual08 须起 user restarter 保 5004(见双-08 前置);D2 podfail 组(dual04/08/16)入前 rollout-restart user+catalog 复位 churn。
#     ③ 环境前置见头部 §前置(proxy8001 / pfwd_start 排除5013 / catalog restarter 全程 / dual07·11 加 inventory restarter)。
#     ④ 打包: package_for_delivery.py --pilot-dir <纯净 dual_v2> --bare --eval-only --flat-traces(源须无 mr2/ 子目录+无根级重复;find_cases 已修跳 mr2/,commit 2ce1b90)。
