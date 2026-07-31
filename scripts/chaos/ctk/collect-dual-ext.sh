#!/bin/bash
# collect-dual-ext.sh — G2ext Phase A dual 扩充批次命令日志（双-18/19/21,15 case,2026-07-24 采完）
# ★这是【命令日志/复现脚本】: 记录实际采集用的确切命令。照 M8 惯例 append-only。
# 采集黑板 = (project docs)/archive/TASK-K8S-G2ext-multiroot-expand.md; 设计权威 = FAULT_DESIGN.md §扩充批次
#
# ===== 环境(照 archive/TASK-K8S-M8-overnight-recollect §1.5-A) =====
# cd /path/to/repo
# export KUBECTL='kubectl'
# export PATH="/c/Program Files/Docker/Docker/resources/bin:$PATH"
# export NO_PROXY='*' NACOS_ENABLED=false PYTHONIOENCODING=utf-8
# 守护: proxy8001 + pfwd_start.sh + pfwd_catalog_restarter.sh(PID-file /tmp/*.pid)
# PY=python3
# RUN=scripts/chaos/ctk/chaos_k8s_runner.py
# UT=0870e257-6cd0-4fe4-b815-0a9da6b25d41
#
# ===== 逐 rep 命令模板(r1..r5 只换 --case-id) =====
# 双-18: $PY $RUN --case-id cart_order_cpu_r<N> --fault cart_cpu_x_order_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) dual_ext
# 双-19: $PY $RUN --case-id search_rq_r<N> --fault search_podfail_x_reviewquery_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) dual_ext
# 双-21: $PY $RUN --case-id user_backend_r<N> --fault user_podfail_x_backend_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) dual_ext
#
# ===== runner 版本 =====
# commit 20aedae(Phase A 编排+selftest 65)+ 8c9d4d0(podfail bite hit_timeout=30 + 自适应 dwell)
# ★smoke 教训(8c9d4d0): PodChaos pause-swap 落地滞后 ~10-20s → bite hit_timeout 必须 30s 等到位,
#   否则 injected_at 打在 swap 前 → F1 窗头掺健康流量 → podfail 臂 err 0.33-0.5 < 0.8 硬门(3 组首 smoke 全栽这)。
#   约束 A(podfail 活跃 ≤60s liveness)由自适应 dwell 保: dwell=min(f2_dur,25,max(8,50-elapsed))。
#
# ===== 采集台账(2026-07-24 04:47-10:46,交替组序;PASS=ready_for_release+zero_drift+contract valid) =====
# cart_order_cpu_r1    PASS attempt1 | search_rq_r1 PASS a1 | user_backend_r1 PASS a1   (04:47-05:45 段)
# cart_order_cpu_r2    PASS a1 | search_rq_r2 PASS a1 | user_backend_r2 PASS a1
# cart_order_cpu_r3    PASS a1 | search_rq_r3 PASS a1 | user_backend_r3 PASS a1
# cart_order_cpu_r4    PASS a1 | search_rq_r4 PASS a1 | user_backend_r4 PASS a1
# cart_order_cpu_r5    PASS a1 | search_rq_r5 PASS a1 | user_backend_r5 PASS a1
# 终检 2026-07-24: 15/15 三项核 PASS + G=2 全部 + canon(service_cpu_saturation/service_unavailable)全对
# CHECKSUM 采后复核: items=3849590678 / inventory=3935678504 (零漂移)
# smoke 证据件: (native trees) _g2ext_smoke_pass/smk_{cart_order_cpu,search_rq,user_backend}_r1(不入正式树)
# 首轮失败 smoke(bite bug 修前): _g2ext_smoke_failed/smk_search_rq_r1(勿用)
#
# ===== Phase B append(2026-07-24 12:43-14:40)=====
# 双-20: $PY $RUN --case-id recagent_backend_r<N> --fault recagent_cpu_x_backend_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) dual_ext
# runner 版本: commit 6a48189(Phase B 编排+selftest 113;含 netdelay stage>270 FATAL 守卫)
# ★环境前置(双-20 特有): rec-agent deploy 必须挂 deepseek-env secret —— G1 采完 restore_recagent_stock.ps1
#   把 secret 摘了, smoke 首发 recommend 载体 0/120 全 500(DeepSeek 401)→ recovery_confirmed fail(post
#   全载体 ok_ratio 0.75<0.8)。修复: kubectl set env deploy/rec-agent --from=secret/deepseek-env -n recweb-chaos
#   + rollout 等 Ready + 实弹 POST /recommend 200(46s)后重跑 smoke PASS。gate 判据不吃 recommend(仅证据),
#   但 recovery_confirmed 是全载体聚合 → DeepSeek 挂会烧重试(fail-closed 不出脏数据)。
# recagent_backend_r1 PASS a1 | r2 PASS a1 | r3 PASS a1 | r4 PASS a1 | r5 PASS a1  (零重试)
# 终检: 5/5 三项核 + G=2['rec-agent','backend'] + throttle 双腿全过(rec-agent ~0.85/backend ~1.45)
# smoke 证据件: _g2ext_smoke_pass/smk_recagent_backend_r1(secret 修后重采版)
#
# ===== Phase C append(2026-07-24 20:33-22:50)=====
# 双-17: $PY $RUN --case-id checkout_inv_r<N> --fault checkout_podfail_x_inv_latency --deep --user-token $UT --stage-seconds 300 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) dual_ext
# runner 版本: commit f83fe5b(Phase C 编排+invlat 时序修复+selftest 167)
# ★★双-17 采集前置(两条,缺一必挂——smoke 三剥血泪):
#   (1) inventory restarter 必起: nohup bash scripts/chaos/ctk/pfwd_inventory_restarter.sh & (FAULT_DELAY env-rollout 杀 5013 PF, 普通 watchdog 8s 跟不上, 3s 节奏专属守护)
#   (2) 每 rep 前 inv env 干净: kubectl set env deploy/inventory FAULT_DELAY_MS- + rollout status 等 settle + 核 inventory_direct 基线<1s(残留 FAULT_DELAY 会污染 pre_fault base → 位移≈0 fail)
# ★invlat 时序(f83fe5b 修): set-env 在 podfail 子窗 recover 之后注(NOT during 起点)——单节点 Docker Desktop rollout ~4s 生效(远快于设计假设的多节点 120s),
#   during 起 fire-forget 会让 inventory 立即慢但 injected_at[F2] 要等 ~76s 后标记 → 中间样本污染 baseline 桶。修后 base~26ms/inwin~2034ms/shift~2008>>800。
# checkout_inv_r1 PASS a1 | r2 PASS a1 | r3 PASS a1 | r4 PASS a1 | r5 PASS a1  (零重试;每 rep 前 inv_clean_wait)
# 终检: 5/5 三项核 + G=2['checkout','inventory'] + canon(service_unavailable/dependency_latency)
# smoke 证据: _g2ext_smoke_pass/smk_checkout_inv_r1(f83fe5b 修后版; 失败版已删)
