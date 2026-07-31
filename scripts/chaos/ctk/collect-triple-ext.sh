#!/bin/bash
# collect-triple-ext.sh — G2ext Phase A triple 扩充批次命令日志（三-05/08,10 case,2026-07-24 采完）
# ★命令日志/复现脚本(append-only)。三-06/07(Phase B/C)采后续 append 本文件。
# 采集黑板 = (project docs)/archive/TASK-K8S-G2ext-multiroot-expand.md; 设计权威 = (project docs)/triple-root-catalog.md §扩充批次
#
# ===== 环境 ===== 同 collect-dual-ext.sh 头部(§1.5-A 全套 + proxy8001/pfwd/catalog restarter)
#
# ===== 逐 rep 命令模板 =====
# 三-05: $PY $RUN --case-id checkout_cart_pricing_r<N> --fault checkout_podfail_x_cart_cpu_x_pricing_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) triple_ext
# 三-08: $PY $RUN --case-id order_rq_catalog_r<N> --fault order_podfail_x_reviewquery_cpu_x_catalog_cpu --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) triple_ext
#
# ===== runner 版本 ===== commit 20aedae + 8c9d4d0(见 collect-dual-ext.sh 注)
# ★三-08 timing 偏离设计表: simultaneous→partial_overlap(podfail ≤60s liveness vs CPU throttle 需 240s 窗;
#   G2EXT_COMBOS design_note + fault-catalog 已同步)。write_case 走真 3 窗 9 路 membership(M8 定稿口径)。
#
# ===== 采集台账(2026-07-24,交替组序) =====
# checkout_cart_pricing_r1 PASS a1 | order_rq_catalog_r1 PASS a1
# checkout_cart_pricing_r2 PASS a1 | order_rq_catalog_r2 PASS a1
# checkout_cart_pricing_r3 PASS a1 | order_rq_catalog_r3 PASS a1
# checkout_cart_pricing_r4 PASS a1 | order_rq_catalog_r4 PASS attempt2(a1 gate-fail,CPU-flake 类,清脏重试过)
# checkout_cart_pricing_r5 PASS attempt2(a1 gate-fail,同上) | order_rq_catalog_r5 PASS a1
# 终检 2026-07-24: 10/10 三项核 PASS + 真 G=3 全部(三腿三服务) + canon 全对
# CHECKSUM 采后复核: items=3849590678 / inventory=3935678504 (零漂移)
# smoke 证据件: _g2ext_smoke_pass/smk_{checkout_cart_pricing,order_rq_catalog}_r1;
# 首轮失败 smoke(bite bug 修前): _g2ext_smoke_failed/(勿用)
# 真三根合计: triple_dense/triple01(5) + 本树(10) = 15;三-06/07 采完 → 25
#
# ===== Phase B append(2026-07-24 12:56-14:53)=====
# 三-06: $PY $RUN --case-id backend_sasrec_gwnet_r<N> --fault backend_cpu_x_sasrec_cpu_x_gw_netdelay --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) triple_ext
# runner 版本: commit 6a48189。★stage 硬上限 270(netdelay 腿静态 NetworkChaos duration=300s, FATAL 守卫防 auto-expire 窗撒谎)
# ★归因结构: backend/sasrec 共 recommend 路径 → 载体无法二分, per-root=双 per-pod cfs_throttle 独立判;
#   netdelay=pricing_direct ≥800ms 绝对门 + catalog_direct 控制臂; sasrec 腿渲染 CRD workers=8(duration 660s)
# backend_sasrec_gwnet_r1 PASS a1 | r2 PASS a1 | r3 PASS a1 | r4 PASS a1 | r5 PASS a1  (零重试)
# 终检: 5/5 三项核 + 真 G=3['backend','sasrec','catalog-gw'] + canon(2×service_cpu_saturation+network_delay)
# smoke 证据件: _g2ext_smoke_pass/smk_backend_sasrec_gwnet_r1
# 真三根合计: triple01(5) + 三-05(5) + 三-06(5) + 三-08(5) = 20;三-07 采完 → 25
#
# ===== Phase C append(2026-07-24 20:49-23:03)=====
# 三-07: $PY $RUN --case-id recagent_sasrec_catalog_r<N> --fault recagent_netdelay_x_sasrec_cpu_x_catalog_podfail --deep --user-token $UT --stage-seconds 240 --poll 2.0 --keep-carrier --out-dir ${REPO_DIR}/(native trees) triple_ext
# runner 版本: commit f83fe5b。★审计判"9 组之最大改"= 三机制拼装(G1 rec-agent netdelay450/90 渲染 CRD + Phase B sasrec workers=8 件 + catalog podfail ≤60s 子窗 BLOCKING#2)
# ★三-07 前置: deepseek-env secret 挂 rec-agent(同双-20; recommend 证据通道)。catalog podfail 由既有 catalog restarter 覆盖(无需额外)。
#   rec-agent 五件套复用双-20 落地件(双载体触发泛化 g2_has_recagent); netdelay 腿 gate=recagent_health 绝对位移>=800(非 CPU ratio 门, control 臂可选)。
# recagent_sasrec_catalog_r1 PASS a1 | r2 PASS a1 | r3 PASS a1 | r4 PASS a1 | r5 PASS a1  (零重试)
# 终检: 5/5 三项核 + 真 G=3['rec-agent','sasrec','catalog'] + canon(network_delay/service_cpu_saturation/service_unavailable)
# smoke 证据: _g2ext_smoke_pass/smk_recagent_sasrec_catalog_r1
# ★真三根合计 = triple01(5) + 三-05(5) + 三-06(5) + 三-07(5) + 三-08(5) = 25 (G2ext 完结)
