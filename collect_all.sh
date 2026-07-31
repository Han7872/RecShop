#!/usr/bin/env bash
# =============================================================================
# collect_all.sh — one-click FULL data collection (traditional 255 + agent 108)
# =============================================================================
# ⚠⚠⚠ 运行前必读 ⚠⚠⚠
#   - 极耗时: traditional 255 + agent 108 case, 通宵级(8h+), 别随手跑
#   - 重前置: 需完整 25 服务 K8S 栈 + Chaos Mesh + 大模型资产 + pfwd 守护 +
#             kubectl proxy(:8001) 全部就位(各 collect 脚本自带 preflight 会核查)
#   - 烧钱:   agent 采集调 DeepSeek API(run_collect_agentfault.sh --yes 才付费)
#   仓内 datasets/ 已经是采好的产物 — 本脚本仅供"从零复现 / 扩充采集",
#   一般使用者不需要跑; 想验证采集流程请先单跑一个, 例:
#       bash scripts/chaos/ctk/collect-single-dense.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

cat <<'EOF'
==============================================================
 FULL COLLECTION — traditional 255 + agent 108
 耗时通宵级; 需 K8S + Chaos Mesh + 大资产 + 守护全部就位
 (见脚本头警告; datasets/ 已是采好的产物, 一般无需重跑)
==============================================================
EOF
read -r -p "确认跑全采集? (输入 yes 继续, 其他取消) " ans
[ "$ans" = "yes" ] || { echo "已取消。"; exit 0; }

# ---- traditional (K8S native 树, 255 case) ----
echo ""; echo "### [1/7] single dense (40)"
bash scripts/chaos/ctk/collect-single-dense.sh
echo ""; echo "### [2/7] dual dense (80)"
bash scripts/chaos/ctk/collect-dual-dense.sh
echo ""; echo "### [3/7] triple dense (20)"
bash scripts/chaos/ctk/collect-triple-dense.sh
echo ""; echo "### [4/7] single spread (55)"
bash scripts/chaos/ctk/collect-single-spread.sh
echo ""; echo "### [5/7] single recagent (15)"
bash scripts/chaos/ctk/collect-single-recagent.sh
echo ""; echo "### [6/7] G2ext batch — dual_ext 25 + triple_ext 20"
bash scripts/chaos/ctk/collect-g2ext.sh

# ---- agent semantic faults (108 case) ----
echo ""; echo "### [7/7] agent semantic faults (108, --yes 付费采集)"
bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes

echo ""
echo "=============================================================="
echo " FULL COLLECTION DONE — traditional 255 + agent 108"
echo "=============================================================="
