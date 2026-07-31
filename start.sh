#!/usr/bin/env bash
# =============================================================================
# start.sh — one-click start all 25 microservices (wraps start_all.py)
# =============================================================================
# 用法:
#   bash start.sh              启动(默认离线模式:不起 Nacos + 绕 Clash 代理)
#   bash start.sh --no-docker  跳过 Docker OTel 栈, 只起 Python 服务
#   bash start.sh --stop       停止全部
# 有 Nacos 装好想用服务发现: NACOS_ENABLED=true bash start.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

# 默认离线: 不起 Nacos(服务发现走 127.0.0.1 固定端口) + 绕 Clash 代理
export NACOS_ENABLED="${NACOS_ENABLED:-false}"
export NO_PROXY="${NO_PROXY:-*}"
export PYTHONIOENCODING="${PYTHONIOENCODING:-utf-8}"

echo "=== 启动 RecShop 25 服务 (NACOS_ENABLED=$NACOS_ENABLED) ==="
python start_all.py "$@"
