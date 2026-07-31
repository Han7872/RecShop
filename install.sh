#!/usr/bin/env bash
# =============================================================================
# install.sh — one-click install Python dependencies
# =============================================================================
# 用法:  bash install.sh
# 说明:  pip install -r requirements.txt。大模型资产体积过大不入仓,需自备(见下)。
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

echo "=== pip install -r requirements.txt ==="
pip install -r requirements.txt

echo ""
echo "=== Python 依赖安装完成 ==="
echo ""
echo "⚠ 大模型资产需自备(体积过大,不入仓),放到以下路径系统才能跑:"
echo "   services/sasrec_api/standard_cache.pkl          (~9.2 GB, SASRec 模型缓存)"
echo "   services/sasrec_api/SASRec-*.pth                 (~260 MB, SASRec checkpoint)"
echo "   services/recommendation_agent/electronics.inter  (~2.4 GB, SASRec 交互数据)"
echo "   shared/data/electronics.item                     (~1.2 GB, 商品元数据)"
echo ""
echo "下一步: bash init_db.sh  (导入数据库) → bash start.sh  (启动系统)"
