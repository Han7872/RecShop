#!/usr/bin/env bash
# =============================================================================
# init_db.sh — one-click DB init: 建库 + schema → 灌商品 → demo 种子
# =============================================================================
# 前置:
#   - MySQL 8.0+ 已在跑
#   - 大资产 shared/data/electronics.item 已就位(灌商品需要; 见 install.sh / README)
# 用法:
#   DB_PASSWORD=<你的MySQL密码> bash init_db.sh
#   可选环境变量: DB_USER(默认 root) / DB_NAME(默认 shopify2) / ITEM_FILE
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

DB_USER="${DB_USER:-root}"
DB_NAME="${DB_NAME:-shopify2}"
DB_PASS="${DB_PASSWORD:?请在环境变量 DB_PASSWORD 提供 MySQL 密码, 例: DB_PASSWORD=xxx bash init_db.sh}"
ITEM_FILE="${ITEM_FILE:-shared/data/electronics.item}"

echo "=== 1/3 建库 + schema (scripts/build_database.sql, 幂等) ==="
mysql -u"$DB_USER" -p"$DB_PASS" < scripts/build_database.sql

echo ""
echo "=== 2/3 灌商品数据 (scripts/import_data.py, 需大资产 $ITEM_FILE) ==="
if [ ! -f "$ITEM_FILE" ]; then
  echo "✗ 大资产 $ITEM_FILE 不存在 — 系统跑起来需要商品数据。" >&2
  echo "  请按 README 指示获取 electronics.item 放到 shared/data/ 后重跑此脚本。" >&2
  exit 1
fi
python scripts/import_data.py \
  --user "$DB_USER" --password "$DB_PASS" --database "$DB_NAME" \
  --item-file "$ITEM_FILE"
# 注: 默认不导交互数据(--limit-interactions=0); 推荐链要历史交互时加 --inter-file <electronics.inter>

echo ""
echo "=== 3/3 demo 种子 (scripts/seed_demo_data.sql, 优惠码/定价规则等) ==="
mysql -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < scripts/seed_demo_data.sql

echo ""
echo "=== init_db 完成 ==="
echo "下一步: bash start.sh  (启动 25 服务)"
