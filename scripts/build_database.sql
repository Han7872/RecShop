-- ============================================================
-- build_database.sql —— RecWeb2 一键建库入口（单条命令，完整 + 幂等）
-- ============================================================
-- 用途：全新一键构建即产出与 live shopify2 一致的完整 schema（25 base 表 + 13 视图
--       + 2 存储过程，含 payment/inventory/pricing/promotion/shipping/notification 等
--       微服务自有域表），从根上杜绝"缺表"复发；对已存在的库重跑也安全。
--
-- 使用（在仓库根目录 ${REPO_DIR} 执行，使 SOURCE 的相对路径成立）：
--     mysql -u root -p < scripts/build_database.sql
--   或进入 mysql 交互后：
--     mysql> SOURCE scripts/build_database.sql;
--
-- 幂等保证（详见 scripts/database_schema.sql 头部）：
--   * 全部 CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW；
--   * 存储过程 DROP PROCEDURE IF EXISTS + CREATE（仅替换过程定义，不碰表/数据）；
--   * 零 DROP TABLE / 零 TRUNCATE / 零危及现有数据语句。
--   → 对已存在的 shopify2 重跑：不报错、不重建、不丢数据。
--
-- 权威单一来源：scripts/database_schema.sql 已自包含全部表 + 视图 + 过程（含评论系统
--   reviews/review_replies 与全部域服务表），是唯一权威 DDL 源。本文件只做 SOURCE 汇总。
--   历史的 (archived)sql_migrations/migrate_v3_reviews.sql 对全新构建已冗余（其内容已并入 database_schema.sql），
--   保留仅为兼容既有的两步文档；全新一键构建只需本入口即可，无需再单独跑它。
--   (archived)sql_migrations/fix_missing_tables.sql 是 6 张曾缺表的应急补丁（同样 IF NOT EXISTS，与本脚本对齐
--   无漂移），现已不再必要 —— 那 6 张表全部包含在 database_schema.sql 内。
-- ============================================================

SOURCE scripts/database_schema.sql;

SELECT 'RecWeb2 one-click DB build completed (idempotent, full schema).' AS result;
