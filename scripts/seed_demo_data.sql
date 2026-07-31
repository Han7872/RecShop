-- ============================================================
-- seed_demo_data.sql  —  幂等种子数据 (idempotent demo seed)
-- 目标库: shopify2   (DB_NAME in .env)
-- 用途: 填充会卡功能的空表, 让 checkout 预览 / 优惠码校验 / 定价链路可用。
--
-- 安全性:
--   * 全部 INSERT, 不含 DDL(建表由各微服务 _ensure_schema() 自动完成);
--   * 全部幂等(ON DUPLICATE KEY UPDATE / INSERT IGNORE / WHERE NOT EXISTS),
--     可重复执行而不报错、不产生重复行;
--   * 只写本脚本声明的 demo 行, 不改 items / orders / users 等业务表。
--
-- 执行方式(本脚本只是文件, 由人工/上游决定何时跑):
--   mysql -u root -p shopify2 < scripts/seed_demo_data.sql
--
-- 注意 — 排序规则(collation):
--   items.item_id      = utf8mb4_unicode_ci
--   inventory.item_id  = utf8mb4_0900_ai_ci
--   price_rules.item_id= utf8mb4_0900_ai_ci
--   所以下面 INSERT ... SELECT 跨表取 items.item_id 时, 用 CONVERT(... USING utf8mb4)
--   COLLATE utf8mb4_0900_ai_ci 显式对齐, 避免 "Illegal mix of collations" (errno 1267)。
-- ============================================================

-- ------------------------------------------------------------
-- 1) inventory  —  库存表 (为什么需要: 必填)
-- ------------------------------------------------------------
-- 现状: 空表。checkout_service 结账预览对购物车每件商品调 inventory_service
--       GET /api/inventory/<id>; 无记录时服务返回 available=0 ⇒ 预览 all_available=false
--       ⇒ 前端 checkout.html 把 "Place Order" 置灰并显示"部分商品库存不足"。
-- 注入: 给所有 status='active' 的商品上充足库存(stock=100, reserved=0 ⇒ available=100),
--       使任意被加入购物车的在售商品都"有货", 下单按钮恢复可用。
-- 取值: stock=100 是 demo 常用充足值; reserved 保持 0(预留由 reserve 接口运行时增减)。
-- 幂等: 主键 item_id; ON DUPLICATE KEY UPDATE 把已存在行的 stock 顶回 100(不动 reserved)。
-- 规模: active 商品约 43.3 万条, INSERT...SELECT 一次写入; 量大但一次性, MySQL 可承受。
--       若只想给"店里实际会用到的"商品上货, 见下方可选的"窄范围"变体。
INSERT INTO inventory (item_id, stock, reserved)
SELECT CONVERT(i.item_id USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS item_id,
       100 AS stock,
       0   AS reserved
FROM items i
WHERE i.status = 'active'
ON DUPLICATE KEY UPDATE stock = VALUES(stock);

-- (可选, 窄范围) 仅给"当前在购物车里 / 历史下过单"的在售商品上货, 避免写 43 万行。
-- 若改用此变体, 请注释掉上面的全量 INSERT, 解开下面两段:
-- INSERT INTO inventory (item_id, stock, reserved)
-- SELECT DISTINCT CONVERT(c.item_id USING utf8mb4) COLLATE utf8mb4_0900_ai_ci, 100, 0
-- FROM cart_items c JOIN items i ON i.item_id = c.item_id
-- WHERE i.status = 'active'
-- ON DUPLICATE KEY UPDATE stock = VALUES(stock);
-- INSERT INTO inventory (item_id, stock, reserved)
-- SELECT DISTINCT CONVERT(oi.item_id USING utf8mb4) COLLATE utf8mb4_0900_ai_ci, 100, 0
-- FROM order_items oi JOIN items i ON i.item_id = oi.item_id
-- WHERE i.status = 'active'
-- ON DUPLICATE KEY UPDATE stock = VALUES(stock);


-- ------------------------------------------------------------
-- 2) promotions  —  优惠码表 (为什么需要: 必填)
-- ------------------------------------------------------------
-- 现状: 空表。promotion_service POST /api/promotions/validate 查不到任何 code ⇒
--       一律返回 valid:false; checkout 页"Apply"券码永远提示"券码无效"。
-- 注入: 至少 1 个有效码。WELCOME 满足 active=1 且 expires_at IS NULL(永不过期) ⇒
--       校验恒为 valid:true。再附几个 demo 码方便演示(含一个故意 inactive / 一个已过期,
--       用于演示无效分支)。
-- 取值: discount 为"立减金额"(promotion_service 直接把它当作减免额返回; 前端按
--       max(total - discount, 0) 显示)。
-- 幂等: code 上有 UNIQUE 约束; ON DUPLICATE KEY UPDATE 重置该码的字段, 重复执行不报错。
INSERT INTO promotions (code, discount, active, expires_at) VALUES
    ('WELCOME',  10.00, 1, NULL),                                  -- 主用: 永久有效, 立减 $10
    ('SAVE5',     5.00, 1, NULL),                                  -- 备用有效码: 立减 $5
    ('VIP20',    20.00, 1, DATE_ADD(NOW(), INTERVAL 1 YEAR)),      -- 有效但带未来过期日
    ('EXPIRED',  15.00, 1, '2020-01-01 00:00:00'),                 -- 演示"已过期"分支
    ('DISABLED',  8.00, 0, NULL)                                   -- 演示"未启用"分支
ON DUPLICATE KEY UPDATE
    discount   = VALUES(discount),
    active     = VALUES(active),
    expires_at = VALUES(expires_at);


-- ------------------------------------------------------------
-- 3) price_rules  —  定价规则表 (为什么需要: 可选 / 默认不注入)
-- ------------------------------------------------------------
-- 现状: 空表。pricing_service GET /api/pricing/<id> 查不到规则时用 markup=0, tax_rate=0
--       ⇒ final == catalog 基价。功能"可用", 不会卡任何流程 —— 因此本表是可选项。
-- 是否注入: 默认【不注入】。仅当你想在结账预览里演示"含税预估"与"原价"不同时, 才解开下面。
-- 取值说明: markup / tax_rate 都是小数倍率(0.1000 = +10%); final = base*(1+markup)*(1+tax_rate)。
-- 幂等: 主键 item_id; ON DUPLICATE KEY UPDATE 重置规则。默认整段注释掉。
--
-- INSERT INTO price_rules (item_id, markup, tax_rate)
-- SELECT CONVERT(i.item_id USING utf8mb4) COLLATE utf8mb4_0900_ai_ci,
--        0.0000 AS markup,    -- 不加价
--        0.1300 AS tax_rate   -- 示例: 13% 税, 让"含税预估"高于原价
-- FROM items i
-- WHERE i.status = 'active'
-- ON DUPLICATE KEY UPDATE markup = VALUES(markup), tax_rate = VALUES(tax_rate);


-- ------------------------------------------------------------
-- 4) sessions  —  用户会话表 (无需注入)
-- ------------------------------------------------------------
-- 现状: 空表, 但全代码库无任何服务读/写 sessions(无 session_service, 无 SQL 引用)。
-- 结论: 空着不影响任何功能, 故不在本脚本注入。保留此说明以防误判。


-- ============================================================
-- 验证(可选, 只读): 跑完后确认行数
--   SELECT (SELECT COUNT(*) FROM inventory)  AS inventory_rows,
--          (SELECT COUNT(*) FROM promotions) AS promotion_rows,
--          (SELECT COUNT(*) FROM price_rules) AS price_rule_rows;
-- ============================================================
