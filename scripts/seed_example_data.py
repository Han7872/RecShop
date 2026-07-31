#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
seed_example_data.py  ——  TASK-Z5 示例数据种子(给重建的空表塞 FK 一致、幂等的演示数据)

用途
----
给 TASK-Z2/Z4 重建后为空(或近空)的 6 张表塞少量真实、外键一致的示例数据,
让前台/后台演示与截图有内容;顺带修正测试评论遗留的"幽灵评分"。

涉及的表(只动这 6 张 + 因评论重算的 items.rating/review_count):
    order_items / reviews / review_replies / announcements / shops / admin_logs

幂等(可重跑不重复)
------------------
- 所有插入都用 WHERE NOT EXISTS / UNIQUE 业务键去重,重跑不产生增量行。
- 示例行统一打 'SEED' 标记(remark/content/title/detail/name 含 'SEED'),便于辨识与回滚。
- order_items 以 (order_id, item_id) 唯一; reviews 以 order_item_id 唯一(表上有 uk_order_item);
  review_replies 以 review_id 唯一(uk_review); announcements 以 title 唯一(含 [SEED]);
  shops 以 (merchant_id, name) 唯一; admin_logs 以 detail 内的固定 seed_key 唯一。

FK 一致(全部指向已存在实体,不悬空)
----------------------------------
- order_items.order_id -> orders.id (用已存在已完成订单 14/15/19/24/31)
- order_items.item_id  -> items.item_id (用已存在商品,subtotal=price*qty)
- reviews.order_item_id-> order_items.id (本脚本刚插的行)
- reviews.user_token   -> users.user_token (取对应 order 的下单人,保证一致)
- reviews.item_id      -> items.item_id
- review_replies.review_id -> reviews.id ; review_replies.merchant_id -> merchants.id(商品归属商家)
- announcements.created_by -> admins.id (admin 1)
- shops.merchant_id    -> merchants.id (1/2/3)
- admin_logs.admin_id  -> admins.id (1/2)

幽灵评分修正
-----------
商品 M2-21f8be83(iphone 17 Pro MAX, merchant 2)在表重建前被测试评论改成
rating=5.00 / review_count=1,但该 review 已随表删 -> 现表里有评分却无评论(幽灵)。
本脚本给它配上真实 approved 评论(由其已完成订单的 order_item 产生),
然后从实际 reviews 重算 rating/review_count,使数据库内部自洽。

运行
----
    conda env recweb2 下:
    set NO_PROXY=*  &&  python3 scripts/seed_example_data.py
读 .env 取库/账号。单事务提交;失败回滚;不删任何已有数据;不重启栈。
"""
import os
import sys
import json

import mysql.connector

SEED = "SEED"  # 统一可辨识标记

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env():
    env = {}
    path = os.path.join(ROOT, ".env")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def connect(env):
    return mysql.connector.connect(
        host=env["DB_HOST"],
        port=int(env["DB_PORT"]),
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        database=env["DB_NAME"],
        charset=env.get("DB_CHARSET", "utf8mb4"),
        autocommit=False,
    )


def fetch_one(cur, sql, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row


# ---------------------------------------------------------------------------
# 1) order_items —— 给已完成订单补真实订单行 (order_id, item_id) 唯一
#    subtotal = item_price * quantity;尽量与 orders.total_amount 一致
# ---------------------------------------------------------------------------
# (order_id, item_id, quantity)  —— 价格/标题/图片从 items 现表快照取
ORDER_ITEM_PLAN = [
    (14, "M2-21f8be83", 1),  # demo_user 已完成单, total 1000 = 1*1000
    (15, "M2-21f8be83", 2),  # demo_user 已完成单, total 2000 = 2*1000
    (19, "M2-21f8be83", 1),  # Alan Wang 已完成单, total 1000 = 1*1000
    (24, "M2-21f8be83", 1),  # Alan Wang 已完成单, total 1000 = 1*1000
    (31, "M2-21f8be83", 1),  # taskz3_buyer 已完成单, total 1000 = 1*1000
]


def seed_order_items(cur):
    inserted = []
    for order_id, item_id, qty in ORDER_ITEM_PLAN:
        # 确认 order / item 存在 (FK 防悬空)
        if not fetch_one(cur, "SELECT 1 FROM orders WHERE id=%s", (order_id,)):
            continue
        item = fetch_one(
            cur,
            "SELECT title, price, image_url FROM items WHERE item_id=%s",
            (item_id,),
        )
        if not item:
            continue
        title, price, image_url = item
        subtotal = (price or 0) * qty
        # 幂等:(order_id,item_id) 已存在则跳过
        cur.execute(
            """
            INSERT INTO order_items (order_id, item_id, item_title, item_image, item_price, quantity, subtotal)
            SELECT %s, %s, %s, %s, %s, %s, %s
            FROM DUAL
            WHERE NOT EXISTS (
                SELECT 1 FROM order_items WHERE order_id=%s AND item_id=%s
            )
            """,
            (order_id, item_id, f"[{SEED}] {title}", image_url, price, qty, subtotal,
             order_id, item_id),
        )
        if cur.rowcount:
            inserted.append((order_id, item_id, cur.lastrowid))
    return inserted


def get_order_item_id(cur, order_id, item_id):
    row = fetch_one(
        cur,
        "SELECT id FROM order_items WHERE order_id=%s AND item_id=%s",
        (order_id, item_id),
    )
    return row[0] if row else None


# ---------------------------------------------------------------------------
# 2) reviews —— 给已完成订单的 order_item 加 approved 评论 (order_item_id 唯一)
# ---------------------------------------------------------------------------
# (order_id, item_id, rating, content)  user_token 取该 order 的下单人(一致)
REVIEW_PLAN = [
    (14, "M2-21f8be83", 5, f"[{SEED}] 这台 iPhone 17 Pro MAX 拍照和续航都很顶,物流也快,五星好评!"),
    (19, "M2-21f8be83", 4, f"[{SEED}] 手机整体不错,屏幕很细腻,扣一星是包装稍简单。"),
    (31, "M2-21f8be83", 5, f"[{SEED}] 用了一周很满意,系统流畅,推荐购买。"),
]


def seed_reviews(cur):
    inserted = []
    for order_id, item_id, rating, content in REVIEW_PLAN:
        oi_id = get_order_item_id(cur, order_id, item_id)
        if oi_id is None:
            continue
        # user_token = 该订单下单人(FK 一致)
        urow = fetch_one(cur, "SELECT user_token FROM orders WHERE id=%s", (order_id,))
        if not urow:
            continue
        user_token = urow[0]
        if not fetch_one(cur, "SELECT 1 FROM users WHERE user_token=%s", (user_token,)):
            continue
        # 幂等:order_item_id 唯一(表上 uk_order_item),已存在则跳过
        cur.execute(
            """
            INSERT INTO reviews (order_item_id, user_token, item_id, rating, content, status)
            SELECT %s, %s, %s, %s, %s, 'approved'
            FROM DUAL
            WHERE NOT EXISTS (SELECT 1 FROM reviews WHERE order_item_id=%s)
            """,
            (oi_id, user_token, item_id, rating, content, oi_id),
        )
        if cur.rowcount:
            inserted.append((oi_id, item_id, cur.lastrowid))
    return inserted


# ---------------------------------------------------------------------------
# 3) review_replies —— 商家对其中几条 review 回复 (review_id 唯一)
#    merchant_id 必须是该商品的归属商家(FK + 业务一致)
# ---------------------------------------------------------------------------
def seed_review_replies(cur):
    inserted = []
    # 取已存在的 SEED approved 评论(按 item 归属商家匹配),回前 2 条
    cur.execute(
        """
        SELECT r.id, r.item_id, i.merchant_id
        FROM reviews r
        JOIN items i ON i.item_id = r.item_id
        WHERE r.content LIKE %s AND i.merchant_id IS NOT NULL
        ORDER BY r.id
        LIMIT 2
        """,
        (f"[{SEED}]%",),
    )
    rows = cur.fetchall()
    contents = [
        f"[{SEED}] 感谢您的支持!很高兴您喜欢,有任何问题欢迎随时联系店铺客服。",
        f"[{SEED}] 谢谢反馈!包装问题我们已优化,后续会改进,期待再次光临。",
    ]
    for idx, (review_id, item_id, merchant_id) in enumerate(rows):
        if not fetch_one(cur, "SELECT 1 FROM merchants WHERE id=%s", (merchant_id,)):
            continue
        content = contents[idx % len(contents)]
        cur.execute(
            """
            INSERT INTO review_replies (review_id, merchant_id, content)
            SELECT %s, %s, %s
            FROM DUAL
            WHERE NOT EXISTS (SELECT 1 FROM review_replies WHERE review_id=%s)
            """,
            (review_id, merchant_id, content, review_id),
        )
        if cur.rowcount:
            inserted.append((review_id, merchant_id, cur.lastrowid))
    return inserted


# ---------------------------------------------------------------------------
# 4) announcements —— 1~2 条 published(created_by -> admin)  title 唯一(含 [SEED])
# ---------------------------------------------------------------------------
ANNOUNCEMENT_PLAN = [
    {
        "title": f"[{SEED}] 平台上线公告",
        "content": "欢迎来到 RecWeb 商城!新品持续上架,智能推荐已开启,祝您购物愉快。",
        "type": "notice",
        "sort_order": 10,
    },
    {
        "title": f"[{SEED}] 双十一大促预告",
        "content": "11.11 全场优惠即将开启,关注店铺第一时间抢券,数量有限先到先得。",
        "type": "banner",
        "sort_order": 20,
    },
]


def seed_announcements(cur, admin_id):
    inserted = []
    for a in ANNOUNCEMENT_PLAN:
        cur.execute(
            """
            INSERT INTO announcements (title, content, type, sort_order, status, published_at, created_by)
            SELECT %s, %s, %s, %s, 'published', NOW(), %s
            FROM DUAL
            WHERE NOT EXISTS (SELECT 1 FROM announcements WHERE title=%s)
            """,
            (a["title"], a["content"], a["type"], a["sort_order"], admin_id, a["title"]),
        )
        if cur.rowcount:
            inserted.append((a["title"], cur.lastrowid))
    return inserted


# ---------------------------------------------------------------------------
# 5) shops —— 给现有 merchants 各建/补店铺行 (merchant_id, name) 唯一
# ---------------------------------------------------------------------------
def seed_shops(cur):
    inserted = []
    cur.execute("SELECT id, username FROM merchants ORDER BY id")
    for merchant_id, username in cur.fetchall():
        name = f"[{SEED}] {username or 'Merchant'} 官方店"
        desc = "本店主营优质数码与生活好物,正品保障,售后无忧。(示例数据)"
        cur.execute(
            """
            INSERT INTO shops (merchant_id, name, description, status)
            SELECT %s, %s, %s, 'active'
            FROM DUAL
            WHERE NOT EXISTS (SELECT 1 FROM shops WHERE merchant_id=%s AND name=%s)
            """,
            (merchant_id, name, desc, merchant_id, name),
        )
        if cur.rowcount:
            inserted.append((merchant_id, name, cur.lastrowid))
    return inserted


# ---------------------------------------------------------------------------
# 6) admin_logs —— 几条审计(admin_id -> admins, detail JSON 含 seed_key 唯一)
# ---------------------------------------------------------------------------
ADMIN_LOG_PLAN = [
    {"admin_id": 1, "action": "publish_announcement", "target_type": "announcement",
     "target_id": "seed-ann-1", "seed_key": "seed-log-publish-ann"},
    {"admin_id": 1, "action": "approve_review", "target_type": "review",
     "target_id": "seed-rev", "seed_key": "seed-log-approve-review"},
    {"admin_id": 2, "action": "approve_merchant", "target_type": "merchant",
     "target_id": "2", "seed_key": "seed-log-approve-merchant"},
]


def seed_admin_logs(cur):
    inserted = []
    for log in ADMIN_LOG_PLAN:
        if not fetch_one(cur, "SELECT 1 FROM admins WHERE id=%s", (log["admin_id"],)):
            continue
        detail = json.dumps(
            {"seed": SEED, "seed_key": log["seed_key"], "note": "示例审计数据"},
            ensure_ascii=False,
        )
        # 幂等:detail 内 seed_key 唯一
        cur.execute(
            """
            INSERT INTO admin_logs (admin_id, action, target_type, target_id, detail, ip_address)
            SELECT %s, %s, %s, %s, %s, '127.0.0.1'
            FROM DUAL
            WHERE NOT EXISTS (
                SELECT 1 FROM admin_logs WHERE detail LIKE %s
            )
            """,
            (log["admin_id"], log["action"], log["target_type"], log["target_id"], detail,
             f'%"seed_key": "{log["seed_key"]}"%'),
        )
        if cur.rowcount:
            inserted.append((log["admin_id"], log["action"], cur.lastrowid))
    return inserted


# ---------------------------------------------------------------------------
# 7) 评分重算 —— 从实际 approved reviews 重算 items.rating/review_count,
#    一并修正 M2-21f8be83 的幽灵评分(有评分无评论 -> 与真实 reviews 一致)
# ---------------------------------------------------------------------------
def recompute_item_ratings(cur, item_ids):
    changed = []
    for item_id in sorted(set(item_ids)):
        row = fetch_one(
            cur,
            "SELECT COUNT(*), ROUND(AVG(rating),2) FROM reviews WHERE item_id=%s AND status='approved'",
            (item_id,),
        )
        cnt = row[0] or 0
        avg = row[1]  # None 当无评论
        before = fetch_one(cur, "SELECT rating, review_count FROM items WHERE item_id=%s", (item_id,))
        cur.execute(
            "UPDATE items SET rating=%s, review_count=%s WHERE item_id=%s",
            (avg, cnt, item_id),
        )
        after = (avg, cnt)
        changed.append((item_id, before, after))
    return changed


def main():
    env = load_env()
    cn = connect(env)
    cur = cn.cursor()
    summary = {}
    try:
        # 选一个存在的 admin 作为公告作者
        admin_row = fetch_one(cur, "SELECT id FROM admins ORDER BY id LIMIT 1")
        admin_id = admin_row[0] if admin_row else None

        summary["order_items"] = seed_order_items(cur)
        summary["reviews"] = seed_reviews(cur)
        summary["review_replies"] = seed_review_replies(cur)
        summary["announcements"] = seed_announcements(cur, admin_id)
        summary["shops"] = seed_shops(cur)
        summary["admin_logs"] = seed_admin_logs(cur)

        # 重算受影响商品(本轮评论涉及的 item + 幽灵评分商品 M2-21f8be83)
        touched_items = {item_id for (_, item_id, _) in summary["reviews"]}
        touched_items.add("M2-21f8be83")  # 显式纳入幽灵评分商品
        summary["rating_recompute"] = recompute_item_ratings(cur, touched_items)

        cn.commit()
    except Exception:
        cn.rollback()
        raise
    finally:
        cur.close()
        cn.close()

    # 打印执行摘要(本次新增了什么;重跑时各项为空表示已存在=幂等)
    print("=== SEED SUMMARY (本次新增;重跑时为空=幂等无增量) ===")
    for k in ["order_items", "reviews", "review_replies", "announcements", "shops", "admin_logs"]:
        print(f"  {k}: +{len(summary[k])}  {summary[k]}")
    print("  rating_recompute (item_id, before(rating,cnt), after(rating,cnt)):")
    for it, before, after in summary["rating_recompute"]:
        print(f"    {it}: {before} -> {after}")


if __name__ == "__main__":
    main()
