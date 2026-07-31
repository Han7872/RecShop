"""
从真实数据自动构建 LLM Rerank 评测样本。

数据来源（两种模式）：
  --source file  (默认) 从原始数据文件读取（electronics.inter + electronics.item）
  --source db           从 MySQL 数据库读取（仅当 DB 中有足够用户时有用）

流程（Leave-One-Out）：
1. 获取有足够历史的用户及其按时间排序的交互序列
2. 最后一个商品作为 ground_truth，前面的作为 user_history
3. 调用 SASRec /score/sampled 接口获取候选列表（保证 ground_truth 在其中）
4. 输出为 sample_eval_data.json

前置条件：
- SASRec API 在运行（默认 http://127.0.0.1:8000）
- file 模式需要 electronics.inter 和 electronics.item 文件
- db 模式需要 MySQL 数据库可用

用法：
    python build_eval_data.py
    python build_eval_data.py --num_samples 30 --output sample_eval_data.json
    python build_eval_data.py --source db --min_history 5
"""

import argparse
import csv
import json
import os
import sys
import random
from collections import defaultdict
from pathlib import Path

import requests

# 加载 .env
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

SASREC_API_URL = os.getenv("SASREC_API_URL", "http://127.0.0.1:8000")

# 默认数据文件路径
_DEFAULT_INTER_FILE = str(
    _PROJECT_ROOT / "services" / "recommendation_agent" / "electronics.inter"
)
_DEFAULT_ITEM_FILE = str(
    _PROJECT_ROOT / "shared" / "data" / "electronics.item"
)


# ============================================================
# 数据来源 A：从原始文件读取（默认）
# ============================================================

def load_item_titles(item_file: str) -> dict:
    """
    从 electronics.item（TSV）加载商品标题。
    返回 {item_id: title, ...}
    """
    titles = {}
    path = Path(item_file)
    if not path.exists():
        print(f"  [WARN] 商品文件不存在: {item_file}，标题将为空")
        return titles

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # RecBole 格式的列名可能带 :token 后缀
            item_id = _get_field(row, "item_id")
            title = _get_field(row, "title") or f"Product_{item_id}"
            if item_id:
                if len(str(title)) > 80:
                    title = str(title)[:77] + "..."
                titles[item_id] = title

    print(f"  已加载 {len(titles)} 个商品标题")
    return titles


def load_user_sequences(inter_file: str, min_history: int) -> dict:
    """
    从 electronics.inter（TSV）加载用户交互序列。

    返回 {user_id: [(timestamp, item_id), ...], ...}
    只保留交互数 >= min_history 的用户，序列已按时间升序排列。
    """
    path = Path(inter_file)
    if not path.exists():
        print(f"  [错误] 交互文件不存在: {inter_file}")
        sys.exit(1)

    by_user = defaultdict(list)

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            user_id = _get_field(row, "user_id")
            item_id = _get_field(row, "item_id")
            ts_str = _get_field(row, "timestamp")
            if not user_id or not item_id or not ts_str:
                continue
            try:
                ts = float(ts_str)
            except ValueError:
                continue
            by_user[user_id].append((ts, item_id))

    # 过滤 + 排序
    eligible = {}
    for uid, events in by_user.items():
        if len(events) >= min_history:
            events.sort(key=lambda x: x[0])
            eligible[uid] = events

    print(f"  交互文件共 {len(by_user)} 个用户，"
          f"其中 {len(eligible)} 个交互 >= {min_history}")
    return eligible


def _get_field(row: dict, base_name: str) -> str:
    """
    从 CSV 行中获取字段值。
    兼容 RecBole 格式（列名可能是 'item_id:token' 而非 'item_id'）。
    """
    # 精确匹配
    if base_name in row:
        return str(row[base_name]).strip()
    # 带类型后缀匹配（如 'item_id:token'）
    for key in row:
        if key.split(":")[0].strip() == base_name:
            return str(row[key]).strip()
    return ""


# ============================================================
# 数据来源 B：从 MySQL 数据库读取
# ============================================================

def get_db_connection():
    import mysql.connector
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "shopify2"),
    }
    return mysql.connector.connect(**db_config)


def load_user_sequences_from_db(min_history: int) -> tuple:
    """
    从 MySQL 加载用户序列和标题。
    返回 (user_sequences, item_titles)
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # 加载标题
    cursor.execute("SELECT item_id, title FROM items")
    titles = {row["item_id"]: row["title"] for row in cursor.fetchall()}

    # 加载交互
    cursor.execute("""
        SELECT user_token, item_id, timestamp
        FROM interactions
        ORDER BY user_token, timestamp ASC
    """)

    by_user = defaultdict(list)
    for row in cursor.fetchall():
        by_user[row["user_token"]].append((float(row["timestamp"]), row["item_id"]))

    cursor.close()
    conn.close()

    eligible = {uid: events for uid, events in by_user.items()
                if len(events) >= min_history}

    print(f"  数据库共 {len(by_user)} 个用户，"
          f"其中 {len(eligible)} 个交互 >= {min_history}")
    return eligible, titles


# ============================================================
# SASRec API 调用
# ============================================================

def call_sasrec_sampled(
    item_sequence: list,
    target_item: str,
    num_negatives: int = 9,
) -> dict | None:
    """
    调用 SASRec /score/sampled 接口。
    返回完整响应 dict，或 None（失败时）。
    """
    url = f"{SASREC_API_URL}/score/sampled"
    payload = {
        "item_sequence": item_sequence,
        "target_item": target_item,
        "num_negatives": num_negatives,
        "exclude_history": True,
        "return_candidates": True,
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            print(f"[WARN] SASRec 返回 {resp.status_code}: {resp.text[:100]}")
            return None

        data = resp.json()
        if not data.get("success") or not data.get("target_valid"):
            print(f"[SKIP] target 不在模型词表中")
            return None

        return data

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] SASRec 请求失败: {e}")
        return None


# ============================================================
# 样本构建（通用，不依赖数据来源）
# ============================================================

def build_one_sample(
    user_id: str,
    sequence: list,
    item_titles: dict,
    sample_id: str,
    min_history: int,
    num_candidates: int,
    history_for_prompt: int,
) -> dict | None:
    """
    为一个用户构建一条评测样本。

    Args:
        user_id: 用户标识
        sequence: [(timestamp, item_id), ...] 按时间升序
        item_titles: {item_id: title, ...}
        其余为构建参数

    返回 sample dict 或 None。
    """
    if len(sequence) < min_history + 1:
        return None

    # 提取 item_id 序列
    item_ids = [iid for _, iid in sequence]

    # ground truth = 最后一个
    gt_id = item_ids[-1]
    gt_title = item_titles.get(gt_id, f"Product_{gt_id}")

    # SASRec 输入 = 除最后一个之外的所有 item_id
    history_ids = item_ids[:-1]

    # 给 LLM 看的 user_history（最后 N 条，带 title）
    prompt_ids = history_ids[-history_for_prompt:]
    user_history = [
        {"item_id": iid, "title": item_titles.get(iid, f"Product_{iid}")}
        for iid in prompt_ids
    ]

    # 调用 SASRec
    result = call_sasrec_sampled(
        item_sequence=history_ids,
        target_item=gt_id,
        num_negatives=num_candidates - 1,
    )
    if result is None:
        return None

    candidates_raw = result.get("candidates", [])
    if not candidates_raw:
        print(f"[SKIP] 候选列表为空")
        return None

    # 转换格式
    candidates = []
    for c in candidates_raw:
        candidates.append({
            "item_id": c["item_id"],
            "title": c.get("title") or item_titles.get(c["item_id"], ""),
            "score": round(c["score"], 4),
        })

    # 确认 ground truth 在候选中
    cand_ids = {c["item_id"] for c in candidates}
    if gt_id not in cand_ids:
        print(f"[SKIP] ground_truth 不在候选列表中")
        return None

    return {
        "sample_id": sample_id,
        "user_token": user_id,
        "user_history": user_history,
        "candidates": candidates,
        "ground_truth_item_id": gt_id,
    }


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="从真实数据构建 LLM Rerank 评测样本")
    parser.add_argument(
        "--source", choices=["file", "db"], default="file",
        help="数据来源: file=原始数据文件(默认), db=MySQL数据库",
    )
    parser.add_argument(
        "--inter_file", default=_DEFAULT_INTER_FILE,
        help="交互文件路径（file 模式）",
    )
    parser.add_argument(
        "--item_file", default=_DEFAULT_ITEM_FILE,
        help="商品文件路径（file 模式）",
    )
    parser.add_argument(
        "--num_samples", "-n", type=int, default=30,
        help="目标样本数（默认 30）",
    )
    parser.add_argument(
        "--output", "-o", default="sample_eval_data.json",
        help="输出文件路径（默认 sample_eval_data.json）",
    )
    parser.add_argument(
        "--min_history", type=int, default=5,
        help="用户最少交互数（默认 5）",
    )
    parser.add_argument(
        "--num_candidates", type=int, default=10,
        help="候选商品数（默认 10）",
    )
    parser.add_argument(
        "--history_for_prompt", type=int, default=10,
        help="给 LLM 看的最近历史条数（默认 10）",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="随机种子（默认 42）",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    required_min = args.min_history + 1  # 至少 min_history + 1 条（留 1 条做 ground truth）

    print("=" * 55)
    print("    构建 LLM Rerank 评测样本")
    print("=" * 55)
    print(f"  数据来源:      {args.source}")
    print(f"  目标样本数:    {args.num_samples}")
    print(f"  最少历史:      {args.min_history}")
    print(f"  候选数:        {args.num_candidates}")
    print(f"  Prompt 历史:   {args.history_for_prompt}")
    print(f"  SASRec API:    {SASREC_API_URL}")
    print(f"  输出:          {args.output}")
    print("=" * 55)

    # 1. 加载数据
    print(f"\n[1/3] 加载数据（{args.source} 模式）...")

    if args.source == "file":
        print(f"  交互文件: {args.inter_file}")
        print(f"  商品文件: {args.item_file}")
        item_titles = load_item_titles(args.item_file)
        user_sequences = load_user_sequences(args.inter_file, required_min)
    else:
        user_sequences, item_titles = load_user_sequences_from_db(required_min)

    if not user_sequences:
        print("[错误] 没有找到符合条件的用户。")
        sys.exit(1)

    # 随机抽取用户（多取一些，因为部分可能构建失败）
    all_user_ids = list(user_sequences.keys())
    random.shuffle(all_user_ids)
    pool = all_user_ids[:args.num_samples * 3]
    print(f"  候选用户池: {len(pool)} 个")

    # 2. 逐个构建样本
    print(f"\n[2/3] 逐个构建样本（目标 {args.num_samples} 条）...")
    samples = []
    attempted = 0

    for user_id in pool:
        if len(samples) >= args.num_samples:
            break

        attempted += 1
        sid = str(len(samples) + 1)
        uid_short = user_id[:16] + ("..." if len(user_id) > 16 else "")
        print(f"  [{len(samples) + 1}/{args.num_samples}] 用户 {uid_short}", end=" ")

        sample = build_one_sample(
            user_id=user_id,
            sequence=user_sequences[user_id],
            item_titles=item_titles,
            sample_id=sid,
            min_history=args.min_history,
            num_candidates=args.num_candidates,
            history_for_prompt=args.history_for_prompt,
        )

        if sample is not None:
            samples.append(sample)
            gt = sample["ground_truth_item_id"]
            ranks = [c["item_id"] for c in sample["candidates"]]
            gt_rank = ranks.index(gt) + 1 if gt in ranks else "?"
            print(f"=> rank={gt_rank}/{len(ranks)}")
        else:
            print("=> skip")

    if not samples:
        print("\n[错误] 未能构建任何样本，请检查 SASRec API 是否正常运行。")
        sys.exit(1)

    # 3. 保存
    print(f"\n[3/3] 保存 {len(samples)} 条样本到 {args.output} ...")
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(samples, f, ensure_ascii=False, indent=2)

    # 汇总
    print("\n" + "=" * 55)
    print("    构建完成!")
    print("=" * 55)
    print(f"  成功样本:    {len(samples)}")
    print(f"  尝试用户:    {attempted}")
    print(f"  成功率:      {len(samples) / attempted * 100:.1f}%")

    rank_dist = {}
    for s in samples:
        gt = s["ground_truth_item_id"]
        ids = [c["item_id"] for c in s["candidates"]]
        r = ids.index(gt) + 1 if gt in ids else -1
        bucket = f"rank{r}" if r <= 3 else "rank4+"
        rank_dist[bucket] = rank_dist.get(bucket, 0) + 1

    print("  Ground truth rank 分布:")
    for key in ["rank1", "rank2", "rank3", "rank4+"]:
        if key in rank_dist:
            print(f"    {key:10s}  {rank_dist[key]}")

    print("=" * 55)
    print(f"\n下一步：python evaluate_rerank.py --input {args.output}")


if __name__ == "__main__":
    main()
