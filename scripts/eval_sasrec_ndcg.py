"""
SASRec Sampled-Ranking 离线评估脚本
===================================
评估指标: NDCG@10, Hit@10 (sampled ranking, 99 negatives)
评估协议: Leave-One-Out — 每个用户取最长历史序列作为 test sample,
         target 为该序列的下一个 item, 从剩余 item 中随机采样 99 个负样本,
         在 [1 正 + 99 负] 候选集上排序, 计算 NDCG@10 和 Hit@10.

用法:
    python scripts/eval_sasrec_ndcg.py
    python scripts/eval_sasrec_ndcg.py --num-negatives 199 --top-k 20
    python scripts/eval_sasrec_ndcg.py --max-users 500  # 只评估前 500 个用户(快速验证)
"""

import argparse
import math
import os
import pickle
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

import torch
import numpy as np

# ==================== 路径设置 ====================

ROOT = Path(__file__).resolve().parent.parent
SASREC_DIR = ROOT / "services" / "sasrec_api"

# 加载 .env
from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=False)

# 确保 vendor 在 sys.path 中
VENDOR_PATH = SASREC_DIR / "vendor"
if str(VENDOR_PATH) not in sys.path:
    sys.path.insert(0, str(VENDOR_PATH))

# ==================== 工具函数 ====================

def _color(text: str, code: int) -> str:
    return f"\033[{code}m{text}\033[0m"

def ndcg_at_k(rank: int, k: int) -> float:
    """计算单个样本的 NDCG@K (binary relevance)"""
    if rank <= k:
        return 1.0 / math.log2(rank + 1)
    return 0.0

def hit_at_k(rank: int, k: int) -> float:
    """计算单个样本的 Hit@K"""
    return 1.0 if rank <= k else 0.0

# ==================== 主评估逻辑 ====================

def main():
    parser = argparse.ArgumentParser(description="SASRec Sampled-Ranking NDCG@K 评估")
    parser.add_argument("--num-negatives", type=int, default=99,
                        help="每个 test sample 采样的负样本数量 (default: 99)")
    parser.add_argument("--top-k", type=int, default=10,
                        help="计算 NDCG@K 和 Hit@K 的 K 值 (default: 10)")
    parser.add_argument("--max-users", type=int, default=None,
                        help="最多评估多少个用户 (默认全部)")
    parser.add_argument("--seed", type=int, default=42,
                        help="随机种子 (default: 42)")
    parser.add_argument("--batch-size", type=int, default=256,
                        help="批量推理大小 (default: 256)")
    parser.add_argument("--cache-path", type=str, default=None,
                        help="缓存文件路径 (默认自动定位)")
    parser.add_argument("--model-path", type=str, default=None,
                        help="模型权重路径 (默认自动定位)")
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    # Windows ANSI 颜色支持
    if sys.platform == "win32":
        os.system("")

    print("=" * 65)
    print(_color("  SASRec Sampled-Ranking 评估", 1))
    print("=" * 65)

    # ---------- 1. 加载缓存和模型 ----------
    cache_file = Path(args.cache_path) if args.cache_path else \
        SASREC_DIR / os.environ.get("SASREC_CACHE_PATH", "standard_cache.pkl")
    model_path = Path(args.model_path) if args.model_path else \
        SASREC_DIR / os.environ.get("SASREC_MODEL_PATH", "SASRec-Feb-24-2026_17-54-22.pth")
    device = "cuda" if torch.cuda.is_available() else "cpu"

    print(f"\n  缓存文件  : {cache_file}")
    print(f"  模型权重  : {model_path}")
    print(f"  设备      : {device}")
    print(f"  负样本数  : {args.num_negatives}")
    print(f"  Top-K     : {args.top_k}")
    print(f"  随机种子  : {args.seed}")
    print(f"  批大小    : {args.batch_size}")
    print()

    if not cache_file.exists():
        print(_color(f"[ERROR] 缓存文件不存在: {cache_file}", 31))
        sys.exit(1)
    if not model_path.exists():
        print(_color(f"[ERROR] 模型权重不存在: {model_path}", 31))
        sys.exit(1)

    print("加载缓存数据 ...")
    t0 = time.time()
    with open(str(cache_file), "rb") as f:
        cache_data = pickle.load(f)
    config = cache_data["config"]
    dataset = cache_data["dataset"]
    print(f"  缓存加载完成 ({time.time() - t0:.1f}s)")
    print(f"  用户数: {dataset.user_num}  物品数: {dataset.item_num}")

    print("加载模型权重 ...")
    t0 = time.time()
    from recbole.model.sequential_recommender.sasrec import SASRec
    model = SASRec(config, dataset)
    checkpoint = torch.load(str(model_path), map_location=device)
    model.load_state_dict(checkpoint["state_dict"])
    model = model.to(device)
    model.eval()
    print(f"  模型加载完成 ({time.time() - t0:.1f}s)")

    # ---------- 2. 提取 test samples (Leave-One-Out: 每用户取最长历史) ----------
    print("\n提取测试样本 (Leave-One-Out) ...")

    inter = dataset.inter_feat
    uid_field = dataset.uid_field       # e.g. "user_id"
    iid_field = dataset.iid_field       # e.g. "item_id"
    iid_list_field = iid_field + config["LIST_SUFFIX"]  # e.g. "item_id_list"
    length_field = config["ITEM_LIST_LENGTH_FIELD"]      # e.g. "item_list_length"

    user_ids = inter[uid_field].numpy()
    item_ids = inter[iid_field].numpy()             # target items
    item_seqs = inter[iid_list_field].numpy()       # history sequences (padded)
    seq_lengths = inter[length_field].numpy()

    # 对每个用户，选取 seq_length 最大的 sample 作为 test
    user_best = {}  # uid -> (index, seq_length)
    for idx in range(len(user_ids)):
        uid = int(user_ids[idx])
        length = int(seq_lengths[idx])
        if uid not in user_best or length > user_best[uid][1]:
            user_best[uid] = (idx, length)

    test_indices = [v[0] for v in user_best.values()]
    test_indices.sort()

    if args.max_users is not None and len(test_indices) > args.max_users:
        random.shuffle(test_indices)
        test_indices = sorted(test_indices[:args.max_users])

    n_test = len(test_indices)
    print(f"  测试样本数: {n_test}")

    # ---------- 3. 构建每用户的历史 item 集合 (用于排除采样) ----------
    # 收集每个用户在所有 augmented samples 中出现过的 items
    print("构建用户历史索引 ...")
    user_history = defaultdict(set)
    for idx in range(len(user_ids)):
        uid = int(user_ids[idx])
        target = int(item_ids[idx])
        user_history[uid].add(target)
        seq = item_seqs[idx]
        length = int(seq_lengths[idx])
        for j in range(length):
            user_history[uid].add(int(seq[j]))

    # ---------- 4. 批量评估 ----------
    print(f"\n开始评估 ({n_test} 个用户, batch_size={args.batch_size}) ...\n")

    max_seq_len = config["MAX_ITEM_LIST_LENGTH"]
    total_items = dataset.item_num  # 包含 padding=0
    num_neg = args.num_negatives
    top_k = args.top_k

    ndcg_scores = []
    hit_scores = []
    skipped = 0

    t_start = time.time()

    for batch_start in range(0, n_test, args.batch_size):
        batch_end = min(batch_start + args.batch_size, n_test)
        batch_indices = test_indices[batch_start:batch_end]
        B = len(batch_indices)

        # 准备 batch tensor
        batch_item_seq = torch.zeros(B, max_seq_len, dtype=torch.long)
        batch_seq_len = torch.zeros(B, dtype=torch.long)
        batch_targets = []          # internal id of target item
        batch_candidates = []       # list of [target_id, neg1, neg2, ...] per sample
        valid_mask = []

        for i, idx in enumerate(batch_indices):
            uid = int(user_ids[idx])
            target_id = int(item_ids[idx])
            seq = item_seqs[idx]
            length = int(seq_lengths[idx])

            # 填充 item sequence
            batch_item_seq[i, :length] = torch.LongTensor(seq[:length].astype(np.int64))
            batch_seq_len[i] = length

            # 采样负样本 (排除 padding + 用户全部历史)
            exclude = user_history[uid] | {0}
            available = [j for j in range(1, total_items) if j not in exclude]

            if len(available) < num_neg:
                # 物品不足，跳过
                valid_mask.append(False)
                batch_targets.append(target_id)
                batch_candidates.append([target_id])
                continue

            neg_ids = random.sample(available, num_neg)
            candidates = [target_id] + neg_ids
            batch_targets.append(target_id)
            batch_candidates.append(candidates)
            valid_mask.append(True)

        # full_sort_predict: 获取所有 item 的分数
        interaction = {
            "item_id_list": batch_item_seq,
            "item_length": batch_seq_len,
            "user_id": torch.zeros(B, dtype=torch.long),  # dummy
        }
        for key in interaction:
            interaction[key] = interaction[key].to(device)

        with torch.no_grad():
            all_scores = model.full_sort_predict(interaction)  # (B, item_num)

        # 逐样本计算排名
        for i in range(B):
            if not valid_mask[i]:
                skipped += 1
                continue

            target_id = batch_targets[i]
            candidates = batch_candidates[i]
            scores = all_scores[i][candidates].cpu().numpy()

            # 降序排序，找 target 的排名 (target 在 candidates[0])
            ranked_indices = np.argsort(-scores)
            rank = int(np.where(ranked_indices == 0)[0][0]) + 1  # 1-indexed

            ndcg_scores.append(ndcg_at_k(rank, top_k))
            hit_scores.append(hit_at_k(rank, top_k))

        # 进度打印
        done = batch_end
        elapsed = time.time() - t_start
        speed = done / elapsed if elapsed > 0 else 0
        eta = (n_test - done) / speed if speed > 0 else 0
        bar_len = 30
        filled = int(bar_len * done / n_test)
        bar = "█" * filled + "░" * (bar_len - filled)
        current_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0
        current_hit = np.mean(hit_scores) if hit_scores else 0
        print(f"\r  [{bar}] {done}/{n_test}  "
              f"NDCG@{top_k}={current_ndcg:.4f}  Hit@{top_k}={current_hit:.4f}  "
              f"ETA={eta:.0f}s", end="", flush=True)

    print()

    # ---------- 5. 汇总结果 ----------
    elapsed_total = time.time() - t_start
    mean_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0
    mean_hit = np.mean(hit_scores) if hit_scores else 0
    evaluated = len(ndcg_scores)

    print()
    print("=" * 65)
    print(_color("  评估结果", 32))
    print("=" * 65)
    print(f"  评估用户数        : {evaluated} / {n_test} (跳过 {skipped})")
    print(f"  负样本数 (per user): {num_neg}")
    print(f"  总耗时            : {elapsed_total:.1f}s")
    print()
    print(f"  {_color(f'NDCG@{top_k}', 36)}  = {_color(f'{mean_ndcg:.4f}', 1)}")
    print(f"  {_color(f'Hit@{top_k}', 36)}   = {_color(f'{mean_hit:.4f}', 1)}")
    print()
    print("=" * 65)

    # 可选：额外统计
    if ndcg_scores:
        print(f"\n  NDCG@{top_k} 分布:")
        zeros = sum(1 for s in ndcg_scores if s == 0)
        perfect = sum(1 for s in ndcg_scores if s == 1.0)
        print(f"    NDCG=0 (target 不在 top-{top_k}): {zeros} ({100*zeros/evaluated:.1f}%)")
        print(f"    NDCG=1 (target 排名第 1)        : {perfect} ({100*perfect/evaluated:.1f}%)")
        print(f"    中位数                           : {np.median(ndcg_scores):.4f}")
        print(f"    标准差                           : {np.std(ndcg_scores):.4f}")
        print()


if __name__ == "__main__":
    main()
