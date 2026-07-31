"""
LLM Rerank 离线评测脚本。

用法：
    # 模式 1（推荐）：直接调用 reranker，无需启动 Flask 服务
    python evaluate_rerank.py --direct
    python evaluate_rerank.py --direct --input sample_eval_data.json

    # 模式 2：通过 HTTP 调用已运行的 Flask 服务
    python evaluate_rerank.py --url http://127.0.0.1:5002/rerank

支持 JSON（数组）和 JSONL（每行一个 JSON）两种输入格式。
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import requests

# 项目根目录 & .env
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")


# ============================================================
# 数据加载
# ============================================================

def load_samples(filepath: str) -> list:
    """加载评测样本，支持 .json 和 .jsonl 格式。"""
    path = Path(filepath)
    if not path.exists():
        print(f"[错误] 文件不存在: {filepath}")
        sys.exit(1)

    text = path.read_text(encoding="utf-8").strip()

    # 尝试 JSON 数组
    if text.startswith("["):
        samples = json.loads(text)
    else:
        # JSONL: 每行一个 JSON 对象
        samples = [json.loads(line) for line in text.splitlines() if line.strip()]

    # 基本校验
    for i, s in enumerate(samples):
        for key in ("sample_id", "user_history", "candidates", "ground_truth_item_id"):
            if key not in s:
                print(f"[错误] 样本 {i} 缺少字段: {key}")
                sys.exit(1)

    print(f"已加载 {len(samples)} 条样本 ({filepath})")
    return samples


# ============================================================
# 单条评测
# ============================================================

def evaluate_one_direct(sample: dict) -> dict:
    """直接调用 reranker.rerank()，无需 Flask 服务。"""
    from reranker import rerank

    sample_id = sample["sample_id"]
    candidates = sample["candidates"]
    ground_truth = str(sample["ground_truth_item_id"]).strip()
    sasrec_top1 = str(candidates[0]["item_id"]).strip()
    cand_ids = [str(c["item_id"]).strip() for c in candidates]

    row = {
        "sample_id": sample_id,
        "ground_truth_item_id": ground_truth,
        "sasrec_top1_item_id": sasrec_top1,
        "llm_selected_item_id": "",
        "llm_selected_title": "",
        "llm_reason": "",
        "llm_source": "",
        "llm_success": False,
        "llm_latency_ms": 0,
        "sasrec_hit_at_1": (ground_truth == sasrec_top1),
        "llm_hit_at_1": False,
        "llm_selected_rank_in_candidates": -1,
    }

    t0 = time.time()
    try:
        result = rerank(sample["user_history"], candidates)
        latency_ms = round((time.time() - t0) * 1000)
        row["llm_latency_ms"] = latency_ms

        selected_id = str(result.get("selected_item_id", "")).strip()
        row["llm_selected_item_id"] = selected_id
        row["llm_selected_title"] = result.get("selected_title", "")
        row["llm_reason"] = result.get("reason", "")
        row["llm_source"] = result.get("source", "")
        row["llm_success"] = True
        row["llm_hit_at_1"] = (selected_id == ground_truth)

        if selected_id in cand_ids:
            row["llm_selected_rank_in_candidates"] = cand_ids.index(selected_id) + 1

    except Exception as e:
        latency_ms = round((time.time() - t0) * 1000)
        row["llm_latency_ms"] = latency_ms
        row["llm_reason"] = f"rerank 异常: {type(e).__name__}: {e}"

    return row


def evaluate_one(sample: dict, url: str) -> dict:
    """通过 HTTP 调用 rerank 接口，返回评测记录。"""
    sample_id = sample["sample_id"]
    candidates = sample["candidates"]
    ground_truth = str(sample["ground_truth_item_id"]).strip()

    # SASRec top-1（候选列表第一个）
    sasrec_top1 = str(candidates[0]["item_id"]).strip()

    # 候选 item_id 列表（用于计算 rank）
    cand_ids = [str(c["item_id"]).strip() for c in candidates]

    # 调用 rerank 接口
    payload = {
        "user_history": sample["user_history"],
        "candidates": candidates,
    }

    row = {
        "sample_id": sample_id,
        "ground_truth_item_id": ground_truth,
        "sasrec_top1_item_id": sasrec_top1,
        "llm_selected_item_id": "",
        "llm_selected_title": "",
        "llm_reason": "",
        "llm_source": "",
        "llm_success": False,
        "llm_latency_ms": 0,
        "sasrec_hit_at_1": (ground_truth == sasrec_top1),
        "llm_hit_at_1": False,
        "llm_selected_rank_in_candidates": -1,
    }

    t0 = time.time()
    try:
        resp = requests.post(url, json=payload, timeout=60)
        latency_ms = round((time.time() - t0) * 1000)
        row["llm_latency_ms"] = latency_ms

        data = resp.json()
        if data.get("success") and data.get("result"):
            result = data["result"]
            selected_id = str(result.get("selected_item_id", "")).strip()

            row["llm_selected_item_id"] = selected_id
            row["llm_selected_title"] = result.get("selected_title", "")
            row["llm_reason"] = result.get("reason", "")
            row["llm_source"] = result.get("source", "")
            row["llm_success"] = True
            row["llm_hit_at_1"] = (selected_id == ground_truth)

            if selected_id in cand_ids:
                row["llm_selected_rank_in_candidates"] = cand_ids.index(selected_id) + 1
        else:
            err_msg = data.get("error", "unknown error")
            row["llm_reason"] = err_msg
            # 打印服务端错误便于调试
            sys.stderr.write(f"\n  [服务端错误] {err_msg}\n")

    except Exception as e:
        latency_ms = round((time.time() - t0) * 1000)
        row["llm_latency_ms"] = latency_ms
        row["llm_reason"] = f"请求异常: {e}"

    return row


# ============================================================
# 结果保存
# ============================================================

CSV_COLUMNS = [
    "sample_id",
    "ground_truth_item_id",
    "sasrec_top1_item_id",
    "llm_selected_item_id",
    "llm_selected_title",
    "llm_reason",
    "llm_source",
    "llm_success",
    "llm_latency_ms",
    "sasrec_hit_at_1",
    "llm_hit_at_1",
    "llm_selected_rank_in_candidates",
]


def save_csv(rows: list, filepath: str):
    """将评测结果保存为 CSV。"""
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n结果已保存: {filepath}")


# ============================================================
# 汇总统计
# ============================================================

def print_summary(rows: list):
    """在控制台打印评测汇总。"""
    total = len(rows)
    if total == 0:
        print("无评测结果。")
        return

    success_count = sum(1 for r in rows if r["llm_success"])
    fallback_count = sum(1 for r in rows if r["llm_source"] == "fallback")
    llm_count = sum(1 for r in rows if r["llm_source"] == "llm")

    latencies = [r["llm_latency_ms"] for r in rows if r["llm_success"]]
    avg_latency = round(sum(latencies) / len(latencies)) if latencies else 0

    sasrec_hits = sum(1 for r in rows if r["sasrec_hit_at_1"])
    llm_hits = sum(1 for r in rows if r["llm_hit_at_1"])

    # rank 分布
    rank_dist = {}
    for r in rows:
        rank = r["llm_selected_rank_in_candidates"]
        if rank == -1:
            key = "failed"
        elif rank <= 3:
            key = f"rank{rank}"
        else:
            key = "rank4+"
        rank_dist[key] = rank_dist.get(key, 0) + 1

    # 选择与 SASRec top-1 相同的次数
    same_as_sasrec = sum(
        1 for r in rows
        if r["llm_success"] and r["llm_selected_item_id"] == r["sasrec_top1_item_id"]
    )

    print("\n" + "=" * 55)
    print("             LLM Rerank 离线评测汇总")
    print("=" * 55)
    print(f"  样本总数:              {total}")
    print(f"  LLM 调用成功:          {success_count} ({_pct(success_count, total)})")
    print(f"    - source=llm:        {llm_count}")
    print(f"    - source=fallback:   {fallback_count}")
    print(f"  平均响应时间:          {avg_latency} ms")
    print("-" * 55)
    print(f"  SASRec Hit@1:          {sasrec_hits}/{total} ({_pct(sasrec_hits, total)})")
    print(f"  LLM Hit@1:             {llm_hits}/{total} ({_pct(llm_hits, total)})")
    print("-" * 55)
    print(f"  LLM 选中 == SASRec #1: {same_as_sasrec}/{success_count} ({_pct(same_as_sasrec, success_count)})")
    print("-" * 55)
    print("  LLM 选择的 rank 分布:")
    for key in ["rank1", "rank2", "rank3", "rank4+", "failed"]:
        if key in rank_dist:
            print(f"    {key:10s}  {rank_dist[key]}")
    print("=" * 55)


def _pct(n, total):
    """百分比字符串。"""
    if total == 0:
        return "N/A"
    return f"{n / total * 100:.1f}%"


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="LLM Rerank 离线评测")
    parser.add_argument(
        "--input", "-i",
        default="sample_eval_data.json",
        help="输入样本文件路径 (JSON 或 JSONL)，默认 sample_eval_data.json",
    )
    parser.add_argument(
        "--output", "-o",
        default="rerank_eval_results.csv",
        help="输出 CSV 路径，默认 rerank_eval_results.csv",
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:5002/rerank",
        help="rerank 服务地址，默认 http://127.0.0.1:5002/rerank",
    )
    parser.add_argument(
        "--direct", "-d",
        action="store_true",
        help="直接调用 reranker.rerank()，无需启动 Flask 服务（推荐）",
    )
    args = parser.parse_args()

    if args.direct:
        print("[模式] 直接调用 reranker（无需 Flask 服务）")

    # 1. 加载样本
    samples = load_samples(args.input)

    # 2. 逐条评测
    rows = []
    for i, sample in enumerate(samples):
        sid = sample["sample_id"]
        print(f"[{i + 1}/{len(samples)}] 评测样本 {sid} ...", end=" ", flush=True)
        row = evaluate_one_direct(sample) if args.direct else evaluate_one(sample, args.url)
        hit = "✓" if row["llm_hit_at_1"] else "✗"
        src = row["llm_source"] or "error"
        print(f"→ {src} | rank={row['llm_selected_rank_in_candidates']} | hit={hit} | {row['llm_latency_ms']}ms")
        rows.append(row)

    # 3. 保存 CSV
    save_csv(rows, args.output)

    # 4. 打印汇总
    print_summary(rows)


if __name__ == "__main__":
    main()
