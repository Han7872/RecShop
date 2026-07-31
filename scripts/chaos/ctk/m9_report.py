# -*- coding: utf-8 -*-
"""m9_report.py — (project docs)/ 全部产物的【唯一存活生成器】(单入口、进仓、可复现)。

背景(这个脚本是在还债):
  (project docs)/{BASELINES-*.md, per_case_scores.csv, _baselines.json} 的数字最初是由
  scratchpad 里一堆一次性临时脚本跑出来的(base_run.py / base_agg.py / vfy_main.py /
  verify_s32.py ...),数字发出去了却没有存活的生成器。本脚本把那条链路收敛成一个入口。

输入 → 输出
  输入: n5_raw.jsonl(BARO + RCD×5seed 的【排名】,由 scratchpad/n5_run.py 产出;
        一行一 case,含 case_dir / arity / type / gt_roots / runs.gap{0,1}.{full,resource})
  输出: <out-dir>/per_case_scores.csv     逐 case × 逐方法 × 逐列宇宙
        <out-dir>/_baselines.json         随机(解析解)/ 常量先验(+LOO)/ GT 分布
        <out-dir>/BASELINES_TABLES.md     主表 + 两个 macro 口径 + per-root 拆解 + bootstrap CI

★GT 与排名是解耦的(重要)
  BARO/RCD/delta 全部 GT-blind(m9_adapter.py:23),排名【不依赖】GT。
  GT 只在 m9_score.mrcbench(ranks, roots) 这一步进来。
  ⇒ GT 更正后【无需重跑方法】,拿旧 n5_raw.jsonl + 新 groundtruth.json 重打分即可。
  --gt-from-raw = 用 raw 里【冻结的】gt_roots(复现旧数字用);默认 = 现读 datasets/ 的 groundtruth.json。

口径(全部照抄既有实现,不自创;逐条注明出处)
  * MRCBench 四族  : 一律 import m9_score.mrcbench,不二次实现。
  * 空排名 = 失败  : 该 case 全部指标记 0(不剔除、不跳过)。—— 既有口径。
  * RCD            : 先 per-case 跨 5 seed 求均值,再 macro(CSV 里仍逐 seed 落行)。
  * 随机基线       : ★解析解(闭式期望),非 MC 采样。抄自 scratchpad/base_run.py:56 analytic_random。
  * delta_z/ratio  : 抄自 scratchpad/base_run.py:27 delta_scores(逐行照抄,零改动)。
  * 列宇宙 full/resource : 抄自 scratchpad/n5_lib.py:50-74。
  * 宽表 prep      : 抄自 scratchpad/n5_lib.py:77 prep(= m9_score 同款,含字节→MB 归一)。
  * bootstrap CI   : 抄自 scratchpad/n5_agg.py:43 boot(np.random.default_rng(7), B=5000, 百分位法)。
  * macro-over-root: 抄自 scratchpad/verify_s32.py:57 macro_root(8 个根等权;
                     "GT 含服务 s 的 case" 的 case 级指标求均值 → 再对 8 个 s 平均;多根 case 计入其每个根)。

用法
  # 复现现有 (project docs)(旧 GT,冻结在 raw 里)
  python scripts/chaos/ctk/m9_report.py --raw <scratchpad>/n5_raw.jsonl \
      --gt-from-raw --out-dir /tmp/repro
  # 出新数(新 GT,现读数据集)
  python scripts/chaos/ctk/m9_report.py --raw <scratchpad>/n5_raw_gtfix.jsonl \
      --out-dir (project docs)
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sys
import zlib
from collections import Counter, defaultdict

import numpy as np

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "third_party", "_cl_patched"))
sys.path.insert(0, os.path.join(REPO_ROOT, "third_party", "RCAEval"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from m9_adapter import build_wide, col_to_service   # noqa: E402
import m9_score as MS                               # noqa: E402  (mrcbench / gt_roots)

# ★勿在此再包一层 sys.stdout:m9_score.py:52 已把 sys.stdout 换成 TextIOWrapper(buffer)。
#   若这里再 TextIOWrapper(sys.stdout.buffer),旧 wrapper 失去引用 → GC → __del__ 关掉同一个
#   底层 buffer → 后续 print 全部 "ValueError: I/O operation on closed file"(实测踩过)。
#   m9_score 的那次重绑已经保证了 UTF-8 输出,直接用即可。

# CSV 表头顺序 = 现有 (project docs)/per_case_scores.csv 的表头(逐列复刻,勿改)
METRICS = ["hit@1", "hit@3", "hit@5", "recall@R",
           "fullhit@1", "fullhit@3", "fullhit@5",
           "ndcg@1", "ndcg@3", "ndcg@5",
           # ★ RCAEval 口径新增(2026-07-14 M11 A1)—— 只【末尾追加】,不插中间,
           #   保证 per_case_scores.csv 新列全在末尾(旧四族列位置/值不变)。
           "avg@1", "avg@3", "avg@5", "mrr"]
ZERO = {k: 0.0 for k in METRICS}
CSV_HEADER = ["case_id", "type", "arity", "n_distinct_roots", "gt_roots", "method", "feature_set",
              "top1", "top5"] + METRICS
UNIVERSES = ("full", "resource")
SEEDS = (0, 1, 2, 3, 4)
EPS = 1e-6                                   # base_run.py:18


# =============================================================================
# 列宇宙(抄自 scratchpad/n5_lib.py:50-74,零改动)
# =============================================================================
KUBE_STATE = {"container_ready", "container_restart_count", "container_running",
              "container_start_time_seconds", "deployment_replicas_ready", "pod_ready"}
OFFGRAPH_METRICS = {"vm_cpu_saturation_ratio", "items_lock_granted_count",
                    "container_cpu_sum_cores", "container_throttled_sum"}


def _suffix(c):
    return c.split("__", 1)[1] if "__" in c else c


def keep_resource(c):
    """resource 宇宙: container cpu/mem + kube_state + off-graph 伪节点。
    剔除: 所有 panel 延迟/错误、http_server_*、container_network_*。"""
    s = _suffix(c)
    if s in OFFGRAPH_METRICS or s in KUBE_STATE:
        return True
    if s.startswith("container_cpu_") or s.startswith("container_memory_"):
        return True
    return False


COL_UNIVERSES = {"full": None, "resource": keep_resource}


def prep(case_dir, gap_aware=True, bucket=2.0):
    """case_dir -> (df, inject);抄自 n5_lib.py:77(= m9_score 同款,含字节→MB 归一真 bug 修复)。"""
    df, inject, info = build_wide(case_dir, bucket=bucket, include_nginx=False, gap_aware=gap_aware)
    if df is None or inject is None or df.shape[0] < 4:
        return None, None, info
    if "stage" in df.columns:
        df = df.drop(columns=["stage"])
    byte_cols = [c for c in df.columns if c != "time" and "bytes" in c]
    if byte_cols:
        df = df.copy()
        for c in byte_cols:
            df[c] = df[c] / 1e6      # bytes → MB(RCAEval convert_mem_mb 同语义)
    return df, inject, info


def subset(df, universe):
    fn = COL_UNIVERSES[universe]
    if fn is None:
        return df
    keep = ["time"] + [c for c in df.columns if c != "time" and fn(c)]
    return df[keep]


# =============================================================================
# 朴素 delta 打分器 —— ★逐行照抄 scratchpad/base_run.py:22-53(delta_scores / med)
#   这两个打分器不在 n5_raw.jsonl 里(n5_run 只跑 BARO/RCD),必须在此重算;
#   照抄是为了保证与产出现有 (project docs) 那批数字时的实现【逐字一致】。
#   语义: 每列 |med(during) - med(pre)| / 尺度;服务分 = 其名下所有列的 max;按分降序。
#     ratio 版尺度 = |med(pre)| + eps
#     z     版尺度 = 1.4826 * MAD(pre) + eps   (robust z)
#   GT-blind:只用 inject_time 切窗,不看任何标签。
# =============================================================================
def _med(a):
    a = a[~np.isnan(a)]
    return float(np.median(a)) if a.size else np.nan


def delta_scores(df, inject, kind="ratio"):
    t = df["time"].values
    pre_m = t < inject
    dur_m = t >= inject
    if pre_m.sum() < 1 or dur_m.sum() < 1:
        return []
    svc = {}
    for c in df.columns:
        if c == "time":
            continue
        v = df[c].values.astype(float)
        p, d = v[pre_m], v[dur_m]
        mp, md_ = _med(p), _med(d)
        if np.isnan(mp) or np.isnan(md_):
            continue
        if kind == "ratio":
            s = abs(md_ - mp) / (abs(mp) + EPS)
        else:  # robust z
            pp = p[~np.isnan(p)]
            mad = np.median(np.abs(pp - mp)) if pp.size else 0.0
            s = abs(md_ - mp) / (1.4826 * mad + EPS)
        if not np.isfinite(s):
            s = 0.0
        k = col_to_service(c)
        svc[k] = max(svc.get(k, 0.0), s)
    return [k for k, _ in sorted(svc.items(), key=lambda kv: -kv[1])]


# =============================================================================
# 随机基线 —— ★解析解(闭式期望),抄自 scratchpad/base_run.py:56 analytic_random。
#   均匀随机排列 N 个候选、其中 R 个是真根:
#     E[Hit@K]     = 1 - C(N-R,K)/C(N,K)          (top-K 与 G 交集非空的概率)
#     E[FullHit@K] = C(K,R)/C(N,R) = prod (K-i)/(N-i)
#     E[NDCG@K]    = [Σ_{j<K} (R/N)/log2(j+2)] / IDCG@K
#     E[Recall@R]  = R/N
#   不是 MC 采样。(注: 现有 _baselines.json 的 random 块是 MC-500 的产物,见 README/对拍报告。)
# =============================================================================
def analytic_random(N, R, K_list=(1, 3, 5)):
    out = {}
    for K in K_list:
        k = min(K, N)
        miss = 1.0
        for i in range(k):
            miss *= max(0.0, (N - R - i)) / (N - i)
        out[f"hit@{K}"] = 1.0 - miss
        if k >= R:
            p = 1.0
            for i in range(R):
                p *= (k - i) / (N - i)
        else:
            p = 0.0
        out[f"fullhit@{K}"] = p
        idcg = sum(1.0 / math.log2(j + 2) for j in range(min(K, R)))
        edcg = sum((R / N) * (1.0 / math.log2(j + 2)) for j in range(k))
        out[f"ndcg@{K}"] = edcg / idcg if idcg > 0 else 0.0
    out["recall@R"] = min(R, N) / float(N)
    # ★ RCAEval 口径新增(2026-07-14 M11 A1)—— 随机排列的闭式期望:
    #   avg@K = E[AC@K] = E[|topK∩G|/R]。|topK∩G|~Hypergeom(N,R,k),E=k·R/N
    #     → E[AC@K] = k/N,k=min(K,N)。
    #   mrr  = E[(1/R)Σ_{g∈G} 1/rank(g)];每根 rank 边缘均匀于 1..N
    #     → E[1/rank] = H_N/N(H_N = Σ_{i=1}^N 1/i),R 个根边缘同分布 → mrr = H_N/N。
    for K in K_list:
        out[f"avg@{K}"] = min(K, N) / float(N) if N > 0 else 0.0
    H_N = sum(1.0 / i for i in range(1, N + 1)) if N > 0 else 0.0
    out["mrr"] = H_N / float(N) if N > 0 else 0.0
    return out


def mc_random(cands, roots, case_id, uni, n_perm=1000):
    """蒙特卡洛随机基线(仅作解析解的自检对照;正式表用解析解)。
    per-case 独立 seed —— 抄自 base_run.py:98(同一 seed 会让所有 case 抽到同一串排列 → CI 假窄)。"""
    rnd = random.Random(1234 + zlib.crc32((case_id + uni).encode()))
    acc = None
    for _ in range(n_perm):
        p = list(cands)
        rnd.shuffle(p)
        m = MS.mrcbench(p, roots)
        acc = m if acc is None else {k: acc[k] + m[k] for k in m}
    return {k: v / n_perm for k, v in acc.items()}


# =============================================================================
# 每 case 的 delta 排名 + 候选集(多进程;结果可缓存)
# =============================================================================
def _delta_one(case):
    """returns {uni: {"n_cands", "cands", "n_cols", "delta_z", "delta_ratio"}}"""
    df, inject, _ = prep(case["case_dir"], gap_aware=case.get("gap_aware", True))
    if df is None:
        return case["case_id"], {"error": "empty_df"}
    out = {}
    for uni in UNIVERSES:
        d = subset(df, uni)
        cands = sorted({col_to_service(x) for x in d.columns if x != "time"})
        out[uni] = {
            "n_cands": len(cands),
            "cands": cands,
            "n_cols": int(d.shape[1] - 1),
            # base_run.py:109 存的是 rk[:12];metrics 只用到 top-5,截断与否等价。此处存全量。
            "delta_ratio": delta_scores(d, inject, "ratio"),
            "delta_z": delta_scores(d, inject, "z"),
        }
    return case["case_id"], out


def compute_deltas(cases, workers, gap_aware, cache_path=None):
    """delta 排名 + 候选集(需重读 adapter,故可缓存)。
    ★缓存必须带 gap 指纹:delta 是在 gap_aware 决定的宽表上算的,拿 gap1 的缓存去跑 --gap gap0
      会静默串味。指纹不匹配 → 重算,不复用。"""
    want = {"gap_aware": bool(gap_aware), "cases": sorted(c["case_id"] for c in cases)}
    if cache_path and os.path.exists(cache_path):
        cached = json.load(open(cache_path, encoding="utf-8"))
        meta = cached.get("__meta__") or {}
        if (meta.get("gap_aware") == want["gap_aware"]
                and set(cached.get("data", {})) >= set(want["cases"])):
            print(f"[deltas] cache hit (gap_aware={gap_aware}) -> {cache_path}", flush=True)
            return cached["data"]
        print(f"[deltas] cache MISS (gap_aware {meta.get('gap_aware')} != {want['gap_aware']} "
              f"or case set changed) → 重算", flush=True)
    payload = [dict(c, gap_aware=gap_aware) for c in cases]
    print(f"[deltas] computing {len(payload)} cases (adapter re-read), workers={workers} ...", flush=True)
    out = {}
    if workers > 1:
        import multiprocessing as mp
        with mp.Pool(workers, maxtasksperchild=1) as pool:
            for i, (cid, rec) in enumerate(pool.imap_unordered(_delta_one, payload), 1):
                out[cid] = rec
                if i % 20 == 0:
                    print(f"[deltas] {i}/{len(payload)}", flush=True)
    else:
        for c in payload:
            cid, rec = _delta_one(c)
            out[cid] = rec
    if cache_path:
        json.dump({"__meta__": {"gap_aware": bool(gap_aware)}, "data": out},
                  open(cache_path, "w", encoding="utf-8"), ensure_ascii=False)
        print(f"[deltas] cached -> {cache_path}", flush=True)
    return out


# =============================================================================
# 统计工具
# =============================================================================
def boot(vals, B=5000, seed=7):
    """case 级 bootstrap 百分位 CI。抄自 n5_agg.py:43。"""
    if not vals:
        return 0.0, 0.0, 0.0
    rng = np.random.default_rng(seed)
    a = np.asarray(vals, float)
    means = a[rng.integers(0, len(a), size=(B, len(a)))].mean(axis=1)
    return float(a.mean()), float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def fixed_ranking_from(cases, tie_break="first-seen"):
    """常量先验榜单 = 按 GT 频次降序。

    ★tie-break 必须显式声明且可复现(现有 fixed_ranking 里 mysql/host/inventory 三方并列 15,
      sasrec/pricing 并列 5 —— 换一个顺序 Hit@3 就从 0.964 变 0.929)。
      first-seen(默认,复现现有 _baselines.json):按 case_id 升序遍历 case,取根因【首次出现】的先后。
      alpha:并列时按字母序。
    ⚠ 历史事实:现有 _baselines.json 的 fixed_ranking 是照 n5_raw.jsonl 的【文件行序】数出来的,
      而该行序 = multiprocessing imap_unordered 的完成顺序(不可复现)。所幸按 case_id 升序
      重数得到【完全相同】的榜单,故此处把 first-seen(case_id 升序)定为可复现的正式口径。
    """
    cnt = Counter()
    first = {}
    for i, c in enumerate(sorted(cases, key=lambda x: x["case_id"])):
        for s in c["roots"]:
            cnt[s] += 1
            first.setdefault(s, i)
    if tie_break == "alpha":
        key = lambda s: (-cnt[s], s)                      # noqa: E731
    else:
        key = lambda s: (-cnt[s], first[s])               # noqa: E731
    return [s for s in sorted(cnt, key=key)], cnt


def loo_ranking(cases, i, tie_break="first-seen"):
    """留一法:把第 i 个 case 自己的 GT 从频次统计里剔掉后重排。抄自 base_agg.py:76 const_ranks_loo。"""
    others = [c for j, c in enumerate(cases) if j != i]
    return fixed_ranking_from(others, tie_break)[0]


def macro(percase, cases=None):
    """macro over cases。"""
    ms = [percase[c["case_id"]] for c in cases] if cases else list(percase.values())
    if not ms:
        return dict(ZERO)
    return {k: float(np.mean([m[k] for m in ms])) for k in METRICS}


def macro_root(percase, cases, roots8, key="hit@1"):
    """macro-over-根因服务(8 个根等权)。抄自 verify_s32.py:57。
    每个根 s: 对【GT 含 s 的所有 case】求该 case 的指标均值 → 再对 8 个 s 平均。
    多根 case 会计入它的每一个根。"""
    per = {}
    for s in roots8:
        sub = [percase[c["case_id"]][key] for c in cases if s in c["roots"]]
        if sub:
            per[s] = float(np.mean(sub))
    return (float(np.mean(list(per.values()))) if per else 0.0), per


# =============================================================================
# 主流程
# =============================================================================
def main():
    ap = argparse.ArgumentParser(description="(project docs) 出表器(单入口、可复现)")
    ap.add_argument("--raw", required=True, help="n5_raw*.jsonl(BARO + RCD×5seed 排名)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--gt-from-raw", action="store_true",
                    help="用 raw 里【冻结的】gt_roots(复现旧数字);默认现读 datasets/groundtruth.json")
    ap.add_argument("--gap", default="gap1", choices=("gap1", "gap0"),
                    help="gap1 = adapter gap_aware=True(正式口径,无跨盲区回灌)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--delta-cache", default=None, help="delta 排名缓存 json(加速重跑)")
    ap.add_argument("--tie-break", default="first-seen", choices=("first-seen", "alpha"))
    ap.add_argument("--boot-b", type=int, default=5000)
    ap.add_argument("--boot-seed", type=int, default=7)
    ap.add_argument("--row-order", default="raw", choices=("raw", "sorted"),
                    help="raw = 保持 n5_raw.jsonl 行序(与现有 per_case_scores.csv 逐行可比)")
    ap.add_argument("--mc-check", action="store_true", help="额外跑 MC-1000 随机基线作解析解自检")
    a = ap.parse_args()

    os.makedirs(a.out_dir, exist_ok=True)
    raw = [json.loads(l) for l in open(a.raw, encoding="utf-8") if l.strip()]

    # ★★ 2026-07-13 可复现性修复:【必须】按 case_id 排序,不能沿用 jsonl 的文件行序。
    #   n5_run.py 用 mp.Pool.imap_unordered 写盘 → 行序 = 各 worker 的【完成顺序】= 不确定。
    #   下游有两处对顺序敏感:
    #     (1) bootstrap CI —— 种子固定(default_rng(7)),但重采样序列取决于遍历顺序
    #         → 同样的数据、不同的行序,CI 边界会漂(实测 BARO/resource [0.41,0.58] vs [0.42,0.59]);
    #     (2) 浮点累加顺序 → 末位 ULP 差(实测 ndcg@5 ...393 vs ...392)。
    #   后果:【发表的 CI 别人重跑复现不出来】。排序后本脚本的输出与 raw 行序【完全无关】。
    #   (点估计本来就不受影响 —— 差的只有 CI 与末位,但可复现性不能打折。)
    raw.sort(key=lambda r: r["case_id"])

    # ---- case 表 ----
    cases = []
    for r in raw:
        roots = r["gt_roots"] if a.gt_from_raw else MS.gt_roots(r["case_dir"])
        cases.append({"case_id": r["case_id"], "type": r["type"], "arity": r["arity"],
                      "case_dir": r["case_dir"], "roots": roots, "raw": r})
    gt_src = "raw(frozen)" if a.gt_from_raw else "dataset(groundtruth.json, live)"
    print(f"[m9_report] {len(cases)} cases | gap={a.gap} | GT source = {gt_src}", flush=True)

    # ---- delta 打分器(需重读 adapter)----
    deltas = compute_deltas(cases, a.workers, gap_aware=(a.gap == "gap1"), cache_path=a.delta_cache)

    # ---- 逐 case 逐方法指标 ----
    # 空排名 = 失败 → 全部指标记 0(不剔除、不跳过)。既有口径。
    def m_of(ranks, roots):
        return MS.mrcbench(ranks, roots) if ranks else dict(ZERO)

    percase = defaultdict(dict)          # (method, uni) -> case_id -> metrics
    ranks_of = defaultdict(dict)         # (method, uni) -> case_id -> ranks
    n_empty = Counter()
    for c in cases:
        cid, roots = c["case_id"], c["roots"]
        g = c["raw"]["runs"].get(a.gap, {})
        for uni in UNIVERSES:
            u = g.get(uni) or {}
            rk = (u.get("baro") or {}).get("ranks") or []
            ranks_of[("BARO", uni)][cid] = rk
            percase[("BARO", uni)][cid] = m_of(rk, roots)
            if not rk:
                n_empty[("BARO", uni)] += 1
            for s in SEEDS:
                rk = ((u.get("rcd") or {}).get(str(s)) or {}).get("ranks") or []
                ranks_of[(f"RCD_seed{s}", uni)][cid] = rk
                percase[(f"RCD_seed{s}", uni)][cid] = m_of(rk, roots)
                if not rk:
                    n_empty[(f"RCD_seed{s}", uni)] += 1
            d = deltas.get(cid, {}).get(uni) or {}
            for meth in ("delta_ratio", "delta_z"):
                rk = d.get(meth) or []
                ranks_of[(meth, uni)][cid] = rk
                percase[(meth, uni)][cid] = m_of(rk, roots)
                if not rk:
                    n_empty[(meth, uni)] += 1
    # RCD 5-seed 均值(先 per-case 跨 seed 平均,再 macro)
    for uni in UNIVERSES:
        for c in cases:
            cid = c["case_id"]
            ms = [percase[(f"RCD_seed{s}", uni)][cid] for s in SEEDS]
            percase[("RCD", uni)][cid] = {k: float(np.mean([m[k] for m in ms])) for k in METRICS}

    # ---- per_case_scores.csv ----
    ordered = cases if a.row_order == "raw" else sorted(
        cases, key=lambda c: (["single", "dual", "triple"].index(c["arity"]), c["type"], c["case_id"]))
    csv_path = os.path.join(a.out_dir, "per_case_scores.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        for c in ordered:
            cid, roots = c["case_id"], c["roots"]
            # gt_roots 列排序输出(与现有 per_case_scores.csv 一致;mrcbench 用 set,排序不影响任何指标)
            head = [cid, c["type"], c["arity"], len(roots), ";".join(sorted(roots))]
            # 方法出行顺序 = 现有 CSV 的顺序(BARO+RCD 按列宇宙分块,delta 随后)
            block = [(m, u) for u in UNIVERSES for m in ("BARO",) + tuple(f"RCD_seed{s}" for s in SEEDS)]
            block += [(m, u) for u in UNIVERSES for m in ("delta_ratio", "delta_z")]
            for meth, uni in block:
                rk = ranks_of[(meth, uni)][cid]
                m = percase[(meth, uni)][cid]
                w.writerow(head + [meth, uni,
                                   rk[0] if rk else "", ";".join(rk[:5]),
                                   *[round(m[k], 4) for k in METRICS]])
    print(f"[m9_report] -> {csv_path}", flush=True)

    # ---- 基线 ----
    FIXED, gt_cnt = fixed_ranking_from(cases, a.tie_break)
    roots8 = [s for s in FIXED]
    for c in cases:
        percase[("const_prior", "-")][c["case_id"]] = MS.mrcbench(FIXED, c["roots"])
    # LOO:按 case_id 升序做留一(与 fixed_ranking_from 的遍历序一致)
    cases_sorted = sorted(cases, key=lambda x: x["case_id"])
    for i, c in enumerate(cases_sorted):
        percase[("const_prior_loo", "-")][c["case_id"]] = MS.mrcbench(
            loo_ranking(cases_sorted, i, a.tie_break), c["roots"])
    # 随机(解析解):N = 该 case 的候选服务数(full 宇宙)
    ncand = Counter()
    for c in cases:
        d = deltas.get(c["case_id"], {})
        N = (d.get("full") or {}).get("n_cands") or 0
        ncand[N] += 1
        percase[("random", "-")][c["case_id"]] = analytic_random(N, max(1, len(c["roots"])))
    if a.mc_check:
        for c in cases:
            cands = (deltas[c["case_id"]]["full"])["cands"]
            percase[("random_mc", "-")][c["case_id"]] = mc_random(cands, c["roots"], c["case_id"], "full")

    n_cands_mode = ncand.most_common(1)[0][0]
    gt_sets = Counter("+".join(sorted(c["roots"])) for c in cases)
    bj = {
        "generated_by": "scripts/chaos/ctk/m9_report.py",
        "raw": os.path.basename(a.raw),
        "gt_source": gt_src,
        "gap": a.gap,
        "n_cases": len(cases),
        "n_cands": n_cands_mode,
        "n_cands_dist": {str(k): v for k, v in sorted(ncand.items())},
        "random": macro(percase[("random", "-")], cases),
        "random_method": ("解析解(闭式期望 E[Hit@K]=1-C(N-R,K)/C(N,K) 等;N=候选服务数,R=|G|)"
                          " —— 非 MC 采样"),
        "const_prior": macro(percase[("const_prior", "-")], cases),
        "const_prior_loo": macro(percase[("const_prior_loo", "-")], cases),
        "fixed_ranking": FIXED,
        "tie_break": ("频次降序;并列时按【根因首次出现的先后】(遍历 case_id 升序的 case)"
                      if a.tie_break == "first-seen" else "频次降序;并列时按字母序"),
        "gt_dist": dict(gt_cnt.most_common()),
        "gt_dist_by_arity": {ar: dict(Counter(
            s for c in cases if c["arity"] == ar for s in c["roots"]).most_common())
            for ar in ("single", "dual", "triple")},
        "gt_set_dist": dict(gt_sets.most_common()),
        "n_distinct_roots_dist": {str(k): v for k, v in sorted(
            Counter(len(c["roots"]) for c in cases).items())},
        "empty_rankings": {f"{m}/{u}": n for (m, u), n in sorted(n_empty.items()) if n},
        "notes": [
            "空排名 = 失败 → 该 case 全部指标记 0(不剔除、不跳过)。",
            "RCD 先 per-case 跨 5 seed 求均值,再 macro。",
            "常量先验是 GT-aware 的 oracle 先验(它用了全量 GT 频次),不是无监督基线;LOO 版见 const_prior_loo。",
        ],
    }
    if a.mc_check:
        bj["random_mc_check"] = macro(percase[("random_mc", "-")], cases)
    bj_path = os.path.join(a.out_dir, "_baselines.json")
    json.dump(bj, open(bj_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[m9_report] -> {bj_path}", flush=True)

    # ---- BASELINES_TABLES.md ----
    L = []

    def P(s=""):
        L.append(s)

    MAIN = ["hit@1", "hit@3", "hit@5", "recall@R", "fullhit@3", "ndcg@3", "avg@5", "mrr"]
    ENTRIES = [("random", "-", "随机排列(N=%d,解析解)" % n_cands_mode),
               ("const_prior", "-", "常量先验(固定榜单,GT-aware)"),
               ("const_prior_loo", "-", "常量先验(留一法 LOO)"),
               ("BARO", "full", "BARO"), ("BARO", "resource", "BARO"),
               ("RCD", "full", "RCD(5 seed 均值)"), ("RCD", "resource", "RCD(5 seed 均值)"),
               ("delta_z", "full", "朴素 delta_z"), ("delta_z", "resource", "朴素 delta_z"),
               ("delta_ratio", "full", "朴素 delta_ratio"), ("delta_ratio", "resource", "朴素 delta_ratio")]

    P(f"# BASELINES(自动生成 by `scripts/chaos/ctk/m9_report.py`)")
    P("")
    P(f"* raw = `{os.path.basename(a.raw)}` · GT = **{gt_src}** · 口径 = **{a.gap}** · "
      f"{len(cases)} case(single={sum(c['arity']=='single' for c in cases)} "
      f"dual={sum(c['arity']=='dual' for c in cases)} triple={sum(c['arity']=='triple' for c in cases)})")
    P(f"* 空排名 = 失败 → 全指标记 0(不剔除)。空排名统计: "
      f"{bj['empty_rankings'] if bj['empty_rankings'] else '无(全部方法全部 case 均非空)'}")
    P(f"* 随机基线 = **解析解**(非 MC)。常量先验 tie-break: {bj['tie_break']}")
    P("")

    # 主表(macro-over-case)
    P(f"## 表1 主表(macro over **case**;[] = Hit@1 的 95% bootstrap CI,case 级 B={a.boot_b})")
    P("")
    P("> avg@5 / mrr = RCAEval 口径(avg@k=per-case AC@k;mrr=全根 reciprocal 均值)。"
      "**常量先验行是 GT-aware oracle 先验(用了全量 GT 频次),不是无监督基线** —— 每一列都以它作上界对照。")
    P("")
    P("| 方法 | 特征列 | Hit@1 | Hit@3 | Hit@5 | Recall@R | FullHit@3 | NDCG@3 | Avg@5 | MRR |")
    P("|---|---|---|---|---|---|---|---|---|---|")
    for meth, uni, label in ENTRIES:
        pc = percase[(meth, uni)]
        mm = macro(pc, cases)
        mu, lo, hi = boot([pc[c["case_id"]]["hit@1"] for c in cases], a.boot_b, a.boot_seed)
        cells = [f"**{mm['hit@1']:.3f}** [{lo:.2f},{hi:.2f}]"] + [f"{mm[k]:.3f}" for k in MAIN[1:]]
        P(f"| {label} | {uni if uni != '-' else '—'} | " + " | ".join(cells) + " |")
    P("")

    # 两个 macro 口径
    P("## 表2 ★两个平均口径都出(macro-case vs macro-over-根因服务)")
    P("")
    P("* **macro-case** = %d 个 case 等权(默认口径,被 GT 偏斜污染)" % len(cases))
    P("* **macro-root** = %d 个根因服务等权:先对「GT 含服务 s 的所有 case」求均值,再对 %d 个 s 平均"
      "(多根 case 计入其每个根)" % (len(roots8), len(roots8)))
    P("")
    P("| 方法 | 特征列 | Hit@1 (macro-case) | Hit@1 (macro-root) | Hit@3 (macro-root) | NDCG@3 (macro-root) |")
    P("|---|---|---|---|---|---|")
    for meth, uni, label in ENTRIES:
        pc = percase[(meth, uni)]
        mc = macro(pc, cases)["hit@1"]
        mr1, _ = macro_root(pc, cases, roots8, "hit@1")
        mr3, _ = macro_root(pc, cases, roots8, "hit@3")
        mrn, _ = macro_root(pc, cases, roots8, "ndcg@3")
        P(f"| {label} | {uni if uni != '-' else '—'} | {mc:.3f} | **{mr1:.3f}** | {mr3:.3f} | {mrn:.3f} |")
    P("")

    # per-root 拆解
    P("## 表3 per-root Hit@1 拆解(%d 个根各一行)" % len(roots8))
    P("")
    P("> 口径 = 该 case 的 top-1 落在 **GT 集合内任一根**(不是「该服务本人被排第 1」)"
      "→ 多根 case 会同时计入它的每个根。")
    P("")
    COLS = [("random", "-"), ("const_prior", "-"), ("BARO", "full"), ("BARO", "resource"),
            ("RCD", "full"), ("RCD", "resource"), ("delta_z", "resource")]
    P("| 根因服务 | #case | GT 占比 | " + " | ".join(f"{m}/{u}" if u != "-" else m for m, u in COLS) + " |")
    P("|---|---|---|" + "---|" * len(COLS))
    per_root_cache = {k: macro_root(percase[k], cases, roots8, "hit@1")[1] for k in COLS}
    for s in roots8:
        n = gt_cnt[s]
        vals = [f"{per_root_cache[k].get(s, float('nan')):.3f}" for k in COLS]
        P(f"| `{s}` | {n} | {n/len(cases):.1%} | " + " | ".join(vals) + " |")
    P("")

    # 按 arity / |G|
    P("## 表4 按 arity 与按真实 |G| 分层(Hit@1)")
    P("")
    P("| 分层 | n | " + " | ".join(f"{m}/{u}" if u != "-" else m for m, u in COLS) + " |")
    P("|---|---|" + "---|" * len(COLS))
    strata = [(f"arity={ar}", [c for c in cases if c["arity"] == ar]) for ar in ("single", "dual", "triple")]
    strata += [(f"|G|={g}", [c for c in cases if len(c["roots"]) == g]) for g in (1, 2, 3)]
    for name, sub in strata:
        if not sub:
            continue
        vals = [f"{macro(percase[k], sub)['hit@1']:.3f}" for k in COLS]
        P(f"| {name} | {len(sub)} | " + " | ".join(vals) + " |")
    P("")

    # GT 分布
    P("## 表5 GT 分布(随机地板与常量先验的来源)")
    P("")
    P(f"* 固定榜单(常量先验,照抄即可复现): `{' → '.join(FIXED)}`")
    P(f"* tie-break: {bj['tie_break']}")
    P(f"* 候选服务数 N = {n_cands_mode}(分布 {bj['n_cands_dist']})· "
      f"不同 GT 集合 {len(gt_sets)} 种 · 不同根因服务 {len(gt_cnt)} 个")
    P("")
    P("| 根因服务 | ALL | 占比 | single | dual | triple |")
    P("|---|---|---|---|---|---|")
    for s, n in gt_cnt.most_common():
        ba = bj["gt_dist_by_arity"]
        P(f"| `{s}` | {n} | {n/len(cases):.1%} | {ba['single'].get(s,0)} | "
          f"{ba['dual'].get(s,0)} | {ba['triple'].get(s,0)} |")
    P("")
    P("| |G| | n |")
    P("|---|---|")
    for g, n in sorted(bj["n_distinct_roots_dist"].items()):
        P(f"| {g} | {n} |")
    P("")

    md_path = os.path.join(a.out_dir, "BASELINES_TABLES.md")
    open(md_path, "w", encoding="utf-8").write("\n".join(L) + "\n")
    print(f"[m9_report] -> {md_path}", flush=True)
    print("\n".join(L))


if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support()
    main()
