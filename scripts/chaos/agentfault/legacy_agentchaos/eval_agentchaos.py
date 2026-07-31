# -*- coding: utf-8 -*-
"""TASK-X agentchaos baseline 评测(group-aware, 多标签, 双轨 A vs B)。

读 (archived) agentchaos/features_agentchaos.csv ->
  - 多标签 per-agent 二分类(每 agent 是否 ∈ root_cause_set)
  - Top@1 / Top@2 根因定位(模型对各 agent 打分排序)
  - Recall@root-set / 多标签 micro-F1 / exact-match
  - 双轨: 轨道 A(纯 e2e/HTTP 黑盒, 无 per-agent span 列) vs 轨道 B(加 per-agent span 列)
  - group-aware: LeaveOneGroupOut(测全新故障组合) + StratifiedGroupKFold(测已知类未见窗)
  - 多 seed [0,1,2,3,4] 报均值±std
  - HTTP500 窗(Synthesizer error, S23)单列说明(silent-degrade vs hard-fail 两套症状空间)

诚实(对齐 TASK-X 护栏): "轨道 A≈随机"是 hypothesis, 由 ΔTop@1/ΔRecall 实测量化, 不预设。
  样本量小、单组根因类 LOGO 结构性 0、cross_layer pilot 只报趋势——都如实写进 BASELINE。

确定性: 固定 SEEDS, 无未播种随机。

用法:
  PYTHONIOENCODING=utf-8 python scripts/chaos/ctk/eval_agentchaos.py
"""
import os
import sys
import io
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.dummy import DummyClassifier
from sklearn.model_selection import LeaveOneGroupOut, StratifiedGroupKFold

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
except Exception:
    pass

SEEDS = [0, 1, 2, 3, 4]
N_SGKF = 5
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CSV = os.path.join(ROOT, "datasets", "_archive", "agentfault", "agentchaos", "features_agentchaos.csv")
OUT_MD = os.path.join(ROOT, "datasets", "_archive", "agentfault", "agentchaos", "BASELINE_RESULTS.md")

AGENT_NAMES = [
    "Sequence_Recommender", "User_Behavior_Analyzer",
    "Product_Analyzer", "Recommendation_Synthesizer",
]
CANDIDATES = AGENT_NAMES  # 主集候选(cross_layer 含 sasrec/deepseek, 主评测只用 4 agent)

# 轨道 B 全特征 / 轨道 A 黑盒特征(无 per-agent span 列)
def feature_cols(df, track):
    span_prefixes = ("span_",)
    conv_prefixes = ("conv_",)
    base_blackbox = [
        "e2e_latency_ms", "http_status", "http_success",
        "total_span_count", "error_span_count",
        "recommendation_confidence", "recommended_product_is_unknown",
        "degrade_message_present", "garbage_message_present",
        "host_cpu_pct", "host_mem_pct",
    ]
    if track == "A":
        # 黑盒: e2e/HTTP + 推荐质量 + host(无 per-agent span / 无 per-agent conv 长度)
        cols = [c for c in base_blackbox if c in df.columns]
    else:
        # 全特征: 黑盒 + per-agent span + per-agent conv 长度
        cols = list(base_blackbox)
        for c in df.columns:
            if c.startswith(span_prefixes) or c.startswith(conv_prefixes):
                cols.append(c)
        cols = [c for c in dict.fromkeys(cols) if c in df.columns]
    return cols


def build_X(df, cols):
    feats = {}
    for c in cols:
        as_str = df[c].astype(str).str.strip()
        isna = (as_str == "") | (as_str.str.lower() == "nan")
        num = pd.to_numeric(as_str.where(~isna, other=np.nan), errors="coerce")
        feats[c] = num.fillna(0.0).astype(float)
        if isna.any():
            feats[c + "_isna"] = isna.astype(float)
    return pd.DataFrame(feats, index=df.index)


def make_models():
    return {
        "Dummy(most_frequent)": lambda s: DummyClassifier(strategy="most_frequent"),
        "RandomForest": lambda s: RandomForestClassifier(n_estimators=300, random_state=s, n_jobs=1),
        "LogReg(scaled)": lambda s: Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=5000, random_state=s)),
        ]),
    }


def cv_iter(splitter_name, X, groups, y_strat):
    if splitter_name == "LOGO":
        sp = LeaveOneGroupOut()
        return list(sp.split(X, y_strat, groups))
    else:
        sp = StratifiedGroupKFold(n_splits=N_SGKF, shuffle=True, random_state=0)
        return list(sp.split(X, y_strat, groups))


def eval_track(df, track, splitter_name):
    """对一个轨道 + 一个划分: 多 seed, per-agent 二分类聚合成 Top@k / Recall@root-set / exact-match.

    返回 dict[model] = {top1, top2, recall_rootset, micro_f1, exact_match} 各 (mean,std)。
    """
    cols = feature_cols(df, track)
    X = build_X(df, cols)
    groups = df["group_id"].values
    # per-agent 0/1 标签矩阵
    Y = {a: df[f"y_rc__{a}"].astype(int).values for a in CANDIDATES}
    n_rc = df["n_root_causes"].astype(int).values
    # 分层键(用于 SGKF): 用 n_root_causes 作分层近似(多标签无单一类)
    y_strat = n_rc

    models = make_models()
    results = {}
    for mname, mfac in models.items():
        per_seed = defaultdict(list)
        for seed in SEEDS:
            # 收集所有 test 窗的预测分数(每 agent 一个分类器)
            n = len(df)
            scores = np.full((n, len(CANDIDATES)), np.nan)
            preds = np.full((n, len(CANDIDATES)), 0)
            for tr, te in cv_iter(splitter_name, X, groups, y_strat):
                for ai, a in enumerate(CANDIDATES):
                    ytr = Y[a][tr]
                    clf = mfac(seed)
                    # 单类训练集(LOGO 留出致某 agent 全 0/全 1)→ 退化预测该常数
                    if len(set(ytr)) < 2:
                        const = int(ytr[0]) if len(ytr) else 0
                        preds[te, ai] = const
                        scores[te, ai] = float(const)
                        continue
                    clf.fit(X.iloc[tr], ytr)
                    preds[te, ai] = clf.predict(X.iloc[te])
                    try:
                        proba = clf.predict_proba(X.iloc[te])
                        # 取正类列
                        classes = list(clf.classes_) if hasattr(clf, "classes_") else None
                        if classes is None and hasattr(clf, "named_steps"):
                            classes = list(clf.named_steps["clf"].classes_)
                        pos_idx = classes.index(1) if (classes and 1 in classes) else -1
                        scores[te, ai] = proba[:, pos_idx]
                    except Exception:
                        scores[te, ai] = preds[te, ai].astype(float)
            # 聚合指标(对所有窗)
            top1_hits, top2_hits, recall_list, f1_num, f1_den_p, f1_den_t, exact = [], [], [], 0, 0, 0, []
            tp = fp = fn = 0
            for i in range(n):
                truth = set(a for a in CANDIDATES if Y[a][i] == 1)
                k = max(1, n_rc[i]) if n_rc[i] > 0 else 0
                order = np.argsort(-np.nan_to_num(scores[i], nan=-1.0))
                ranked = [CANDIDATES[j] for j in order]
                # Top@1 / Top@2: 真根因(若有)是否进前 k'
                if truth:
                    top1_hits.append(1 if ranked[0] in truth else 0)
                    top2_hits.append(1 if (set(ranked[:2]) & truth) else 0)
                    # Recall@|root-set|: 前 |root-set| 命中比例
                    kk = len(truth)
                    hit = len(set(ranked[:kk]) & truth)
                    recall_list.append(hit / kk)
                else:
                    # normal 窗: 真根因空集。Top@k 不适用(只算 exact-match via preds)
                    pass
                # exact-match: 预测正类集 == 真集
                pred_set = set(a for ai, a in enumerate(CANDIDATES) if preds[i, ai] == 1)
                exact.append(1 if pred_set == truth else 0)
                tp += len(pred_set & truth)
                fp += len(pred_set - truth)
                fn += len(truth - pred_set)
            micro_p = tp / (tp + fp) if (tp + fp) else 0.0
            micro_r = tp / (tp + fn) if (tp + fn) else 0.0
            micro_f1 = (2 * micro_p * micro_r / (micro_p + micro_r)) if (micro_p + micro_r) else 0.0
            per_seed["top1"].append(np.mean(top1_hits) if top1_hits else float("nan"))
            per_seed["top2"].append(np.mean(top2_hits) if top2_hits else float("nan"))
            per_seed["recall_rootset"].append(np.mean(recall_list) if recall_list else float("nan"))
            per_seed["micro_f1"].append(micro_f1)
            per_seed["exact_match"].append(np.mean(exact) if exact else float("nan"))
        results[mname] = {
            k: (round(float(np.nanmean(v)), 3), round(float(np.nanstd(v)), 3))
            for k, v in per_seed.items()
        }
    return results, cols


def fmt(res):
    lines = []
    for m, d in res.items():
        parts = [f"{k}={d[k][0]:.3f}±{d[k][1]:.3f}" for k in
                 ("top1", "top2", "recall_rootset", "micro_f1", "exact_match")]
        lines.append(f"| {m} | " + " | ".join(f"{d[k][0]:.3f}±{d[k][1]:.3f}" for k in
                     ("top1", "top2", "recall_rootset", "micro_f1", "exact_match")) + " |")
    return lines


def main():
    if not os.path.exists(CSV):
        print(f"[ERR] {CSV} 不存在; 先 make_agentchaos_features.py")
        return 2
    df = pd.read_csv(CSV, dtype=str).fillna("")
    # 类型规约
    df["n_root_causes"] = pd.to_numeric(df["n_root_causes"], errors="coerce").fillna(0).astype(int)
    for a in CANDIDATES:
        df[f"y_rc__{a}"] = pd.to_numeric(df[f"y_rc__{a}"], errors="coerce").fillna(0).astype(int)
    n = len(df)
    groups = sorted(df["group_id"].unique())
    http500 = int((pd.to_numeric(df["http_status"], errors="coerce") == 500).sum())

    md = []
    md.append("# agentchaos baseline 评测结果(group-aware, 多标签, 双轨 A vs B)\n")
    md.append("> 由 `scripts/chaos/ctk/eval_agentchaos.py` 生成(确定性, SEEDS=[0,1,2,3,4])。")
    md.append("> 评测协议见 `FEATURES_README.md` / `SUMMARY.md`。\n")
    md.append(f"- 样本: {n} 窗, {len(groups)} 组(group_id=scenario_id)。")
    md.append(f"- HTTP500 窗(Synthesizer error hard-fail): {http500} 个(silent-degrade HTTP200 vs hard-fail HTTP500 两套症状空间, 评测分开理解)。")
    md.append(f"- 候选根因空间: {CANDIDATES}")
    md.append("- 指标: Top@1 / Top@2(真根因进前 k) / Recall@root-set / 多标签 micro-F1 / exact-match(集合完全命中)。\n")

    for splitter in ("LOGO", "SGKF5"):
        md.append(f"\n## 划分: {splitter}\n")
        for track, desc in (("A", "黑盒(e2e/HTTP/推荐质量/host, 无 per-agent span)"),
                            ("B", "全特征(加 per-agent span + conv 长度)")):
            res, cols = eval_track(df, track, splitter)
            md.append(f"\n### 轨道 {track} — {desc}  (X 列数={len(cols)})\n")
            md.append("| 模型 | Top@1 | Top@2 | Recall@root-set | micro-F1 | exact-match |")
            md.append("|---|---|---|---|---|---|")
            md.extend(fmt(res))

    md.append("\n## 双轨增益(ΔTop@1 = 轨道B - 轨道A, RandomForest)\n")
    # 计算 RF 的 ΔTop@1（LOGO）
    for splitter in ("LOGO", "SGKF5"):
        resA, _ = eval_track(df, "A", splitter)
        resB, _ = eval_track(df, "B", splitter)
        for metric in ("top1", "recall_rootset", "exact_match"):
            a = resA["RandomForest"][metric][0]
            b = resB["RandomForest"][metric][0]
            md.append(f"- {splitter} RF Δ{metric} = {b - a:+.3f} (A={a:.3f} → B={b:.3f})")

    md.append("\n## 诚实 caveat")
    md.append("- 样本量小(比 chaos25 120 行/15 组更小), 统计力紧, 结论作趋势性; 多 seed±std 已报。")
    md.append("- LOGO 留出单组根因类时训练集对该 agent 全 0 → 该类 Top@k 结构性为 0(数据集同质组结构产物, 非模型缺陷), 须与 SGKF5 并读。")
    md.append("- per-agent 二分类对 Synthesizer(仅 2 组出现)/某些 garbage agent(仅 1 组)正例数极少。")
    md.append("- 轨道 A 是否对 garbage 上游根因接近随机是 hypothesis, 由上表 ΔTop@1/ΔRecall 实测量化; 若 A 也能定位(如 garbage 致 confidence/unknown 异常被黑盒捕获)即如实报告(同样有价值)。")
    md.append("- ground_truth 来自 programmed-injection(零标注噪声); LLM 输出 temp=0.7 有文本方差(conv_*_text_len 弱特征)。")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    print("wrote " + OUT_MD)
    print("\n".join(md[:12]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
