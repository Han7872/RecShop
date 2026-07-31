# -*- coding: utf-8 -*-
"""TASK-X agentchaos 脱敏特征视图生成器(防标签泄漏 + group-aware)。

dataset_agentchaos.csv -> features_agentchaos.csv

防泄漏(对齐 chaos25 FEATURES_README §1 剔除表 + TASK-X Reviewer 护栏 #3):
  剔出 X(标签/溯源/派生答案): run_id, scenario_id, kind, root_cause_set, n_root_causes,
    fault_type_set, fault_<Agent>(4 列, 直接编码哪个 agent 故障 = 答案), trace_id,
    window_start/window_end, note。
  保留作标签(y, 不进 X): root_cause_set(多标签根因集) + per-agent y_rc__<Agent>(多热) +
    fault_type_set + n_root_causes(分层维度)。
  X = 纯遥测可观测量: e2e/http + per-agent span(duration/status/子span计数/childmax/present) +
    total/error span + confidence/unknown/degrade/garbage message + conv 文本长度(弱特征) +
    host cpu/mem(低判别力, 见 SUMMARY caveat)。
    注意: 每个 per-agent 列对所有 4 agent 都有(不只被注入 agent), 否则"哪个 agent 有这列"泄漏答案。

group_id = scenario_id(场景身份 = 注入 agent 集合 × 故障类型组合)。同场景 N 窗高度同质,
  必须 group-aware(LeaveOneGroupOut / StratifiedGroupKFold)防同质窗跨 train/test。

候选根因空间(多热 y_rc__*, 供 Recall@root-set / exact-match 评测):
  4 个 agent;cross_layer 额外 sasrec_api / deepseek(若数据含)。

用法:
  python scripts/chaos/ctk/make_agentchaos_features.py
"""
import csv
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
SRC = os.path.join(ROOT, "datasets", "_archive", "agentfault", "agentchaos", "dataset_agentchaos.csv")
OUT = os.path.join(ROOT, "datasets", "_archive", "agentfault", "agentchaos", "features_agentchaos.csv")

AGENT_NAMES = [
    "Sequence_Recommender",
    "User_Behavior_Analyzer",
    "Product_Analyzer",
    "Recommendation_Synthesizer",
]

# 直接编码答案/注入身份/溯源的列 —— 剔出 X
LEAK_COLS = (
    ["run_id", "scenario_id", "kind", "root_cause_set", "n_root_causes",
     "fault_type_set", "trace_id", "window_start", "window_end", "note"]
    + [f"fault_{a}" for a in AGENT_NAMES]   # 每 agent 的注入类型 = 答案
)
# 标签/分层(不进 X)
LABEL_COLS = ["root_cause_set", "fault_type_set", "n_root_causes"]
# 候选根因空间(多热 y_rc__*)
ROOT_CAUSE_CANDIDATES = list(AGENT_NAMES) + ["sasrec_api", "deepseek"]

# X 特征列(纯遥测): 顺序固定
def _feature_cols():
    cols = ["e2e_latency_ms", "http_status", "http_success"]
    for a in AGENT_NAMES:
        cols += [
            f"span_{a}_duration_ms",
            f"span_{a}_status",
            f"span_{a}_child_httpx_count",
            f"span_{a}_child_sasrec_requests_count",
            f"span_{a}_child_max_duration_ms",
            f"span_{a}_present",
        ]
    cols += [
        "total_span_count", "error_span_count",
        "recommendation_confidence", "recommended_product_is_unknown",
        "degrade_message_present", "garbage_message_present",
    ]
    for a in AGENT_NAMES:
        cols.append(f"conv_{a}_text_len")
    cols += ["host_cpu_pct", "host_mem_pct"]
    return cols


FEATURE_COLS = _feature_cols()
# span status 字符串 -> 数值编码(OK=0, ERROR=1, UNSET=0; 空=缺失)
STATUS_MAP = {"OK": 0, "ERROR": 1, "UNSET": 0, "": ""}


def _status_to_num(v):
    v = (v or "").strip()
    return STATUS_MAP.get(v, "")


def main():
    if not os.path.exists(SRC):
        print(f"[ERR] 数据源不存在: {SRC}")
        return 2
    with open(SRC, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    cand_cols = [f"y_rc__{c}" for c in ROOT_CAUSE_CANDIDATES]
    out_fields = ["sample_id", "group_id"] + FEATURE_COLS + LABEL_COLS + cand_cols
    out_rows = []
    for r in rows:
        o = {"sample_id": r.get("run_id", ""), "group_id": r.get("scenario_id", "")}
        for c in FEATURE_COLS:
            v = r.get(c, "")
            # 仅 per-agent span 的 *_status 列做 OK/ERROR/UNSET->数值编码;
            # http_status(数值 200/500)不能被 _status 后缀误捕(否则 STATUS_MAP 查不到→丢空,
            # 全量含 S23 HTTP500 后此列有判别力)。— TASK-X Reviewer 必修 #4
            if c.endswith("_status") and c != "http_status":
                o[c] = _status_to_num(v)
            else:
                o[c] = v
        for c in LABEL_COLS:
            o[c] = r.get(c, "")
        rc_set = {p.strip() for p in (r.get("root_cause_set", "") or "").split(";") if p.strip()}
        for c in ROOT_CAUSE_CANDIDATES:
            o[f"y_rc__{c}"] = 1 if c in rc_set else 0
        out_rows.append(o)

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(out_rows)

    groups = sorted({o["group_id"] for o in out_rows})
    print(f"[features_agentchaos] {len(out_rows)} 行 -> {OUT}")
    print(f"  X 特征列: {len(FEATURE_COLS)} | 标签/分层 y: {LABEL_COLS}")
    print(f"  候选根因空间(多热 y_rc__*): {ROOT_CAUSE_CANDIDATES}")
    print(f"  剔除泄漏列: {LEAK_COLS}")
    print(f"  组数(group_id=scenario_id): {len(groups)} -> {groups}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
