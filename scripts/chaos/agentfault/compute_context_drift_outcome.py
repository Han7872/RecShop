# -*- coding: utf-8 -*-
"""context_drift outcome 判定(离线后处理,照抄 chaosgraph outcome 思想)。

context_drift 的 GT(注入 agent = target)由**注入台账**给,与 outcome 无关。outcome 是**附加难度标签**:
注入(删上游结论)对系统最终推荐是否**致偏**——

  - 找该 case **同 carrier_seq_id 的 normal case**作 clean 对照;
  - 对比两者 recommendation 的 recommended_product(asin)(附带 product_title):
      相同   -> recovered     (系统容错,注入无害;方法要靠 content 轨而非 outcome 定位)
      不同   -> silent_wrong  (故障致下游推荐偏移;黑盒可能看不出,内容层才见)
      无同载体 normal -> unknown(记警告)

产物:(v1)context_drift_outcomes.json
  {case_id: {outcome, clean_rec, drift_rec, clean_title, drift_title, carrier_seq_id}}
并打印 recovered/silent_wrong/unknown 计数。

纯离线、幂等、utf-8。数据来源 = 采完的数据集树(journal/*.json 的 carrier_seq_id+kind + raw/*.json 的 resp)。

用法:
  python compute_context_drift_outcome.py                       # 默认 (archived) agentfault
  python compute_context_drift_outcome.py --dataset-dir <tree>  # 指定树
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))                 # .../agentfault
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))    # repo root
DEFAULT_DATASET_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault")


def _load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _rec_of_raw(raw):
    """从 raw/*.json 取 recommendation(recommended_product asin + product_title)。"""
    if not isinstance(raw, dict):
        return None, None
    resp = raw.get("resp") or {}
    rec = resp.get("recommendation") if isinstance(resp, dict) else None
    if not isinstance(rec, dict):
        return None, None
    return rec.get("recommended_product"), rec.get("product_title")


def _iter_cases(dataset_dir):
    """遍历 journal/*.json,yield (case_id, kind, carrier_seq_id, asin, title)。"""
    jr_dir = os.path.join(dataset_dir, "journal")
    raw_dir = os.path.join(dataset_dir, "raw")
    if not os.path.isdir(jr_dir):
        return
    for fn in sorted(os.listdir(jr_dir)):
        if not fn.endswith(".json"):
            continue
        case_id = fn[:-5]
        jj = _load_json(os.path.join(jr_dir, fn))
        if not isinstance(jj, dict):
            continue
        kind = jj.get("kind")
        cseq = (jj.get("probe") or {}).get("carrier_seq_id", "")
        raw = _load_json(os.path.join(raw_dir, fn))
        asin, title = _rec_of_raw(raw)
        yield case_id, kind, cseq, asin, title


def compute(dataset_dir):
    # 建 normal 对照索引:carrier_seq_id -> (asin, title, case_id)
    normal_by_carrier = {}
    drift_cases = []
    for case_id, kind, cseq, asin, title in _iter_cases(dataset_dir):
        if kind == "normal":
            # 同 carrier 多条 normal 理论上唯一(每 rep 一 carrier);后者覆盖前者无妨(同载体)
            normal_by_carrier[cseq] = (asin, title, case_id)
        elif kind == "context_drift":
            drift_cases.append((case_id, cseq, asin, title))

    outcomes = {}
    counts = {"recovered": 0, "silent_wrong": 0, "unknown": 0}
    warnings = []
    for case_id, cseq, drift_asin, drift_title in drift_cases:
        clean = normal_by_carrier.get(cseq)
        if clean is None:
            outcome = "unknown"
            clean_asin = clean_title = None
            warnings.append(f"{case_id}: no normal case with carrier_seq_id={cseq!r}")
        else:
            clean_asin, clean_title, _clean_case = clean
            # 主判据 = recommended_product asin(稳定存在);None 视为不同(除非两侧都 None)
            same = (clean_asin == drift_asin)
            outcome = "recovered" if same else "silent_wrong"
        counts[outcome] += 1
        outcomes[case_id] = {
            "outcome": outcome,
            "carrier_seq_id": cseq,
            "clean_rec": clean_asin if clean else None,
            "drift_rec": drift_asin,
            "clean_title": clean_title if clean else None,
            "drift_title": drift_title,
        }
    return outcomes, counts, warnings


def main():
    ap = argparse.ArgumentParser(description="context_drift outcome (recovered/silent_wrong/unknown)")
    ap.add_argument("--dataset-dir", default=DEFAULT_DATASET_DIR,
                    help="dataset tree root (has journal/ + raw/); default (archived) agentfault")
    ap.add_argument("--out", default=None,
                    help="output json (default <dataset-dir>/context_drift_outcomes.json)")
    args = ap.parse_args()

    dataset_dir = os.path.abspath(args.dataset_dir)
    out_path = args.out or os.path.join(dataset_dir, "context_drift_outcomes.json")

    outcomes, counts, warnings = compute(dataset_dir)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(outcomes, f, ensure_ascii=False, indent=2)

    total = sum(counts.values())
    print(f"[ctxdrift-outcome] dataset_dir={dataset_dir}")
    print(f"[ctxdrift-outcome] context_drift cases = {total}")
    print(f"[ctxdrift-outcome]   recovered    = {counts['recovered']}")
    print(f"[ctxdrift-outcome]   silent_wrong = {counts['silent_wrong']}")
    print(f"[ctxdrift-outcome]   unknown      = {counts['unknown']}")
    for w in warnings:
        print(f"[ctxdrift-outcome]   WARN {w}")
    print(f"[ctxdrift-outcome] -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
