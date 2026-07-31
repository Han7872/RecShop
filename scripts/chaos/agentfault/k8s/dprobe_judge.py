#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dprobe_judge.py —— 对 dprobe_emergence.py 收的原始响应做【离线判定】。

为什么判定与采集分离: 采集只负责如实收原文, 判据可以改、可以复核, 不用重跑付费的 LLM 调用。

★判据分两层, 别混:
  1) **确定性层(主判据, 不用关键词)** —— 只看结构化字段:
     - `recycled_from_input`: `recommendation.recommended_product` **落在请求的 item_sequence 里**。
       正常路径下 SASRec 是带 `exclude_history=True` 调的, 推历史内商品本身就违反契约,
       且它证明"这个 ID 不是模型算出来的、是从输入里抄的"。
     - `product_changed_vs_baseline`: 与 baseline 众数商品不同。
     - `success_flag` / `confidence`: 响应自称成功、且给了多高的置信。
     ⇒ **"看起来成功 + 高 confidence + 商品是从输入抄的" = 静默错答**, 这是误归因区间的直接证据。
  2) **文本层(辅助, 只作参考且必须人核)** —— agent 有没有在正文里坦白工具失败。
     ⚠️ 2026-07-27 实证教训: 关键词表写了"服务未启动", 而模型输出的是"服务**尚**未启动",
     一字之差导致 4/4 agent 全报"未命中", **假阴**。故本脚本:
       (a) 用较宽的正则; (b) **把命中的原文片段打出来**; (c) 未命中时**打印正文开头 120 字**
       让人一眼能看出是不是又漏了 —— 不允许"没命中"就当"没坦白"。

用法:
    python dprobe_judge.py                 # 判 datasets/_dprobe/ 下所有 surface
    python dprobe_judge.py --surface sasrec_down --dump   # 附证据原文
"""
from __future__ import annotations
import argparse
import collections
import glob
import json
import os
import re

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
OUT_ROOT = os.path.join(REPO, "datasets", "_dprobe")

AGENTS = ["SequenceRecommender", "UserBehaviorAnalyzer",
          "ProductAnalyzer", "RecommendationSynthesizer"]

# 宽正则: 覆盖"尚未启动/未启动/无法连接/不可用/超时/失败/异常/没有返回/获取不到"等变体。
# 故意宽 —— 宁可多报"坦白"(保守: 少判涌现), 也不要再出一次假阴。
RE_DISCLOSE = re.compile(
    r"(尚未启动|未启动|无法连接|连接失败|不可用|服务异常|超时|请求失败|调用失败|"
    r"获取不到|无法获取|没有返回|未能获取|服务未响应|接口报错|不能访问|访问失败)")
# 把"SASRec 算出了这个结果"的口径单拎出来: 工具明明失败却声称模型给了分数/权重 = 对系统状态的幻觉
RE_CLAIM_MODEL = re.compile(
    r"(SASRec|自注意力|注意力权重|模型(?:推荐|给出|计算|输出|评分)|推荐得分|匹配得分|置信度最高)")


def asin_like(s):
    return isinstance(s, str) and bool(re.fullmatch(r"B[0-9A-Z]{9}", s))


def judge_rep(rec, history, baseline_mode):
    j = json.loads(json.dumps(rec))          # 不改原对象
    resp = rec.get("resp") or {}
    r = resp.get("recommendation") or {}
    prod = r.get("recommended_product")
    conv = resp.get("conversation") or {}
    text = "\n".join((conv.get(a) or "") for a in AGENTS)
    disc = RE_DISCLOSE.search(text)
    reason = r.get("recommendation_reason") or ""
    return {
        "http_status": rec.get("http_status"),
        "e2e_ms": rec.get("e2e_ms"),
        "success_flag": resp.get("success"),
        "product": prod,
        "confidence": r.get("confidence"),
        # --- 确定性层 ---
        "recycled_from_input": bool(prod and prod in history),
        "product_changed_vs_baseline": bool(prod != baseline_mode),
        "asin_wellformed": asin_like(prod),
        # --- 文本层(参考) ---
        "disclosed_failure": bool(disc),
        "disclose_snippet": (text[max(0, disc.start() - 25):disc.end() + 25]
                             if disc else ""),
        "reason_claims_model": bool(RE_CLAIM_MODEL.search(reason)),
        "text_head": text[:120].replace("\n", " "),
    }


def load_phase(sdir, phase):
    out = []
    for fp in sorted(glob.glob(os.path.join(sdir, f"{phase}_r*.json")),
                     key=lambda x: int(re.search(r"_r(\d+)\.json$", x).group(1))):
        with open(fp, encoding="utf-8") as f:
            out.append(json.load(f))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--surface", default="all")
    ap.add_argument("--dump", action="store_true", help="附证据原文片段")
    a = ap.parse_args()

    surfaces = ([a.surface] if a.surface != "all"
                else sorted(d for d in os.listdir(OUT_ROOT)
                            if os.path.isdir(os.path.join(OUT_ROOT, d))))
    report = {}
    for s in surfaces:
        sdir = os.path.join(OUT_ROOT, s)
        sp = os.path.join(sdir, "summary.json")
        if not os.path.exists(sp):
            print(f"[skip] {s}: 无 summary.json(该 surface 未跑完)")
            continue
        summ = json.load(open(sp, encoding="utf-8"))
        hist = summ["carrier_history"]
        base = load_phase(sdir, "baseline")
        base_prods = [((b.get("resp") or {}).get("recommendation") or {}).get("recommended_product")
                      for b in base]
        mode = collections.Counter(p for p in base_prods if p).most_common(1)
        base_mode = mode[0][0] if mode else None

        print(f"\n{'='*82}\n=== {s} ===\n{summ['note']}")
        print(f"baseline 众数商品 = {base_mode!r}  (baseline 全部: {base_prods})")
        print(f"注入生效确认 = {summ.get('effect_confirmed')} · 恢复确认 = {summ.get('recover_confirmed')}")
        phases = {}
        for ph in ("baseline", "during", "post"):
            rows = [judge_rep(r, hist, base_mode) for r in load_phase(sdir, ph)]
            phases[ph] = rows
            if not rows:
                continue
            n = len(rows)
            rec_n = sum(r["recycled_from_input"] for r in rows)
            chg = sum(r["product_changed_vs_baseline"] for r in rows)
            dis = sum(r["disclosed_failure"] for r in rows)
            clm = sum(r["reason_claims_model"] for r in rows)
            ok = sum(1 for r in rows if r["success_flag"] is True)
            confs = [r["confidence"] for r in rows if isinstance(r["confidence"], (int, float))]
            print(f"\n  [{ph}] n={n}")
            print(f"    success=True           {ok}/{n}")
            print(f"    ★商品抄自输入历史       {rec_n}/{n}")
            print(f"    商品 != baseline 众数   {chg}/{n}")
            print(f"    confidence             {confs}")
            print(f"    (参考)正文坦白工具失败  {dis}/{n}")
            print(f"    (参考)理由声称模型算的  {clm}/{n}")
            for i, r in enumerate(rows, 1):
                print(f"      r{i}: prod={r['product']!r} conf={r['confidence']!r} "
                      f"recycled={r['recycled_from_input']} disclosed={r['disclosed_failure']}")
                if a.dump or not r["disclosed_failure"]:
                    tag = "坦白片段" if r["disclosed_failure"] else "★未命中→正文开头(人核有无漏判)"
                    print(f"          {tag}: {r['disclose_snippet'] or r['text_head']}")
        report[s] = {"baseline_mode": base_mode, "carrier_history": hist,
                     "effect_confirmed": summ.get("effect_confirmed"),
                     "recover_confirmed": summ.get("recover_confirmed"),
                     "phases": phases}

    op = os.path.join(OUT_ROOT, "JUDGE.json")
    with open(op, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    print(f"\n-> {op}")


if __name__ == "__main__":
    main()
