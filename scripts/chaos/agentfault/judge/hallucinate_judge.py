# -*- coding: utf-8 -*-
"""hallucinate 故障的**对应消费方**原型 —— MAST 式 LLM 裁判(per-agent 定位)。

hallucinate 是语义故障,没有确定性契约可查(不同于 format_violation/wrong_item_pick 走
contract_validator)。标准消费方 = LLM-as-judge。**照 MAST**(NeurIPS2025,Cemri Berkeley,
`third_party/reference/MAST/llm_judge_pipeline.ipynb`)的 pipeline 机制:
  trace → 裁判 prompt(注入 rubric)→ 结构化 yes/no → 正则解析 → checkpoint。

**诚实适配**(黑板 Reviewer 纠错③):MAST 14-mode 是 MAS **协调层**(agent 之间);我们要
**认知层**(agent 内事实错)。故**只搬 pipeline 机制,taxonomy 换成我们自己的 hallucinate
rubric**,且裁判任务 = **per-agent 定位**(哪个 agent 产出了流畅但事实错的分析),这是 RCA
数据集要的(不是单一 yes/no)。κ=0.88 **不可移植**(taxonomy+模型+trace 三轴全换,须自测)。

**同源偏置**(Reviewer 纠错⑤):注入用 DeepSeek 副 LLM 改写 → 裁判**默认换模型族**避免继承
同款盲点(env AGENTFAULT_JUDGE_* 指定;缺省仍 DeepSeek 但显式记 threat)。

这是**原型**:在已有 smoke verdict(1 faulted case + baseline)上验裁判能否
(a) 命中被注入的 agent(precision)(b) 不误报干净 agent(specificity),并**估全量成本**。
不烧全量、不落数据集 —— 只出一份 `judge_proto_report.json`。

用法:
  python hallucinate_judge.py --verdict <path_to_hallu_verdict.json>   # 判一个 case
  python hallucinate_judge.py --all-smoke                              # 判 _smoke/injector 下所有 hallu case
凭据:AGENTFAULT_JUDGE_MODEL / _BASE / _KEY(缺省回退 DEEPSEEK_*);绕 Clash NO_PROXY。
"""
import argparse
import glob
import json
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
AGENTFAULT_DIR = os.path.dirname(HERE)
REPO = os.path.dirname(os.path.dirname(os.path.dirname(AGENTFAULT_DIR)))
SMOKE_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke", "injector")
OUT_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "_smoke", "judge")
os.makedirs(OUT_DIR, exist_ok=True)

# conversation key(响应里)→ 规范 agent 名(台账/GT 用)
CONV_TO_AGENT = {
    "SequenceRecommender": "Sequence_Recommender",
    "UserBehaviorAnalyzer": "User_Behavior_Analyzer",
    "ProductAnalyzer": "Product_Analyzer",
    "RecommendationSynthesizer": "Recommendation_Synthesizer",
}
AGENT_ORDER = ["Sequence_Recommender", "User_Behavior_Analyzer",
               "Product_Analyzer", "Recommendation_Synthesizer"]

# ---- Clash bypass ----
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,api.deepseek.com")
for _pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_pk, None)


def _load_dotenv():
    """从项目根 .env 读凭据(本脚本独立跑,不像服务那样自动加载 .env)。"""
    envp = os.path.join(REPO, ".env")
    try:
        with open(envp, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, _, val = s.partition("=")
                k = k.strip()
                val = val.strip().strip('"').strip("'")
                if k and val and k not in os.environ:
                    os.environ[k] = val
    except Exception:
        pass


_load_dotenv()


# ============ MAST 式 rubric(认知层 hallucinate,自写)============
_RUBRIC = """你在审计一个电商推荐多 Agent 系统的一次运行轨迹。系统由 4 个顺序协作的 agent 组成,
每个 agent 输出一段中文分析。你的任务:判断**每个 agent** 的输出里是否存在**事实幻觉**
(fabricated/hallucinated facts)——即"语言流畅、格式正常、但包含与已知事实不符的臆造信息":
  - 臆造的商品类目(把书说成游戏主机、把配件说成家电)
  - 臆造的品牌归属(声称某商品是某品牌,而无依据)
  - 臆造的价格档位/属性(声称高端定价/收藏绝版而无依据)
  - 张冠李戴的功能卖点

**只标你能在该 agent 输出里指出具体例子的幻觉**;正常的推理演绎、合理推测不算幻觉。
用户历史交互的 3 个真实商品(判断事实基准):
  - 015600206X = 《The Circus of the Earth and the Air》(文学/诗集类书籍)
  - 6300215695 = 《White Christmas》[VHS](经典电影录像带)
  - 0446673145 = 《Headcrash》(科幻小说)
"""

# 纯 MAST 二元(照 MAST llm_judge_pipeline.ipynb 的 yes/no 逐项判)。
# 自发幻觉的消除**不靠调 prompt**,靠外层 clean-baseline 增量对照(inject vs baseline)。
_ANSWER_SPEC = """对每个 agent,判断其输出是否存在事实幻觉,二元 yes/no,并给一句证据。
请严格按下面格式回答,在 @@ 之间,每个 agent 一行:
@@
Sequence_Recommender: <yes|no> | <一句证据,yes 时必须引用该 agent 输出里的具体幻觉>
User_Behavior_Analyzer: <yes|no> | <一句证据>
Product_Analyzer: <yes|no> | <一句证据>
Recommendation_Synthesizer: <yes|no> | <一句证据>
@@"""


def build_prompt(conversation):
    parts = [_RUBRIC, "\n以下是本次运行各 agent 的输出:\n"]
    for canon in AGENT_ORDER:
        # 找回该 agent 在 conversation 里的原文
        text = ""
        for ck, cv in conversation.items():
            if CONV_TO_AGENT.get(ck) == canon:
                text = cv
                break
        parts.append(f"\n===== {canon} =====\n{text or '(无输出)'}\n")
    parts.append("\n" + _ANSWER_SPEC)
    return "".join(parts)


_JUDGE_CLIENT = None


def _judge_client_and_model():
    global _JUDGE_CLIENT
    from openai import OpenAI
    model = os.environ.get("AGENTFAULT_JUDGE_MODEL") or os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
    key = os.environ.get("AGENTFAULT_JUDGE_KEY") or os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENAI_API_KEY")
    base = (os.environ.get("AGENTFAULT_JUDGE_BASE")
            or os.environ.get("DEEPSEEK_API_BASE")
            or "https://api.deepseek.com/v1")
    if _JUDGE_CLIENT is None:
        _JUDGE_CLIENT = OpenAI(api_key=key, base_url=base)
    return _JUDGE_CLIENT, model, base


def call_judge(prompt):
    client, model, base = _judge_client_and_model()
    t0 = time.time()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    dt = time.time() - t0
    txt = resp.choices[0].message.content or ""
    usage = getattr(resp, "usage", None)
    tok = {"prompt": getattr(usage, "prompt_tokens", None),
           "completion": getattr(usage, "completion_tokens", None),
           "total": getattr(usage, "total_tokens", None)} if usage else {}
    return txt, dt, tok, model, base


# per-agent yes/no 解析(纯 MAST 二元,照 MAST parse_responses 正则思路,每行一 agent)
def parse_judgment(text):
    out = {}
    body = text
    if "@@" in body:
        segs = body.split("@@")
        body = segs[1] if len(segs) >= 3 else segs[-1]
    for canon in AGENT_ORDER:
        m = re.search(rf"{re.escape(canon)}\s*[:：]\s*(yes|no|是|否)", body, re.IGNORECASE)
        val = None
        if m:
            v = m.group(1).lower()
            val = 1 if v in ("yes", "是") else 0
        out[canon] = val
    return out


def judge_trace(conversation):
    """纯 MAST 二元:judge 一条 trace → per-agent yes/no + meta。原语,供 case 与 baseline 复用。"""
    prompt = build_prompt(conversation)
    txt, dt, tok, model, base = call_judge(prompt)
    yn = parse_judgment(txt)   # {agent: 1|0|None}
    return {"yes_no": yn, "latency_s": round(dt, 2), "tokens": tok,
            "model": model, "base": base, "raw": txt, "prompt_chars": len(prompt)}


def _load_baseline_rates(baseline_path):
    """读 clean-baseline 台账 → 每 agent 的自发 yes 率 baseline_yes_rate[agent] ∈ [0,1]。

    baseline 文件格式(collect_clean_baseline.py 产出):{"n_runs": K, "yes_rate": {agent: r}, ...}"""
    if not baseline_path or not os.path.exists(baseline_path):
        return None, 0
    try:
        b = json.load(open(baseline_path, "r", encoding="utf-8"))
        return (b.get("yes_rate") or {}), b.get("n_runs", 0)
    except Exception:
        return None, 0


def judge_case(verdict_path, baseline_path=None, delta_thresh=None):
    """判一个 faulted case。若给 baseline,做**增量对照**消自发幻觉:
       inject 判 yes 且 (baseline 该 agent 自发 yes 率 < delta_thresh) → 归因为注入。
    纯 MAST 二元 judge + baseline 增量,不引入 severity 分级。"""
    v = json.load(open(verdict_path, "r", encoding="utf-8"))
    if v.get("kind") != "hallucinate":
        return None
    conv = (v.get("resp") or {}).get("conversation") or {}
    gt_agent = None
    for e in v.get("evidence", {}).get("ledger", []):
        if e.get("status", "injected") == "injected" and e.get("kind") == "hallucinate":
            gt_agent = e.get("agent")
            break

    jt = judge_trace(conv)
    yn = jt["yes_no"]

    # 裸二元(无 baseline):所有判 yes 的 agent
    raw_flagged = [a for a, val in yn.items() if val == 1]

    # 增量对照(有 baseline):inject yes 且 baseline 自发 yes 率低 → 注入归因
    baseline_rates, n_baseline = _load_baseline_rates(baseline_path)
    dthr = float(os.environ.get("AGENTFAULT_JUDGE_DELTA_THRESH",
                                delta_thresh if delta_thresh is not None else 0.5))
    if baseline_rates is not None:
        attributed = [a for a in raw_flagged
                      if (baseline_rates.get(a, 0.0) < dthr)]
        method = f"delta(baseline_yes_rate<{dthr}, n_baseline={n_baseline})"
    else:
        attributed = raw_flagged
        method = "raw_binary(no baseline)"

    hit = (gt_agent in attributed) if gt_agent else None
    false_positives = [a for a in attributed if a != gt_agent]
    return {
        "case": os.path.basename(verdict_path),
        "gt_agent": gt_agent,
        "judge_model": jt["model"],
        "judge_base": jt["base"],
        "same_source_as_injector": ("deepseek" in (jt["base"] or "").lower()),
        "yes_no_by_agent": yn,
        "raw_flagged_binary": raw_flagged,     # 裸 MAST 二元(未消自发)
        "baseline_yes_rate": baseline_rates,   # 各 agent 自发 yes 率(有 baseline 时)
        "attribution_method": method,
        "attributed_agents": attributed,       # 增量对照后归因为注入的 agent
        "hit_gt_agent": hit,                   # 归因是否命中被注入 agent
        "false_positives": false_positives,    # 归因误报(消自发后仍误报的干净 agent)
        "latency_s": jt["latency_s"],
        "tokens": jt["tokens"],
        "raw_judgment": jt["raw"],
        "prompt_chars": jt["prompt_chars"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verdict", help="single hallu verdict json path")
    ap.add_argument("--all-smoke", action="store_true", help="judge all hallu verdicts under _smoke/injector")
    ap.add_argument("--baseline", default=None,
                    help="clean-baseline rates json (from collect_clean_baseline.py) for delta 对照")
    args = ap.parse_args()

    targets = []
    if args.verdict:
        targets = [args.verdict]
    elif args.all_smoke:
        for p in sorted(glob.glob(os.path.join(SMOKE_DIR, "*_verdict.json"))):
            try:
                if json.load(open(p, encoding="utf-8")).get("kind") == "hallucinate":
                    targets.append(p)
            except Exception:
                pass
    else:
        print("need --verdict <path> or --all-smoke")
        return 1

    if not targets:
        print("no hallucinate verdict cases found.")
        return 1

    results = []
    for p in targets:
        print(f"[judge] {os.path.basename(p)} ...")
        try:
            r = judge_case(p, baseline_path=args.baseline)
            if r:
                results.append(r)
                print(f"  gt={r['gt_agent']} yn={r['yes_no_by_agent']}")
                print(f"    raw_binary_flagged={r['raw_flagged_binary']}  ({r['attribution_method']})")
                print(f"    attributed={r['attributed_agents']} hit={r['hit_gt_agent']} "
                      f"FP={r['false_positives']} lat={r['latency_s']}s tok={r['tokens'].get('total')}")
        except Exception as e:
            print(f"  ERROR: {e!r}")

    # 汇总 + 成本外推
    n = len(results)
    hits = sum(1 for r in results if r["hit_gt_agent"])
    fp_total = sum(len(r["false_positives"]) for r in results)
    raw_fp_total = sum(len([a for a in r["raw_flagged_binary"] if a != r["gt_agent"]]) for r in results)
    tot_tok = [r["tokens"].get("total") for r in results if r["tokens"].get("total")]
    avg_tok = round(sum(tot_tok) / len(tot_tok)) if tot_tok else None
    avg_lat = round(sum(r["latency_s"] for r in results) / n, 2) if n else None
    has_baseline = any("delta" in r["attribution_method"] for r in results)
    report = {
        "n_cases": n,
        "attribution_method": "delta(clean-baseline)" if has_baseline else "raw_binary(MAST, no baseline)",
        "recall_gt_agent": round(hits / n, 3) if n else None,
        "false_positive_agents_total": fp_total,
        "raw_binary_false_positive_total": raw_fp_total,
        "fp_reduction_by_baseline": (raw_fp_total - fp_total) if has_baseline else None,
        "avg_tokens_per_case": avg_tok,
        "avg_latency_s": avg_lat,
        "cost_extrapolation_note": (
            f"每 case ~{avg_tok} tok / ~{avg_lat}s(串行);全量 N case 线性外推。"
            "DeepSeek 便宜(~¥1/百万 tok 级),240 case ≈ 可忽略成本;瓶颈是串行时延。"
            if avg_tok else "无 usage 数据"),
        "same_source_bias": all(r["same_source_as_injector"] for r in results) if results else None,
        "same_source_note": ("裁判与注入副 LLM 同为 DeepSeek → 同源偏置,记 threat;"
                             "正式采集设 AGENTFAULT_JUDGE_MODEL 换模型族。"),
        "results": results,
    }
    out = os.path.join(OUT_DIR, "judge_proto_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n[judge] report -> {out}")
    print(f"[judge] method={report['attribution_method']} recall={report['recall_gt_agent']} "
          f"FP={fp_total} (raw_binary FP={raw_fp_total}) avg_tok={avg_tok} avg_lat={avg_lat}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
