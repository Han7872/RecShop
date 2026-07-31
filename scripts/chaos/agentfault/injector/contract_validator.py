# -*- coding: utf-8 -*-
"""Synthesizer 输出契约校验器 —— format_violation 故障的**对应消费方**。

照 `services/llm_rerank_service/utils/validator.py` 的 `validate_rerank_response` 四查结构
(JSON 可解析 / 必需字段 / item∈候选 / 值一致),适配 rec_agent 的 Synthesizer 契约
(强制 tool_call `Synthesize_Recommendation`,四必需字段 recommended_product / product_title /
recommendation_reason / confidence,见 workflow.py `_SYNTH_TOOL_RECOMMEND`)。

**这一个校验器同时是 format_violation 与 wrong_item_pick 的消费方**(你要的"被对应方法消费"):
- format_violation → 违反"JSON 可解析 / 必需字段 / 类型"三查之一;
- wrong_item_pick  → 违反"recommended_product ∈ 候选 ASIN"一查(candidates 给定时)。
一个契约,故障表现为**哪一项 check 失败**不同 → 采集/评测据 check 结果标 GT、算契约有效率。

设计见项目 README 现实(memory recweb2-progress):rec_agent 的 /recommend 无候选集
(候选是 SASRec 全词表,不在响应里),故 item∈候选 查**仅在 candidates 显式给定时**启用
(/recommend/from_candidates 或采集时喂 SASRec top-100);无候选时该查跳过、只做结构四查。

用法:
  from contract_validator import validate_synthesizer_contract
  ok, checks = validate_synthesizer_contract(tool_call_args, candidates=None)
  # ok: bool(全过); checks: 逐项 {name: {"passed": bool, "detail": str}}
"""
import json


REQUIRED_FIELDS = [
    "recommended_product",
    "product_title",
    "recommendation_reason",
    "confidence",
]


def _try_parse(args):
    """args 可能是 dict(已解析 tool_call)或 JSON 串(raw function.arguments)。
    返回 (dict, err) —— 照 validator._try_parse_json 支持裸串/截断串失败即 None。"""
    if isinstance(args, dict):
        return args, None
    if isinstance(args, str):
        s = args.strip()
        try:
            return json.loads(s), None
        except json.JSONDecodeError:
            # 尝试提取第一个 {...} 块(同 llm_rerank validator 容错)
            b0, b1 = s.find("{"), s.rfind("}")
            if b0 != -1 and b1 > b0:
                try:
                    return json.loads(s[b0:b1 + 1]), None
                except json.JSONDecodeError:
                    pass
            return None, "JSON 解析失败"
    return None, f"不支持的 args 类型: {type(args).__name__}"


def validate_synthesizer_contract(args, candidates=None):
    """校验 Synthesizer 输出契约。返回 (all_passed: bool, checks: dict)。

    checks 逐项(照 validate_rerank_response 四查):
      1. json_parsable      : args 可解析为对象
      2. required_fields    : 四必需字段齐全
      3. field_types        : recommended_product/product_title/recommendation_reason 为非空串,
                              confidence 为可转 float 的数值(接受 int/float/数值串)
      4. item_in_candidates : recommended_product ∈ candidates 的 item_id(仅 candidates 给定时)
    """
    checks = {}

    # --- 1. JSON 可解析 ---
    parsed, perr = _try_parse(args)
    if parsed is None:
        checks["json_parsable"] = {"passed": False, "detail": perr or "解析失败"}
        return False, checks
    checks["json_parsable"] = {"passed": True, "detail": "ok"}

    # --- 2. 必需字段 ---
    missing = [k for k in REQUIRED_FIELDS if k not in parsed]
    if missing:
        checks["required_fields"] = {"passed": False, "detail": f"缺少字段: {missing}"}
        # 缺字段则后续类型/候选查无意义,直接返回(与 llm_rerank validator 早退一致)
        return False, checks
    checks["required_fields"] = {"passed": True, "detail": "ok"}

    # --- 3. 字段类型/非空 ---
    type_errs = []
    for k in ("recommended_product", "product_title", "recommendation_reason"):
        v = parsed.get(k)
        if not isinstance(v, str) or not v.strip():
            type_errs.append(f"{k} 非非空串(={v!r})")
    conf = parsed.get("confidence")
    try:
        float(conf)  # 接受 int/float/数值串;非数值(如"非常高")抛 → 类型违反
    except (TypeError, ValueError):
        type_errs.append(f"confidence 非数值(={conf!r})")
    if type_errs:
        checks["field_types"] = {"passed": False, "detail": "; ".join(type_errs)}
        return False, checks
    checks["field_types"] = {"passed": True, "detail": "ok"}

    # --- 4. item ∈ 候选(仅 candidates 给定时) ---
    if candidates:
        cand_ids = set()
        for c in candidates:
            cid = (c.get("item_id") if isinstance(c, dict) else c)
            if cid is not None:
                cand_ids.add(str(cid).strip())
        pick = str(parsed.get("recommended_product")).strip()
        if pick not in cand_ids:
            checks["item_in_candidates"] = {
                "passed": False,
                "detail": f"recommended_product '{pick}' 不在候选集({len(cand_ids)} 项)中",
            }
            return False, checks
        checks["item_in_candidates"] = {"passed": True, "detail": "ok"}
    else:
        checks["item_in_candidates"] = {"passed": None, "detail": "跳过(无 candidates)"}

    all_passed = all(c["passed"] for c in checks.values() if c["passed"] is not None)
    return all_passed, checks


def first_failed_check(checks):
    """返回第一个失败的 check 名(供 GT 标注:故障表现为哪一项契约违反)。无失败返回 None。"""
    order = ["json_parsable", "required_fields", "field_types", "item_in_candidates"]
    for name in order:
        c = checks.get(name)
        if c and c.get("passed") is False:
            return name
    return None
