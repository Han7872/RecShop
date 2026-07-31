# -*- coding: utf-8 -*-
"""agentfault 数据集采集 runner —— (agent, kind[, subtype]) × K reps 双轨采集。

设计权威 = scripts/chaos/agentfault/COLLECTION_DESIGN.md(10 节 + ★采前审 FIX-A..F)。
本 runner 抄 ~60-70% agentchaos_runner 的"一实例→K probe 窗"结构(摊薄 ~150s 模型加载),
只换 fault-arming(注入器机制不同,不能 import agentchaos 的 AGENT_FAULT_* 钩子),content
提取器 / ledger 读器 / checksum / 契约校验器 **import** 自已验通的 injector_smoke /
contract_validator(单一真相源,不复制)。

铁律:不改 services/** 与 chaos/ctk/*;新文件只落 scripts/chaos/agentfault/collect/。
GT 由**注入台账**给(非 env 意图、非 judge);§6b 硬化 = 每 rep 非空 trace_id 精确匹配一条
status!=inject_failed 记录才打 faulted。

用法:
  # 列 combo:
  python collect/agentfault_runner.py --list
  # 小规模冒烟(1 combo×2 rep,写 _smoke 子区,主循环亲驱):
  python collect/agentfault_runner.py --only hallu_Product_Analyzer --runs 2 \
         --out-dir (v1)_smoke/collect
  # 全量(K reps,主循环 nohup 亲驱):
  python collect/agentfault_runner.py --runs 8
  # ★K8S 全栈(B 档,2026-07-27 加;combo/GT/CSV 全同,只换执行后端):
  python collect/agentfault_runner.py --backend k8s --runs 12 \
         --out-dir datasets/agentfault_k8s

★双后端(2026-07-27,B 档)
--------------------------------------------------------------------------------
本 runner 现在有两个执行后端(`collect/backends.py`):
  local(默认)= 本机隔离 harness,phase1 venv 起临时 rec_agent 进程 —— 产出 agentfault_v2 的口径;
  k8s        = 25 微服务全栈里的常驻 rec-agent pod(kubectl set env + rollout + proxy 探针)。
消 overclaim 用:v2 的 108 case 是"只起 rec-agent + sasrec 两个进程"采的,交付包里
"agent 跑在全栈内"这句话原本只能靠降级措辞遮掩。

★头号红线:`--backend local` 与改造前**逐字节等价**。本文件内的等价性依据:
  · start_instance / stop_instance / slot_ready(原 port_free)/ sample_host 现在是**一行
    dispatcher**,函数体原封不动搬进 backends.LocalBackend(逐字,见该文件每个方法上方的
    "搬自 …" 注释);
  · probe / wait_health / read_spans 由 LocalBackend **运行时转调** ISM 的同名函数;
  · 新增的 `BACKEND.sync_ledger()` 钩子在 local 后端是 `pass`;
  · `BACKEND.extra_csv_columns()` 在 local 返回 `[]` → COLS 与 csv_columns() 逐字相同 →
    CSV 表头字节不变;`extra_row_fields()` 返回 `{}` → 行内容不变;
  · `BACKEND.summary_meta()` 在 local 返回 None → run_summary.json 不多写 key。
  · 环境无关的部分(build_combos / _determine_gt / _content_track / build_row /
    csv_columns / journal resume 门 / checksum 闸 / 契约校验器)**一个字符没动**。

注意:本 runner **不**自动跑全量、不 spawn 于 import 期(所有起实例都在 __main__ 下的函数里)。
"""
import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
# 注:subprocess 随 start_instance 一起搬去了 backends.LocalBackend,本文件不再需要

# ---------------- 绕 Clash(本 driver 自身 HTTP)----------------
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")
for _pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_pk, None)

# ---------------- 路径 ----------------
HERE = os.path.dirname(os.path.abspath(__file__))            # .../agentfault/collect
AGENTFAULT_DIR = os.path.dirname(HERE)                       # .../agentfault
INJECTOR_DIR = os.path.join(AGENTFAULT_DIR, "injector")
JUDGE_DIR = os.path.join(AGENTFAULT_DIR, "judge")
ASSETS_DIR = os.path.join(AGENTFAULT_DIR, "assets")
CARRIER_POOL = os.path.join(ASSETS_DIR, "carrier_pool.json")   # 载体池(历史序列轮换,P0-2)
REPO = os.path.dirname(os.path.dirname(os.path.dirname(AGENTFAULT_DIR)))  # repo root

# ---------------- 复用 injector_smoke(content 提取器 / ledger 读器 / checksum / env 常量)----------------
# 注:import injector_smoke 只触发其模块级(设 env、建 _smoke 目录、import contract_validator),
# 绝不跑 main()(__main__ 守卫)——无 spawn 副作用。
if INJECTOR_DIR not in sys.path:
    sys.path.insert(0, INJECTOR_DIR)
import injector_smoke as ISM  # noqa: E402
from contract_validator import (  # noqa: E402  (与 injector_smoke 同源,直接 import 更清)
    validate_synthesizer_contract,
    first_failed_check,
)

# 执行后端(local = 原行为逐字搬迁;k8s = 全栈 pod)。与本文件同目录,单独一层是为了
# 让"环境相关的 12 个 seam"全部收敛到一处,runner 本体只剩环境无关逻辑。
# ★用**显式文件路径**装载,不往 sys.path 插目录(回归审查 F4):`sys.path.insert(0, HERE)`
#   会把 collect/ 塞到**所有** importer 的搜索首位,将来这里多一个与 stdlib/三方同名的模块
#   就会静默遮蔽。同时把它注册进 sys.modules["backends"],让 tests_dev 里的 `import backends`
#   拿到的是**同一个**模块对象(monkeypatch 才有意义)。
if "backends" in sys.modules:
    BK = sys.modules["backends"]
else:
    import importlib.util as _ilu  # noqa: E402
    _bk_spec = _ilu.spec_from_file_location("backends", os.path.join(HERE, "backends.py"))
    BK = _ilu.module_from_spec(_bk_spec)
    sys.modules["backends"] = BK          # 先注册再 exec:允许模块内自引用,也避免重复装载
    _bk_spec.loader.exec_module(BK)
BackendTransientError = BK.BackendTransientError  # 瞬时:该 combo 作废、可干净 resume
BackendFatalError = BK.BackendFatalError          # 结构性:硬停整轮(rc=5)

# 从 injector_smoke 拿常量/机制(单一真相源)
AGENT_NAMES = ISM.AGENT_NAMES
WRONG_ASIN = ISM.WRONG_ASIN
PROBE_SEQ = ISM.PROBE_SEQ
PROBE_TOPK = ISM.PROBE_TOPK
VENV_PY = ISM.VENV_PY                 # phase1_venv(非 conda python)
SVC_DIR = ISM.SVC_DIR                 # (现由 backends.LocalBackend 使用;此处保留供外部引用)
DEAD_OTLP = ISM.DEAD_OTLP
LOADER_DIR = ISM.LOADER_DIR           # injector/loader(sitecustomize 先行)
FORMAT_EXPECT_CHECK = ISM.FORMAT_EXPECT_CHECK

NAN = float("nan")                    # (sample_host 已搬去 backends;保留符号供外部引用)


class ChecksumDriftError(Exception):
    """业务表(items/inventory)CHECKSUM 漂移 = 污染 → main 硬停整轮(非只跳该 combo)。"""

# ---------------- 默认输出根(正式采集区;_smoke 由 --out-dir 覆盖)----------------
DEFAULT_OUT_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault")

# 注:host 水位的 psutil 依赖已随 sample_host 一起搬进 backends.LocalBackend
#     (那边保留了同样的 try-import 与失败回退 NAN,行为不变)。


# ============================================================
# Case 矩阵(§1)——每 combo = K reps;combo id = group_id = scenario_id
# ============================================================
def _combo(cid, kind, agent, subtype=None, field=None, faulted=True,
           drop=None, subtypes=None):
    """一个 combo 定义。
    drop     : context_drift 用——要从下游上下文丢弃的上游 agent 名(→ AGENTFAULT_DROP_AGENT)。
    subtypes : format_violation 收敛用——[(subtype, field), ...] 列表,rep 间轮换(见 rep_subtype)。
               subtypes 非空 → 该 combo 为 per-rep-instance 模式(每 rep 重起实例带该 rep 的 subtype env)。
    """
    return {"id": cid, "kind": kind, "agent": agent,
            "subtype": subtype, "field": field, "faulted": faulted,
            "drop": drop, "subtypes": subtypes}


# format_violation 4 subtype(收敛进 1 combo,rep 间轮换);field 选可复现被抓的检
#   missing_field -> 缺必需字段 -> required_fields;type_violation -> confidence 非数值 -> field_types;
#   empty_required -> 必需串字段置空 -> field_types;malformed_json -> 截断 -> json_parsable。
FORMAT_SUBTYPES = [
    ("missing_field", "confidence"),
    ("type_violation", "confidence"),
    ("empty_required", "recommended_product"),
    ("malformed_json", None),
]


def rep_subtype(combo, rep_i):
    """给 per-rep-instance combo(format)第 rep_i(1-based)选轮换 subtype/field。
    非 format(subtypes 为空)→ 回退 combo 固定 subtype/field(None/None)。"""
    subs = combo.get("subtypes")
    if not subs:
        return combo.get("subtype"), combo.get("field")
    subtype, field = subs[(rep_i - 1) % len(subs)]
    return subtype, field


def build_combos():
    """v2 P0-2 矩阵:8 faulted + 1 normal = 9 combo。GT(root_cause_set)= 被注入 agent。
    先验去退化:Synth 从 62.5%(v1:wrongpick + 4 format 独立)→ 3/8=37.5%
    (format 4 subtype 收敛为 1 combo + context_drift 铺到 UB/Product/Synth)。"""
    combos = []
    # hallucinate × 3 analyzer(agent 终答改写)—— GT: Seq / UB / Product
    for a in ("Sequence_Recommender", "User_Behavior_Analyzer", "Product_Analyzer"):
        combos.append(_combo(f"hallu_{a}", "hallucinate", a))
    # context_drift × 3(新)—— pre-call 删上游 agent 结论;GT = target(下游被注入 agent)
    #   env = AGENTFAULT_KIND_<target>=context_drift + AGENTFAULT_DROP_AGENT=<drop>
    ctxdrift_specs = [
        ("ctxdrift_ub_from_seq",     "User_Behavior_Analyzer",     "Sequence_Recommender"),
        ("ctxdrift_prod_from_ub",    "Product_Analyzer",           "User_Behavior_Analyzer"),
        ("ctxdrift_synth_from_prod", "Recommendation_Synthesizer", "Product_Analyzer"),
    ]
    for cid, target, drop in ctxdrift_specs:
        combos.append(_combo(cid, "context_drift", target, drop=drop))
    # wrong_item_pick × Synthesizer(ASIN 换哨兵)—— GT: Synth
    combos.append(_combo("wrongpick_Recommendation_Synthesizer",
                         "wrong_item_pick", "Recommendation_Synthesizer"))
    # format_violation × Synthesizer —— 4 subtype 收敛为 1 combo,rep 间轮换 subtype;GT: Synth
    combos.append(_combo("format_Recommendation_Synthesizer",
                         "format_violation", "Recommendation_Synthesizer",
                         subtypes=list(FORMAT_SUBTYPES)))
    # normal(不注入)—— 负类 + judge 率对照臂(FIX-B/§9.6:必走主流程产 CSV 负类行)
    combos.append(_combo("normal", "normal", None, faulted=False))
    return combos


COMBOS = build_combos()
COMBO_BY_ID = {c["id"]: c for c in COMBOS}


# ============================================================
# 临时实例 env(re-implement injector_smoke.build_env 配方;支持 normal 不注入 + 显式 subtype)
# ============================================================
def build_env(combo, port, span_file, ledger_file, subtype=None, field=None):
    """起临时 rec_agent 实例的 env。faulted combo arm 注入器;normal combo 只开内容捕获不注入。
    subtype/field:format_violation 用,per-rep 传入(不再 combo 固定);None 则回退 combo/默认。"""
    env = dict(os.environ)
    env["RECOMMENDATION_PORT"] = str(port)
    env["RECOMMENDATION_HOST"] = "127.0.0.1"
    env["NACOS_ENABLED"] = "false"
    env["OTEL_ENABLED"] = "true"
    env["OTEL_SERVICE_NAME"] = f"recweb_agentfault_collect_{combo['id']}"
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = DEAD_OTLP       # BSP 静默失败,不污染持久 Jaeger
    env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = DEAD_OTLP
    env["SPAN_FILE"] = span_file
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    for pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(pk, None)
    env.pop("SASREC_API_URL", None)                      # 真实 8200,永不经代理(护栏)
    # 清掉所有继承的 agent-fault 旋钮(§2:先 pop 所有 AGENTFAULT_KIND_*/AGENT_FAULT_*)
    for k in list(env.keys()):
        if k.startswith("AGENTFAULT_KIND_") or k.startswith("AGENT_FAULT_"):
            env.pop(k, None)
    for k in ("AGENTFAULT_INJECT", "AGENTFAULT_OBSERVE", "AGENTFAULT_FORMAT_SUBTYPE",
              "AGENTFAULT_FORMAT_FIELD",
              "AGENTFAULT_WRONG_ASIN", "AGENTFAULT_LEDGER", "AGENTFAULT_DROP_AGENT"):
        env.pop(k, None)
    # 内容捕获 always on(faulted 需看注入后内容;normal baseline 也要 openinference 内容)
    env["AGENTFAULT_INSTRUMENT"] = "1"
    env["AGENTFAULT_LEDGER"] = ledger_file               # 每 combo 独立台账
    if combo["faulted"]:
        env["AGENTFAULT_INJECT"] = "1"                   # arm 注入器
        env["AGENTFAULT_KIND_" + combo["agent"]] = combo["kind"]
        if combo["kind"] == "wrong_item_pick":
            env["AGENTFAULT_WRONG_ASIN"] = WRONG_ASIN
        elif combo["kind"] == "context_drift":
            # pre-call 删上游 agent 结论;drop = 该 combo 要丢弃的上游 agent
            drop = combo.get("drop")
            if drop:
                env["AGENTFAULT_DROP_AGENT"] = drop
        elif combo["kind"] == "format_violation":
            # subtype/field per-rep 传入(收敛后不再 combo 固定);None 回退 combo 值
            sub = subtype if subtype is not None else combo.get("subtype")
            fld = field if field is not None else combo.get("field")
            if sub:
                env["AGENTFAULT_FORMAT_SUBTYPE"] = sub
            if fld:
                env["AGENTFAULT_FORMAT_FIELD"] = fld
    else:
        # normal(clean):AGENTFAULT_INJECT 仍**不设** → loader 不 install 注入器 → 零注入(§2 clean)。
        # 改为 arm **只观测不注入** patch(install_observer):唯一动作 = 发 agentfault.resolved_input
        # span 后原样调原 _generate;不碰 messages、不写台账、无任何后处理。
        # 目的:基线也有 resolved_input 轨迹 → 可测结构化检测器在无故障运行上的误报率。
        # 副作用(有意):observer 与 injector 同样强制非流式,使 clean 与 faulted 调用模式对齐
        # (消除既存的 基线流式 / 故障非流式 口径不对称)。
        env["AGENTFAULT_OBSERVE"] = "1"
    # loader dir FIRST 让其 sitecustomize 先行
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = LOADER_DIR + (os.pathsep + pp if pp else "")
    return env


# ============================================================
# 执行后端(seam)—— 以下 4 个函数在改造前是本机实现,现在是**一行 dispatcher**。
#   函数体逐字搬进 backends.LocalBackend(见该文件每个方法上方的"搬自 …"标注),
#   local 路径新增的执行差异只有"一次属性查找 + 一层函数调用"。
# BACKEND 在**模块级**就初始化成 LocalBackend(不是 None):
#   tests_dev/test_p0_2_runner.py 直接 `import agentfault_runner as R` 后调
#   R.run_one_rep(...) 做离线自检,从不走 main() —— 若 BACKEND 留 None 那个自检会 AttributeError。
#   main() 里按 --backend 决定是否替换成 K8sBackend。
# ============================================================
BACKEND = BK.LocalBackend(build_env=build_env)


def start_instance(combo, port, span_file, ledger_file, log_path, subtype=None, field=None):
    """(seam S1)起一个"实例"。local=Popen 临时进程;k8s=set env + rollout + arm 校验。"""
    return BACKEND.start_instance(combo, port, span_file, ledger_file, log_path,
                                  subtype=subtype, field=field)


def stop_instance(proc):
    """(seam S2)停实例。local=terminate + 关日志句柄;k8s=转存 kubectl logs(不还原镜像)。"""
    return BACKEND.stop_instance(proc)


def slot_ready(port, combo_id=""):
    """(seam S3)采集位置是否可用。返回 (ok, reason)。

    ★改名自 `port_free`:两个后端语义**反转** —— local 是"端口必须空着"(要新起进程),
      k8s 是"服务必须活着且是变体镜像"(pod 常驻)。继续叫 port_free 会主动误导。
      只有 run_combo 一个调用点;LocalBackend 里的判断体仍是原 port_free 逐字搬迁。
    ★combo_id 传给后端而不是由调用点二次拼接:这样 LocalBackend 能原样吐出改造前那句
      `port {port} not free; refuse to start temp instance for {cid}`(逐字节,回归审查 F2)。
    """
    return BACKEND.slot_ready(port, combo_id)


# ============================================================
# span 聚合(ADAPT agentchaos aggregate_agent_spans:换 content 轨靠 ISM 提取器,infra 轨保 agentchaos 口径)
# ============================================================
def aggregate_agent_spans(spans):
    """按 parent_span_id 链聚合 4 条 agent.<Name> span(recweb.agent.name 属性)的
    duration/status + 子树 httpx(DeepSeek)/requests(SASRec)子 span 计数与 child_max。
    与 agentchaos 同口径 → eval_agentchaos Track B 列可复用。"""
    children = {}
    for s in spans:
        pid = s.get("parent_span_id") or ""
        children.setdefault(pid, []).append(s)

    agent_spans = {}
    for s in spans:
        attrs = s.get("attributes") or {}
        an = attrs.get("recweb.agent.name")
        if an and s.get("name", "").startswith("agent."):
            if an not in agent_spans or (s.get("duration_ms") or 0) > (agent_spans[an].get("duration_ms") or 0):
                agent_spans[an] = s

    def _http_url(attrs):
        for k in ("http.url", "url.full", "http.target", "http.route"):
            if k in attrs:
                return str(attrs[k])
        return ""

    def _is_sasrec(attrs):
        u = _http_url(attrs).lower()
        return (":8200" in u or ":18200" in u or "8200/" in u)

    out = {}
    for name in AGENT_NAMES:
        sp = agent_spans.get(name)
        if not sp:
            out[name] = {"duration_ms": None, "duration_corrected_ms": None,
                         "subllm_overhead_ms": 0.0, "status": None, "httpx_count": None,
                         "sasrec_requests_count": None, "child_max_duration_ms": None,
                         "present": False}
            continue
        sid = sp.get("span_id")
        httpx_count = 0
        sasrec_count = 0
        child_max = 0.0
        subllm_ms = 0.0   # ★注入伪影:该 agent 子树内 agentfault.subllm_rewrite span 总时长(可减)
        stack = list(children.get(sid, []))
        while stack:
            c = stack.pop()
            attrs = c.get("attributes") or {}
            # ★埋细:注入器给副 LLM 调用起的专属 span → 精确扣除延迟伪影(span 减 span,同测量体系)
            if c.get("name") == "agentfault.subllm_rewrite":
                subllm_ms += c.get("duration_ms") or 0.0
            has_http = any(k.startswith("http.") or k in ("url.full", "url.scheme") for k in attrs.keys())
            kind = c.get("kind", "")
            is_client = (kind == "CLIENT") or has_http
            if is_client and has_http:
                d = c.get("duration_ms") or 0.0
                if _is_sasrec(attrs):
                    sasrec_count += 1
                else:
                    httpx_count += 1
                if d > child_max:
                    child_max = d
            stack.extend(children.get(c.get("span_id"), []))
        raw_dur = sp.get("duration_ms")
        # corrected = 边界 span 时长 − 注入开销(subllm span);normal/无注入时 subllm_ms=0 → corrected==raw
        corrected = (raw_dur - subllm_ms) if isinstance(raw_dur, (int, float)) else raw_dur
        out[name] = {
            "duration_ms": raw_dur,
            "duration_corrected_ms": corrected,   # ★消延迟伪影后的时长(infra eval 用它)
            "subllm_overhead_ms": subllm_ms if subllm_ms > 0 else 0.0,
            "status": sp.get("status_code"),
            "httpx_count": httpx_count,
            "sasrec_requests_count": sasrec_count,
            "child_max_duration_ms": child_max if child_max > 0 else None,
            "present": True,
        }
    total_span_count = len(spans)
    error_span_count = sum(1 for s in spans if s.get("status_code") == "ERROR")
    return out, total_span_count, error_span_count


# ============================================================
# 推荐质量派生量(COPY agentchaos derive_quality;保 degrade/garbage 列名对齐 eval FEATURE_COLS)
# ============================================================
_GARBAGE_STR = "本环节分析结果不可用"
_DEGRADE_STR = "暂时不可用"
_CONV_KEY_MAP = {
    "Sequence_Recommender": "SequenceRecommender",
    "User_Behavior_Analyzer": "UserBehaviorAnalyzer",
    "Product_Analyzer": "ProductAnalyzer",
    "Recommendation_Synthesizer": "RecommendationSynthesizer",
}


def derive_quality(resp_json):
    out = {
        "confidence": None,
        "recommended_product": None,
        "recommended_product_is_unknown": None,
        "degrade_message_present": None,
        "garbage_message_present": None,
        "conv_text_len": {a: None for a in AGENT_NAMES},
        "conversation_captured": 0,
    }
    if not isinstance(resp_json, dict):
        return out
    rec = resp_json.get("recommendation") or {}
    if isinstance(rec, dict):
        out["confidence"] = rec.get("confidence")
        rp = rec.get("recommended_product")
        out["recommended_product"] = rp
        out["recommended_product_is_unknown"] = (rp == "unknown")
    conv = resp_json.get("conversation") or {}
    out["conversation_captured"] = 1 if (isinstance(conv, dict) and conv) else 0
    all_text = ""
    for name in AGENT_NAMES:
        txt = ""
        if isinstance(conv, dict):
            txt = conv.get(_CONV_KEY_MAP[name], "") or ""
        out["conv_text_len"][name] = len(txt)
        all_text += txt
    out["degrade_message_present"] = (_DEGRADE_STR in all_text)
    out["garbage_message_present"] = (_GARBAGE_STR in all_text)
    return out


# ============================================================
# host 水位(seam S9)—— local: psutil 进程级(逐字搬迁);k8s: Prometheus 容器级
#   ★两个后端的量纲不同(见 backends.K8sBackend.sample_host 注释),所以 K8S 树的 CSV
#     每行都带 collect_backend / host_metric_source 两列标口径(local 树不加列)。
# ============================================================
def sample_host(proc):
    return BACKEND.sample_host(proc)


# ============================================================
# CSV schema —— ★FIX-C:label 列名钉死 make_agentchaos_features 期望值
#   (root_cause_set 分号连接 + fault_<Agent> + 全 FEATURE_COLS + scenario_id→group_id + run_id→sample_id)
#   之后追加 content 轨列(make_agentchaos_features 按名取列,忽略额外列 → 安全)。
# ============================================================
def csv_columns():
    cols = [
        # 标签/溯源(make_agentchaos_features LEAK_COLS 剔出 X)
        "run_id", "scenario_id", "group_id", "kind",
        "root_cause_set", "n_root_causes", "fault_type_set",
    ]
    for a in AGENT_NAMES:
        cols.append(f"fault_{a}")            # 每 agent 注入类型(= 答案,LEAK)
    cols += [
        "window_start", "window_end", "trace_id",
        # infra Track A 黑盒
        "e2e_latency_ms", "http_status", "http_success",
    ]
    for a in AGENT_NAMES:                     # infra Track B per-agent span
        cols += [
            f"span_{a}_duration_ms",
            f"span_{a}_duration_corrected_ms",   # ★消注入延迟伪影后时长(infra eval 应用它,非 raw)
            f"span_{a}_subllm_overhead_ms",       # 扣掉的注入开销(provenance;normal=0)
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
    cols += [
        "host_cpu_pct", "host_mem_pct",
        "span_count_matched", "wallclock_sanity_ok", "note",
    ]
    # ---- content 轨列(NEW;注入指纹/契约违约/隔离/judge 输入指针)----
    cols += [
        "injected",                       # 主 faulted 标签(=ledger 非空 trace 精确匹配)
        "ledger_status",                  # injected / inject_failed / no_ledger_match / no_trace_id / none
        "format_subtype",                 # combo/violation 子类型(format 用)
        # hallucinate content
        "divergent_needle",               # 注入指纹(GT 溯源)
        "divergent_needle_present",       # 该 agent span 内容里 needle 出现?(内容层可见)
        # wrong_item_pick content
        "response_asin",                  # 响应 recommended_product
        "response_asin_is_sentinel",      # 响应 ASIN==哨兵?(主判据)
        "toolcall_asin_is_sentinel",      # span tool_call ASIN==哨兵?(不用 span_ 前缀,避免被 eval Track-B glob 误当特征)
        # format_violation content(消费方 = contract_validator)
        "contract_first_failed_check",    # 契约校验器首个失败项
        "contract_expected_check",        # 该 subtype 期望失败项
        "contract_check_matches_expected",
        # context_drift content(消费方 = canary;outcome 由离线脚本后填)
        "context_drift_dropped_agent",    # 被丢弃的上游 agent(GT 溯源)
        "context_drift_dropped_chars",    # int;canary:>0=注入点可见(上游结论已从下游上下文移除)
        "context_drift_outcome",          # 占位空;compute_context_drift_outcome.py 后填(recovered/silent_wrong/unknown)
        # 载体轮换(P0-2)
        "carrier_seq_id",                 # 本 case 用的载体池序列 id(GT/outcome 配对溯源)
        # 隔离负检 + judge 输入
        "non_target_injected",            # 除 target 外被注入的 agent(应空)
        "isolation_ok",
        "conversation_captured",          # 响应含 conversation(供 offline judge)
    ]
    return cols


# ★COLS = 环境无关的 csv_columns() + 后端追加列。
#   local 后端 extra_csv_columns() 返回 [] → COLS 与 csv_columns() **逐字相同** →
#   agentfault_v2 的表头一个字节不变(csv_columns() 本体一个字符没动)。
#   k8s 后端追加 4 列口径/provenance 标签(collect_backend / host_metric_source /
#   k8s_pod_name / k8s_pod_restarts),追加在**末尾**;四套 eval 与
#   make_agentchaos_features 都按列名取列、忽略额外列,故对下游安全。
#   main() 在装好 BACKEND 后会重算一次(见 main 内 `global COLS`)。
COLS = csv_columns() + BACKEND.extra_csv_columns()


def _r2(x):
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float) and not math.isnan(x):
        return round(x, 2)
    return x if x is not None else ""


def _b(x):
    """bool/None → 1/0/''。"""
    if x is None:
        return ""
    return int(bool(x))


# ============================================================
# 单 rep 判定(GT/labeling 逻辑——★核心,§3/§6b)
# ============================================================
def _determine_gt(combo, trace_id, ledger_file):
    """★§6b 硬化 + §3 台账 GT。返回 dict(faulted, root_cause_set, per_agent_fault,
    fault_type_set, ledger_status, matched_entries, non_target)。

    faulted 标签**只**在:combo faulted 且 trace_id 非空 且 台账有 status!=inject_failed 且
    该 rep trace_id 精确匹配(strict_trace=True)的记录时打(绝不按 env 意图标)。
    未落地(inject_failed/无匹配)→ 该 rep 视为 clean 负行(root_cause_set 空)+ note 溯源。
    """
    gt = {
        "faulted": False,
        "root_cause_set": [],
        "per_agent_fault": {a: "none" for a in AGENT_NAMES},
        "fault_type_set": [],
        "ledger_status": "none",
        "matched": [],
        "non_target": [],
    }
    if not combo["faulted"]:
        gt["ledger_status"] = "none"          # normal:无注入
        return gt
    agent = combo["agent"]
    kind = combo["kind"]
    if not trace_id:
        gt["ledger_status"] = "no_trace_id"   # 无 trace_id → 无法验证 → 不标 faulted
        return gt
    matched = ISM.read_ledger_entries(ledger_file, trace_id, agent, kind, strict_trace=True)
    if matched:
        gt["faulted"] = True
        rc = sorted({e.get("agent") for e in matched if e.get("agent")})
        gt["root_cause_set"] = rc
        for a in rc:
            gt["per_agent_fault"][a] = kind
        gt["fault_type_set"] = sorted({e.get("kind") for e in matched if e.get("kind")})
        gt["ledger_status"] = matched[0].get("status", "injected")
        gt["matched"] = matched
    else:
        # 组合意图注入但该 rep 未落地:区分 inject_failed vs 完全无记录
        raw = ISM.read_all_ledger(ledger_file)
        had_failed = any(e.get("agent") == agent and e.get("kind") == kind
                         and e.get("status") == "inject_failed"
                         and (e.get("trace_id") == trace_id)
                         for e in raw)
        gt["ledger_status"] = "inject_failed" if had_failed else "no_ledger_match"
    # 隔离负检(非 target 是否被注入,应空;strict_trace 防跨 rep 污染)
    gt["non_target"] = ISM.non_target_injected_agents(ledger_file, trace_id, agent, strict_trace=True)
    return gt


def _content_track(combo, gt, resp, spans, by_id, quality, subtype=None):
    """抽 content 轨特征(注入指纹/契约违约/哨兵 ASIN/context_drift canary),按 kind 分派。
    subtype:format_violation 该 rep 的意图 subtype(per-rep 轮换);None 回退 combo。"""
    ct = {
        "divergent_needle": "",
        "divergent_needle_present": "",
        "response_asin": quality.get("recommended_product") if quality else "",
        "response_asin_is_sentinel": "",
        "toolcall_asin_is_sentinel": "",
        "contract_first_failed_check": "",
        "contract_expected_check": "",
        "contract_check_matches_expected": "",
        "format_subtype": (subtype if subtype is not None else combo.get("subtype")) or "",
        "context_drift_dropped_agent": "",
        "context_drift_dropped_chars": "",
    }
    if ct["response_asin"] is None:
        ct["response_asin"] = ""
    kind = combo["kind"]
    matched = gt.get("matched") or []

    if kind == "hallucinate":
        needle = (matched[0].get("divergent_needle") if matched else "") or ""
        if not needle and matched:
            ie = (matched[0].get("injected_excerpt", "") or "").rstrip("…")
            needle = ie[-40:] if len(ie) > 40 else ie
        ct["divergent_needle"] = needle
        tgt_contents = ISM.chatopenai_output_contents(
            spans, by_id=by_id, only_agent=combo["agent"], agent_names=AGENT_NAMES)
        if needle:
            ct["divergent_needle_present"] = 1 if any(needle in c for c in tgt_contents) else 0

    elif kind == "wrong_item_pick":
        picked = ct["response_asin"]
        ct["response_asin_is_sentinel"] = 1 if picked == WRONG_ASIN else 0
        span_asins = ISM.chatopenai_toolcall_asins(spans)
        ct["toolcall_asin_is_sentinel"] = 1 if WRONG_ASIN in span_asins else 0

    elif kind == "context_drift":
        # canary:从台账取 dropped_agent/dropped_chars(注入生效 = removed>0)
        if matched:
            ct["context_drift_dropped_agent"] = matched[0].get("dropped_agent") or (combo.get("drop") or "")
            dc = matched[0].get("dropped_chars")
            ct["context_drift_dropped_chars"] = "" if dc is None else dc
        else:
            ct["context_drift_dropped_agent"] = combo.get("drop") or ""

    elif kind == "format_violation":
        eff_subtype = (subtype if subtype is not None else combo.get("subtype"))
        actual_subtype = (matched[0].get("violation", {}).get("subtype")
                          if matched else eff_subtype)
        ct["format_subtype"] = actual_subtype or (eff_subtype or "")
        expect = FORMAT_EXPECT_CHECK.get(actual_subtype)
        ct["contract_expected_check"] = expect or ""
        arg_strs = ISM.synthesizer_toolcall_arg_strings(spans)
        first_failed = None
        for a in arg_strs:
            ok_c, checks = validate_synthesizer_contract(a, candidates=None)
            fc = first_failed_check(checks)
            if not ok_c:
                first_failed = fc
                if expect is None or fc == expect:
                    break
        ct["contract_first_failed_check"] = first_failed or ""
        if expect is not None:
            ct["contract_check_matches_expected"] = 1 if (first_failed == expect) else 0
    return ct


def build_row(combo, case_id, trace_id, http_status, e2e_ms, agg, total_spans,
              error_spans, quality, host_cpu, host_mem, win_start, win_end,
              span_matched, wallclock_ok, gt, ct, note="", carrier_seq_id=""):
    rc_set = gt["root_cause_set"]
    row = {
        "run_id": case_id,
        "scenario_id": combo["id"],
        "group_id": combo["id"],
        "kind": combo["kind"],
        "root_cause_set": ";".join(rc_set),         # ★分号连接(FIX-C)
        "n_root_causes": len(rc_set),
        "fault_type_set": ";".join(gt["fault_type_set"]),
        "window_start": win_start, "window_end": win_end, "trace_id": trace_id,
        "e2e_latency_ms": _r2(e2e_ms),
        "http_status": http_status,
        "http_success": int(http_status == 200),
        "total_span_count": total_spans,
        "error_span_count": error_spans,
        "recommendation_confidence": _r2(quality["confidence"]) if quality["confidence"] is not None else "",
        "recommended_product_is_unknown": _b(quality["recommended_product_is_unknown"]),
        "degrade_message_present": _b(quality["degrade_message_present"]),
        "garbage_message_present": _b(quality["garbage_message_present"]),
        "host_cpu_pct": _r2(host_cpu), "host_mem_pct": _r2(host_mem),
        "span_count_matched": int(span_matched),
        "wallclock_sanity_ok": int(wallclock_ok),
        "note": note,
        # content 轨
        "injected": int(gt["faulted"]),
        "ledger_status": gt["ledger_status"],
        "format_subtype": ct["format_subtype"],
        "divergent_needle": ct["divergent_needle"],
        "divergent_needle_present": ct["divergent_needle_present"],
        "response_asin": ct["response_asin"],
        "response_asin_is_sentinel": ct["response_asin_is_sentinel"],
        "toolcall_asin_is_sentinel": ct["toolcall_asin_is_sentinel"],
        "contract_first_failed_check": ct["contract_first_failed_check"],
        "contract_expected_check": ct["contract_expected_check"],
        "contract_check_matches_expected": ct["contract_check_matches_expected"],
        "context_drift_dropped_agent": ct.get("context_drift_dropped_agent", ""),
        "context_drift_dropped_chars": ct.get("context_drift_dropped_chars", ""),
        "context_drift_outcome": "",            # 占位;由 compute_context_drift_outcome.py 后填
        "carrier_seq_id": carrier_seq_id,
        "non_target_injected": ";".join(gt["non_target"]),
        "isolation_ok": int(len(gt["non_target"]) == 0),
        "conversation_captured": quality.get("conversation_captured", 0),
    }
    for a in AGENT_NAMES:
        row[f"fault_{a}"] = gt["per_agent_fault"].get(a, "none")
        ag = agg.get(a, {})
        row[f"span_{a}_duration_ms"] = _r2(ag.get("duration_ms")) if ag.get("duration_ms") is not None else ""
        row[f"span_{a}_duration_corrected_ms"] = _r2(ag.get("duration_corrected_ms")) if ag.get("duration_corrected_ms") is not None else ""
        row[f"span_{a}_subllm_overhead_ms"] = _r2(ag.get("subllm_overhead_ms") or 0.0)
        row[f"span_{a}_status"] = ag.get("status") or ""
        row[f"span_{a}_child_httpx_count"] = "" if ag.get("httpx_count") is None else ag.get("httpx_count")
        row[f"span_{a}_child_sasrec_requests_count"] = "" if ag.get("sasrec_requests_count") is None else ag.get("sasrec_requests_count")
        row[f"span_{a}_child_max_duration_ms"] = _r2(ag.get("child_max_duration_ms")) if ag.get("child_max_duration_ms") is not None else ""
        row[f"span_{a}_present"] = int(bool(ag.get("present")))
        cl = quality["conv_text_len"].get(a)
        row[f"conv_{a}_text_len"] = "" if cl is None else cl
    return row


# ============================================================
# 增量落盘(ADAPT agentchaos append_csv + write_journal;加 skip-if-exists)
# ============================================================
def read_csv_header(csv_path):
    """读盘上 CSV 的表头(列名 list);文件不存在/空/读不动返 None。"""
    if not os.path.exists(csv_path):
        return None
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            return next(csv.reader(f))
    except Exception:
        return None


def append_csv(csv_path, row):
    new = not os.path.exists(csv_path)
    # ★★表头一致性硬闸(回归审查 F1 的配套缺口)。
    #   改造前 COLS 是模块级常量,盘上表头与它必然一致,所以这道校验不需要存在;
    #   现在 COLS = csv_columns() + BACKEND.extra_csv_columns() 会**随 backend 变**,
    #   一旦用 k8s 后端往 local 树追加(或反过来),DictWriter 会照着 86 列写进一个 82 列
    #   表头的文件,产出 ragged CSV —— pandas 读出来直接错位,而且是 append-only 无回退。
    #   宁可在这里硬失败(该 rep 不落盘、不写 journal → 下次 resume 干净重跑)。
    if not new:
        disk_cols = read_csv_header(csv_path)
        if disk_cols is not None and disk_cols != COLS:
            raise RuntimeError(
                f"CSV 表头与当前 backend 的列集不一致,拒绝追加(会写出 ragged CSV):\n"
                f"    盘上 {len(disk_cols)} 列: {csv_path}\n"
                f"    代码 {len(COLS)} 列 (backend={BACKEND.name})\n"
                f"    盘上独有: {[c for c in disk_cols if c not in COLS]}\n"
                f"    代码独有: {[c for c in COLS if c not in disk_cols]}\n"
                f"  -> 换一棵输出树(K8S 用 datasets/agentfault_k8s),别把两个 backend 的行混进同一棵")
    # ★采前审 FIX:append 前修复上次崩溃可能留下的**无换行尾行**(否则本次 writerow 拼到旧行 → 合并坏行)
    if not new:
        try:
            with open(csv_path, "rb") as rf:
                rf.seek(0, os.SEEK_END)
                if rf.tell() > 0:
                    rf.seek(-1, os.SEEK_END)
                    last = rf.read(1)
            if last not in (b"\n", b"\r"):
                with open(csv_path, "a", encoding="utf-8") as af:
                    af.write("\n")   # 补断尾换行,防拼接坏行
        except Exception:
            pass
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in COLS})
        f.flush()
        try:
            os.fsync(f.fileno())   # 原子落盘:CSV 行确保先于 journal 持久化
        except OSError:
            pass


def journal_path(out_dir, case_id):
    return os.path.join(out_dir, "journal", f"{case_id}.json")


def write_raw_journal(out_dir, combo, case_id, row, trace_id, win_start, win_end,
                      checksum_before, checksum_after, agg, gt, ct, raw_resp,
                      item_sequence=None, carrier_seq_id=""):
    raw_dir = os.path.join(out_dir, "raw")
    jr_dir = os.path.join(out_dir, "journal")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(jr_dir, exist_ok=True)
    # raw:存完整响应(含 conversation)供 offline judge 消费
    with open(os.path.join(raw_dir, f"{case_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"row": row, "agg": agg, "resp": raw_resp}, f, ensure_ascii=False, indent=2)
    journal = {
        "case_id": case_id, "combo_id": combo["id"], "kind": combo["kind"],
        "agent": combo["agent"], "subtype": row.get("format_subtype") or combo.get("subtype"),
        "field": combo.get("field"), "drop": combo.get("drop"),
        # ★P0-2:记该 case 实际用的载体序列 + carrier_seq_id(GT/outcome 溯源,非固定 PROBE_SEQ)
        "probe": {"item_sequence": item_sequence if item_sequence else PROBE_SEQ,
                  "top_k": PROBE_TOPK, "carrier_seq_id": carrier_seq_id},
        "trace_id": trace_id,
        "window": {"start": win_start, "end": win_end},
        # ★GT 由台账给(§3),照 agentchaos ground_truth 结构
        "ground_truth": {
            "root_cause_agent_set": gt["root_cause_set"],
            "n_root_causes": len(gt["root_cause_set"]),
            "fault_type_set": gt["fault_type_set"],
            "per_agent_fault": gt["per_agent_fault"],
            "faulted": gt["faulted"],
            "ledger_status": gt["ledger_status"],
            "source": "injection_ledger",
        },
        "content_track": {
            "divergent_needle": ct["divergent_needle"],
            "divergent_needle_present": ct["divergent_needle_present"],
            "response_asin": ct["response_asin"],
            "response_asin_is_sentinel": ct["response_asin_is_sentinel"],
            "toolcall_asin_is_sentinel": ct["toolcall_asin_is_sentinel"],
            "contract_first_failed_check": ct["contract_first_failed_check"],
            "contract_expected_check": ct["contract_expected_check"],
            "contract_check_matches_expected": ct["contract_check_matches_expected"],
            "context_drift_dropped_agent": ct.get("context_drift_dropped_agent", ""),
            "context_drift_dropped_chars": ct.get("context_drift_dropped_chars", ""),
            "non_target_injected": gt["non_target"],
        },
        "checksum": {
            "before": checksum_before, "after": checksum_after,
            "alarm": (_checksum_drift(checksum_before, checksum_after)),
        },
    }
    with open(journal_path(out_dir, case_id), "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)


def _checksum_drift(before, after):
    """两 checksum dict 是否有真实业务表漂移(仅当两侧都是有效 int 时比较)。"""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None
    if "_error" in before or "_error" in after:
        return None
    for t in ISM.CHECKSUM_TABLES:
        b, a = before.get(t), after.get(t)
        if isinstance(b, int) and isinstance(a, int) and b != a:
            return True
    return False


# ============================================================
# 单 rep
# ============================================================
def run_one_rep(combo, port, proc, span_file, ledger_file, case_id, timeout,
                carrier=None, subtype=None, trace_retries=2):
    # 载体轮换(P0-2):该 rep 用 carrier.history 作 item_sequence;缺则回退默认 PROBE_SEQ
    seq = (carrier or {}).get("history") if carrier else None
    carrier_seq_id = (carrier or {}).get("seq_id", "") if carrier else ""
    win_start = datetime.now(timezone.utc).isoformat()
    t0 = time.time()
    http_status, e2e_ms, resp = BACKEND.probe(port, seq=seq, top_k=PROBE_TOPK)
    trace_id = (resp or {}).get("trace_id", "") if isinstance(resp, dict) else ""
    # ★FIX-A:断言 trace_id 非空;空则有限次重试(空 trace 会污染 §6b 匹配)
    attempts = 0
    while (not trace_id) and attempts < trace_retries:
        attempts += 1
        http_status, e2e_ms, resp = BACKEND.probe(port, seq=seq, top_k=PROBE_TOPK)
        trace_id = (resp or {}).get("trace_id", "") if isinstance(resp, dict) else ""
    t1 = time.time()
    win_end = datetime.now(timezone.utc).isoformat()
    host_cpu, host_mem = sample_host(proc)

    spans = BACKEND.read_spans(span_file, trace_id) if trace_id else []
    by_id = {s["span_id"]: s for s in spans if s.get("span_id")}
    agg, total_spans, error_spans = aggregate_agent_spans(spans)
    quality = derive_quality(resp)

    # ★(seam S8)GT 判定前把远端注入台账同步到本地 ledger_file。
    #   local 后端 = pass(台账本来就写在本地盘上);k8s 后端 = 从 pod 的
    #   /agentfault-data/ledger.jsonl 增量 tail 回来。**不做这一步,_determine_gt 会
    #   恒返 no_ledger_match,96 个 faulted case 全部退化成负类**,而且只在 combo 末尾
    #   报一句 [QC-FAIL] —— 是"跑通了但数据全废"的头号成因。
    BACKEND.sync_ledger(ledger_file)
    gt = _determine_gt(combo, trace_id, ledger_file)
    ct = _content_track(combo, gt, resp, spans, by_id, quality, subtype=subtype)

    present = sum(1 for a in AGENT_NAMES if agg.get(a, {}).get("present"))
    span_matched = (present == len(AGENT_NAMES))

    wallclock_ok = True
    if spans:
        starts = [s.get("start_unix_nano") for s in spans if s.get("start_unix_nano")]
        if starts:
            lo = (t0 - 5.0) * 1e9
            hi = (t1 + 5.0) * 1e9
            wallclock_ok = all((lo <= s <= hi) for s in starts)

    note = ""
    if not trace_id:
        note = "no_trace_id_INVALID"
    elif combo["faulted"] and not gt["faulted"]:
        note = gt["ledger_status"]   # inject_failed / no_ledger_match(该 rep 未落地 → 负行)

    row = build_row(combo, case_id, trace_id, http_status, e2e_ms, agg, total_spans,
                    error_spans, quality, host_cpu, host_mem, win_start, win_end,
                    span_matched, wallclock_ok, gt, ct, note=note,
                    carrier_seq_id=carrier_seq_id)
    # ★后端 provenance/口径列(build_row 本体一个字符没动 —— 它是环境无关的)。
    #   local 返回 {} → row 与改造前逐字节相同;k8s 补 collect_backend / host_metric_source /
    #   k8s_pod_name / k8s_pod_restarts 四个值(对应 extra_csv_columns 的四列)。
    row.update(BACKEND.extra_row_fields())
    return row, gt, ct, resp, trace_id, win_start, win_end, agg, seq, carrier_seq_id


# ============================================================
# 单 combo(起实例→wait_health→warmup(不计,清台账)→K reps→checksum→stop)
# ============================================================
def run_combo(combo, runs, warmup, out_dir, port, timeout, carriers, verbose=True):
    cid = combo["id"]
    span_file = os.path.join(out_dir, "spans", f"{cid}.jsonl")
    ledger_file = os.path.join(out_dir, "ledgers", f"{cid}.jsonl")
    log_path = os.path.join(out_dir, "spans", f"{cid}.serverlog")
    csv_path = os.path.join(out_dir, "dataset_agentfault.csv")
    for d in ("spans", "ledgers", "journal", "raw"):
        os.makedirs(os.path.join(out_dir, d), exist_ok=True)

    case_ids = [f"{cid}__r{i}" for i in range(1, runs + 1)]
    # skip-if-exists 整 combo(所有 rep 已 journal)→ 跳过起实例(省 ~150s)
    if all(os.path.exists(journal_path(out_dir, c)) for c in case_ids):
        if verbose:
            print(f"[combo {cid}] all {runs} reps journaled -> SKIP (resume)", flush=True)
        return {"combo": cid, "kind": combo["kind"], "skipped": True,
                "reps": runs, "faulted_reps": None, "qc_ok": None}

    # 全新起(清 combo 的 span/ledger;既有 rep 的 journal/csv 保留,只补缺 rep)
    for p in (span_file, ledger_file, log_path):
        try:
            os.remove(p)
        except OSError:
            pass

    ok_slot, why_slot = slot_ready(port, cid)
    if not ok_slot:
        # ★why_slot 已由后端拼全(含 cid)。local 后端给出的正是改造前那句原文,
        #   不要在这里再套一层 f-string —— 那会让 run_summary.json 的 error 文本发生漂移。
        raise RuntimeError(why_slot)

    # ★同载体铁律(P0-2):所有 combo 的 rep_i 用同一 carrier[i-1]。载体池须 >= runs。
    if not carriers or len(carriers) < runs:
        raise RuntimeError(
            f"carrier pool has {len(carriers) if carriers else 0} < runs={runs}; "
            f"raise --max-users and rebuild {CARRIER_POOL}")

    # per-rep-instance 模式:format_violation(subtype 每 rep 轮换,注入器只在进程 env 读 subtype
    #   → 每 rep 必须重起实例带该 rep 的 subtype)。其它 combo = 单实例 → K reps(摊薄模型加载)。
    #   ★K8S 侧代价说明:这意味着 format combo 每 rep 一次 rollout(9 combo + 11 个 format
    #     额外 rep ≈ 20 次 rollout,单次 30-60s,合计 +10~20min)。相对 108 rep × 30-64s 的主体
    #     开销可忽略,且**本机对该 combo 本来就是每 rep 重起实例**,不是新增代价 —— 所以按
    #     "照本机语义"实现,不为省 rollout 去改注入器(那是最安全关键的文件,v1/v2 复现依赖它)。
    per_rep = bool(combo.get("subtypes"))

    checksum_before = ISM.checksum_tables()
    if verbose:
        print(f"\n=== COMBO {cid} kind={combo['kind']} agent={combo['agent']} "
              f"per_rep_instance={per_rep} runs={runs} ===", flush=True)
        print(f"  checksum before: {checksum_before}", flush=True)
        print(f"  SPAN_FILE={span_file}", flush=True)
        print(f"  LEDGER   ={ledger_file}", flush=True)

    counters = {"written": 0, "faulted_reps": 0}

    def _bring_up(subtype, field):
        """起实例→wait_health→warmup(不计)→清台账(隔离 warmup 注入)。返回 live proc。"""
        proc = start_instance(combo, port, span_file, ledger_file, log_path,
                              subtype=subtype, field=field)
        if not BACKEND.wait_health(port):
            stop_instance(proc)
            raise RuntimeError(f"temp instance {cid} on {port} not healthy; see {log_path}")
        if verbose:
            print(f"  healthy on {port}; warmup x{warmup} (uncounted) ...", flush=True)
        for w in range(warmup):
            # ★warmup 用默认 PROBE_SEQ(不是 carrier)—— 两个后端都必须保持这一条,
            #   别顺手"优化"成 carrier[0](会改动 v2 口径)。
            st, e2e, _ = BACKEND.probe(port)
            if verbose:
                print(f"    warmup {w+1}/{warmup}: http={st} e2e={e2e:.0f}ms", flush=True)
        # ★FIX-A:warmup 后**清空台账**——warmup 的注入记录(尤其空 trace)绝不混进 K reps
        #   (seam S7)local = 清空本地文件(原逐字实现);k8s = 把 pod 侧台账行号基线抬到
        #   当前值 + 清本地镜像(不能动 pod 里的文件,后续 rep 还要按行号取增量)。效果等价。
        try:
            BACKEND.reset_ledger(ledger_file)
        except (BK.BackendFatalError, BackendTransientError):
            # ★这一句 re-raise 是必需的:k8s 后端在 reset_ledger 里顺带做"span 真的在写吗"
            #   的硬断言(那是 warmup 之后、第一个计数 rep 之前的唯一时机),以及"pod 有没有
            #   在 warmup 期间被换掉"的瞬时判定。不 re-raise 就会被下面那个
            #   `except Exception -> [WARN]` 吞成一行警告 —— 而它要防的正是
            #   "整晚 108 行 total_span_count=0 且 resume 也救不回来"。
            #   LocalBackend 两类都不抛 → 本机路径行为逐字不变。
            raise
        except Exception as e:
            print(f"  [WARN] ledger truncate after warmup failed: {e!r}", flush=True)
        return proc

    def _emit_rep(i, proc, subtype):
        """跑并落一个 rep(carrier[i-1] + 该 rep subtype);返回 True=写了新行,False=skip。"""
        case_id = case_ids[i - 1]
        if os.path.exists(journal_path(out_dir, case_id)):
            if verbose:
                print(f"    rep {i}/{runs} {case_id}: journal exists -> skip", flush=True)
            return False
        carrier = carriers[i - 1]
        (row, gt, ct, resp, tid, ws, we, agg,
         seq, cseq) = run_one_rep(combo, port, proc, span_file, ledger_file,
                                  case_id, timeout, carrier=carrier, subtype=subtype)
        # ★采前审 FIX(resume-checkpoint,corrupts_dataset):CSV 行**先写**、journal **后写**
        # (journal = 最后落盘标记)。resume 门是 journal-exists → 若崩在两写之间,journal 不存在
        # → 该 rep 重跑(不会静默丢 CSV 行)。append_csv 后 flush+fsync 保原子。
        append_csv(csv_path, row)
        write_raw_journal(out_dir, combo, case_id, row, tid, ws, we,
                          checksum_before, None, agg, gt, ct, resp,
                          item_sequence=seq, carrier_seq_id=cseq)
        counters["written"] += 1
        counters["faulted_reps"] += int(gt["faulted"])
        if verbose:
            print(f"    rep {i}/{runs} {case_id}: http={row['http_status']} "
                  f"e2e={row['e2e_latency_ms']}ms spans={row['total_span_count']} "
                  f"carrier={cseq} subtype={row['format_subtype']} "
                  f"injected={row['injected']} ledger={row['ledger_status']} "
                  f"needle_present={row['divergent_needle_present']} "
                  f"asin_sentinel={row['response_asin_is_sentinel']} "
                  f"contract_match={row['contract_check_matches_expected']} "
                  f"drop_chars={row['context_drift_dropped_chars']} "
                  f"isolation_ok={row['isolation_ok']}", flush=True)
        return True

    if per_rep:
        # format:每 rep 重起实例(带该 rep 的 subtype/field env)
        for i in range(1, runs + 1):
            if os.path.exists(journal_path(out_dir, case_ids[i - 1])):
                if verbose:
                    print(f"    rep {i}/{runs} {case_ids[i-1]}: journal exists -> skip", flush=True)
                continue
            subtype, field = rep_subtype(combo, i)
            if verbose:
                print(f"  [per-rep] rep {i}/{runs} subtype={subtype} field={field}", flush=True)
            proc = _bring_up(subtype, field)
            try:
                _emit_rep(i, proc, subtype)
            finally:
                stop_instance(proc)
                time.sleep(1.0)
    else:
        proc = _bring_up(None, None)
        try:
            for i in range(1, runs + 1):
                _emit_rep(i, proc, None)
        finally:
            stop_instance(proc)
            time.sleep(1.0)

    written = counters["written"]
    faulted_reps = counters["faulted_reps"]

    checksum_after = ISM.checksum_tables()
    drift = _checksum_drift(checksum_before, checksum_after)
    if verbose:
        print(f"  checksum after : {checksum_after}  drift={drift}", flush=True)
    # 回填 journal after-checksum(combo 级)
    for c in case_ids:
        jp = journal_path(out_dir, c)
        if not os.path.exists(jp):
            continue
        try:
            with open(jp, "r", encoding="utf-8") as f:
                jj = json.load(f)
            jj["checksum"]["after"] = checksum_after
            jj["checksum"]["alarm"] = drift
            with open(jp, "w", encoding="utf-8") as f:
                json.dump(jj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ★zero-drift 断言(§2.5):真实业务表漂移 = 硬失败(无 DB 写本应恒等)。
    # 用专属异常 ChecksumDriftError,让 main **硬停整轮**(非只跳该 combo),否则后续 combo 在已污染 DB 上采。
    if drift is True:
        raise ChecksumDriftError(
            f"[ALARM] CHECKSUM DRIFT on combo {cid}: {checksum_before} -> {checksum_after}; "
            f"business table modified — ABORTING WHOLE RUN.")

    # ★FIX-B:faulted combo QC —— 至少 1 faulted rep,否则告警(不静默)
    qc_ok = True
    if combo["faulted"]:
        # 若整 combo 全 skip(resume 已完),written==0 → 从既有 journal 复核
        if written == 0:
            faulted_reps = _count_faulted_from_journals(out_dir, case_ids)
        if faulted_reps < 1:
            qc_ok = False
            print(f"  [QC-FAIL] combo {cid}: 0 faulted reps out of {runs} "
                  f"(all inject_failed / no_ledger_match) — 正类为空,须查副 LLM 拒答/注入失败!",
                  flush=True)
        elif verbose:
            print(f"  [QC-OK] combo {cid}: {faulted_reps} faulted reps", flush=True)

    return {"combo": cid, "kind": combo["kind"], "skipped": False,
            "reps": runs, "written": written, "faulted_reps": faulted_reps,
            "qc_ok": qc_ok, "checksum_drift": drift,
            "checksum_before": checksum_before, "checksum_after": checksum_after}


def load_carrier_pool(path=CARRIER_POOL, need=None):
    """读载体池(assets/carrier_pool.json)的 sequences。need 给了则校验 >=need,否则报错退出提示。
    返回 list[dict]({seq_id, user_id, history, label, hist_len})。"""
    with open(path, "r", encoding="utf-8") as f:
        pool = json.load(f)
    seqs = pool.get("sequences") or []
    if need is not None and len(seqs) < need:
        raise RuntimeError(
            f"carrier pool {path} has {len(seqs)} sequences < required {need}; "
            f"raise --max-users and rebuild the pool")
    return seqs


def _count_faulted_from_journals(out_dir, case_ids):
    n = 0
    for c in case_ids:
        jp = journal_path(out_dir, c)
        try:
            with open(jp, "r", encoding="utf-8") as f:
                jj = json.load(f)
            if jj.get("ground_truth", {}).get("faulted"):
                n += 1
        except Exception:
            pass
    return n


# ============================================================
# main
# ============================================================
def _tree_backend_guard(out_dir, backend_name, allow_mixed=False):
    """防"两个后端的行混进同一棵树"。

    CSV 是 append-only + resume 靠 journal-exists,一旦把 K8S 行追进 agentfault_v2,
    既污染了已交付的 108,又没有回退路径(只能手工剥行 + 删 journal/raw/ledger/span)。

    ★★两条判据,顺序不能反(回归审查 F1:原实现只有标记文件那条,而 local 后端**不写**
      标记 → 已冻结的 agentfault_v2 上根本没有 `.collect_backend` → `--backend k8s
      --out-dir (archived) agentfault_v2` 一路放行,实测能写出 82 列表头 / 86 列数据行的
      ragged CSV。方向正好反了。):
        (1) **盘上 CSV 表头**是最硬的判据,而且**不需要往树里写任何文件** ——
            local 树(82 列)遇上 k8s 后端(86 列)必然不等,反之亦然。冻结的两棵树因此
            一个字节都不会被动到,却也拦得住。
        (2) `.collect_backend` 标记只作**补充**(能覆盖"树是空的、CSV 还没生成"那一小段窗口),
            且仍然**只在非 local 后端写**:local 什么都不写 → agentfault_v2 / agentfault
            两棵已交付的树不会多出任何文件(逐字节红线)。
      另外 `append_csv` 里还有第三道同款表头闸(每次追加都查),防的是"守卫过了但树中途被换"。
    """
    # (1) 表头判据
    disk_cols = read_csv_header(os.path.join(out_dir, "dataset_agentfault.csv"))
    if disk_cols is not None and disk_cols != COLS and not allow_mixed:
        extra = [c for c in disk_cols if c not in COLS] or [c for c in COLS if c not in disk_cols]
        return (f"输出树 {out_dir} 的 CSV 表头是 {len(disk_cols)} 列,当前 --backend "
                f"{backend_name} 要写 {len(COLS)} 列(差异列: {extra[:6]})—— 两个 backend 的行"
                f"混进同一棵 append-only CSV 会写出 ragged 表且无回退路径。"
                f"换 --out-dir(K8S 建议 datasets/agentfault_k8s),或明知故犯加 --allow-mixed-tree")

    # (2) 标记文件判据
    marker = os.path.join(out_dir, ".collect_backend")
    prev = None
    if os.path.exists(marker):
        try:
            with open(marker, "r", encoding="utf-8") as f:
                prev = f.read().strip()
        except Exception:
            prev = None
    if prev and prev != backend_name and not allow_mixed:
        return (f"输出树 {out_dir} 是 backend={prev} 采的,当前 --backend {backend_name} —— "
                f"两个后端的行混进同一棵 append-only CSV 会污染既有数据集且无回退路径。"
                f"换 --out-dir(K8S 建议 datasets/agentfault_k8s),或明知故犯加 --allow-mixed-tree")
    if backend_name != "local" and prev != backend_name:
        try:
            with open(marker, "w", encoding="utf-8") as f:
                f.write(backend_name + "\n")
        except Exception:
            pass
    return None


def main():
    global BACKEND, COLS
    ap = argparse.ArgumentParser(
        description="agentfault dataset collection runner",
        epilog="TREE RULES: 一棵输出树只能属于一个 backend。默认 out-dir 已按 backend 分开"
               "(local -> (archived) agentfault_v2 由一键脚本给;k8s -> datasets/agentfault_k8s),"
               "别手动指到同一个 —— CSV 是 append-only,混进去会写出 ragged 表且无回退路径"
               "(runner 有三道闸:表头比对 / .collect_backend 标记 / append 时再查一次表头)。")
    # ★复现审查⑩1:这里的 default=5 与一键脚本 run_collect_agentfault.sh 的 RUNS=12 不一致。
    #   保留 5 不动(改默认值 = 改本机 CLI 行为,踩红线);但必须在 help 里写死复现口径。
    ap.add_argument("--runs", type=int, default=5,
                    help="K reps per combo (default 5)。★复现 agentfault_v2 / agentfault_k8s "
                         "的口径是 --runs 12(一键脚本默认就是 12);载体池须 >= runs")
    ap.add_argument("--warmup", type=int, default=1, help="uncounted warmup probes per combo")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                    help="output tree root (default (archived) agentfault; use _smoke subdir for smoke)")
    ap.add_argument("--only", default=None,
                    help="comma-separated combo ids to run (subset; default all)")
    ap.add_argument("--port", type=int, default=5131, help="base temp-instance port")
    # 注:--timeout 是**历史死参**(run_one_rep 收下但从不使用);真正生效的是
    #     ISM.PROBE_TIMEOUT_S=180(local) / K8sBackend.probe_timeout=300(k8s)。保留不删
    #     以免动到既有命令行,但别指望改它有用。
    ap.add_argument("--timeout", type=int, default=200,
                    help="(DEAD ARG, kept for CLI compat) per-probe timeout seconds")
    ap.add_argument("--strict-qc", action="store_true",
                    help="raise if any faulted combo produced 0 faulted reps")
    ap.add_argument("--list", action="store_true", help="print combo ids and exit")
    # ---- 执行后端(2026-07-27 B 档)----
    ap.add_argument("--backend", choices=("local", "k8s"), default="local",
                    help="execution backend: local=本机隔离 harness(默认,= agentfault_v2 口径); "
                         "k8s=25 微服务全栈里的常驻 rec-agent pod")
    ap.add_argument("--allow-mixed-tree", action="store_true",
                    help="允许把不同 backend 的行写进同一棵树(默认拒绝,防污染既有数据集)")
    ap.add_argument("--k8s-ns", default="recweb-chaos")
    ap.add_argument("--k8s-deploy", default="rec-agent")
    ap.add_argument("--k8s-container", default="rec-agent")
    ap.add_argument("--k8s-service", default="rec-agent")
    ap.add_argument("--k8s-proxy", default="http://127.0.0.1:8001",
                    help="kubectl proxy 地址(★必须 proxy 不能 port-forward:后者绑定具体 pod,"
                         "rollout 后立刻 failed to find sandbox)")
    ap.add_argument("--kubectl", default=None, help="kubectl 可执行路径(默认 env KUBECTL 或 Docker Desktop 自带)")
    ap.add_argument("--prom-url", default="http://localhost:9090")
    ap.add_argument("--k8s-host-metrics", choices=("prom", "none"), default="prom",
                    help="host_cpu_pct/host_mem_pct 取值来源(prom=cadvisor 容器级;none=留空)")
    ap.add_argument("--k8s-image-hint", default="agentfault-v2",
                    help="rec-agent 镜像必须含的子串(变体镜像守卫)。★默认 agentfault-v2 不是"
                         " agentfault:后者是子串匹配,会把 G1 用的旧 tag :agentfault 也放行,"
                         "而那个镜像没有 _filter_real_title、也没挂 PVC(与本机 v2 不同口径)")
    ap.add_argument("--k8s-allow-inject-residue", action="store_true",
                    help="放行 pod 上既有的 AGENTFAULT_INJECT 残留(仅崩溃后 resume 用)")
    ap.add_argument("--k8s-skip-code-parity", action="store_true",
                    help="跳过 _filter_real_title / electronics.item 口径校验(明知不同口径时)")
    ap.add_argument("--skip-preflight", action="store_true", help="跳过后端 preflight(不建议)")
    args = ap.parse_args()

    if args.list:
        print("combo ids:")
        for c in COMBOS:
            extra = ""
            if c.get("drop"):
                extra = f" drop={c['drop']}"
            elif c.get("subtypes"):
                extra = f" subtypes={[s[0] for s in c['subtypes']]}"
            print(f"  {c['id']:45s} kind={c['kind']:16s} agent={c['agent']}{extra}")
        return 0

    # ---- 装后端(local 时与模块级已建好的那个等价,这里重建只为统一代码路径)----
    if args.backend == "local":
        BACKEND = BK.LocalBackend(build_env=build_env)
    else:
        BACKEND = BK.make_backend(
            "k8s", build_env=build_env, ns=args.k8s_ns, deploy=args.k8s_deploy,
            container=args.k8s_container, service=args.k8s_service,
            proxy=args.k8s_proxy, kubectl=args.kubectl, prom_url=args.prom_url,
            host_metrics=args.k8s_host_metrics, image_hint=args.k8s_image_hint,
            allow_inject_residue=args.k8s_allow_inject_residue,
            skip_code_parity=args.k8s_skip_code_parity)
    # 后端可能追加 CSV 列(local 追加 []) —— 必须在任何 append_csv 之前定下来
    COLS = csv_columns() + BACKEND.extra_csv_columns()

    # VENV_PY 门只对本机后端成立(K8S 侧跑的是镜像里的 python)
    if BACKEND.needs_phase1_venv and not os.path.exists(VENV_PY):
        print(f"FATAL venv missing: {VENV_PY} (run phase1_bootstrap.sh)")
        return 2

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    guard = _tree_backend_guard(out_dir, BACKEND.name, allow_mixed=args.allow_mixed_tree)
    if guard:
        print(f"FATAL {guard}")
        return 2

    if not args.skip_preflight:
        problems = BACKEND.preflight()
        for p in problems:
            print(f"  [PREFLIGHT-FATAL] {p}", flush=True)
        if problems:
            print("FATAL backend preflight failed —— 按上方逐条修好再跑(勿加 --skip-preflight 硬闯)")
            return 2

    # ★载体池(P0-2):取前 R 条,每 rep_i 用 carrier[i-1](所有 combo 同 rep-index 同序列)
    try:
        carriers = load_carrier_pool(need=args.runs)
    except (FileNotFoundError, RuntimeError) as e:
        print(f"FATAL carrier pool: {e}")
        return 2
    carriers = carriers[:args.runs]
    print(f"[agentfault-runner] carrier_pool={CARRIER_POOL} using {len(carriers)}/{args.runs} "
          f"(seq_ids={[c.get('seq_id') for c in carriers]})")

    targets = COMBOS
    if args.only:
        want = [s.strip() for s in args.only.split(",") if s.strip()]
        bad = [w for w in want if w not in COMBO_BY_ID]
        if bad:
            print(f"unknown combo id(s): {bad}; use --list")
            return 1
        targets = [COMBO_BY_ID[w] for w in want]

    print(f"[agentfault-runner] out_dir={out_dir}")
    print(f"[agentfault-runner] backend={BACKEND.name}")
    print(f"[agentfault-runner] combos={len(targets)} runs={args.runs} warmup={args.warmup}")

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "out_dir": out_dir, "runs": args.runs, "warmup": args.warmup,
        "probe_seq": PROBE_SEQ,
        "carrier_pool": CARRIER_POOL,
        "carrier_seq_ids": [c.get("seq_id") for c in carriers],
        "combos": [],
    }
    # ★后端 provenance 只在**非 local** 时写进 run_summary.json:
    #   local 返回 None → summary 的 key 集合与改造前逐字相同(v2 的 run_summary.json 可复现)。
    _meta = BACKEND.summary_meta()
    if _meta:
        summary["backend_meta"] = _meta
    worst_rc = 0
    for combo in targets:
        try:
            res = run_combo(combo, args.runs, args.warmup, out_dir,
                            args.port, args.timeout, carriers)
        except ChecksumDriftError as e:
            # ★采前审 FIX:业务表污染 = 硬停整轮(不吞、不继续采后续 combo)
            print(f"[agentfault-runner] {e} — HARD ABORT (不继续采,防在污染 DB 上采)", flush=True)
            summary["combos"].append({"combo": combo["id"], "kind": combo["kind"],
                                      "error": "ChecksumDriftError", "aborted": True})
            _write_summary(out_dir, summary)
            return 4
        except BK.BackendFatalError as e:
            # 执行环境结构性损坏(目前唯一来源:pod 内 span 根本没落盘 = /agentfault-data
            # 没挂上)。重试无意义,后续 combo 只会继续产出 total_span_count=0 的废行,
            # 而且 journal 一写 resume 就永远跳过 —— 所以照 ChecksumDriftError 的规格硬停整轮。
            print(f"[agentfault-runner] combo {combo['id']} BACKEND-FATAL: {e}\n"
                  f"  — HARD ABORT(不继续采,防整晚产出废数据)", flush=True)
            summary["combos"].append({"combo": combo["id"], "kind": combo["kind"],
                                      "error": f"BackendFatalError: {e}", "aborted": True})
            _write_summary(out_dir, summary)
            return 5
        except BackendTransientError as e:
            # 执行环境瞬时故障(proxy 抖 / pod 被换 / emptyDir 被清)。抛点都在 append_csv
            # **之前**,所以这个 combo 没有落下任何 CSV 行或 journal → 直接重跑本脚本
            # (同 --out-dir)即可干净续采,不需要手工剥行。
            print(f"[agentfault-runner] combo {combo['id']} BACKEND-TRANSIENT: {e} "
                  f"— 本 combo 作废(无 CSV/journal 落盘),修好环境后重跑同一条命令即可 resume",
                  flush=True)
            res = {"combo": combo["id"], "kind": combo["kind"],
                   "error": f"BackendTransientError: {e}", "transient": True}
            worst_rc = max(worst_rc, 1)
        except Exception as e:
            print(f"[agentfault-runner] combo {combo['id']} ERROR: {e!r}", flush=True)
            res = {"combo": combo["id"], "kind": combo["kind"], "error": repr(e)}
            worst_rc = max(worst_rc, 1)
        summary["combos"].append(res)
        if res.get("qc_ok") is False:
            worst_rc = max(worst_rc, 1)
            if args.strict_qc:
                print("[agentfault-runner] --strict-qc: aborting on QC-FAIL", flush=True)
                _write_summary(out_dir, summary)
                return 3
        print("-" * 64, flush=True)

    _write_summary(out_dir, summary)
    print(f"[agentfault-runner] done -> {os.path.join(out_dir, 'run_summary.json')}")
    for res in summary["combos"]:
        print(f"   {res.get('combo'):45s} "
              f"faulted={res.get('faulted_reps')} qc_ok={res.get('qc_ok')} "
              f"{'ERROR' if res.get('error') else ''}")
    return worst_rc


def _write_summary(out_dir, summary):
    try:
        with open(os.path.join(out_dir, "run_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[agentfault-runner] write summary failed: {e!r}", flush=True)


K8S_INTERRUPT_NOTICE = """
==================== 中断了 —— K8S 侧不是干净态 ====================
K8sBackend.stop_instance 有意**不摘 env**(摘 env 会触发 rollout,清空 emptyDir 里
还没拉走的 span)。所以现在 rec-agent pod 上仍挂着本轮的 AGENTFAULT_* 旋钮。
三条出路,按你的意图选一条:

 (a) 想干净重来 —— 重跑 patch 把旋钮全量重置(它会 rollout,★会清空 emptyDir,
     但 runner 每个 rep 都已把 span/台账 tail 回本地,所以只损失"最后一个未完成的 rep"):
       powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1
     然后原样重跑采集命令(journal-exists 会跳过已采完的 combo)。

 (b) 想接着采 —— 带残留 resume(preflight 会因 AGENTFAULT_INJECT 残留 fatal,加这个放行):
       bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s --allow-inject-residue
     ★注意:normal 臂(combo `normal`)**不能**这样 resume —— INJECT 残留会把 observer 顶掉
       (loader 打 'AGENTFAULT_OBSERVE ignored')。若 normal 还没采,请走 (a)。

 (c) 不打算继续 —— 先核 <out-dir>/spans/ 与 ledgers/ 里各 combo 文件非空,再还原:
       powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected

★不还原的后果:rec-agent 会停在 :agentfault-v2 + 1536Mi + Recreate + 挂 PVC 的形态。
  之后任何 traditional 线采集的 container_spec_memory_limit_bytes / pod 重建语义都与
  产出 255 的口径不一致 —— 跨批次不可比。
★本 runner **不自动还原**(自动 restore 会清 emptyDir,永久丢没拉走的轨迹)。
====================================================================
"""


if __name__ == "__main__":
    # ★复现审查⑧:Ctrl-C 会把集群留在"脏注入态",而这三条出路原本只写在 shell 的一段
    #   fatal 文案里(README 与 --help 都没有)。这里在退出路径上把它打出来。
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        if BACKEND.name != "local":
            print(K8S_INTERRUPT_NOTICE, flush=True)
        else:
            print("\n[agentfault-runner] 已中断。本机后端无遗留态:临时进程随本进程退出;"
                  "已写 journal 的 rep 会在下次 resume 时跳过。", flush=True)
        sys.exit(130)
