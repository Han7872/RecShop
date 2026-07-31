# -*- coding: utf-8 -*-
"""TASK-X: recommendation_agent(LangGraph 4-agent 顺序链) agent 协作内生多根因故障注入 runner。

承 TASK-W 交付的 per-agent 带名 span(agent.<Name> + recweb.agent.name) + per-agent 故障开关
(env AGENT_FAULT_<NAME>=delay|error|garbage, AGENT_FAULT_DELAY_MS)。

设计(对齐 TASK-X 方案, 见 (project docs)/archive/TASK-X-agent-fault-injection.md):
  - 每场景 = 一个临时实例进程(端口 5101+, NACOS_ENABLED=false, OTEL_ENABLED=true,
    SPAN_FILE 指本地 JSONL, OTLP endpoint 指死端口让 BSP 静默失败不污染持久 Jaeger,
    该场景的 AGENT_FAULT_* env)。起后预热再正式探, 跑完 kill。绝不碰持久 5001/8200/3000。
  - 每窗 = 一次 POST /recommend 探针(词表内序列, top_k=5)。
  - 归窗(#1): endpoint 回写 trace_id 进响应 JSON, runner 按该 trace_id 从 SPAN_FILE
    精确捞该窗全部 span(不靠 wall-clock), 按 parent_span_id 聚合取 4 条 agent.<Name>
    span 的 duration/status + 各 agent span 下 httpx(DeepSeek)/requests(SASRec) 子 span
    计数与 child_max_duration_ms。wall-clock 仅兜底 sanity(检测串窗)。
  - 增量落盘: 每窗即时写 raw/<run_id>.json + journal/<run_id>.json(注入配置+ground_truth)
    + 追加 dataset_agentchaos.csv; 每场景一个 spans/<scenario>.jsonl(同进程同文件, trace_id
    归窗故同文件多窗不串)。
  - 安全: 每场景前后核 items/inventory CHECKSUM(基线 1088112223/944901079, 本服务无 DB
    应恒不变); 绕 Clash(no-proxy opener + 进程级 NO_PROXY + trust_env=False); 不 commit。

用法:
  # 预热冒烟(1 实例, 起→预热→单 normal/delay/garbage 探针, 出可分性 6 项报告):
  python scripts/chaos/ctk/agentchaos_runner.py --smoke
  # 单场景试跑(端到端验证管线, N 小):
  python scripts/chaos/ctk/agentchaos_runner.py --scenario S00_normal_full --runs 1
  # 全量(过夜):
  python scripts/chaos/ctk/agentchaos_runner.py --all --runs 8
"""
import argparse
import csv
import json
import math
import os
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

# ---------------- 绕 Clash ----------------
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")
for _pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_pk, None)
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

CTK_DIR = os.path.dirname(os.path.abspath(__file__))  # 模块加载时定 dir(atexit 时 __file__ 已失效)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CTK_DIR)))
SVC_DIR = os.path.join(ROOT, "services", "recommendation_agent")
OUT_DIR = os.path.join(ROOT, "datasets", "_archive", "agentfault", "agentchaos")
SMOKE_DIR = os.path.join(OUT_DIR, "smoke")
CSV_PATH = os.path.join(OUT_DIR, "dataset_agentchaos.csv")
PY = sys.executable
NAN = float("nan")

AGENT_NAMES = [
    "Sequence_Recommender",
    "User_Behavior_Analyzer",
    "Product_Analyzer",
    "Recommendation_Synthesizer",
]

# 词表内可用输入(TASK-W 冒烟 HTTP200)
PROBE_SEQ = ["015600206X", "6300215695", "0446673145"]
PROBE_TOPK = 5

# #2 delay_ms 由预热实测定: 预热(smoke)实测 normal per-agent span 被多轮 LLM tool-calling
# 主导(Product_Analyzer mean=14074ms/std=3138/max=16787; UserBehavior mean=13082/max=15574;
# Sequence mean=8883; Synthesizer mean=4055)。+5000ms 落在 analyzer 自然方差内(delay 与
# normal 不可分, smoke verdict delay_vs_normal_separable=false)。需 delay > (max-min) span
# spread(Product 最坏 ~8102ms)才保证 delayed_min > normal_max。取 15000ms(≈3× 最坏 std,
# ≈2× max-min spread), 保证全 agent delay 与 normal 干净可分; 代价: delay 场景 e2e +15s
# (S41 双 delay +30s), 仍在 120s timeout 内, 过夜可控。详见 smoke_report.json + SUMMARY #2。
DELAY_MS = 15000
CHECKSUM_BASELINE = {"items": 1088112223, "inventory": 944901079}

# 临时实例 OTLP 指死端口(让 BSP 静默失败, 不污染持久 Jaeger)
DEAD_OTLP = "http://127.0.0.1:14318"


# ============================================================
# 场景集(对齐 plan scenarios; cross_layer 留接口, 主集先跑)
# ============================================================
def _scn(sid, kind, faults, rc_set):
    return {"id": sid, "kind": kind, "faults": faults, "root_cause_agent_set": rc_set}


SCENARIOS = [
    _scn("S00_normal_full", "normal", {}, []),
    # single delay
    _scn("S10_delay_Sequence", "single_root", {"Sequence_Recommender": "delay"}, ["Sequence_Recommender"]),
    _scn("S11_delay_UserBehavior", "single_root", {"User_Behavior_Analyzer": "delay"}, ["User_Behavior_Analyzer"]),
    _scn("S12_delay_Product", "single_root", {"Product_Analyzer": "delay"}, ["Product_Analyzer"]),
    _scn("S13_delay_Synthesizer", "single_root", {"Recommendation_Synthesizer": "delay"}, ["Recommendation_Synthesizer"]),
    # single error
    _scn("S20_error_Sequence", "single_root", {"Sequence_Recommender": "error"}, ["Sequence_Recommender"]),
    _scn("S21_error_UserBehavior", "single_root", {"User_Behavior_Analyzer": "error"}, ["User_Behavior_Analyzer"]),
    _scn("S22_error_Product", "single_root", {"Product_Analyzer": "error"}, ["Product_Analyzer"]),
    _scn("S23_error_Synthesizer", "single_root", {"Recommendation_Synthesizer": "error"}, ["Recommendation_Synthesizer"]),
    # single garbage (analyzer only)
    _scn("S30_garbage_Sequence", "single_root", {"Sequence_Recommender": "garbage"}, ["Sequence_Recommender"]),
    _scn("S31_garbage_UserBehavior", "single_root", {"User_Behavior_Analyzer": "garbage"}, ["User_Behavior_Analyzer"]),
    _scn("S32_garbage_Product", "single_root", {"Product_Analyzer": "garbage"}, ["Product_Analyzer"]),
    # multi
    _scn("S40_multi_garbageUp_delayDown", "multi_root",
         {"Sequence_Recommender": "garbage", "Product_Analyzer": "delay"},
         ["Sequence_Recommender", "Product_Analyzer"]),
    _scn("S41_multi_dualDelay", "multi_root",
         {"User_Behavior_Analyzer": "delay", "Product_Analyzer": "delay"},
         ["User_Behavior_Analyzer", "Product_Analyzer"]),
    _scn("S42_multi_triple_mixed", "multi_root",
         {"Sequence_Recommender": "garbage", "User_Behavior_Analyzer": "error", "Product_Analyzer": "delay"},
         ["Sequence_Recommender", "User_Behavior_Analyzer", "Product_Analyzer"]),
]
SCENARIO_BY_ID = {s["id"]: s for s in SCENARIOS}


# ============================================================
# cross_layer 场景集(S50 必做 / S51 best-effort) —— 绝不进主 SCENARIOS / 主 --all
# ============================================================
# [隔离 #5] cross 场景独立列表 + 独立入口(--cross-scenario / --include-cross), 写独立
# dataset_agentchaos_cross.csv(run_id 前缀 S50_/S51_), 绝不 append 主 120 行。
# 复用 chaos25 现役 sasrec 代理(零新建): listen [::]:18200 → host.docker.internal:8200。
CROSS_CSV_PATH = os.path.join(OUT_DIR, "dataset_agentchaos_cross.csv")
SASREC_PROXY_NAME = "sasrec"          # chaos25 现役共享代理(toxi_ensure_proxy 幂等 reuse)
SASREC_PROXY_LISTEN = "0.0.0.0:18200"
SASREC_PROXY_UPSTREAM = "host.docker.internal:8200"
SASREC_PROXY_URL = "http://127.0.0.1:18200"  # 临时实例 SASREC_API_URL 指此(经代理)
CROSS_NET_DELAY_MS = 15000  # =主集 agent delay_ms 同量级; 可分性靠列(child_max/child_sasrec_count)不靠幅度

# DeepSeek 代理(S51 best-effort, 默认砍): chaos25 I6 口径, 当前不在 /proxies(需重建)。
DEEPSEEK_PROXY_NAME = "deepseek_real"
DEEPSEEK_PROXY_LISTEN = "0.0.0.0:18443"
DEEPSEEK_PROXY_UPSTREAM = "api.deepseek.com:443"


def _cross_scn(sid, kind, faults, rc_set, dep_proxy, dep_root, sasrec_url=None,
               deepseek_base=None):
    """cross 场景: 在主场景结构上加 dep_proxy(Toxiproxy 代理名)+ dep_root(依赖层根因标识)
    + sasrec_url/deepseek_base(临时实例 env 重定向, 仅 cross 传)。"""
    d = _scn(sid, kind, faults, rc_set)
    d["dep_proxy"] = dep_proxy          # 加 net_delay toxic 的代理名
    d["dep_root"] = dep_root            # 依赖层根因(sasrec_api / deepseek), 进 root_cause_set
    d["sasrec_url"] = sasrec_url        # 仅 S50 传 SASREC_PROXY_URL(#N1)
    d["deepseek_base"] = deepseek_base  # 仅 S51 传 deepseek 代理 base
    return d


CROSS_SCENARIOS = [
    # S50(优先必做, 不烧 token): Product_Analyzer garbage + SASRec net_delay(注 sasrec 代理)
    _cross_scn(
        "S50_cross_garbageProduct_sasrecDelay", "cross_layer",
        {"Product_Analyzer": "garbage"},
        ["Product_Analyzer", "sasrec_api"],
        dep_proxy=SASREC_PROXY_NAME, dep_root="sasrec_api",
        sasrec_url=SASREC_PROXY_URL,
    ),
    # S51(best-effort, 默认砍): User_Behavior_Analyzer delay + DeepSeek net_delay(全 agent 共因)
    _cross_scn(
        "S51_cross_delayUserBehavior_deepseekDelay", "cross_layer",
        {"User_Behavior_Analyzer": "delay"},
        ["User_Behavior_Analyzer", "deepseek"],
        dep_proxy=DEEPSEEK_PROXY_NAME, dep_root="deepseek",
        deepseek_base="https://127.0.0.1:18443/v1",
    ),
]
CROSS_SCENARIO_BY_ID = {s["id"]: s for s in CROSS_SCENARIOS}


# ---- 复用 chaos25 Toxiproxy 工具(别重造客户端; chaos25_runner 有 __main__ 守卫, import 无副作用) ----
def _import_chaos25():
    """延后 import chaos25_runner 的 4 个 toxi 函数。失败返回 None(由调用方非致命处理)。
    用模块加载时定的 CTK_DIR(非 __file__)——atexit 兜底在解释器拆除时 __file__ 可能已失效。"""
    try:
        if CTK_DIR not in sys.path:
            sys.path.insert(0, CTK_DIR)
        import chaos25_runner as c25
        return c25
    except Exception as e:
        try:
            print(f"  [WARN] import chaos25_runner 失败({e}); cross_layer 不可用", flush=True)
        except Exception:
            pass
        return None


# [安全四层防线 之 (d)] 进程级 atexit 兜底清 sasrec 代理 toxic —— 明确知悉它**不响应**
# SIGKILL/-9/任务管理器强杀/断电/蓝屏, 非唯一防线(入口/窗末/场景末三层 try/finally 才是主防线)。
import atexit  # noqa: E402

_CROSS_ATEXIT_PROXIES = set()


def _cross_atexit_clear():
    if not _CROSS_ATEXIT_PROXIES:
        return
    c25 = _import_chaos25()
    if not c25:
        return
    for px in list(_CROSS_ATEXIT_PROXIES):
        try:
            c25.toxic_clear(px)
        except Exception:
            pass


atexit.register(_cross_atexit_clear)


# ============================================================
# HTTP (no-proxy)
# ============================================================
def _req(url, method="GET", body=None, timeout=90):
    data = body.encode("utf-8") if isinstance(body, str) else body
    r = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        r.add_header("Content-Type", "application/json")
    return _OPENER.open(r, timeout=timeout)


def probe_recommend(port, timeout=90):
    """单次 /recommend 探针。返回 (http_status, e2e_ms, resp_json or None)。"""
    url = f"http://127.0.0.1:{port}/recommend"
    body = json.dumps({"item_sequence": PROBE_SEQ, "top_k": PROBE_TOPK})
    t0 = time.time()
    try:
        with _req(url, method="POST", body=body, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "ignore")
            e2e = (time.time() - t0) * 1000.0
            try:
                j = json.loads(raw)
            except Exception:
                j = None
            return r.status, e2e, j
    except urllib.error.HTTPError as e:
        e2e = (time.time() - t0) * 1000.0
        raw = ""
        try:
            raw = e.read().decode("utf-8", "ignore")
            j = json.loads(raw)
        except Exception:
            j = None
        return e.code, e2e, j
    except Exception as e:
        e2e = (time.time() - t0) * 1000.0
        return -1, e2e, {"_runner_error": str(e)}


def wait_health(port, timeout_s=120):
    """等临时实例 /recommend/health 返回(模型/图就绪)。"""
    url = f"http://127.0.0.1:{port}/recommend/health"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with _req(url, timeout=8) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(2.0)
    return False


# ============================================================
# 临时实例生命周期
# ============================================================
def start_temp_instance(port, faults, span_file, log_path, sasrec_url=None):
    """起一个临时 recommendation_agent 实例(working tree 新代码)。

    faults: {AgentName: 'delay'|'error'|'garbage'} 进程级 env。
    span_file: 本地 JSONL 路径(SimpleSpanProcessor 即时 flush)。
    sasrec_url: [cross_layer 专用 #N1] 仅 cross 场景传 'http://127.0.0.1:18200'
        把 Sequence_Recommender 的 SASRec 调用导向 Toxiproxy 代理(tools.py:14 import
        时读 SASREC_API_URL)。主 15 场景**绝不传**(=None), 实例 fallback tools.py 默认
        真实 8200 → 主集临时实例永不经代理, 即便代理上有残留/他人 toxic 也不污染主集重跑。
        这是把"主集 toxic-free"从"现状天然成立"钉成"改码后仍成立"的护栏。
    返回 Popen。
    """
    env = dict(os.environ)
    env["RECOMMENDATION_PORT"] = str(port)
    env["RECOMMENDATION_HOST"] = "127.0.0.1"
    env["NACOS_ENABLED"] = "false"
    env["OTEL_ENABLED"] = "true"
    env["OTEL_SERVICE_NAME"] = "recommendation_agent_taskx"  # 与持久栈区分(虽走本地 JSONL)
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = DEAD_OTLP  # BSP 静默失败, 不污染持久 Jaeger
    env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = DEAD_OTLP
    env["SPAN_FILE"] = span_file
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    for pk in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(pk, None)
    # [#N1 护栏] SASREC_API_URL 默认从环境继承; 仅 cross 显式传 sasrec_url 时才重定向到代理。
    # 主场景 sasrec_url=None → 主动剔除 SASREC_API_URL(若环境恰有), 让 tools.py:14 fallback
    # 真实 8200, 杜绝主集临时实例误经 18200 代理。
    if sasrec_url:
        env["SASREC_API_URL"] = sasrec_url
    else:
        env.pop("SASREC_API_URL", None)
    # 清掉所有 AGENT_FAULT_*, 再设本场景的
    for k in list(env.keys()):
        if k.startswith("AGENT_FAULT_"):
            env.pop(k, None)
    for name, kind in (faults or {}).items():
        env[f"AGENT_FAULT_{name}"] = kind
    env["AGENT_FAULT_DELAY_MS"] = str(DELAY_MS)

    logf = open(log_path, "w", encoding="utf-8")
    p = subprocess.Popen(
        [PY, "app.py"], cwd=SVC_DIR, env=env,
        stdout=logf, stderr=subprocess.STDOUT,
    )
    p._logf = logf  # 保引用
    return p


def stop_temp_instance(p):
    if p is None:
        return
    try:
        p.terminate()
        try:
            p.wait(timeout=10)
        except Exception:
            p.kill()
    except Exception:
        pass
    try:
        if getattr(p, "_logf", None):
            p._logf.close()
    except Exception:
        pass


def port_free(port):
    """简单核端口是否空闲(无监听)。"""
    try:
        with _req(f"http://127.0.0.1:{port}/recommend/health", timeout=2):
            return False
    except Exception:
        return True


# ============================================================
# span 归窗与聚合
# ============================================================
def read_spans_for_trace(span_file, trace_id, retries=6, sleep_s=0.5):
    """从 JSONL 按 trace_id 精确捞该窗全部 span。SimpleSpanProcessor 在 END 时 flush,
    根/子 span END 顺序不定, 故 retry 几次直到 span 数稳定(或耗尽 retries)。"""
    last = []
    stable = 0
    for _ in range(retries):
        spans = []
        try:
            with open(span_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if rec.get("trace_id") == trace_id:
                        spans.append(rec)
        except FileNotFoundError:
            spans = []
        if len(spans) == len(last) and len(spans) > 0:
            stable += 1
            if stable >= 2:
                return spans
        else:
            stable = 0
        last = spans
        time.sleep(sleep_s)
    return last


def aggregate_agent_spans(spans):
    """按 parent_span_id 链聚合, 取 4 条 agent.<Name> span 的 duration/status +
    各 agent span 下 httpx/requests 子 span 计数与 child_max_duration_ms。

    httpx 子 span (DeepSeek): name 形如 'POST' (httpx instrumentation) 且属性含
      http.url/url.full 指向 deepseek; 更稳的判别用 span name 'HTTP ...' 或属性。
    requests 子 span (SASRec): requests instrumentation span。
    这里用通用判别: 子 span 属性里 http.* 存在即视为出站 HTTP 调用, 再用 url 区分
      sasrec(8200/18200) vs deepseek(其余/443)。"""
    by_id = {s["span_id"]: s for s in spans if s.get("span_id")}
    children = {}
    for s in spans:
        pid = s.get("parent_span_id") or ""
        children.setdefault(pid, []).append(s)

    # 找 4 条 agent span (name == agent.<Name>)
    agent_spans = {}
    for s in spans:
        attrs = s.get("attributes") or {}
        an = attrs.get("recweb.agent.name")
        if an and s.get("name", "").startswith("agent."):
            # 同名取 duration 最大那条(正常每窗每 agent 只一条)
            if an not in agent_spans or (s.get("duration_ms") or 0) > (agent_spans[an].get("duration_ms") or 0):
                agent_spans[an] = s

    def _http_url(attrs):
        for k in ("http.url", "url.full", "http.target", "http.route"):
            if k in attrs:
                return str(attrs[k])
        return ""

    def _is_sasrec(attrs):
        u = _http_url(attrs).lower()
        # 端口 8200/18200 或 path /recommend|/health(SASRec)
        return (":8200" in u or ":18200" in u or "8200/" in u)

    out = {}
    for name in AGENT_NAMES:
        sp = agent_spans.get(name)
        if not sp:
            out[name] = {
                "duration_ms": None, "status": None,
                "httpx_count": None, "sasrec_requests_count": None,
                "child_max_duration_ms": None, "present": False,
            }
            continue
        sid = sp["span_id"]
        # 递归收集该 agent span 子树的所有 HTTP 子 span
        httpx_count = 0
        sasrec_count = 0
        child_max = 0.0
        # [cross 增强] child_max 单列混 sasrec+httpx 不区分类型(Reviewer cross #3); 额外分别对
        # sasrec / httpx 子 span 取 max, 仅写进独立 cross csv, 主 features schema 不动。
        child_max_sasrec = 0.0
        child_max_httpx = 0.0
        stack = list(children.get(sid, []))
        while stack:
            c = stack.pop()
            attrs = c.get("attributes") or {}
            has_http = any(k.startswith("http.") or k in ("url.full", "url.scheme") for k in attrs.keys())
            cname = c.get("name", "")
            kind = c.get("kind", "")
            is_client = (kind == "CLIENT") or has_http
            if is_client and has_http:
                d = c.get("duration_ms") or 0.0
                if _is_sasrec(attrs):
                    sasrec_count += 1
                    if d > child_max_sasrec:
                        child_max_sasrec = d
                else:
                    httpx_count += 1
                    if d > child_max_httpx:
                        child_max_httpx = d
                if d > child_max:
                    child_max = d
            stack.extend(children.get(c["span_id"], []))
        out[name] = {
            "duration_ms": sp.get("duration_ms"),
            "status": sp.get("status_code"),
            "httpx_count": httpx_count,
            "sasrec_requests_count": sasrec_count,
            "child_max_duration_ms": child_max if child_max > 0 else None,
            "child_max_sasrec_ms": child_max_sasrec if child_max_sasrec > 0 else None,
            "child_max_httpx_ms": child_max_httpx if child_max_httpx > 0 else None,
            "present": True,
            "fault_attr": (sp.get("attributes") or {}).get("recweb.agent.fault"),
        }
    # 全局
    total_span_count = len(spans)
    error_span_count = sum(1 for s in spans if s.get("status_code") == "ERROR")
    return out, total_span_count, error_span_count


# ============================================================
# 推荐质量派生量
# ============================================================
_GARBAGE_STR = "本环节分析结果不可用"
_DEGRADE_STR = "暂时不可用"


def derive_quality(resp_json):
    """从 /recommend 响应 JSON 提取 confidence / recommended_product_is_unknown /
    degrade/garbage message present + per-agent conversation 文本长度。"""
    out = {
        "confidence": None,
        "recommended_product": None,
        "recommended_product_is_unknown": None,
        "degrade_message_present": None,
        "garbage_message_present": None,
        "conv_text_len": {a: None for a in AGENT_NAMES},
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
    # conversation key 是 PascalCase(extract_recommendation_conversation key_mapping)
    key_map = {
        "Sequence_Recommender": "SequenceRecommender",
        "User_Behavior_Analyzer": "UserBehaviorAnalyzer",
        "Product_Analyzer": "ProductAnalyzer",
        "Recommendation_Synthesizer": "RecommendationSynthesizer",
    }
    all_text = ""
    for name in AGENT_NAMES:
        txt = ""
        if isinstance(conv, dict):
            txt = conv.get(key_map[name], "") or ""
        out["conv_text_len"][name] = len(txt)
        all_text += txt
    out["degrade_message_present"] = (_DEGRADE_STR in all_text)
    out["garbage_message_present"] = (_GARBAGE_STR in all_text)
    return out


# ============================================================
# CSV schema
# ============================================================
def csv_columns():
    cols = [
        # 标签/溯源(剔出 features 视图)
        "run_id", "scenario_id", "kind",
        "root_cause_set", "n_root_causes", "fault_type_set",
    ]
    for a in AGENT_NAMES:
        cols.append(f"fault_{a}")
    cols += [
        # 时间窗
        "window_start", "window_end", "trace_id",
        # 探针可观测
        "e2e_latency_ms", "http_status", "http_success",
    ]
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
    cols += [
        "host_cpu_pct", "host_mem_pct",
        "span_count_matched", "wallclock_sanity_ok", "note",
    ]
    return cols


COLS = csv_columns()


def _r2(x):
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, (int,)):
        return x
    if isinstance(x, float) and not math.isnan(x):
        return round(x, 2)
    return x if x is not None else ""


def build_row(scn, run_id, trace_id, http_status, e2e_ms, agg, total_spans,
              error_spans, quality, host_cpu, host_mem, win_start, win_end,
              span_matched, wallclock_ok, note=""):
    faults = scn["faults"]
    rc_set = scn["root_cause_agent_set"]
    fault_types = sorted(set(faults.values())) if faults else []
    row = {
        "run_id": run_id, "scenario_id": scn["id"], "kind": scn["kind"],
        "root_cause_set": ";".join(rc_set),
        "n_root_causes": len(rc_set),
        "fault_type_set": ";".join(fault_types),
        "window_start": win_start, "window_end": win_end, "trace_id": trace_id,
        "e2e_latency_ms": _r2(e2e_ms),
        "http_status": http_status,
        "http_success": int(http_status == 200),
        "total_span_count": total_spans,
        "error_span_count": error_spans,
        "recommendation_confidence": _r2(quality["confidence"]) if quality["confidence"] is not None else "",
        "recommended_product_is_unknown": "" if quality["recommended_product_is_unknown"] is None else int(quality["recommended_product_is_unknown"]),
        "degrade_message_present": "" if quality["degrade_message_present"] is None else int(quality["degrade_message_present"]),
        "garbage_message_present": "" if quality["garbage_message_present"] is None else int(quality["garbage_message_present"]),
        "host_cpu_pct": _r2(host_cpu), "host_mem_pct": _r2(host_mem),
        "span_count_matched": int(span_matched),
        "wallclock_sanity_ok": int(wallclock_ok),
        "note": note,
    }
    for a in AGENT_NAMES:
        row[f"fault_{a}"] = faults.get(a, "none")
        ag = agg.get(a, {})
        row[f"span_{a}_duration_ms"] = _r2(ag.get("duration_ms")) if ag.get("duration_ms") is not None else ""
        row[f"span_{a}_status"] = ag.get("status") or ""
        row[f"span_{a}_child_httpx_count"] = "" if ag.get("httpx_count") is None else ag.get("httpx_count")
        row[f"span_{a}_child_sasrec_requests_count"] = "" if ag.get("sasrec_requests_count") is None else ag.get("sasrec_requests_count")
        row[f"span_{a}_child_max_duration_ms"] = _r2(ag.get("child_max_duration_ms")) if ag.get("child_max_duration_ms") is not None else ""
        row[f"span_{a}_present"] = int(bool(ag.get("present")))
        cl = quality["conv_text_len"].get(a)
        row[f"conv_{a}_text_len"] = "" if cl is None else cl
    return row


def append_csv(row):
    os.makedirs(OUT_DIR, exist_ok=True)
    new = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in COLS})


def write_raw_journal(scn, run_id, row, trace_id, win_start, win_end,
                      checksum_before, checksum_after, agg, raw_resp):
    os.makedirs(os.path.join(OUT_DIR, "raw"), exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "journal"), exist_ok=True)
    with open(os.path.join(OUT_DIR, "raw", f"{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"row": row, "agg": agg, "resp": raw_resp}, f, ensure_ascii=False, indent=2)
    rc_set = scn["root_cause_agent_set"]
    fault_types = sorted(set(scn["faults"].values())) if scn["faults"] else []
    journal = {
        "run_id": run_id, "scenario_id": scn["id"], "kind": scn["kind"],
        "faults": scn["faults"], "delay_ms": DELAY_MS,
        "probe": {"item_sequence": PROBE_SEQ, "top_k": PROBE_TOPK},
        "trace_id": trace_id,
        "window": {"start": win_start, "end": win_end},
        "ground_truth": {
            "root_cause_agent_set": rc_set,
            "n_root_causes": len(rc_set),
            "fault_type_set": fault_types,
            "per_agent_fault": {a: scn["faults"].get(a, "none") for a in AGENT_NAMES},
        },
        "checksum": {"before": checksum_before, "after": checksum_after,
                     "alarm": (checksum_before != checksum_after) if (checksum_before and checksum_after) else None},
    }
    with open(os.path.join(OUT_DIR, "journal", f"{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)


# ============================================================
# CHECKSUM 守卫
# ============================================================
def get_checksum():
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import db_contention_injector as inj
        return inj.checksum_tables()
    except Exception as e:
        return {"_error": str(e)}


# ============================================================
# psutil host 水位(对临时实例进程级采)
# ============================================================
try:
    import psutil
except Exception:
    psutil = None


def sample_host(proc, dur_s):
    """窗期间对临时实例进程采 cpu/mem 均值(进程级)。无 psutil 返回 NaN。"""
    if psutil is None:
        return NAN, NAN
    try:
        ps = psutil.Process(proc.pid)
        cpus, mems = [], []
        # 这里探针是阻塞的, 故在调用前后简单取一次进程级 + 整机级混合
        ps.cpu_percent(interval=None)  # 预热
        time.sleep(0.2)
        cpus.append(ps.cpu_percent(interval=None))
        mems.append(psutil.virtual_memory().percent)
        return (statistics.mean(cpus) if cpus else NAN), (statistics.mean(mems) if mems else NAN)
    except Exception:
        return NAN, NAN


# ============================================================
# 一窗
# ============================================================
def run_one_window(scn, port, proc, span_file, idx, timeout=120):
    win_start = datetime.now(timezone.utc).isoformat()
    t0 = time.time()
    http_status, e2e_ms, resp = probe_recommend(port, timeout=timeout)
    t1 = time.time()
    win_end = datetime.now(timezone.utc).isoformat()
    host_cpu, host_mem = sample_host(proc, t1 - t0)

    trace_id = ""
    if isinstance(resp, dict):
        trace_id = resp.get("trace_id") or ""

    spans = read_spans_for_trace(span_file, trace_id) if trace_id else []
    agg, total_spans, error_spans = aggregate_agent_spans(spans)
    quality = derive_quality(resp)

    # span_count_matched: 该窗期望 4(或 3 if Synthesizer error 早冒泡? 仍 4 span 因 synth span 起了)
    expected_agents = 4
    present = sum(1 for a in AGENT_NAMES if agg.get(a, {}).get("present"))
    span_matched = (present == expected_agents)

    # wall-clock sanity: 抽根 server span 时间应落在 [t0-2s, t1+2s]
    wallclock_ok = True
    if spans:
        starts = [s.get("start_unix_nano") for s in spans if s.get("start_unix_nano")]
        if starts:
            lo = (t0 - 5.0) * 1e9
            hi = (t1 + 5.0) * 1e9
            wallclock_ok = all((lo <= s <= hi) for s in starts)

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{scn['id']}_{idx}"
    row = build_row(scn, run_id, trace_id, http_status, e2e_ms, agg, total_spans,
                    error_spans, quality, host_cpu, host_mem, win_start, win_end,
                    span_matched, wallclock_ok,
                    note=("no_trace_id" if not trace_id else ""))
    return row, agg, resp, trace_id, win_start, win_end


# ============================================================
# 场景跑(起实例→预热→N 探针→kill)
# ============================================================
def run_scenario(scn, runs, warmup, span_file=None, port=5101, timeout=120,
                 raw_journal=True, verbose=True):
    if span_file is None:
        span_file = os.path.join(OUT_DIR, "spans", f"{scn['id']}.jsonl")
    os.makedirs(os.path.dirname(span_file), exist_ok=True)
    # 同场景同进程同文件; 若已存在(重跑)先清, 防旧窗 trace 混入(trace_id 归窗本不会串, 仍清净)
    if os.path.exists(span_file):
        os.remove(span_file)
    log_path = os.path.join(OUT_DIR, "spans", f"{scn['id']}.serverlog")

    if not port_free(port):
        raise RuntimeError(f"port {port} not free; refuse to start temp instance")

    checksum_before = get_checksum()
    if verbose:
        print(f"\n=== SCENARIO {scn['id']} ({scn['kind']}) faults={scn['faults']} runs={runs} ===", flush=True)
        print(f"  checksum before: {checksum_before}", flush=True)

    proc = start_temp_instance(port, scn["faults"], span_file, log_path)
    rows = []
    try:
        if not wait_health(port, timeout_s=150):
            raise RuntimeError(f"temp instance on {port} did not become healthy; see {log_path}")
        if verbose:
            print(f"  instance healthy on {port}; warming up x{warmup} ...", flush=True)
        # 预热(不计窗)
        for w in range(warmup):
            st, e2e, _ = probe_recommend(port, timeout=timeout)
            if verbose:
                print(f"    warmup {w+1}/{warmup}: http={st} e2e={e2e:.0f}ms", flush=True)
        # 正式窗
        for i in range(1, runs + 1):
            row, agg, resp, tid, ws, we = run_one_window(scn, port, proc, span_file, i, timeout=timeout)
            checksum_after_win = None  # CHECKSUM per-scenario, not per-window (省连接)
            if raw_journal:
                write_raw_journal(scn, row["run_id"], row, tid, ws, we,
                                  checksum_before, None, agg, resp)
                append_csv(row)
            rows.append(row)
            if verbose:
                print(f"    win {i}/{runs}: http={row['http_status']} e2e={row['e2e_latency_ms']}ms "
                      f"spans={row['total_span_count']} matched={row['span_count_matched']} "
                      f"conf={row['recommendation_confidence']} "
                      f"garbage_msg={row['garbage_message_present']} degrade_msg={row['degrade_message_present']}",
                      flush=True)
    finally:
        stop_temp_instance(proc)
        time.sleep(1.0)

    checksum_after = get_checksum()
    alarm = (checksum_before != checksum_after)
    if verbose:
        print(f"  checksum after : {checksum_after}  ALARM={alarm}", flush=True)
    if alarm:
        print("  [ALARM] CHECKSUM changed! business table may have been modified!", flush=True)
    # 回填 journal 的 after checksum(scenario 级)
    for r in rows:
        jp = os.path.join(OUT_DIR, "journal", f"{r['run_id']}.json")
        try:
            with open(jp, "r", encoding="utf-8") as f:
                jj = json.load(f)
            jj["checksum"]["after"] = checksum_after
            jj["checksum"]["alarm"] = alarm
            with open(jp, "w", encoding="utf-8") as f:
                json.dump(jj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return rows, checksum_before, checksum_after, alarm


# ============================================================
# cross_layer: 独立 CSV schema + build_row + writer + 场景跑(复用主 build_row 再补 cross 列)
# ============================================================
def cross_csv_columns():
    """cross CSV schema = 主 COLS + cross 专属列(dep_proxy/dep_root/net_delay + 子 span 分列)。
    独立文件 dataset_agentchaos_cross.csv, 绝不动主 120 行/主 features schema。"""
    cols = list(COLS)
    # cross 专属溯源/标签列(剔出 features 视图同 run_id/scenario_id)
    cross_extra = ["dep_root", "dep_proxy", "net_delay_ms"]
    for a in AGENT_NAMES:
        cross_extra.append(f"span_{a}_child_max_sasrec_ms")
        cross_extra.append(f"span_{a}_child_max_httpx_ms")
    for c in cross_extra:
        if c not in cols:
            cols.append(c)
    return cols


CROSS_COLS = cross_csv_columns()


def append_cross_csv(row):
    os.makedirs(OUT_DIR, exist_ok=True)
    new = not os.path.exists(CROSS_CSV_PATH)
    with open(CROSS_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CROSS_COLS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in CROSS_COLS})


def write_cross_raw_journal(scn, run_id, row, trace_id, win_start, win_end,
                            checksum_before, checksum_after, agg, raw_resp):
    """cross raw/journal 复用主目录但 run_id 前缀 S50_/S51_ 物理区分; journal 多记
    dep_root/dep_proxy/net_delay_ms + sasrec_url 重定向。"""
    os.makedirs(os.path.join(OUT_DIR, "raw"), exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "journal"), exist_ok=True)
    with open(os.path.join(OUT_DIR, "raw", f"{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"row": row, "agg": agg, "resp": raw_resp}, f, ensure_ascii=False, indent=2)
    rc_set = scn["root_cause_agent_set"]
    fault_types = sorted(set(scn["faults"].values())) if scn["faults"] else []
    journal = {
        "run_id": run_id, "scenario_id": scn["id"], "kind": scn["kind"],
        "faults": scn["faults"], "delay_ms": DELAY_MS,
        "cross_layer": {
            "dep_root": scn.get("dep_root"), "dep_proxy": scn.get("dep_proxy"),
            "net_delay_ms": CROSS_NET_DELAY_MS,
            "sasrec_url": scn.get("sasrec_url"), "deepseek_base": scn.get("deepseek_base"),
        },
        "probe": {"item_sequence": PROBE_SEQ, "top_k": PROBE_TOPK},
        "trace_id": trace_id,
        "window": {"start": win_start, "end": win_end},
        "ground_truth": {
            "root_cause_agent_set": rc_set,  # 已含依赖层根因(sasrec_api/deepseek)
            "n_root_causes": len(rc_set),
            "fault_type_set": fault_types,
            "dep_root": scn.get("dep_root"),
            "per_agent_fault": {a: scn["faults"].get(a, "none") for a in AGENT_NAMES},
        },
        "checksum": {"before": checksum_before, "after": checksum_after,
                     "alarm": (checksum_before != checksum_after) if (checksum_before and checksum_after) else None},
    }
    with open(os.path.join(OUT_DIR, "journal", f"{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump(journal, f, ensure_ascii=False, indent=2)


def run_cross_scenario(scn, runs, warmup, port=5101, timeout=120, verbose=True):
    """cross_layer 一场景: Toxiproxy 代理建/复用 + net_delay toxic + 临时实例 env 重定向
    (SASREC_API_URL→代理, 仅 cross) + N 探针 + 四层 toxic_clear + 独立 csv。

    安全四层 toxic_clear 防线(Reviewer cross #1):
      (a) 入口第一步**无条件** toxic_clear(清任何残留, 含上次硬杀遗留);
      (b) **每窗末 try/finally** toxic_clear(窗粒度收敛残留面);
      (c) 场景级最外层 try/finally 覆盖正常+异常路径再 clear;
      (d) 进程级 atexit 仅作 SIGKILL **之外**兜底(不响应 -9/断电, 非唯一防线)。
    隔离: 失败 → toxic_clear + 日志 + 返回 None(由调用方 continue), 绝不冒泡退出码。
    """
    c25 = _import_chaos25()
    if c25 is None:
        print(f"  [SKIP cross {scn['id']}] chaos25_runner 不可用", flush=True)
        return None

    proxy = scn["dep_proxy"]
    sasrec_url = scn.get("sasrec_url")  # 仅 S50 非空; #N1: 主场景永不传
    listen = SASREC_PROXY_LISTEN if proxy == SASREC_PROXY_NAME else DEEPSEEK_PROXY_LISTEN
    upstream = SASREC_PROXY_UPSTREAM if proxy == SASREC_PROXY_NAME else DEEPSEEK_PROXY_UPSTREAM

    span_file = os.path.join(OUT_DIR, "spans", f"{scn['id']}.jsonl")
    os.makedirs(os.path.dirname(span_file), exist_ok=True)
    if os.path.exists(span_file):
        os.remove(span_file)
    log_path = os.path.join(OUT_DIR, "spans", f"{scn['id']}.serverlog")

    if not port_free(port):
        print(f"  [SKIP cross {scn['id']}] port {port} not free", flush=True)
        return None

    proc = None
    rows = []
    checksum_before = get_checksum()
    if verbose:
        print(f"\n=== CROSS SCENARIO {scn['id']} ({scn['kind']}) faults={scn['faults']} "
              f"dep_root={scn['dep_root']} net_delay={CROSS_NET_DELAY_MS}ms runs={runs} ===", flush=True)
        print(f"  checksum before: {checksum_before}", flush=True)

    # ---- (a) 入口第一步无条件清残留 + 注册 atexit 兜底 ----
    _CROSS_ATEXIT_PROXIES.add(proxy)
    try:
        c25.toxic_clear(proxy)
    except Exception as e:
        print(f"  [WARN] 入口 toxic_clear({proxy}) 失败({e}), 继续(幂等建代理后再注)", flush=True)

    # ---- (c) 场景级最外层 try/finally: 正常+异常路径都 clear + kill 实例 ----
    try:
        # 建/复用代理(幂等; 现役 sasrec 直接 reuse, 绝不改 listen/upstream)
        ok, how = c25.toxi_ensure_proxy(proxy, listen, upstream)
        if not ok:
            print(f"  [SKIP cross {scn['id']}] toxi_ensure_proxy({proxy}) 失败: {how}", flush=True)
            return None
        if verbose:
            print(f"  [toxiproxy] proxy={proxy} listen={listen} upstream={upstream} -> {how}", flush=True)

        # 起临时实例(cross: SASREC_API_URL 重定向到代理; 仅此处传 sasrec_url, #N1)
        proc = start_temp_instance(port, scn["faults"], span_file, log_path, sasrec_url=sasrec_url)
        if not wait_health(port, timeout_s=150):
            print(f"  [SKIP cross {scn['id']}] temp instance not healthy; see {log_path}", flush=True)
            return None
        if verbose:
            print(f"  instance healthy on {port} (SASREC_API_URL={sasrec_url}); warmup x{warmup} ...", flush=True)

        # 预热(无 toxic, 不计窗) — 让 lru_cache 编图/title_cache 加载, 排除冷启动污染
        for w in range(warmup):
            st, e2e, _ = probe_recommend(port, timeout=timeout)
            if verbose:
                print(f"    warmup {w+1}/{warmup}: http={st} e2e={e2e:.0f}ms", flush=True)

        # 正式窗: 每窗 加 toxic → 探针 → (b) 窗末 finally 必清 toxic
        for i in range(1, runs + 1):
            try:
                c25.toxic_latency(proxy, CROSS_NET_DELAY_MS, 0)  # net_delay 只加在代理上
                row, agg, resp, tid, ws, we = run_one_window(scn, port, proc, span_file, i, timeout=timeout)
            finally:
                # (b) 每窗末无条件清 toxic(窗粒度收敛; 即便探针抛错也清)
                try:
                    c25.toxic_clear(proxy)
                except Exception:
                    pass
            run_id = row["run_id"]
            # run_one_window→build_row 已产出完整主行(含 quality/per-agent span 列); cross 行
            # = 该主行(scenario_id/kind/root_cause_set 已是 cross scn 的, 因 run_one_window
            # 用同一 scn 调 build_row, root_cause_agent_set 已含 dep_root) 再补 cross 专属列。
            crow = dict(row)
            crow["scenario_id"] = scn["id"]
            crow["kind"] = scn["kind"]
            crow["root_cause_set"] = ";".join(scn["root_cause_agent_set"])
            crow["n_root_causes"] = len(scn["root_cause_agent_set"])
            crow["dep_root"] = scn.get("dep_root", "")
            crow["dep_proxy"] = scn.get("dep_proxy", "")
            crow["net_delay_ms"] = CROSS_NET_DELAY_MS
            for a in AGENT_NAMES:
                ag = agg.get(a, {})
                cms = ag.get("child_max_sasrec_ms")
                cmh = ag.get("child_max_httpx_ms")
                crow[f"span_{a}_child_max_sasrec_ms"] = _r2(cms) if cms is not None else ""
                crow[f"span_{a}_child_max_httpx_ms"] = _r2(cmh) if cmh is not None else ""

            write_cross_raw_journal(scn, run_id, crow, tid, ws, we, checksum_before, None, agg, resp)
            append_cross_csv(crow)
            rows.append(crow)
            if verbose:
                seq = agg.get("Sequence_Recommender", {})
                print(f"    win {i}/{runs}: http={crow['http_status']} e2e={crow['e2e_latency_ms']}ms "
                      f"Seq_child_max={crow['span_Sequence_Recommender_child_max_duration_ms']}ms "
                      f"Seq_sasrec_max={crow['span_Sequence_Recommender_child_max_sasrec_ms']}ms "
                      f"Seq_sasrec_cnt={crow['span_Sequence_Recommender_child_sasrec_requests_count']} "
                      f"garbage_msg={crow['garbage_message_present']}", flush=True)
    except Exception as e:
        print(f"  [SKIP cross {scn['id']}] 非致命异常({type(e).__name__}: {e}); 已清 toxic 跳过", flush=True)
    finally:
        # (c) 场景级: 无论正常/异常都清 toxic + kill 实例
        try:
            c25.toxic_clear(proxy)
        except Exception:
            pass
        if proc is not None:
            stop_temp_instance(proc)
        time.sleep(1.0)

    checksum_after = get_checksum()
    alarm = (checksum_before != checksum_after)
    if verbose:
        print(f"  checksum after : {checksum_after}  ALARM={alarm}", flush=True)
    if alarm:
        print("  [ALARM] CHECKSUM changed! business table may have been modified!", flush=True)
    # 回填 journal after checksum + 核 toxic 已清(GET /proxies/<proxy>.toxics 应空)
    for r in rows:
        jp = os.path.join(OUT_DIR, "journal", f"{r['run_id']}.json")
        try:
            with open(jp, "r", encoding="utf-8") as f:
                jj = json.load(f)
            jj["checksum"]["after"] = checksum_after
            jj["checksum"]["alarm"] = alarm
            with open(jp, "w", encoding="utf-8") as f:
                json.dump(jj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    # 验证 toxic 残留已清(诚实自检)
    try:
        cur = c25._get_json(f"{c25.TOXI}/proxies/{proxy}", timeout=5)
        leftover = (cur or {}).get("toxics") or []
        if leftover:
            print(f"  [WARN] cross 后 {proxy} 仍有 toxic 残留: {leftover}; 再清一次", flush=True)
            c25.toxic_clear(proxy)
        elif verbose:
            print(f"  [OK] {proxy} toxics 已清空(无残留)", flush=True)
    except Exception:
        pass

    return rows, checksum_before, checksum_after, alarm


# ============================================================
# 预热冒烟(可分性 6 项报告)
# ============================================================
def smoke():
    """两阶段硬门槛: 起 1 临时实例, 预热, 单 normal/delay/garbage 探针, 出 6 项可分性报告。
    任一不可分→回报不跑全量。本函数写 smoke/ 不污染主集。"""
    print("=" * 70, flush=True)
    print("TASK-X 预热冒烟(可分性 6 项报告)", flush=True)
    print("=" * 70, flush=True)
    report = {"generated_at": datetime.now().astimezone().isoformat()}

    # --- 阶段0: exporter 4-span 对拍(InMemory vs JSONL) + parent 链 ---
    print("\n[0] InMemory vs JSONL exporter 对拍 + parent_span_id 父子链验证 ...", flush=True)
    exporter_check = _smoke_exporter_check()
    report["exporter_check"] = exporter_check
    print(f"    {json.dumps(exporter_check, ensure_ascii=False)}", flush=True)

    # --- 起实例(无故障), 预热实测 normal 分布 ---
    port = 5101
    span_file = os.path.join(SMOKE_DIR, "normal.jsonl")
    if os.path.exists(span_file):
        os.remove(span_file)
    log_path = os.path.join(SMOKE_DIR, "normal.serverlog")
    if not port_free(port):
        print(f"  [FATAL] port {port} busy", flush=True)
        return report
    checksum_before = get_checksum()
    print(f"\n[checksum before] {checksum_before}", flush=True)
    proc = start_temp_instance(port, {}, span_file, log_path)
    normal_windows = []
    delay_windows = {}
    garbage_windows = {}
    try:
        if not wait_health(port, 180):
            print(f"  [FATAL] instance not healthy; see {log_path}", flush=True)
            stop_temp_instance(proc)
            return report
        print("  instance healthy; warmup x3 ...", flush=True)
        for _ in range(3):
            st, e2e, _ = probe_recommend(port)
            print(f"    warmup http={st} e2e={e2e:.0f}ms", flush=True)
        # normal N=5
        print("\n[1+2+3] normal N=5: 实测各 agent span duration 分布 + httpx_count 分布 ...", flush=True)
        scn_norm = SCENARIO_BY_ID["S00_normal_full"]
        for i in range(1, 6):
            row, agg, resp, tid, ws, we = run_one_window(scn_norm, port, proc, span_file, i)
            normal_windows.append((row, agg))
            print(f"    n{i}: http={row['http_status']} spans={row['total_span_count']} "
                  f"matched={row['span_count_matched']} conf={row['recommendation_confidence']}", flush=True)
    finally:
        stop_temp_instance(proc)
        time.sleep(1.0)

    # 起 delay 实例(Product_Analyzer delay) — 单探针
    print("\n[4a] single delay (Product_Analyzer=delay 5000ms) N=2 ...", flush=True)
    sf2 = os.path.join(SMOKE_DIR, "delay_product.jsonl")
    if os.path.exists(sf2):
        os.remove(sf2)
    proc = start_temp_instance(port, {"Product_Analyzer": "delay"}, sf2, os.path.join(SMOKE_DIR, "delay.serverlog"))
    try:
        if wait_health(port, 180):
            for _ in range(2):
                probe_recommend(port)  # 预热
            scn_d = SCENARIO_BY_ID["S12_delay_Product"]
            for i in range(1, 3):
                row, agg, resp, tid, ws, we = run_one_window(scn_d, port, proc, sf2, i)
                delay_windows[i] = (row, agg)
                print(f"    d{i}: http={row['http_status']} "
                      f"prod_span={row['span_Product_Analyzer_duration_ms']}ms "
                      f"matched={row['span_count_matched']}", flush=True)
    finally:
        stop_temp_instance(proc)
        time.sleep(1.0)

    # 起 garbage 实例(Product_Analyzer garbage) — 单探针 + UserBehavior garbage
    print("\n[4b] single garbage (Product_Analyzer=garbage) N=2 ...", flush=True)
    sf3 = os.path.join(SMOKE_DIR, "garbage_product.jsonl")
    if os.path.exists(sf3):
        os.remove(sf3)
    proc = start_temp_instance(port, {"Product_Analyzer": "garbage"}, sf3, os.path.join(SMOKE_DIR, "garbage.serverlog"))
    try:
        if wait_health(port, 180):
            for _ in range(2):
                probe_recommend(port)
            scn_g = SCENARIO_BY_ID["S32_garbage_Product"]
            for i in range(1, 3):
                row, agg, resp, tid, ws, we = run_one_window(scn_g, port, proc, sf3, i)
                garbage_windows[i] = (row, agg)
                print(f"    g{i}: http={row['http_status']} "
                      f"prod_span={row['span_Product_Analyzer_duration_ms']}ms "
                      f"prod_httpx={row['span_Product_Analyzer_child_httpx_count']} "
                      f"garbage_msg={row['garbage_message_present']} "
                      f"matched={row['span_count_matched']}", flush=True)
    finally:
        stop_temp_instance(proc)
        time.sleep(1.0)

    # --- S42 HTTP 状态实测 ---
    print("\n[5] S42 triple-mixed HTTP 状态实测 N=2 ...", flush=True)
    sf4 = os.path.join(SMOKE_DIR, "s42.jsonl")
    if os.path.exists(sf4):
        os.remove(sf4)
    s42 = SCENARIO_BY_ID["S42_multi_triple_mixed"]
    proc = start_temp_instance(port, s42["faults"], sf4, os.path.join(SMOKE_DIR, "s42.serverlog"))
    s42_status = []
    try:
        if wait_health(port, 180):
            for _ in range(1):
                probe_recommend(port)
            for i in range(1, 3):
                row, agg, resp, tid, ws, we = run_one_window(s42, port, proc, sf4, i)
                s42_status.append(row["http_status"])
                print(f"    s42-{i}: http={row['http_status']} conf={row['recommendation_confidence']} "
                      f"unknown={row['recommended_product_is_unknown']}", flush=True)
    finally:
        stop_temp_instance(proc)
        time.sleep(1.0)

    checksum_after = get_checksum()
    print(f"\n[checksum after] {checksum_after} ALARM={checksum_before != checksum_after}", flush=True)

    # --- 汇总分析 ---
    report.update(_smoke_analyze(normal_windows, delay_windows, garbage_windows, s42_status,
                                 checksum_before, checksum_after))
    with open(os.path.join(SMOKE_DIR, "smoke_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("\n" + "=" * 70, flush=True)
    print("SMOKE REPORT:", flush=True)
    print(json.dumps(report.get("verdict", {}), ensure_ascii=False, indent=2), flush=True)
    print("full -> " + os.path.join(SMOKE_DIR, "smoke_report.json"), flush=True)
    return report


def _smoke_exporter_check():
    """InMemorySpanExporter 与 LocalJSONLSpanExporter 对拍 4-span 父子链(无 LLM, 纯合成)。"""
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        sys.path.insert(0, SVC_DIR)
        from local_span_exporter import LocalJSONLSpanExporter
        from opentelemetry import trace as t

        tmp = os.path.join(SMOKE_DIR, "_exporter_check.jsonl")
        if os.path.exists(tmp):
            os.remove(tmp)
        prov = TracerProvider()
        inmem = InMemorySpanExporter()
        prov.add_span_processor(SimpleSpanProcessor(inmem))
        prov.add_span_processor(SimpleSpanProcessor(LocalJSONLSpanExporter(tmp)))
        tracer = prov.get_tracer("smoke")
        # 合成: root -> agent.Test (attr recweb.agent.name) -> child HTTP
        with tracer.start_as_current_span("root") as root:
            with tracer.start_as_current_span("agent.Test") as a:
                a.set_attribute("recweb.agent.name", "Test")
                with tracer.start_as_current_span("POST") as c:
                    c.set_attribute("http.url", "http://api.deepseek.com/v1/chat")
        prov.force_flush()
        inmem_spans = inmem.get_finished_spans()
        # 读 JSONL
        jsonl = []
        with open(tmp, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    jsonl.append(json.loads(line))
        # 验 trace_id 一致 + parent 链
        tids = set(s["trace_id"] for s in jsonl)
        agg, total, errs = aggregate_agent_spans(jsonl)
        test_agg = agg.get("Test", {}) if "Test" in agg else None
        # aggregate 只认 AGENT_NAMES, Test 不在内; 直接核 parent 结构
        by_id = {s["span_id"]: s for s in jsonl}
        names = {s["span_id"]: s["name"] for s in jsonl}
        chain_ok = False
        for s in jsonl:
            if s["name"] == "POST":
                p = s.get("parent_span_id")
                if p and names.get(p) == "agent.Test":
                    gp = by_id.get(p, {}).get("parent_span_id")
                    if gp and names.get(gp) == "root":
                        chain_ok = True
        return {
            "inmem_span_count": len(inmem_spans),
            "jsonl_span_count": len(jsonl),
            "single_trace_id": (len(tids) == 1),
            "parent_chain_ok": chain_ok,
            "pass": (len(inmem_spans) == len(jsonl) == 3 and len(tids) == 1 and chain_ok),
        }
    except Exception as e:
        return {"error": str(e), "pass": False}


def _stats(vals):
    vals = [v for v in vals if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return {"n": 0, "mean": None, "std": None, "min": None, "max": None}
    return {
        "n": len(vals),
        "mean": round(statistics.mean(vals), 2),
        "std": round(statistics.pstdev(vals), 2) if len(vals) > 1 else 0.0,
        "min": round(min(vals), 2), "max": round(max(vals), 2),
    }


def _smoke_analyze(normal_windows, delay_windows, garbage_windows, s42_status,
                   cb, ca):
    out = {}
    # 1+2: normal 各 agent span duration 分布 + httpx_count 分布
    per_agent = {}
    for a in AGENT_NAMES:
        durs = []
        httpx = []
        sasrec = []
        for row, agg in normal_windows:
            ag = agg.get(a, {})
            if ag.get("duration_ms") is not None:
                durs.append(ag["duration_ms"])
            if ag.get("httpx_count") is not None:
                httpx.append(ag["httpx_count"])
            if ag.get("sasrec_requests_count") is not None:
                sasrec.append(ag["sasrec_requests_count"])
        per_agent[a] = {
            "span_duration_ms": _stats(durs),
            "httpx_count": _stats(httpx),
            "sasrec_requests_count": _stats(sasrec),
        }
    out["normal_per_agent"] = per_agent

    # host cpu/mem 方差(normal)
    out["normal_host"] = {
        "cpu_pct": _stats([row["host_cpu_pct"] for row, _ in normal_windows if isinstance(row["host_cpu_pct"], (int, float))]),
        "mem_pct": _stats([row["host_mem_pct"] for row, _ in normal_windows if isinstance(row["host_mem_pct"], (int, float))]),
    }

    # delay(Product) span 分布
    delay_prod = [agg.get("Product_Analyzer", {}).get("duration_ms") for _, agg in delay_windows.values()]
    delay_prod = [d for d in delay_prod if d is not None]
    out["delay_product_span_ms"] = _stats(delay_prod)
    # garbage(Product) span + httpx
    garbage_prod_span = [agg.get("Product_Analyzer", {}).get("duration_ms") for _, agg in garbage_windows.values()]
    garbage_prod_span = [d for d in garbage_prod_span if d is not None]
    garbage_prod_httpx = [agg.get("Product_Analyzer", {}).get("httpx_count") for _, agg in garbage_windows.values()]
    garbage_prod_httpx = [d for d in garbage_prod_httpx if d is not None]
    out["garbage_product_span_ms"] = _stats(garbage_prod_span)
    out["garbage_product_httpx"] = _stats(garbage_prod_httpx)
    out["s42_http_status"] = s42_status
    out["checksum"] = {"before": cb, "after": ca, "alarm": cb != ca}

    # ---- verdict(可分性判定) ----
    verdict = {}
    # #1 trace_id 归窗: normal 窗都 matched 4 span
    matched = [row["span_count_matched"] for row, _ in normal_windows]
    verdict["traceid_window_ok"] = bool(matched) and all(matched)
    # #3 garbage 命门: 两纯本地 analyzer normal httpx >=1
    ub_httpx = per_agent["User_Behavior_Analyzer"]["httpx_count"]
    pr_httpx = per_agent["Product_Analyzer"]["httpx_count"]
    verdict["userbehavior_normal_httpx_min"] = ub_httpx.get("min")
    verdict["product_normal_httpx_min"] = pr_httpx.get("min")
    verdict["garbage_locatable_httpx"] = (
        ub_httpx.get("min") is not None and ub_httpx.get("min") >= 1 and
        pr_httpx.get("min") is not None and pr_httpx.get("min") >= 1 and
        (out["garbage_product_httpx"].get("max") == 0 if out["garbage_product_httpx"].get("n") else None)
    )
    # #2 delay 三峰可分: normal product span mean/std vs delay vs garbage
    norm_prod = per_agent["Product_Analyzer"]["span_duration_ms"]
    verdict["normal_product_span_mean_std"] = (norm_prod.get("mean"), norm_prod.get("std"))
    verdict["delay_vs_normal_separable"] = (
        out["delay_product_span_ms"].get("min") is not None and norm_prod.get("max") is not None
        and out["delay_product_span_ms"].get("min") > norm_prod.get("max")
    )
    verdict["garbage_vs_normal_span_separable"] = (
        out["garbage_product_span_ms"].get("max") is not None and norm_prod.get("min") is not None
        and out["garbage_product_span_ms"].get("max") < norm_prod.get("min")
    )
    out["verdict"] = verdict
    return out


# ============================================================
# cross 冒烟(2 项硬门槛: POST 子 span ~15s 来源 + 多窗稳定 + e2e<100s)
# ============================================================
def cross_smoke(port=5101, timeout=120, runs=3):
    """S50 cross 冒烟(Reviewer cross 第2轮 2 项硬门槛, pilot 前必过):
    (a) POST /recommend 子 span 实测 ~15s 且成功(非被 5s 截断的 health GET),
        Seq child_sasrec_requests_count>=1 且 child_max_sasrec_ms 来源确为该 POST;
        多窗(N>=3)确认稳定(排除"只调 health 不调 recommend"的窗); Product span≈0/httpx0/garbage_msg=1。
    (b) S50 e2e 峰值 < 100s 留安全垫; 窗末 toxic_clear 后直打真实 8200 /health<100ms 证未残留,
        且 GET /proxies/sasrec toxics:[]。任一不达标→打印 FAIL, 不写主集/不跑 pilot。"""
    print("=" * 70, flush=True)
    print("TASK-X cross_layer S50 冒烟(POST 子 span ~15s 来源 + 多窗稳定 + e2e<100s)", flush=True)
    print("=" * 70, flush=True)
    c25 = _import_chaos25()
    if c25 is None:
        print("[FATAL] chaos25_runner 不可用, 无法跑 cross 冒烟", flush=True)
        return {"pass": False, "error": "chaos25_runner import failed"}

    scn = CROSS_SCENARIO_BY_ID["S50_cross_garbageProduct_sasrecDelay"]
    proxy = scn["dep_proxy"]
    os.makedirs(SMOKE_DIR, exist_ok=True)
    span_file = os.path.join(SMOKE_DIR, "cross_s50.jsonl")
    if os.path.exists(span_file):
        os.remove(span_file)
    log_path = os.path.join(SMOKE_DIR, "cross_s50.serverlog")

    if not port_free(port):
        print(f"[FATAL] port {port} busy", flush=True)
        return {"pass": False, "error": "port busy"}

    checksum_before = get_checksum()
    print(f"[checksum before] {checksum_before}", flush=True)

    proc = None
    windows = []
    _CROSS_ATEXIT_PROXIES.add(proxy)
    try:
        c25.toxic_clear(proxy)  # 入口无条件清残留
    except Exception:
        pass
    try:
        ok, how = c25.toxi_ensure_proxy(proxy, SASREC_PROXY_LISTEN, SASREC_PROXY_UPSTREAM)
        print(f"[toxiproxy] proxy={proxy} -> {how}", flush=True)
        proc = start_temp_instance(port, scn["faults"], span_file, log_path, sasrec_url=scn["sasrec_url"])
        if not wait_health(port, 180):
            print(f"[FATAL] instance not healthy; see {log_path}", flush=True)
            return {"pass": False, "error": "not healthy"}
        print("  instance healthy; warmup x2 (no toxic) ...", flush=True)
        for _ in range(2):
            st, e2e, _ = probe_recommend(port, timeout=timeout)
            print(f"    warmup http={st} e2e={e2e:.0f}ms", flush=True)
        for i in range(1, runs + 1):
            try:
                c25.toxic_latency(proxy, CROSS_NET_DELAY_MS, 0)
                row, agg, resp, tid, ws, we = run_one_window(scn, port, proc, span_file, i, timeout=timeout)
            finally:
                try:
                    c25.toxic_clear(proxy)
                except Exception:
                    pass
            seq = agg.get("Sequence_Recommender", {})
            prod = agg.get("Product_Analyzer", {})
            windows.append({
                "http": row["http_status"], "e2e_ms": row["e2e_latency_ms"],
                "seq_child_max_ms": seq.get("child_max_duration_ms"),
                "seq_child_max_sasrec_ms": seq.get("child_max_sasrec_ms"),
                "seq_child_max_httpx_ms": seq.get("child_max_httpx_ms"),
                "seq_sasrec_count": seq.get("sasrec_requests_count"),
                "prod_span_ms": prod.get("duration_ms"),
                "prod_httpx": prod.get("httpx_count"),
                "garbage_msg": row["garbage_message_present"],
            })
            print(f"    s50-{i}: http={row['http_status']} e2e={row['e2e_latency_ms']}ms "
                  f"seq_child_max={seq.get('child_max_duration_ms')}ms "
                  f"seq_sasrec_max={seq.get('child_max_sasrec_ms')}ms "
                  f"seq_sasrec_cnt={seq.get('sasrec_requests_count')} "
                  f"prod_span={prod.get('duration_ms')}ms prod_httpx={prod.get('httpx_count')} "
                  f"garbage_msg={row['garbage_message_present']}", flush=True)
    except Exception as e:
        print(f"[cross_smoke] 异常({type(e).__name__}: {e})", flush=True)
    finally:
        try:
            c25.toxic_clear(proxy)
        except Exception:
            pass
        if proc is not None:
            stop_temp_instance(proc)
        time.sleep(1.0)

    checksum_after = get_checksum()
    # 门 (b) 残留自检: 直打真实 8200 /health 应快 + 代理 toxics 空
    real_8200_ms = None
    try:
        t0 = time.time()
        with _req("http://127.0.0.1:8200/health", timeout=5):
            real_8200_ms = (time.time() - t0) * 1000.0
    except Exception as e:
        real_8200_ms = f"err:{e}"
    toxics_after = None
    try:
        cur = c25._get_json(f"{c25.TOXI}/proxies/{proxy}", timeout=5)
        toxics_after = (cur or {}).get("toxics") or []
    except Exception:
        toxics_after = "unknown"

    # ---- 判据 ----
    # 门(a1): >=1 窗 POST 成功(http200) 且 seq_sasrec_count>=1 且 child_max_sasrec ~15s(>=12000)
    sasrec_delayed = [w for w in windows
                      if w["http"] == 200 and (w["seq_sasrec_count"] or 0) >= 1
                      and isinstance(w["seq_child_max_sasrec_ms"], (int, float))
                      and w["seq_child_max_sasrec_ms"] >= 12000]
    # 门(a2): child_max 来源确为 sasrec(child_max ≈ child_max_sasrec, 即 sasrec 子 span 是新 max)
    sasrec_is_childmax = [w for w in sasrec_delayed
                          if isinstance(w["seq_child_max_ms"], (int, float))
                          and abs((w["seq_child_max_ms"] or 0) - (w["seq_child_max_sasrec_ms"] or 0)) < 1.0]
    # 门(a3): Product garbage 内生(span≈0 + httpx0 + garbage_msg=1)
    garbage_ok = [w for w in windows
                  if (w["prod_httpx"] in (0, None)) and (w["garbage_msg"] in (1, "1", True))]
    # 门(b): e2e 峰值 < 100s
    e2es = [w["e2e_ms"] for w in windows if isinstance(w["e2e_ms"], (int, float))]
    e2e_max = max(e2es) if e2es else None
    e2e_ok = (e2e_max is not None and e2e_max < 100000)
    residual_ok = (isinstance(real_8200_ms, (int, float)) and real_8200_ms < 100.0
                   and isinstance(toxics_after, list) and len(toxics_after) == 0)

    verdict = {
        "n_windows": len(windows),
        "sasrec_delayed_windows": len(sasrec_delayed),
        "sasrec_is_childmax_windows": len(sasrec_is_childmax),
        "garbage_ok_windows": len(garbage_ok),
        "e2e_max_ms": e2e_max, "e2e_under_100s": e2e_ok,
        "real_8200_health_ms": real_8200_ms,
        "proxy_toxics_after": toxics_after,
        "residual_clear_ok": residual_ok,
        "checksum_alarm": (checksum_before != checksum_after),
        # 多窗稳定: >=max(2,N-1) 窗 POST 子 span ~15s 且为 child_max 来源
        "gate_a_post_subspan_15s_stable": (len(sasrec_is_childmax) >= max(2, runs - 1)),
        "gate_a_garbage_internal": (len(garbage_ok) >= max(2, runs - 1)),
        "gate_b_e2e_and_residual": (e2e_ok and residual_ok),
    }
    verdict["pass"] = bool(
        verdict["gate_a_post_subspan_15s_stable"]
        and verdict["gate_a_garbage_internal"]
        and verdict["gate_b_e2e_and_residual"]
        and not verdict["checksum_alarm"]
    )
    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "scenario": scn["id"], "net_delay_ms": CROSS_NET_DELAY_MS,
        "windows": windows, "verdict": verdict,
        "checksum": {"before": checksum_before, "after": checksum_after},
    }
    with open(os.path.join(SMOKE_DIR, "cross_smoke_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("\n" + "=" * 70, flush=True)
    print("CROSS SMOKE VERDICT:", flush=True)
    print(json.dumps(verdict, ensure_ascii=False, indent=2), flush=True)
    print(f"[checksum after] {checksum_after}", flush=True)
    print("full -> " + os.path.join(SMOKE_DIR, "cross_smoke_report.json"), flush=True)
    if not verdict["pass"]:
        print("\n[CROSS SMOKE FAIL] 任一门未达标, 不应跑 pilot; 回报 Reviewer。", flush=True)
    return report


# ============================================================
# main
# ============================================================
def _run_cross_targets(cross_targets, runs, warmup, port, timeout):
    """跑一组 cross 场景, 每个非致命包裹(失败 continue 不冒泡), 写独立 run_summary_cross.json。
    返回 grand 字典(供合并/打印)。绝不影响主集退出码。"""
    grand = {"started_at": datetime.now().astimezone().isoformat(), "scenarios": []}
    for scn in cross_targets:
        try:
            res = run_cross_scenario(scn, runs, warmup, port=port, timeout=timeout)
        except Exception as e:
            # run_cross_scenario 内部已 try/except, 这里是最外层兜底, 绝不让 cross 异常冒泡
            print(f"  [SKIP cross {scn['id']}] 顶层异常({type(e).__name__}: {e}); 跳过", flush=True)
            res = None
        if res is None:
            grand["scenarios"].append({"id": scn["id"], "windows": 0, "skipped": True})
            continue
        rows, cb, ca, alarm = res
        grand["scenarios"].append({
            "id": scn["id"], "windows": len(rows),
            "checksum_alarm": alarm,
            "http_status_set": sorted(set(r["http_status"] for r in rows)),
        })
    grand["finished_at"] = datetime.now().astimezone().isoformat()
    with open(os.path.join(OUT_DIR, "run_summary_cross.json"), "w", encoding="utf-8") as f:
        json.dump(grand, f, ensure_ascii=False, indent=2)
    print("\nCROSS DONE. summary -> " + os.path.join(OUT_DIR, "run_summary_cross.json"), flush=True)
    return grand


def main():
    ap = argparse.ArgumentParser(description="TASK-X agent 故障注入 runner")
    ap.add_argument("--smoke", action="store_true", help="主集预热冒烟(可分性 6 项报告), 不写主集")
    ap.add_argument("--scenario", default=None, help="单主场景 id")
    ap.add_argument("--all", action="store_true", help="全部 15 主场景(绝不含 cross_layer)")
    ap.add_argument("--runs", type=int, default=8, help="每场景窗数 N")
    ap.add_argument("--warmup", type=int, default=3, help="每实例预热次数(不计窗)")
    ap.add_argument("--port", type=int, default=5101)
    ap.add_argument("--timeout", type=int, default=120)
    # ---- cross_layer 入口(隔离 #5: 默认不跑; 主 --all 绝不含 cross) ----
    ap.add_argument("--cross-scenario", default=None, dest="cross_scenario",
                    help=f"单跑 cross 场景: {list(CROSS_SCENARIO_BY_ID)}")
    ap.add_argument("--include-cross", action="store_true", dest="include_cross",
                    help="仅与 --all 合用: 主 15 场景全落盘后于末尾追加 cross(S50; S51 best-effort)")
    ap.add_argument("--cross-runs", type=int, default=4, dest="cross_runs",
                    help="cross pilot 窗数 N(默认 4, 只验可分性不报点估计)")
    ap.add_argument("--cross-smoke", action="store_true", dest="cross_smoke",
                    help="cross 冒烟(S50 N=3): 实测 POST 子 span ~15s 来源/多窗稳定/e2e<100s, 不达标回报")
    args = ap.parse_args()

    if args.smoke:
        smoke()
        return

    if args.cross_smoke:
        cross_smoke(port=args.port, timeout=args.timeout)
        return

    os.makedirs(OUT_DIR, exist_ok=True)

    # ---- 单跑 cross 场景(独立入口, 不碰主集) ----
    if args.cross_scenario:
        if args.cross_scenario not in CROSS_SCENARIO_BY_ID:
            print(f"unknown cross scenario {args.cross_scenario}; valid: {list(CROSS_SCENARIO_BY_ID)}",
                  file=sys.stderr)
            sys.exit(2)
        _run_cross_targets([CROSS_SCENARIO_BY_ID[args.cross_scenario]],
                           args.cross_runs, args.warmup, args.port, args.timeout)
        return

    targets = []
    if args.all:
        targets = SCENARIOS  # 钉死: 主 --all 永远只 15 主场景, CROSS_SCENARIOS 绝不进此列表
    elif args.scenario:
        if args.scenario not in SCENARIO_BY_ID:
            print(f"unknown scenario {args.scenario}; valid: {list(SCENARIO_BY_ID)}", file=sys.stderr)
            sys.exit(2)
        targets = [SCENARIO_BY_ID[args.scenario]]
    else:
        print("specify --smoke | --scenario <id> | --all | --cross-scenario <id> | --cross-smoke",
              file=sys.stderr)
        sys.exit(2)

    grand = {"started_at": datetime.now().astimezone().isoformat(), "scenarios": []}
    for scn in targets:
        rows, cb, ca, alarm = run_scenario(scn, args.runs, args.warmup, port=args.port,
                                           timeout=args.timeout)
        grand["scenarios"].append({
            "id": scn["id"], "windows": len(rows),
            "checksum_alarm": alarm,
            "http_status_set": sorted(set(r["http_status"] for r in rows)),
        })
    grand["finished_at"] = datetime.now().astimezone().isoformat()
    with open(os.path.join(OUT_DIR, "run_summary.json"), "w", encoding="utf-8") as f:
        json.dump(grand, f, ensure_ascii=False, indent=2)
    print("\nALL DONE. summary -> " + os.path.join(OUT_DIR, "run_summary.json"), flush=True)

    # ---- (隔离 #5) cross 仅在主 15 场景全部落盘 *之后* 才追加, 且失败绝不冒泡退出码 ----
    if args.include_cross and args.all:
        print("\n=== --include-cross: 主 15 场景已落盘, 末尾追加 cross_layer pilot ===", flush=True)
        # 默认只跑 S50(必做); S51 best-effort 默认砍, 须显式 --cross-scenario S51 单跑
        cross_targets = [CROSS_SCENARIO_BY_ID["S50_cross_garbageProduct_sasrecDelay"]]
        try:
            _run_cross_targets(cross_targets, args.cross_runs, args.warmup, args.port, args.timeout)
        except Exception as e:
            print(f"  [cross 全跳过] 顶层异常({type(e).__name__}: {e}); 主集已落盘不受影响", flush=True)


if __name__ == "__main__":
    main()
