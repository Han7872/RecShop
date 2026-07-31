#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""验收校验器 —— agent 语义故障 K8S 全栈重采(内部代号 B 档)

规范全文: (project docs)/agentfault-k8s-recollect-20260727.md
本脚本是该规范的**唯一可执行实现**;规范里的每一条闸都对应这里的一个 `@check`。

━━━ 三条设计铁律(违反即等于把上一份被降级的 acceptance-spec 重写一遍)━━━

  1. **脚本内零字面期望值。** 所有阈值现算自 `--ref` 参考树(默认 (archived) agentfault_v2)。
     出现 `== 108` / `== 82` / `== 9` 这类裸数字即视为 bug。唯一例外是"比率 0/1"这种
     由判定语义本身决定的常数(如"解析成功率 1.0"),且必须在 REF 上实测可达。

  2. **每条闸都标 `ref_expect`** —— 它在参考树上**应当** PASS 还是 FAIL(按构造)。
     `--selftest` 会逐条比对。一条在 REF 上永远 PASS 又永远不可能 FAIL 的闸没有信息量;
     一条在 REF 上意外 FAIL 的闸说明判据写错了(上一版草案的 H3b 就是这么被抓出来的)。

  3. **判据必须有区分力,且区分力要被证明。** `--selftest` 第二阶段跑**变异电池**:
     在内存里把参考树按已知失败模式弄坏(删行/GT 整树坍缩/候选清零/占位符回潮/span 截断),
     断言指定的闸**确实翻成 FAIL**。翻不动的闸 = 空闸,与 `count(up{job=...})` 同类。

━━━ 退出码(非零一律=未通过;数值是分类不是严重度)━━━
    0  全绿
    1  BLOCK-RECOLLECT 失败 —— 必须返工重采,改文档无效
    2  BLOCK-RELEASE   失败 —— 数据可留,不许发
    6  有非 DISCLOSE 级的 SKIP —— **未验收**(缺参数/缺证据),不是通过
    3  DISCLOSE        失败 —— 必须写进 SUMMARY 的机器可读披露
    4  用法/上下文错误(树不存在、--only 写了不存在的 ID 等)
    5  校验器自身崩溃 —— 状态未知,不得当作任何结论
  优先级: 5 > 4 > 1 > 2 > 6 > 3 > 0

━━━ 典型用法 ━━━
    # 自验(证明校验器本身没写错 + 证明它有区分力)
    python verify_recollect_acceptance.py --selftest

    # 新树完整验收(缺参数 = 未验收,不是通过)
    python verify_recollect_acceptance.py \
        --tree datasets/agentfault_k8s --ref (archived) agentfault_v2 \
        --live --with-item-file --with-eval \
        --rerun-log <幂等重跑日志> --json-out (内部验证报告)
"""

import argparse
import collections
import csv
import glob
import hashlib
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime

# scripts/chaos/agentfault/k8s/<this> -> 5 层到仓根
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))))

RECOLLECT = "BLOCK-RECOLLECT"
RELEASE = "BLOCK-RELEASE"
DISCLOSE = "DISCLOSE"

# 证据独立性(规范 §0.5):BLOCK-RECOLLECT 级至少要有 cross/third 之一,
# 纯 self(采集器自己算自己写)最高只能到 BLOCK-RELEASE。
EV_SELF = "self"          # 采集器自陈,采集器没察觉的失败它一律察觉不到
EV_CROSS = "cross"        # CSV ↔ journal ↔ spans ↔ ledger 互证
EV_THIRD = "third"        # 采集器之外的观测者:宿主文件 / git / collector / kubectl

CHECKS = []               # [(id, group, level, plane, evidence, ref_expect, title, fn)]


class R(object):
    """一条检查的结果。`numbers` 里必须放**现算出来的数**(禁-10:不报不可重算的数)。"""

    __slots__ = ("status", "msg", "numbers")

    def __init__(self, status, msg, numbers=None):
        self.status = status
        self.msg = msg
        self.numbers = numbers or {}

    @staticmethod
    def ok(msg, **nums):
        return R("PASS", msg, nums)

    @staticmethod
    def bad(msg, **nums):
        return R("FAIL", msg, nums)

    @staticmethod
    def warn(msg, **nums):
        return R("WARN", msg, nums)

    @staticmethod
    def skip(msg, **nums):
        return R("SKIP", msg, nums)


def check(cid, group, level, plane, evidence, ref_expect, title):
    def deco(fn):
        CHECKS.append((cid, group, level, plane, evidence, ref_expect, title, fn))
        return fn
    return deco


# ==========================================================================
# 读取层 —— 一律 utf-8-sig(仓内真实 K8S 采集树的 spans.jsonl 带过 BOM,
#           用 encoding='utf-8' + json.loads 会直接 JSONDecodeError)
# ==========================================================================
def _open(path):
    return io.open(path, "r", encoding="utf-8-sig", errors="strict")


def _rel(p, start=None):
    """相对路径,**跨盘符不抛异常**。

    ★2026-07-27:`os.path.relpath('C:/tmp/x', 'D:/repo')` 在 Windows 上抛
    `ValueError: path is on mount 'C:', start on mount 'D:'`。原来 chk_A6 直接用
    relpath 拼报错文案,于是"树不在仓所在盘"时闸会变成【校验器内部异常(ERROR)】——
    ERROR 的语义是"状态未知,不得当作任何结论",比老老实实 FAIL 更糟。
    (由 --selftest 阶段 3 用系统临时目录跑时撞出来的。)
    """
    try:
        return os.path.relpath(p, start if start is not None else REPO)
    except ValueError:
        return p


def read_text(path, default=""):
    try:
        return _open(path).read()
    except Exception:
        return default


def read_csv_rows(path):
    """CSV 一律走 DictReader。**禁止任何行式判据**(wc -l / grep -c):
    参考树 dataset_agentfault.csv 物理 178 行 / 逻辑 108 行,divergent_needle 列内嵌换行。"""
    with _open(path) as f:
        rd = csv.DictReader(f)
        cols = list(rd.fieldnames or [])
        rows = [dict(r) for r in rd]
    return cols, rows


def read_jsonl(path):
    """返回 (entries, n_lines, n_bad)。解析失败不吞:n_bad 会被闸拿去判。"""
    out, n, bad = [], 0, 0
    try:
        f = _open(path)
    except Exception:
        return out, 0, 0
    with f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n += 1
            try:
                out.append(json.loads(line))
            except Exception:
                bad += 1
    return out, n, bad


# 工具输出解析(候选侧 / 历史侧)。**解析不出 = FAIL**,不是"没有占位符所以通过"。
RANK_RE = re.compile(u"排名\\s*\\d+\\s*[:：]\\s*(\\S+)\\s*[\\(（]"
                     u"得分\\s*[:：][^\\)）]*[\\)）]\\s*[-–—]\\s*(.*)")
HIST_RE = re.compile(u"^\\s*\\d+\\s*\\.\\s*(\\S+?)\\s*[:：]\\s*(.*?)\\s*$")
TOOL_CAND = "get_sequence_recommendations"
TOOL_HIST = "get_product_details"
POD_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(-[0-9a-f]{6,12})?(-[a-z0-9]{5})$")


class CtxError(Exception):
    """上下文错误(树不存在 / 缺 CSV / 参考树坏了)。**必须 exit 4,不是 exit 1** ——
    exit 1 的语义是 BLOCK-RECOLLECT('数据不对,去返工重采'),把"路径打错了"报成
    "数据不对"会直接误导下一个人去重采 108 个 case。"""


class Tree(object):
    """一棵采集树的全部只读视图。lazy + 缓存;`--selftest` 的变异电池直接改这些缓存。"""

    def __init__(self, path):
        self.path = path
        self.csv_path = os.path.join(path, "dataset_agentfault.csv")
        if not os.path.isdir(path):
            raise CtxError("树不存在: %s" % path)
        if not os.path.isfile(self.csv_path):
            raise CtxError("缺 dataset_agentfault.csv: %s" % self.csv_path)
        self.cols, self.rows = read_csv_rows(self.csv_path)
        self._spans = None
        self._journals = None
        self._ledgers = None
        self._tools = None

    # ---- 派生视图(全部现算,不缓存字面量)----------------------------------
    @property
    def combos(self):
        return sorted({r.get("group_id", "") for r in self.rows})

    @property
    def faulted(self):
        return [r for r in self.rows if r.get("injected") == "1"]

    @property
    def zero_root(self):
        return [r for r in self.rows if r.get("injected") != "1"]

    def combo_kind(self):
        d = {}
        for r in self.rows:
            d.setdefault(r.get("group_id", ""), r.get("kind", ""))
        return d

    def reps_per_combo(self):
        """**per-combo dict,不是标量。** resume / 作废重跑下各 combo reps 会不等,
        标量会把不齐掩盖成一个数(这正是上一版草案 main() 表头的隐性硬编码)。"""
        return collections.Counter(r.get("group_id", "") for r in self.rows)

    def case_ids(self):
        return {r.get("run_id", "") for r in self.rows}

    def journal_ids(self):
        return {os.path.basename(p)[:-5]
                for p in glob.glob(os.path.join(self.path, "journal", "*.json"))}

    def raw_ids(self):
        return {os.path.basename(p)[:-5]
                for p in glob.glob(os.path.join(self.path, "raw", "*.json"))}

    # ---- spans -----------------------------------------------------------
    def spans(self):
        """{combo: {"traces": Counter(trace_id->span 数),
                    "names": Counter(span name->条数),
                    "by_name_trace": {name: set(trace_id)},
                    "recs": [span dict, ...]  # 只留判据要用的瘦身副本
                   }}"""
        if self._spans is not None:
            return self._spans
        out = {}
        for p in sorted(glob.glob(os.path.join(self.path, "spans", "*.jsonl"))):
            combo = os.path.basename(p)[:-6]
            tr, nm, bnt, recs = collections.Counter(), collections.Counter(), {}, []
            entries, _, bad = read_jsonl(p)
            for o in entries:
                tid = o.get("trace_id", "")
                name = o.get("name", "")
                tr[tid] += 1
                nm[name] += 1
                bnt.setdefault(name, set()).add(tid)
                at = o.get("attributes") or {}
                # status:F1 要拿它区分"业务调用失败"与"workflow.py:599 硬编码探针必然失败"
                #        (探针 span 在 K8S 里 100% ERROR,而 error_span_count 却 108/108 全 0 ——
                #         正是这一对数字证明探针 span 没进任何 case 的特征聚合)
                recs.append({"trace_id": tid, "name": name,
                             "url": at.get("http.url"),
                             "status": o.get("status_code"),
                             "model": at.get("llm.model_name"),
                             "out": at.get("output.value"),
                             "ri_agent": at.get("agentfault.resolved_input.agent"),
                             "ri_names": at.get("agentfault.resolved_input.msg_names")})
            out[combo] = {"traces": tr, "names": nm, "by_name_trace": bnt,
                          "recs": recs, "bad_lines": bad}
        self._spans = out
        return out

    # ---- journal / ledger -------------------------------------------------
    def journals(self):
        """{case_id: dict}  解析失败的存 {"__parse_error__": repr}"""
        if self._journals is not None:
            return self._journals
        out = {}
        for p in sorted(glob.glob(os.path.join(self.path, "journal", "*.json"))):
            cid = os.path.basename(p)[:-5]
            try:
                out[cid] = json.load(_open(p))
            except Exception as e:  # noqa: BLE001
                out[cid] = {"__parse_error__": repr(e)}
        self._journals = out
        return out

    def ledgers(self):
        """{combo: {"entries": [...], "n_lines": int, "n_bad": int, "traces": set}}"""
        if self._ledgers is not None:
            return self._ledgers
        out = {}
        for p in sorted(glob.glob(os.path.join(self.path, "ledgers", "*.jsonl"))):
            combo = os.path.basename(p)[:-6]
            ent, n, bad = read_jsonl(p)
            out[combo] = {"entries": ent, "n_lines": n, "n_bad": bad,
                          "traces": {e.get("trace_id") for e in ent if e.get("trace_id")}}
        self._ledgers = out
        return out

    # ---- 工具 I/O(候选侧 / 历史侧语义)------------------------------------
    def tools(self):
        """{"cand": {trace_id: [(item_id, title), ...]},
            "hist": {trace_id: [(item_id, title), ...]},
            "cand_calls": n, "hist_calls": n,
            "cand_unparsed": n}  —— 解析不出的调用单独计数,不许静默当 0。"""
        if self._tools is not None:
            return self._tools
        cand, hist = {}, {}
        ncall = nhist = unparsed = 0
        for sp in self.spans().values():
            for rec in sp["recs"]:
                ov = rec.get("out") or ""
                if rec["name"] == TOOL_CAND:
                    ncall += 1
                    got = [m.groups() for m in (RANK_RE.search(x) for x in ov.splitlines()) if m]
                    if not got and ov.strip():
                        unparsed += 1
                    cand.setdefault(rec["trace_id"], []).extend(got)
                elif rec["name"] == TOOL_HIST:
                    nhist += 1
                    got = [m.groups() for m in (HIST_RE.match(x) for x in ov.splitlines()) if m]
                    hist.setdefault(rec["trace_id"], []).extend(got)
        self._tools = {"cand": cand, "hist": hist, "cand_calls": ncall,
                       "hist_calls": nhist, "cand_unparsed": unparsed}
        return self._tools


class Ctx(object):
    def __init__(self, args, force_distinct=False):
        """force_distinct=True 时即便 tree==ref 也加载两个**独立**的 Tree 对象。
        ★这是 --selftest 阶段 2 的必要条件:变异电池改的是 T 的缓存,若 T 与 REF 是同一个
        对象,所有"与 REF 比对"的闸都会因为分子分母同时被改而恒真 —— 实跑第一版时
        drop_rows/collapse_gt/zero_candidates/placeholder 四组变异全部没翻动闸,
        正是这个原因。这本身就是本规范反复强调的"分子分母同时坍缩 ⇒ 比率类闸恒真"。"""
        self.args = args
        self.tree_path = args.tree if os.path.isabs(args.tree) else os.path.join(REPO, args.tree)
        self.ref_path = args.ref if os.path.isabs(args.ref) else os.path.join(REPO, args.ref)
        self.T = Tree(self.tree_path)
        same = os.path.abspath(self.tree_path) == os.path.abspath(self.ref_path)
        self.REF = self.T if (same and not force_distinct) else Tree(self.ref_path)
        self.self_compare = self.T is self.REF
        self.python = args.python or sys.executable
        self.kubectl = args.kubectl or os.environ.get("KUBECTL") or "kubectl"
        self._mutated = []          # --selftest 变异电池记录

    # 便捷别名
    @property
    def tree(self):
        return self.tree_path

    @property
    def ref(self):
        return self.ref_path

    @property
    def rows(self):
        return self.T.rows

    @property
    def ref_rows(self):
        return self.REF.rows

    def has_col(self, name):
        return name in self.T.cols

    def kube(self, argv, timeout=90):
        try:
            p = subprocess.run([self.kubectl] + argv, capture_output=True, timeout=timeout)
            return p.returncode, p.stdout.decode("utf-8", "replace"), p.stderr.decode("utf-8", "replace")
        except Exception as e:  # noqa: BLE001
            return 127, "", repr(e)


# ==========================================================================
# 通用小工具
# ==========================================================================
def fnum(v):
    """空串/None -> None;不可解析 -> "NaN" 哨兵(调用方必须显式处理,不许当 0)。"""
    if v is None:
        return None
    v = str(v).strip()
    if v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return "NaN"


def sha256_file(path, limit=None):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            h.update(b)
            if limit and f.tell() > limit:
                break
    return h.hexdigest()


def quantile(vals, q):
    """线性插值分位数(numpy 不是本脚本的依赖 —— 验收器不该引入采集环境之外的包)。"""
    if not vals:
        return None
    s = sorted(vals)
    if len(s) == 1:
        return float(s[0])
    pos = q * (len(s) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(s) - 1)
    return float(s[lo] + (s[hi] - s[lo]) * (pos - lo))


def dotenv_values(key):
    """从仓根 `.env`(其次 `.env.example`)取一个键 → {文件名: 取值}。

    ★为什么这算"零字面期望值":REF(agentfault_v2,本机 harness 批次)就是加载这份 `.env`
    跑出来的,K8S 的 `deepseek-env` secret 也是照它建的(k8s/pilot/README.md §2)。
    所以"仓库 .env 里的 alias"= **REF 与 B 档共用的请求侧口径**,不是我拍的常量。
    `.env` gitignored(可被本地改),`.env.example` 在 HEAD 里(可核);两者不一致时
    调用方必须显式判 FAIL —— 口径本身不自洽就不该放行(同 G0 的思路)。"""
    out = {}
    for name in (".env", ".env.example"):
        p = os.path.join(REPO, name)
        if not os.path.isfile(p):
            continue
        for line in read_text(p).splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                out[name] = v.strip().strip('"').strip("'")
    return out


def live_pod_env(c, key):
    """按可靠性递降取 rec-agent 侧的一个 env → (value|None, source, detail)。

      1) `kubectl exec deploy/<dep> -- printenv <key>` —— **真跑着的进程看到的值**,最硬;
      2) deployment spec 的 `env[]` 直给;
      3) deployment spec 的 `envFrom.secretRef` → 该 secret 里 base64 解出来。

    ★2/3 不是冗余:`deepseek-env` **只被 agentfault 的 patch 脚本引用**,
      `restore_recagent_stock.ps1` 跑完 envFrom 就被摘掉(k8s/pilot/README.md §2 明写
      stock 的 01-rec-agent.yaml 不引用它)。采后验收常常正好落在 restore 之后,
      只做 1) 会把"可核"误判成"不可核"(和 B8/§6.3 同一个坑)。
    """
    ns, dep, ct = c.args.k8s_ns, c.args.k8s_deploy, c.args.k8s_container
    detail = {"ns": ns, "deploy": dep, "container": ct}

    rc, so, se = c.kube(["exec", "-n", ns, "deploy/" + dep, "-c", ct, "--", "printenv", key],
                        timeout=120)
    detail["printenv"] = {"rc": rc, "out": (so or "").strip()[:120], "err": (se or "")[:160]}
    if rc == 0 and (so or "").strip():
        return (so or "").strip().splitlines()[0].strip(), "pod-env(printenv)", detail

    rc2, so2, se2 = c.kube(["get", "deploy", dep, "-n", ns, "-o", "json"], timeout=120)
    detail["get_deploy_rc"] = rc2
    if rc2 != 0:
        detail["get_deploy_err"] = (se2 or "")[:200]
        return None, "unavailable", detail
    try:
        spec = json.loads(so2)["spec"]["template"]["spec"]["containers"]
    except Exception as e:  # noqa: BLE001
        detail["parse_err"] = repr(e)
        return None, "unavailable", detail
    cont = next((x for x in spec if x.get("name") == ct), spec[0] if spec else {})
    for ev in cont.get("env") or []:
        if ev.get("name") == key and ev.get("value") is not None:
            return str(ev["value"]), "deploy.spec.env[]", detail

    import base64
    secrets = [(f.get("secretRef") or {}).get("name") for f in (cont.get("envFrom") or [])]
    secrets = [s for s in secrets if s]
    detail["envFrom_secrets"] = secrets
    for sname in secrets:
        rc3, so3, _ = c.kube(["get", "secret", sname, "-n", ns, "-o", "json"], timeout=120)
        if rc3 != 0:
            continue
        try:
            b64 = (json.loads(so3).get("data") or {}).get(key)
        except Exception:  # noqa: BLE001
            continue
        if b64:
            try:
                return (base64.b64decode(b64).decode("utf-8", "replace").strip(),
                        "secret/%s" % sname, detail)
            except Exception as e:  # noqa: BLE001
                detail["b64_err"] = repr(e)
    return None, "unavailable", detail


# ---- host 归类(F1)------------------------------------------------------------
_LOOPBACK = {"127.0.0.1", "localhost", "::1", "0.0.0.0"}
_IPV4_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


def netloc_host(netloc):
    """从 `host:port` 取 host(兼容 `[::1]:8200`)。"""
    h = netloc
    if h.startswith("["):
        return h[1:].split("]")[0]
    return h.rsplit(":", 1)[0] if ":" in h else h


def is_loopback_netloc(netloc):
    return netloc_host(netloc).lower() in _LOOPBACK


def is_ip_literal_netloc(netloc):
    """裸 IP(v4 点四段 / v6 含冒号)= 不是集群 DNS 名。
    非 loopback 的裸 IP 也不合格:K8S 里 rec-agent 只应经 Service DNS 找 sasrec,
    直连 Pod IP 说明 SASREC_API_URL 被人手改成了具体地址(下次 rollout 就失效,不可复现)。"""
    h = netloc_host(netloc)
    return bool(_IPV4_RE.match(h)) or (":" in h)


def mannwhitney(a, b):
    """双侧 Mann-Whitney U,带 tie 校正的正态近似。**检验名与实现都钉死在本脚本里**,
    不依赖 scipy —— 否则结论会随 scipy 版本漂移(禁-3:阈值/口径必须自家可复算)。
    返回 (p, z, U1) 或 None(样本为空 / 方差为 0)。"""
    n1, n2 = len(a), len(b)
    if n1 == 0 or n2 == 0:
        return None
    allv = sorted([(float(v), 0) for v in a] + [(float(v), 1) for v in b])
    ranks = [0.0] * len(allv)
    i, tie = 0, 0.0
    while i < len(allv):
        j = i
        while j + 1 < len(allv) and allv[j + 1][0] == allv[i][0]:
            j += 1
        r = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[k] = r
        t = j - i + 1
        tie += t ** 3 - t
        i = j + 1
    r1 = sum(ranks[k] for k in range(len(allv)) if allv[k][1] == 0)
    u1 = r1 - n1 * (n1 + 1) / 2.0
    n = n1 + n2
    if n < 3:
        return None
    var = n1 * n2 / 12.0 * ((n + 1) - tie / float(n * (n - 1)))
    if var <= 0:
        return None
    z = (u1 - n1 * n2 / 2.0) / math.sqrt(var)
    return math.erfc(abs(z) / math.sqrt(2)), z, u1


def flat_paths(obj, prefix=""):
    """把 dict 摊成点路径集合(遇到 list/标量就停)。用于**从 REF 现算** journal 必需键,
    而不是把键名硬编码进脚本。"""
    out = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = prefix + ("." if prefix else "") + str(k)
            out.add(p)
            out |= flat_paths(v, p)
    return out


def get_path(obj, path):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None, False
        cur = cur[part]
    return cur, True


def norm_title(s):
    """标题归一化:脱 CSV/TSV 外层包裹引号 + 脱双写引号 + 压空白。
    ★两步都不可省,实测于参考树的权威表 shared/data/electronics.item:
      · 不脱双写引号 → `18""/48cm` vs `18"/48cm` 误报;
      · 不脱外层包裹引号 → 含引号/逗号的字段整体被 `"..."` 包住,再误报 3 处
        (Kindle Paperwhite / NEEWER Ring Light / Fire HD 10)。"""
    if s is None:
        return ""
    t = str(s).strip()
    if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
        t = t[1:-1]
    return re.sub(r"\s+", " ", t.replace('""', '"')).strip()


def title_matches(span_title, file_title):
    """候选标题 vs 权威表标题的比对。**两处已知假阳,都必须先归一化掉**(实测于参考树):
      (1) CSV 双写引号: `18""/48cm` vs `18"/48cm` —— norm_title 处理;
      (2) ★工具输出会把过长标题截断成 `...` 结尾(实测 46 个不同候选里 32 个被截),
          不做前缀比对会在参考树上误报 32 处"标题不符" —— 这是本任务第二次撞见
          "判据在自家参考树上就 FAIL"。
    """
    s, f = norm_title(span_title), norm_title(file_title)
    if s == f:
        return True
    for suf in ("...", u"…"):
        if s.endswith(suf):
            return f.startswith(s[:-len(suf)])
    return False


def is_placeholder(item_id, title):
    """占位符判据是**结构化**的:title 恰好等于 "Product_" + item_id。
    ★禁止用正则:参考树实测 `Product_\\d+` 命中 0(占位符 ASIN 含字母,100% 假阴);
      `Product_[A-Za-z0-9]{6,}` 命中 17362 条,其中 17334 条是 agent 名 `Product_Analyzer`
      (99.8% 假阳)。见规范 §5.2。"""
    return norm_title(title) == ("Product_" + str(item_id))


# ==========================================================================
# A 组 —— 一键可复现 / 采集编排(用户方向 ① ③)
# ==========================================================================
@check("A1", "A", RECOLLECT, "csv", EV_CROSS, "PASS",
       "CSV 表头:REF 的列按原序原位在前缀,新增列只许追加在尾部")
def chk_A1(c):
    ref_cols, cols = c.REF.cols, c.T.cols
    n = len(ref_cols)
    prefix_ok = cols[:n] == ref_cols
    extra = cols[n:]
    known = _backend_extra_cols()
    nums = {"n_cols": len(cols), "n_ref_cols": n, "extra": extra,
            "backend_declared_extra": known, "prefix_identical": prefix_ok}
    if not prefix_ok:
        first = next((i for i in range(min(len(cols), n)) if cols[i] != ref_cols[i]), n)
        return R.bad("前 %d 列与 REF 不一致(首个分歧在第 %d 列: %r vs REF %r)—— "
                     "新旧批次不可直接对比,四套 eval 全要改" %
                     (n, first, cols[first] if first < len(cols) else None,
                      ref_cols[first] if first < n else None), **nums)
    if known is None:
        if extra:
            return R.warn("尾部新增 %d 列 %s,但 backends.extra_csv_columns() 读不到(无法核对来源)"
                          % (len(extra), extra), **nums)
        return R.ok("列集与 REF 逐字节一致(%d 列,无新增)" % len(cols), **nums)
    if set(extra) - set(known):
        return R.bad("尾部出现 backend 未声明的新增列 %s(声明的是 %s)"
                     % (sorted(set(extra) - set(known)), known), **nums)
    return R.ok("前 %d 列与 REF 逐字节同序,尾部新增 %d 列且全部来自 backend 声明 %s"
                % (n, len(extra), extra), **nums)


def _backend_extra_cols():
    """从 backends.py 现读 K8sBackend.extra_csv_columns() 的返回列表(不 import,避免拖起
    kubectl/网络依赖;也不硬编码列名 —— 该文件正在被另一条工作流并发修改)。"""
    p = os.path.join(REPO, "scripts", "chaos", "agentfault", "collect", "backends.py")
    try:
        src = io.open(p, "r", encoding="utf-8", errors="replace").read()
    except Exception:
        return None
    best = None
    for m in re.finditer(r"def extra_csv_columns\(self\):(.{0,1600}?)return\s*(\[[^\]]*\])",
                         src, re.S):
        try:
            lst = json.loads(m.group(2).replace("'", '"'))
        except Exception:
            continue
        if lst and (best is None or len(lst) > len(best)):
            best = lst
    return best


@check("A2", "A", RECOLLECT, "csv+journal+raw", EV_CROSS, "PASS",
       "覆盖度只认数据面:CSV / journal/ / raw/ 三处 case_id 集合恒等,且 == REF 的 combo×rep")
def chk_A2(c):
    """★不读 run_summary.json。参考树的 run_summary 只记 1 个 combo(会话级、被覆盖写),
    任何以它判覆盖度的闸在参考树上就先自爆 —— 见 A5。"""
    ref_reps = c.REF.reps_per_combo()
    expect = set()
    for cid, n in ref_reps.items():
        for i in range(1, n + 1):
            expect.add("%s__r%d" % (cid, i))
    got_csv, got_jr, got_raw = c.T.case_ids(), c.T.journal_ids(), c.T.raw_ids()
    nums = {"n_expect": len(expect), "n_csv": len(got_csv), "n_journal": len(got_jr),
            "n_raw": len(got_raw),
            "csv_missing": sorted(expect - got_csv)[:8],
            "csv_extra": sorted(got_csv - expect)[:8],
            "journal_vs_csv_diff": sorted(got_jr ^ got_csv)[:8],
            "raw_vs_csv_diff": sorted(got_raw ^ got_csv)[:8],
            "ref_reps_per_combo": dict(ref_reps)}
    bad = []
    if got_csv != expect:
        bad.append("CSV 少 %d / 多 %d" % (len(expect - got_csv), len(got_csv - expect)))
    if got_jr != got_csv:
        bad.append("journal 与 CSV 差 %d" % len(got_jr ^ got_csv))
    if got_raw != got_csv:
        bad.append("raw 与 CSV 差 %d" % len(got_raw ^ got_csv))
    if bad:
        return R.bad("case_id 三处笛卡尔积不恒等: %s(删行/漏采/假 journal 补数都会在这里现形)"
                     % "; ".join(bad), **nums)
    return R.ok("%d 个 case_id 在 CSV/journal/raw 三处逐一恒等,且 == REF 的 combo×rep 笛卡尔积"
                % len(expect), **nums)


@check("A3", "A", RECOLLECT, "journal+csv", EV_CROSS, "PASS",
       "journal 全部可解析 + 键路径覆盖 REF 公共集 + case_id/trace_id 与 CSV 自洽")
def chk_A3(c):
    """resume 门是 `os.path.exists(journal_path)` —— `touch journal/<case>.json` 就能让
    runner 全 skip、连实例都不起、退出 0。本闸在验收侧把"文件存在"升级成"内容自洽"。"""
    ref_j = c.REF.journals()
    need = None
    for j in ref_j.values():
        if "__parse_error__" in j:
            continue
        p = flat_paths(j)
        need = p if need is None else (need & p)
    if not need:
        return R.bad("REF 的 journal 公共键路径算不出(参考树本身坏了),阈值无依据")
    jr = c.T.journals()
    by_run = {r.get("run_id"): r for r in c.rows}
    parse_err, missing_key, mismatch = [], [], []
    for cid, j in sorted(jr.items()):
        if "__parse_error__" in j:
            parse_err.append((cid, j["__parse_error__"][:60]))
            continue
        miss = sorted(k for k in need if not get_path(j, k)[1])
        if miss:
            missing_key.append((cid, miss[:4]))
        if j.get("case_id") != cid:
            mismatch.append((cid, "journal.case_id=%r != 文件名" % j.get("case_id")))
        row = by_run.get(cid)
        if row is None:
            mismatch.append((cid, "CSV 无同名 run_id"))
        elif (j.get("trace_id") or "") != (row.get("trace_id") or ""):
            mismatch.append((cid, "trace_id 与 CSV 不一致"))
    nums = {"n_journal": len(jr), "n_required_paths": len(need),
            "parse_error": parse_err[:6], "missing_key": missing_key[:6],
            "mismatch": mismatch[:6],
            "required_paths_sample": sorted(need)[:12]}
    if parse_err or missing_key or mismatch:
        return R.bad("journal 自洽性破裂: 解析失败 %d / 缺键 %d / 与 CSV 不符 %d"
                     % (len(parse_err), len(missing_key), len(mismatch)), **nums)
    return R.ok("%d 个 journal 全部可解析,%d 条 REF 公共键路径齐全,case_id/trace_id 与 CSV 逐条自洽"
                % (len(jr), len(need)), **nums)


@check("A4", "A", RELEASE, "harness", EV_THIRD, "PASS",
       "一键性:沿用同一个采集器 —— runner --list 的 combo 集合 == REF 的 group_id 集合")
def chk_A4(c):
    """这是"不许另起并行采集器"的机器判据。同时顺带证明故障规格是**声明式数据**
    (可 dump、可比对),而不是散在代码里的 if-else。"""
    runner = os.path.join(REPO, "scripts", "chaos", "agentfault", "collect", "agentfault_runner.py")
    sh = os.path.join(REPO, "scripts", "chaos", "agentfault", "run_collect_agentfault.sh")
    nums = {"runner": runner, "wrapper_exists": os.path.isfile(sh)}
    if not os.path.isfile(runner):
        return R.bad("采集器不存在: %s" % runner, **nums)
    try:
        p = subprocess.run([c.python, runner, "--list"], capture_output=True, timeout=180,
                           cwd=REPO)
    except Exception as e:  # noqa: BLE001
        return R.bad("runner --list 跑不起来: %r" % e, **nums)
    out = (p.stdout or b"").decode("utf-8", "replace") + (p.stderr or b"").decode("utf-8", "replace")
    ref_combos = set(c.REF.combos)
    listed = {t for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", out)} & ref_combos
    nums.update({"rc": p.returncode, "ref_combos": sorted(ref_combos),
                 "listed_hit": sorted(listed), "out_head": out[:400]})
    if p.returncode != 0:
        return R.bad("runner --list 退出码 %d(一键性不成立)" % p.returncode, **nums)
    if not os.path.isfile(sh):
        return R.bad("一键封装 run_collect_agentfault.sh 不存在", **nums)
    if listed != ref_combos:
        return R.bad("--list 的 combo 集合与 REF 的 group_id 不等(缺 %s)"
                     % sorted(ref_combos - listed)[:6], **nums)
    return R.ok("--list 列出的 %d 个 combo 与 REF group_id 逐字相等,一键封装存在"
                % len(ref_combos), **nums)


@check("A5", "A", DISCLOSE, "run_summary", EV_SELF, "WARN",
       "run_summary.json 只作旁证:它是会话级覆盖写的,覆盖度不得由它判")
def chk_A5(c):
    p = os.path.join(c.tree, "run_summary.json")
    if not os.path.isfile(p):
        return R.warn("无 run_summary.json(不阻塞:覆盖度由 A2 的数据面笛卡尔积判)", path=p)
    try:
        s = json.load(_open(p))
    except Exception as e:  # noqa: BLE001
        return R.bad("run_summary.json 解析失败: %r" % e, path=p)
    listed = [x.get("combo") for x in (s.get("combos") or [])]
    nums = {"combos_in_summary": listed, "n_in_summary": len(listed),
            "n_combos_in_tree": len(c.T.combos), "out_dir": s.get("out_dir"),
            "warmup": s.get("warmup"), "runs": s.get("runs")}
    if len(listed) < len(c.T.combos):
        return R.warn("run_summary 只记了 %d/%d 个 combo(会话级覆盖写,断点续采后必然不全)"
                      " —— 这是**正常态**,必须在 SUMMARY 写明它不是全树账"
                      % (len(listed), len(c.T.combos)), **nums)
    return R.ok("run_summary 记满 %d 个 combo(仍只作旁证)" % len(listed), **nums)


@check("A6", "A", RELEASE, "provenance", EV_SELF, "FAIL",
       "逃生开关落痕:四个 --k8s-allow-*/--skip-preflight/--allow-mixed-tree 必须全 false")
def chk_A6(c):
    """★BLOCKED-BY 采集侧改动(规范 §8-P1)。runner 现在**不记 argv、不记逃生开关、不记
    preflight 结果**,`--only 1 个 combo` / `--runs 3` / `--warmup 0` / `--skip-preflight`
    全部退出 0 且不留痕。REF 上按构造 FAIL。FALLBACK = A7。"""
    cands = [os.path.join(c.tree, "provenance", "invocations.json"),
             os.path.join(c.tree, "provenance.json")]
    got = [p for p in cands if os.path.isfile(p)]
    if not got:
        return R.bad("无采集调用留痕(找过 %s)—— 无法证明没开逃生开关;"
                     "FALLBACK 见 A7(退化兜底,挡不住 --warmup 0 且卷正常的情形)"
                     % [_rel(x) for x in cands],
                     searched=[_rel(x) for x in cands])
    try:
        obj = json.load(_open(got[0]))
    except Exception as e:  # noqa: BLE001
        return R.bad("留痕文件解析失败: %r" % e, path=got[0])

    # ---- ★2026-07-27 补的证据等级闸(原判据的盲点)-------------------------------
    # 原实现只读 invocations 里的字段,**不问这文件是谁写的**:采集完人工补一份
    # {"invocations":[{...全 false...}]} 就能让 A6 从 FAIL 翻 PASS,而一条新证据都没有。
    # 那正是规范 §1.5「空闸黑名单」拉黑的形态 —— 一条翻不动/可自证的闸等于没有。
    # 修法:强制申报 evidence_class,且只认两类;`post_hoc_reconstruction` 显式判 FAIL。
    #   runner_emitted            采集器自己在采集时写的(最强;需 §8-P1 落地)
    #   contemporaneous_run_log   采集当时的运行日志(第三方=shell 写的),且日志必须随树同发
    #                             —— 此类**不信 JSON 自陈**,回到日志原文里重新验一遍开关
    A6_OK_CLASSES = ("runner_emitted", "contemporaneous_run_log")
    ecls = (obj.get("evidence_class") if isinstance(obj, dict) else None) or ""
    if ecls not in A6_OK_CLASSES:
        return R.bad(
            "留痕文件的 evidence_class=%r 不在可采信集合 %s 内 —— "
            "采后人工补写的留痕(post_hoc_reconstruction)证明不了任何事,不予采信"
            % (ecls or "<缺该字段>", list(A6_OK_CLASSES)),
            path=_rel(got[0]), evidence_class=ecls or None)

    # 逃生开关的命令行拼法:runner 侧带 k8s- 前缀,一键脚本侧不带,两种都查
    ESCAPE_CLI = ("--skip-preflight", "--allow-mixed-tree",
                  "--k8s-skip-code-parity", "--skip-code-parity",
                  "--k8s-allow-inject-residue", "--allow-inject-residue")
    log_audit = None
    if ecls == "contemporaneous_run_log":
        rel = (obj.get("run_log") or "").strip()
        if not rel:
            return R.bad("evidence_class=contemporaneous_run_log 但没给 run_log 路径 —— "
                         "无法回到原始日志复核", path=_rel(got[0]))
        lp = rel if os.path.isabs(rel) else os.path.join(c.tree, rel)
        if not os.path.isfile(lp):
            return R.bad("run_log 指向的日志不存在: %s —— 日志必须随树同发,否则该证据不可复核"
                         % rel, run_log=rel)
        try:
            txt = _open(lp).read()
        except Exception as e:  # noqa: BLE001
            return R.bad("run_log 读取失败: %r" % e, run_log=rel)
        argv_lines = [ln.strip() for ln in txt.splitlines()
                      if "agentfault_runner.py" in ln]
        if not argv_lines:
            return R.bad("run_log 里找不到 runner 的 argv 行(应含 'agentfault_runner.py')"
                         " —— 该日志不能作为调用留痕", run_log=rel)
        # ★不信 JSON,直接在日志原文里查开关
        hits = sorted({f for ln in argv_lines for f in ESCAPE_CLI if f in ln})
        if hits:
            return R.bad("run_log 的 argv 里出现逃生开关 %s —— 该批数据的 preflight 保证不成立"
                         % hits, run_log=rel, argv_lines=len(argv_lines))
        if any("--warmup 0" in ln or "--warmup=0" in ln for ln in argv_lines):
            return R.bad("run_log 的 argv 里 --warmup 0 —— 关掉了『span 真的在写』那道硬闸",
                         run_log=rel)
        log_audit = {"run_log": rel, "argv_lines": len(argv_lines),
                     "escape_flags_in_log": []}
    # --------------------------------------------------------------------------

    invs = obj.get("invocations") if isinstance(obj, dict) else obj
    if not isinstance(invs, list) or not invs:
        return R.bad("留痕文件里没有 invocations 列表", path=got[0])
    flags = ["skip_preflight", "k8s_skip_code_parity", "k8s_allow_inject_residue",
             "allow_mixed_tree"]
    on = [(i, f) for i, inv in enumerate(invs) for f in flags if inv.get(f)]
    warm = [inv.get("warmup") for inv in invs]
    covered = set()
    for inv in invs:
        covered |= set(inv.get("combos") or [])
    nums = {"n_invocations": len(invs), "escape_flags_on": on, "warmup": warm,
            "combos_covered": sorted(covered), "ref_combos": c.REF.combos,
            "evidence_class": ecls, "log_audit": log_audit}
    if on:
        return R.bad("采集时开了逃生开关 %s —— 该批数据的 preflight 保证不成立" % on[:6], **nums)
    if any((w or 0) < 1 for w in warm):
        return R.bad("有调用 warmup<1 —— 关掉了『span 真的在写』那道硬闸", **nums)
    if set(covered) != set(c.REF.combos):
        return R.bad("留痕的 combo 并集 != REF combo 集(缺 %s)"
                     % sorted(set(c.REF.combos) - covered)[:6], **nums)
    return R.ok("%d 次调用全部零逃生开关、warmup>=1、combo 并集覆盖全集(证据等级 %s%s)"
                % (len(invs), ecls,
                   "，已回日志原文复核" if log_audit else ""), **nums)


@check("A7", "A", RECOLLECT, "csv", EV_CROSS, "PASS",
       "A6 的退化兜底:每行 total_span_count >= REF 最小值(挡『卷没挂 / span 全空』)")
def chk_A7(c):
    ref_v = [fnum(r.get("total_span_count")) for r in c.ref_rows]
    ref_v = [v for v in ref_v if isinstance(v, float)]
    if not ref_v:
        return R.bad("REF 的 total_span_count 算不出,阈值无依据")
    floor = min(ref_v)
    bad = []
    for r in c.rows:
        v = fnum(r.get("total_span_count"))
        if not isinstance(v, float) or v < floor:
            bad.append((r.get("run_id"), r.get("total_span_count")))
    nums = {"ref_min": floor, "ref_max": max(ref_v), "n_below": len(bad), "sample": bad[:8]}
    if bad:
        return R.bad("%d 行 total_span_count < REF 下界 %g" % (len(bad), floor), **nums)
    return R.ok("全部 %d 行 total_span_count >= REF 下界 %g" % (len(c.rows), floor), **nums)


@check("A8", "A", RELEASE, "harness", EV_THIRD, "SKIP",
       "幂等重跑:同一条命令二次执行必须零新增行(resume 门有效)")
def chk_A8(c):
    log = c.args.rerun_log
    if not log:
        return R.skip("未提供 --rerun-log(缺证据 = 未验收,不是通过)")
    p = log if os.path.isabs(log) else os.path.join(REPO, log)
    if not os.path.isfile(p):
        return R.bad("--rerun-log 不存在: %s" % p)
    txt = read_text(p)
    n_skip = len(re.findall(r"skip", txt, re.I))
    nums = {"log": p, "bytes": len(txt), "skip_mentions": n_skip,
            "post_sha_given": bool(c.args.rerun_post_sha)}
    if c.args.rerun_post_sha:
        cur = sha256_file(c.T.csv_path)
        nums["csv_sha256"] = cur
        if cur.lower() != c.args.rerun_post_sha.strip().lower():
            return R.bad("重跑后 CSV sha256 与 --rerun-post-sha 不符(重跑写入了新行)", **nums)
        return R.ok("重跑后 CSV sha256 与声明一致 = 零新增行", **nums)
    if n_skip <= 0:
        return R.bad("重跑日志里一次 skip 都没有 —— resume 门可能没生效", **nums)
    return R.warn("重跑日志有 %d 处 skip,但未给 --rerun-post-sha(无法证明 CSV 零增行)"
                  % n_skip, **nums)


# ==========================================================================
# B 组 —— Agent 真实商品语义(用户方向 ②)
# ==========================================================================
@check("B1", "B", RECOLLECT, "spans+csv", EV_CROSS, "PASS",
       "分母不许为 0:每个 case 的候选条数 >= REF 下界")
def chk_B1(c):
    """★这是方向 ② 最关键的一条,也是"只补代码更糟"那条警告的直接闸。
    `_filter_real_title` 的规则是"不在 title cache 里的候选一律剔除";cache 空 →
    候选全被滤光 → 推荐变空列表,而工具**不抛异常、不返错误串**。此时任何
    "占位符率 <= x%" 的闸都会因为 0/0 而 PASS —— 比不修更糟。"""
    ref_t, t = c.REF.tools(), c.T.tools()
    ref_per = [len(ref_t["cand"].get(r.get("trace_id"), [])) for r in c.ref_rows]
    if not ref_per or max(ref_per) == 0:
        return R.bad("REF 的候选侧解析不出(工具输出格式变了?),阈值无依据",
                     ref_cand_calls=ref_t["cand_calls"])
    floor = min(ref_per)
    bad = [(r.get("run_id"), len(t["cand"].get(r.get("trace_id"), []))) for r in c.rows
           if len(t["cand"].get(r.get("trace_id"), [])) < floor]
    nums = {"ref_floor": floor, "ref_dist": dict(collections.Counter(ref_per)),
            "dist": dict(collections.Counter(len(t["cand"].get(r.get("trace_id"), []))
                                             for r in c.rows)),
            "cand_calls": t["cand_calls"], "cand_unparsed": t["cand_unparsed"],
            "n_below": len(bad), "sample": bad[:8]}
    if t["cand_unparsed"]:
        return R.bad("%d 次候选工具调用的输出解析不出(格式变了或被截断)—— 解析不出 != 通过"
                     % t["cand_unparsed"], **nums)
    if bad:
        return R.bad("%d 个 case 的候选条数 < REF 下界 %d(0 = title cache 空 → 候选被滤光)"
                     % (len(bad), floor), **nums)
    return R.ok("%d/%d case 候选条数 >= REF 下界 %d" % (len(c.rows), len(c.rows), floor), **nums)


@check("B2", "B", RECOLLECT, "spans", EV_CROSS, "PASS",
       "候选侧占位符:结构化判据 title == 'Product_'+item_id,阈值现算自 REF")
def chk_B2(c):
    ref_t, t = c.REF.tools(), c.T.tools()
    ref_ph = sum(1 for lst in ref_t["cand"].values() for i, ti in lst if is_placeholder(i, ti))
    ph = [(i, ti) for lst in t["cand"].values() for i, ti in lst if is_placeholder(i, ti)]
    slots = sum(len(v) for v in t["cand"].values())
    ref_slots = sum(len(v) for v in ref_t["cand"].values())
    nums = {"ref_placeholder": ref_ph, "ref_slots": ref_slots,
            "placeholder": len(ph), "slots": slots, "sample": ph[:6]}
    if len(ph) > ref_ph:
        return R.bad("候选侧占位符 %d > REF 的 %d(候选侧过滤 _filter_real_title 失效)"
                     % (len(ph), ref_ph), **nums)
    return R.ok("候选侧 %d 槽,占位符 %d(REF %d/%d)" % (slots, len(ph), ref_ph, ref_slots), **nums)


@check("B3", "B", RECOLLECT, "spans", EV_CROSS, "PASS",
       "统计充分性:候选槽数够大 + 占位符 0 才能断言『过滤器确实执行过』")
def chk_B3(c):
    """先验占位符率 26.05%(shared/data/electronics.item 1,946,169 条里 26.1% 是 Product_<ASIN>)。
    无过滤时 N 槽全干净的概率 ~ 0.7395^N。REF 的 636 槽 → 1e-84。
    槽数不足时不许用小样本自证 —— 判 FAIL(不是 PASS)。"""
    ref_t, t = c.REF.tools(), c.T.tools()
    ref_slots = sum(len(v) for v in ref_t["cand"].values())
    slots = sum(len(v) for v in t["cand"].values())
    ph = sum(1 for lst in t["cand"].values() for i, ti in lst if is_placeholder(i, ti))
    # 无过滤零假设下的 p(纯报数,门槛是槽数不是 p)
    prior_clean = 1.0 - 0.2605
    p_null = prior_clean ** slots if slots and slots < 5000 else 0.0
    nums = {"slots": slots, "ref_slots": ref_slots, "placeholder": ph,
            "p_no_filter": ("%.3e" % p_null) if slots else None,
            "prior_placeholder_rate": 0.2605}
    if slots < ref_slots:
        return R.bad("候选槽数 %d < REF %d —— 样本不足以自证过滤器执行过(不许小样本自证)"
                     % (slots, ref_slots), **nums)
    if ph:
        return R.bad("候选槽 %d 个里有 %d 个占位符,过滤器未生效" % (slots, ph), **nums)
    return R.ok("%d 槽零占位符 ⇒ 无过滤零假设下 p≈%.1e,过滤器确实执行过" % (slots, p_null), **nums)


@check("B4", "B", DISCLOSE, "spans", EV_CROSS, "PASS",
       "历史侧独立算:载体池是离线筛好的,阈值与候选侧分开(合并会被历史侧噪声吸收)")
def chk_B4(c):
    """★候选侧与历史侧必须分开算。历史侧天然有少量『未知商品』打印(carrier 的 label 与
    部分 ASIN 不在 item 表里),这是正常态;合并成一个"占位符率"会让历史侧噪声吸收掉
    候选侧几十条退化。"""
    def stat(tree):
        t = tree.tools()
        unk = ph = tot = 0
        for lst in t["hist"].values():
            for i, ti in lst:
                tot += 1
                if is_placeholder(i, ti):
                    ph += 1
                if u"未知商品" in norm_title(ti):
                    unk += 1
        return {"slots": tot, "unknown": unk, "placeholder": ph, "calls": t["hist_calls"]}
    rs, s = stat(c.REF), stat(c.T)
    rate = (s["unknown"] + s["placeholder"]) / float(s["slots"]) if s["slots"] else None
    rrate = (rs["unknown"] + rs["placeholder"]) / float(rs["slots"]) if rs["slots"] else None
    nums = {"tree": s, "ref": rs, "rate": rate, "ref_rate": rrate}
    if not s["slots"]:
        return R.bad("历史侧解析不出(载体池没被用上?)", **nums)
    if rrate is not None and rate is not None and rate > rrate + 1e-9:
        return R.warn("历史侧劣化率 %.4f > REF %.4f —— 载体池或 item 表口径变了,必须披露"
                      % (rate, rrate), **nums)
    return R.ok("历史侧劣化率 %.4f <= REF %.4f" % (rate, rrate if rrate is not None else -1), **nums)


@check("B5", "B", RECOLLECT, "csv", EV_SELF, "PASS",
       "推荐没变空列表:recommended_product_is_unknown / conv_*_text_len 的直接体征")
def chk_B5(c):
    ref_unk = {r.get("recommended_product_is_unknown") for r in c.ref_rows}
    unk_bad = [r.get("run_id") for r in c.rows
               if r.get("recommended_product_is_unknown") not in ref_unk]
    conv_cols = [x for x in c.T.cols if re.match(r"^conv_.+_text_len$", x)]
    ref_floor = {}
    for col in conv_cols:
        v = [fnum(r.get(col)) for r in c.ref_rows]
        v = [x for x in v if isinstance(x, float)]
        ref_floor[col] = min(v) if v else None
    zero = []
    for r in c.rows:
        for col in conv_cols:
            v = fnum(r.get(col))
            if v is None or v == "NaN" or v <= 0:
                zero.append((r.get("run_id"), col, r.get(col)))
    nums = {"ref_unknown_values": sorted(ref_unk), "unknown_offenders": unk_bad[:8],
            "conv_cols": conv_cols, "ref_conv_min": ref_floor, "zero_len": zero[:8],
            "n_zero_len": len(zero)}
    if unk_bad:
        return R.bad("%d 行 recommended_product_is_unknown 取值超出 REF 取值域 %s"
                     % (len(unk_bad), sorted(ref_unk)), **nums)
    if zero:
        return R.bad("%d 处 conv_*_text_len <= 0 —— 该 agent 的会话为空(推荐变空列表的直接体征)"
                     % len(zero), **nums)
    return R.ok("recommended_product_is_unknown 全在 REF 取值域内;%d 个会话长度列全 > 0"
                % len(conv_cols), **nums)


@check("B6", "B", RELEASE, "csv", EV_CROSS, "PASS",
       "response_asin 分族空值率 <= REF 同族(★不许设『全非空』,REF 的 format 族本身就有空)")
def chk_B6(c):
    def rate(rows):
        d = collections.defaultdict(lambda: [0, 0])
        for r in rows:
            k = r.get("kind", "")
            d[k][1] += 1
            if not (r.get("response_asin") or "").strip():
                d[k][0] += 1
        return {k: (v[0], v[1], v[0] / float(v[1])) for k, v in d.items()}
    rr, tr = rate(c.ref_rows), rate(c.rows)
    bad = [(k, tr[k], rr.get(k)) for k in tr
           if k in rr and tr[k][2] > rr[k][2] + 1e-9]
    unknown = [k for k in tr if k not in rr]
    nums = {"ref": rr, "tree": tr, "worse": bad[:6], "kinds_not_in_ref": unknown}
    if unknown:
        return R.bad("出现 REF 没有的故障族 %s —— 跨批次不可比" % unknown, **nums)
    if bad:
        return R.bad("以下族的 response_asin 空值率高于 REF: %s" % bad[:4], **nums)
    return R.ok("各族 response_asin 空值率均 <= REF 同族水位", **nums)


@check("B7", "B", RELEASE, "spans+hostfile", EV_THIRD, "SKIP",
       "--with-item-file:候选 (id,title) 全部能在宿主权威 electronics.item 里对上")
def chk_B7(c):
    if not c.args.with_item_file:
        return R.skip("未加 --with-item-file(要扫 267MB 权威表,约 30-60s)")
    item = os.path.join(REPO, "shared", "data", "electronics.item")
    if not os.path.isfile(item):
        return R.bad("宿主权威表不存在: %s" % item)
    t = c.T.tools()
    want = {}
    for lst in t["cand"].values():
        for i, ti in lst:
            want.setdefault(i, norm_title(ti))
    found, mism = {}, []
    with io.open(item, "r", encoding="utf-8", errors="replace") as f:
        f.readline()                      # header
        for line in f:
            iid = line.split("\t", 1)[0]
            if iid in want:
                parts = line.rstrip("\n").split("\t")
                found[iid] = norm_title(parts[1] if len(parts) > 1 else "")
    n_trunc = 0
    for iid, ti in want.items():
        if ti.endswith("...") or ti.endswith(u"…"):
            n_trunc += 1
        if iid in found and not title_matches(ti, found[iid]):
            mism.append((iid, ti[:60], found[iid][:60]))
    missing = sorted(set(want) - set(found))
    nums = {"n_distinct_candidates": len(want), "n_found": len(found),
            "n_missing": len(missing), "missing": missing[:6],
            "n_truncated_titles": n_trunc,
            "n_title_mismatch": len(mism), "mismatch": mism[:4]}
    if missing:
        return R.bad("%d 个候选 item_id 不在权威表里(候选是编出来的?)" % len(missing), **nums)
    if mism:
        return R.bad("%d 个候选标题与权威表不符(归一化后仍不符)" % len(mism), **nums)
    return R.ok("%d 个不同候选全部在权威表中且标题逐字一致" % len(want), **nums)


@check("B8", "B", RECOLLECT, "live-k8s", EV_THIRD, "SKIP",
       "--live:pod 内 electronics.item / tools.py 的 sha256 == 宿主(体积闸挡不住填充文件)")
def chk_B8(c):
    if not c.args.live:
        return R.skip("未加 --live(采集期环境已被 restore 回 stock 时本条不可跑,见规范 §6.3)")
    ns, dep, ct = c.args.k8s_ns, c.args.k8s_deploy, c.args.k8s_container
    pairs = [("/app/shared/data/electronics.item",
              os.path.join(REPO, "shared", "data", "electronics.item")),
             ("/app/services/recommendation_agent/agents/tools.py",
              os.path.join(REPO, "services", "recommendation_agent", "agents", "tools.py"))]
    out, bad = {}, []
    for inpod, host in pairs:
        rc, so, se = c.kube(["exec", "-n", ns, "deploy/" + dep, "-c", ct, "--",
                             "sha256sum", inpod], timeout=300)
        pod_sha = (so or "").strip().split()[0] if rc == 0 and so.strip() else None
        host_sha = sha256_file(host) if os.path.isfile(host) else None
        out[inpod] = {"pod": pod_sha, "host": host_sha, "rc": rc, "err": se[:120]}
        if not pod_sha or not host_sha or pod_sha != host_sha:
            bad.append(inpod)
    if bad:
        return R.bad("pod 内文件与宿主 sha256 不符/取不到: %s(镜像没重建 或 PVC 灌的是填充文件)"
                     % bad, **out)
    return R.ok("pod 内 electronics.item 与 tools.py 的 sha256 均 == 宿主", **out)


@check("B9", "B", RECOLLECT, "spans", EV_CROSS, "PASS",
       "★候选必须真有语义:不许是 tools.py:126 的兜底串『未知商品』")
def chk_B9(c):
    """2026-07-27 新增。补的是 B 组一个**实证过的洞**。

    实测:B 档前两轮的候选面 46 个 distinct 候选**全是"未知商品"**(v2 反而是真标题),
    而 **B1/B2/B3 三道闸全 PASS** —— 因为:
      · B1 只数候选条数(条数是够的);
      · B2/B3 的占位符判据是结构化的 `title == "Product_" + item_id`,
        而 "未知商品" **不长这样**,于是整整一批"零语义候选"从三道闸底下走过去了。
    B7 其实抓到了(报"46 个标题与权威表不符"),但它是 RELEASE 级、要 --with-item-file
    扫 267MB,且失败信息把人往"截断/归一化"上引(本任务确实被引偏过一次)。

    ⇒ 本闸是**纯离线、零参数、结构化**的直判:出现兜底串就是候选侧没有语义。
    判 RECOLLECT 级:这是数据的物理性质(agent 当时确实没看到商品语义),改文档无效。

    根因备查(修法不在 rec-agent):tools.py:126 `title = rec.get("title") or "未知商品"`
    取的是 **sasrec 响应里的 title**;sasrec 要挂 electronics.item 才有 item_info。
    K8S 的 sasrec pod 没挂 => title=None => 落到兜底串。
    修:scripts/chaos/agentfault/k8s/patch_sasrec_itemfile.ps1
    (★别改 tools.py 用本地 cache 兜底:那会把口径从 [:77]+'...' 变成 [:80] 硬截断,
      与 v2 逐字不一致 => 反而把 B7 弄成真 FAIL。)
    """
    FALLBACK = "未知商品"      # tools.py:126 的兜底串
    t = c.T.tools()
    bad_tr, uniq_bad = [], set()
    n_pair = 0
    for tr, lst in t["cand"].items():
        hit = 0
        for iid, ti in lst:
            n_pair += 1
            if norm_title(ti) == FALLBACK:
                hit += 1
                uniq_bad.add(iid)
        if hit:
            bad_tr.append((tr[:16], hit))
    # 顺带把 hist 面报出来(只报不判:v2 的 hist 本来就有 26 个兜底串,
    # 判它会在参考树上误报 —— 那是 v2 的既有性质,不是本批次的问题)
    hist_bad = sum(1 for lst in t["hist"].values() for (_i, x) in lst
                   if norm_title(x) == FALLBACK)
    nums = {"n_cand_pairs": n_pair, "n_cand_calls": t.get("cand_calls"),
            "n_traces_with_fallback": len(bad_tr),
            "n_distinct_items_with_fallback": len(uniq_bad),
            "sample": bad_tr[:5], "hist_fallback_pairs_INFO_ONLY": hist_bad}
    if n_pair == 0:
        return R.bad("候选面一条 (id,title) 都没抽到 —— 抽取口径或 span 面有问题", **nums)
    if bad_tr:
        return R.bad("★候选侧出现兜底串『未知商品』:%d/%d 条 (id,title) 落在 %d 个 trace 上,"
                     "%d 个 distinct item —— agent 当时看不到任何商品语义。"
                     "根因在 sasrec 没挂 electronics.item,修:"
                     "k8s/patch_sasrec_itemfile.ps1(勿改 tools.py 兜底,见本函数 docstring)"
                     % (sum(h for _tr, h in bad_tr), n_pair, len(bad_tr), len(uniq_bad)),
                     **nums)
    return R.ok("候选侧 %d 条 (id,title) 全部有真实标题(零兜底串)" % n_pair, **nums)


# ==========================================================================
# C 组 —— GT 完整性(本次最危险的静默失败模式)
# ==========================================================================
@check("C1", "C", RECOLLECT, "csv", EV_CROSS, "PASS",
       "★GT 守恒:每 combo 的行数与 faulted 数逐一 == REF(删行 / 整树降级都抓)")
def chk_C1(c):
    """两个静默失败模式在这里同时现形:
      (a) 把 inject_failed/no_ledger_match 的坏行从 append-only CSV 剥掉 → 残留率归零;
      (b) 台账随 pod 重建丢失 → 96 个 faulted 全变 injected=0,而"无坏行残留"仍为真
          (分子分母同时坍缩,比率类闸恒真)。
    per-combo 的**绝对数**对这两种坍缩都不免疫。"""
    ref_n = collections.Counter(r.get("group_id") for r in c.ref_rows)
    ref_f = collections.Counter(r.get("group_id") for r in c.ref_rows if r.get("injected") == "1")
    n = collections.Counter(r.get("group_id") for r in c.rows)
    f = collections.Counter(r.get("group_id") for r in c.rows if r.get("injected") == "1")
    bad, detail = [], {}
    for cid in sorted(set(ref_n) | set(n)):
        detail[cid] = {"rows": n.get(cid, 0), "ref_rows": ref_n.get(cid, 0),
                       "faulted": f.get(cid, 0), "ref_faulted": ref_f.get(cid, 0)}
        if n.get(cid, 0) != ref_n.get(cid, 0):
            bad.append((cid, "rows %d != REF %d" % (n.get(cid, 0), ref_n.get(cid, 0))))
        if f.get(cid, 0) != ref_f.get(cid, 0):
            bad.append((cid, "faulted %d != REF %d" % (f.get(cid, 0), ref_f.get(cid, 0))))
    nums = {"per_combo": detail, "n_faulted": len(c.T.faulted),
            "n_ref_faulted": len(c.REF.faulted)}
    if bad:
        return R.bad("GT 守恒破裂: %s" % bad[:6], **nums)
    return R.ok("%d 个 combo 的行数与 faulted 数与 REF 逐一相等(faulted 合计 %d)"
                % (len(detail), len(c.T.faulted)), **nums)


@check("C2", "C", DISCLOSE, "csv+ledger", EV_CROSS, "WARN",
       "盘上台账 trace 命中率按 combo 分档 >= REF —— 且**必须披露**它不是 1.0")
def chk_C2(c):
    """★上游勘察原判据是"命中率恰为 1.0,1 条 miss 即 FAIL"。在参考树上现算:96 个 faulted
    行有 11 行查不到台账 trace,全部来自 format_Recommendation_Synthesizer
    (per-rep instance → reset_ledger → 盘上只剩最后一 rep,该 combo 台账 1 行)。
    **该判据在参考基线自身就 FAIL**,照抄进规范就是第二次"提前按理想写"。

    ⚠️ 但"按 REF 现算"本身有毒:它会把 REF 的**已知缺陷**当成容忍带,等于给
    "台账丢 → GT 恒 no_ledger_match"这个本次头号失败模式发通行证。所以本条:
      · 降级为 DISCLOSE(不是 BLOCK),
      · 真正的闸是 C1(绝对数守恒)与 C3(journal 内嵌 matched,可事后复算),
      · 且无论 PASS 与否,只要有 combo < 1.0 就必须在 SUMMARY 机器可读披露(G3 兜底)。"""
    def hit(tree):
        led = tree.ledgers()
        d = {}
        for r in tree.rows:
            if r.get("injected") != "1":
                continue
            cid = r.get("group_id")
            a = d.setdefault(cid, [0, 0])
            a[1] += 1
            if r.get("trace_id") in led.get(cid, {}).get("traces", set()):
                a[0] += 1
        return {k: (v[0], v[1], v[0] / float(v[1]) if v[1] else None) for k, v in d.items()}
    rh, th = hit(c.REF), hit(c.T)
    bad = [(k, th[k], rh.get(k)) for k in th if k in rh and th[k][2] < rh[k][2] - 1e-9]
    below1 = {k: v for k, v in th.items() if v[2] is not None and v[2] < 1.0}
    nums = {"ref": rh, "tree": th, "below_ref": bad[:6], "below_1.0": below1}
    if bad:
        return R.bad("以下 combo 台账命中率低于 REF 同档: %s" % bad[:4], **nums)
    if below1:
        return R.warn("台账命中率 < 1.0 的 combo: %s —— per-rep instance 的 reset_ledger 会截断,"
                      "必须在 SUMMARY 写明『可复算证据在 journal(C3)不在盘上台账』"
                      % {k: "%d/%d" % (v[0], v[1]) for k, v in below1.items()}, **nums)
    return R.ok("各 combo 台账 trace 命中率均 >= REF 同档且 == 1.0", **nums)


@check("C3", "C", RELEASE, "journal", EV_CROSS, "FAIL",
       "★GT 事后可复算:journal 内嵌 matched 台账条目,逐条与 combo/trace 自洽")
def chk_C3(c):
    """★BLOCKED-BY 采集侧改动(规范 §8-P2):`_determine_gt` 在内存里就有 `gt["matched"]`,
    但 `write_raw_journal` 只落了派生结论(source/ledger_status),原始条目没进 journal。
    ⇒ 参考树 108 个 journal **0 个**带 matched,那 11 行台账已丢的 GT 现在事后不可证伪。
    journal 不是 CSV,加字段**零 schema 风险**,与"沿用现采集器"不冲突。REF 按构造 FAIL。"""
    jr = c.T.journals()
    by_run = {r.get("run_id"): r for r in c.rows}
    have, miss, incon = 0, [], []
    for cid, j in sorted(jr.items()):
        row = by_run.get(cid)
        if row is None or row.get("injected") != "1":
            continue
        m, ok = get_path(j, "ground_truth.matched")
        if not ok or not isinstance(m, list) or not m:
            miss.append(cid)
            continue
        have += 1
        for e in m:
            if e.get("trace_id") != row.get("trace_id"):
                incon.append((cid, "matched.trace_id != CSV trace_id"))
            if e.get("kind") and e.get("kind") != row.get("kind"):
                incon.append((cid, "matched.kind=%r != CSV kind=%r" % (e.get("kind"), row.get("kind"))))
    nums = {"n_faulted": len(c.T.faulted), "n_with_matched": have,
            "n_missing": len(miss), "missing_sample": miss[:6], "inconsistent": incon[:6]}
    if miss:
        return R.bad("%d/%d 个 faulted case 的 journal 没有 ground_truth.matched —— "
                     "GT 事后不可复算(采集侧未落地,见规范 §8-P2)"
                     % (len(miss), len(c.T.faulted)), **nums)
    if incon:
        return R.bad("matched 与 CSV 不自洽 %d 处: %s" % (len(incon), incon[:4]), **nums)
    return R.ok("%d 个 faulted case 的 journal 全部内嵌 matched 且与 CSV 自洽" % have, **nums)


@check("C4", "C", RECOLLECT, "csv+ledger", EV_CROSS, "PASS",
       "零根臂纯净:零根 combo 的台账必须 0 行(有行 = 注入串到对照组,整组作废)")
def chk_C4(c):
    led = c.T.ledgers()
    zero_combos = sorted({r.get("group_id") for r in c.T.zero_root})
    ref_zero = sorted({r.get("group_id") for r in c.REF.zero_root})
    dirty = {k: led[k]["n_lines"] for k in zero_combos if led.get(k, {}).get("n_lines", 0) > 0}
    nums = {"zero_root_combos": zero_combos, "ref_zero_root_combos": ref_zero,
            "ledger_lines": {k: led.get(k, {}).get("n_lines", 0) for k in zero_combos}}
    if set(zero_combos) != set(ref_zero):
        return R.bad("零根臂 combo 集与 REF 不同(%s vs %s)" % (zero_combos, ref_zero), **nums)
    if dirty:
        return R.bad("零根臂出现台账行 %s —— 对照组被污染" % dirty, **nums)
    return R.ok("零根臂 %s 台账 0 行" % zero_combos, **nums)


@check("C5", "C", RECOLLECT, "csv", EV_SELF, "PASS",
       "kind × injected 交叉表 + note 列:任何 faulted 族的 injected=0 行 / 任何非空 note 都 FAIL")
def chk_C5(c):
    """台账丢 / 注入未落地时,runner 把该 rep 写成 injected=0 + note=no_ledger_match 的**负行**,
    CSV 行数照样齐、每行看着都正常。这是"静默为空 = FAIL"点名的形状。
    边界:零根族(REF 里 injected=0 的那个 kind)的 injected=0 是**正确态**,不计。"""
    ref_zero_kinds = {r.get("kind") for r in c.REF.zero_root}
    ref_status = {r.get("ledger_status") for r in c.ref_rows}
    bad_inj = [(r.get("run_id"), r.get("kind")) for r in c.rows
               if r.get("kind") not in ref_zero_kinds and r.get("injected") != "1"]
    bad_note = [(r.get("run_id"), (r.get("note") or "")[:40]) for r in c.rows
                if (r.get("note") or "").strip()]
    bad_st = [(r.get("run_id"), r.get("ledger_status")) for r in c.rows
              if r.get("ledger_status") not in ref_status]
    nums = {"zero_root_kinds": sorted(ref_zero_kinds),
            "ref_ledger_status_domain": sorted(ref_status),
            "cross": {"%s|%s" % k: v for k, v in
                      collections.Counter((r.get("kind"), r.get("injected"))
                                          for r in c.rows).items()},
            "faulted_kind_with_injected0": bad_inj[:8],
            "nonempty_note": bad_note[:8], "status_out_of_domain": bad_st[:8]}
    if bad_inj:
        return R.bad("%d 行属故障族却 injected=0(注入未落地 / 台账丢)" % len(bad_inj), **nums)
    if bad_note:
        return R.bad("%d 行 note 非空(runner 只在异常路径写 note)" % len(bad_note), **nums)
    if bad_st:
        return R.bad("%d 行 ledger_status 超出 REF 取值域" % len(bad_st), **nums)
    return R.ok("kind×injected 交叉表干净,note 列全空,ledger_status 全在 REF 取值域内", **nums)


@check("C6", "C", RELEASE, "ledger", EV_CROSS, "PASS",
       "台账每行 json 解析成功率 == 1.0(_kc 用 errors='ignore',字节切割会静默吞字)")
def chk_C6(c):
    led = c.T.ledgers()
    tot = sum(v["n_lines"] for v in led.values())
    bad = {k: v["n_bad"] for k, v in led.items() if v["n_bad"]}
    nums = {"n_lines": tot, "bad_by_combo": bad,
            "lines_by_combo": {k: v["n_lines"] for k, v in led.items()}}
    if not led:
        return R.bad("ledgers/ 目录为空 —— GT 的唯一出处不在", **nums)
    if bad:
        return R.bad("台账有 %d 行解析失败: %s" % (sum(bad.values()), bad), **nums)
    return R.ok("%d 行台账 100%% 可解析" % tot, **nums)


# ==========================================================================
# D 组 —— 遥测完整性与评测可用性
# ==========================================================================
@check("D1", "D", RECOLLECT, "csv+spans", EV_CROSS, "PASS",
       "★span 覆盖不分档:每行 CSV 的 trace 在 spans 里必须可查(REF 实测零缺,阈值 1.0)")
def chk_D1(c):
    """★这条**不要**跟 C2 一起放水。C2 分档是对的(REF 自身 11 缺),但 span 覆盖 REF 是满的,
    那是一条更强且免费的判据 —— 专抓"12 个 rep 只剩第 12 个"(read_spans 不变量被破坏 /
    resume 后前缀从空开始 / 拉取超时静默截断)。whowhen、A2P、内容轨三套 eval 全按
    spans/<combo>.jsonl + trace_id 取数,退化是**静默**的。"""
    sp = c.T.spans()
    miss = [(r.get("group_id"), r.get("run_id")) for r in c.rows
            if r.get("trace_id") not in sp.get(r.get("group_id"), {}).get("traces", {})]
    ref_sp = c.REF.spans()
    ref_miss = sum(1 for r in c.ref_rows
                   if r.get("trace_id") not in ref_sp.get(r.get("group_id"), {}).get("traces", {}))
    nums = {"n_rows": len(c.rows), "n_miss": len(miss), "sample": miss[:8],
            "ref_miss": ref_miss,
            "traces_by_combo": {k: len(v["traces"]) for k, v in sp.items()}}
    if ref_miss:
        return R.bad("REF 自身有 %d 条 span 缺失,阈值 1.0 无依据(需重定 REF)" % ref_miss, **nums)
    if miss:
        return R.bad("%d 条 CSV 行的 trace 在 spans 里查不到" % len(miss), **nums)
    return R.ok("%d/%d case 的 trace 在 spans 中可查(阈值 1.0,REF 实测零缺)"
                % (len(c.rows), len(c.rows)), **nums)


@check("D2", "D", RECOLLECT, "csv+spans", EV_CROSS, "PASS",
       "span 条数三级判:CSV 自陈 total_span_count == spans/ 盘上实测(跨面恒等,无阈值)"
       "+ 每 combo 中位数 >= REF 同 combo 最小值 + 每 case >= 地板(REF 最小值 × (1 − REF 自身抖动))")
def chk_D2(c):
    """★这条判据被改过两次,两次都是同一个病:**拿一个 12 抽样的顺序统计量去卡另一个 12 抽样**。

      · 第一版用 REF 同 combo 的 **p05**:实跑在参考树上自己就 FAIL(6/108)—— 把一个分布的
        p05 拿回去卡同一个分布,按构造就剔掉底部约 5%(规范 §R5 已记)。
      · 第二版改成 REF 同 combo 的**实测最小值**。它在 REF 上恒 PASS(自比),看着没问题,
        但对**新批次**依然不成立:两个各 12 抽样的独立样本,**P(新批次 min < REF min) ≈ 0.5**。
        也就是说这个"地板"平均每两次重采就无故 BLOCK-RECOLLECT 一次。
        实证(2026-07-27,B 档 vs v2):唯一越界的是 `ctxdrift_synth_from_prod__r9`
        = **77 条 vs REF 同 combo 最小 79 条,差 2 条**,而 REF 该 combo 自己就在 79..97 之间抖
        (中位数 87)—— 2 条 span 的差是 LLM 工具调用轮次的正常波动,判它"必须返工重采 108 个
        case"是判据的锅。
        ⚠️ 顺带纠一条错账:那次验收口头传成"23 个 case 越界",实际现算就是 **1 个**
          (`n_below=1`)。别照 23 这个数去查。

    ⇒ 现在拆成三级。**主力是 (c),它没有阈值**;(a)(b) 是 REF 相对水位的兜底:

      (c) **跨面恒等(无阈值,1 条 span 的分辨率)**:每 case 的 CSV `total_span_count`
          必须 == 本脚本从 `spans/<combo>.jsonl` **独立数出来**的该 trace 的 span 条数。
          实测在 B 档与 v2 **都是 108/108 逐条相等**(差值分布 {0: 108})。
          它抓的是"采集之后 span 盘面被截断/丢块"—— 那一类失败会让两个面对不上,
          而 CSV 里的 `span_count_matched` 是采集器自陈(D3 只查取值域),自陈对这种事没有分辨力。
          ★它抓不到什么(必须写清):如果**采集当时**就只拉到了较少的 span,采集器会把那个
            较小的数一起写进 CSV,两面一致 ⇒ (c) 无感,只能靠 (a)(b) 的绝对水位。

      (a) **系统性(combo 级)**:每 combo 的 span 条数**中位数** >= REF 同 combo 的**最小值**。
          在 REF 上恒成立(任何样本的中位数 >= 自身最小值)。
      (b) **逐 case 地板**:n >= floor(REF_min × (1 − tol_combo)),
          `tol_combo = (REF_med − REF_min) / REF_med` = **REF 自己在同一个 combo 内实测的
          向下相对抖动**(不是我拍的 5%)。REF 说这个 combo 能抖多少,就容忍多少:
            - REF 该 combo min == med(分布退化,如 ctxdrift_prod_from_ub 的 87/87)⇒ tol=0
              ⇒ 地板退回**严格最小值**,一条不放水;
            - 抖得多的 combo 地板才松(实测 tol 落在 0.000~0.198)。

    ⚠️ **(a)(b) 的灵敏度有限,别写成"极敏感"/"span 完整性已全面保证"**。(a)(b) 卡的是 REF 的
      **绝对**水位,而 B 档有些 combo 本来就比 REF 高一大截(hallu_User_Behavior_Analyzer:
      B 中位数 99 vs REF 最小 73),这些 combo 上的"两面一致地少采"要掉很多才碰到线。
      每个 combo 现算的可检出比例已放进 numbers 的 `detect_sensitivity`
      (B 档实测:逐 case 地板要掉 **0.0~35.4%**、系统性中位数要掉 **0.0~26.8%** 才翻 FAIL),
      写 SUMMARY 时照抄这两个区间,别写成"完整"。

    ⇒ 变异电池实测(2026-07-27,B 档,逐条跑过,不是推断):
        [盘面 = 只动 spans/,CSV 不动 ⇒ 采集之后盘面被截断/丢块]
        · 每 combo 只剩 1 个 trace              → FAIL (c) 105/108 对不上
        · 单 case 丢一个 agent 子树(-21%)        → FAIL (c) 87 vs 68
        · 单 case 只少 2 条 span                → FAIL (c) 87 vs 85  ← 1 条 span 的分辨率
        [两面 = spans/ 与 CSV total_span_count 同步动 ⇒ 采集当时就少采]
        · 单 case 只剩 1 条 root span            → FAIL (b) 1 < 地板 71
        · 整 combo 均匀矮 **40%**                → FAIL (a) 中位数 59 < REF 最小 73
        · 整 combo 均匀矮 **15%**                → **PASS = 已知盲区**(诚实负例,见上面 ⚠️)
        · 单 case 少 2 条(现实里那个 77 vs 79)   → **PASS = 本次修复的目标**
        [其它]
        · CSV total_span_count 空/不可解析        → FAIL(不许把空当 0 混过去)
    """
    ref_sp, sp = c.REF.spans(), c.T.spans()
    ref_stat, floor, tol = {}, {}, {}
    for cid in c.REF.combos:
        v = [ref_sp.get(cid, {}).get("traces", {}).get(r.get("trace_id"), 0)
             for r in c.ref_rows if r.get("group_id") == cid]
        v = [x for x in v if x > 0]
        if not v:
            ref_stat[cid] = None
            floor[cid] = tol[cid] = None
            continue
        mn, med = float(min(v)), quantile(v, 0.5)
        t = ((med - mn) / med) if med > 0 else 0.0
        ref_stat[cid] = {"n": len(v), "min": int(mn), "med": round(med, 1), "max": int(max(v))}
        tol[cid] = round(t, 4)
        floor[cid] = math.floor(mn * (1.0 - t))

    # (a) combo 级:中位数 >= REF 同 combo 最小值
    sys_bad, med_by_combo = [], {}
    for cid in sorted({r.get("group_id") for r in c.rows}):
        v = [sp.get(cid, {}).get("traces", {}).get(r.get("trace_id"), 0)
             for r in c.rows if r.get("group_id") == cid]
        m = quantile(v, 0.5)
        med_by_combo[cid] = None if m is None else round(m, 1)
        rs = ref_stat.get(cid)
        if rs and m is not None and m < rs["min"]:
            sys_bad.append((cid, med_by_combo[cid], rs["min"]))

    # (b) case 级:>= 地板
    bad = []
    for r in c.rows:
        cid = r.get("group_id")
        n = sp.get(cid, {}).get("traces", {}).get(r.get("trace_id"), 0)
        f = floor.get(cid)
        if f is not None and n < f:
            bad.append((r.get("run_id"), n, int(f), ref_stat[cid]["min"], tol[cid]))

    # (c) 跨面恒等:CSV 自陈 total_span_count == 本脚本从 spans/ 独立数出的条数
    col = "total_span_count"
    mism, nonnum = [], []
    for r in c.rows:
        cid = r.get("group_id")
        disk = sp.get(cid, {}).get("traces", {}).get(r.get("trace_id"), 0)
        v = fnum(r.get(col))
        if v is None or v == "NaN":
            nonnum.append((r.get("run_id"), r.get(col)))
            continue
        if int(v) != disk:
            mism.append((r.get("run_id"), int(v), disk))

    # 可检出灵敏度(现算,写 SUMMARY 照抄;别把 (a)(b) 说成"极敏感")
    sens = {}
    for cid, m in med_by_combo.items():
        rs, f = ref_stat.get(cid), floor.get(cid)
        if not rs or f is None or not m:
            continue
        sens[cid] = {
            "case_floor": int(f),
            "min_detectable_loss_at_tree_median": (
                round(max(0.0, (m - f) / m), 3) if m > 0 else None),
            "systemic_uniform_shave_to_trip_median": (
                round(max(0.0, 1.0 - rs["min"] / m), 3) if m > 0 else None),
        }

    nums = {"ref_by_combo": ref_stat,
            "tol_by_combo(=(REF_med-REF_min)/REF_med)": tol,
            "case_floor_by_combo": {k: (int(v) if v is not None else None)
                                    for k, v in floor.items()},
            "tree_median_by_combo": med_by_combo,
            "n_csv_vs_disk_mismatch": len(mism), "csv_vs_disk_sample": mism[:8],
            "n_total_span_count_nonnumeric": len(nonnum), "nonnumeric_sample": nonnum[:4],
            "n_systemic_below": len(sys_bad), "systemic_sample": sys_bad[:6],
            "n_case_below_floor": len(bad), "case_sample": bad[:8],
            "detect_sensitivity": sens}
    if nonnum:
        return R.bad("%d 行 %s 取不到数(空/不可解析)—— 跨面恒等无从核对: %s"
                     % (len(nonnum), col, nonnum[:3]), **nums)
    if mism:
        return R.bad("%d 个 case 的 CSV %s 与 spans/ 盘上实测条数不等(span 盘面事后被截断/丢块): %s"
                     % (len(mism), col, mism[:3]), **nums)
    if sys_bad:
        return R.bad("%d 个 combo 的 span 条数中位数低于 REF 同 combo 最小值(系统性矮一截,"
                     "不是单 rep 抖动): %s" % (len(sys_bad), sys_bad[:3]), **nums)
    if bad:
        return R.bad("%d 个 case 的 span 条数低于地板(地板 = REF 同 combo 最小值 × (1-该 combo "
                     "REF 实测向下抖动),已容忍正常波动仍不够): %s" % (len(bad), bad[:3]), **nums)
    return R.ok("CSV %s 与 spans/ 盘上实测逐条相等 %d/%d(无阈值,1 条 span 分辨率);combo 中位数"
                "全部 >= REF 同 combo 最小值;逐 case 全部 >= 地板 %s(容忍度现算自 REF 同 combo "
                "向下抖动 %s,退化分布 tol=0 即严格最小值)。(a)(b) 的可检出损失比例见 "
                "detect_sensitivity —— 不许写成『span 完整性已全面保证』"
                % (col, len(c.rows), len(c.rows),
                   {k: int(v) for k, v in floor.items() if v is not None},
                   {k: v for k, v in tol.items() if v}), **nums)


@check("D3", "D", RECOLLECT, "csv", EV_SELF, "PASS",
       "采集器自陈的四个健康标志取值域 == REF(弱证据,但能抓真故障)")
def chk_D3(c):
    """⚠️ 证据独立性 = self:这四列是采集器自己算自己写的,采集器没察觉的失败它一律察觉不到
    (实证:REF 有 11 行台账已丢,ledger_status 仍 100% 'injected')。故本条只作辅助。
    另注:window_end-window_start 与 e2e_latency_ms 的差在 REF 是 -0.0~1.0 ms —— 窗口本来
    就是由 e2e 派生的,"窗口与 e2e 自洽"是**恒 PASS 的空闸**,已从本规范剔除。"""
    cols = [x for x in ("span_count_matched", "wallclock_sanity_ok", "isolation_ok",
                        "conversation_captured") if x in c.T.cols]
    bad, dom = [], {}
    for col in cols:
        d = {r.get(col) for r in c.ref_rows}
        dom[col] = sorted(d)
        off = [(r.get("run_id"), r.get(col)) for r in c.rows if r.get(col) not in d]
        if off:
            bad.append((col, len(off), off[:3]))
    nums = {"cols": cols, "ref_domain": dom, "violations": bad}
    if not cols:
        return R.bad("四个健康标志列一个都不在(schema 变了)", **nums)
    if bad:
        return R.bad("健康标志取值超出 REF 取值域: %s" % bad[:3], **nums)
    return R.ok("%d 个健康标志列取值全在 REF 取值域内 %s" % (len(cols), dom), **nums)


@check("D4", "D", RELEASE, "eval", EV_THIRD, "SKIP",
       "--with-eval:四套 eval 零改跑通(采集/评测解耦的可执行判据)")
def chk_D4(c):
    """⚠️ 两条必须写清的副作用:
      (1) eval 脚本会**写回被测树**(content_ctxdrift_results.json / RESULTS_*.md)。
          故本条一律跑在 --tree 上而**绝不允许指向 --ref**(会改冻结基线)。
      (2) whowhen 的 run_whowhen.py **会调 LLM = 花钱**,藏在单独的 --with-eval-paid 后面。"""
    if not c.args.with_eval:
        return R.skip("未加 --with-eval(会写回被测树;whowhen 那套还要花钱,另需 --with-eval-paid)")
    if os.path.abspath(c.tree) == os.path.abspath(c.ref):
        return R.bad("拒绝在 --ref 上跑 eval(会覆盖写冻结基线的结果文件)")
    base = os.path.join(REPO, "scripts", "chaos", "agentfault", "eval")
    # ★2026-07-27 修:原来漏了前置步 compute_context_drift_outcome.py,
    #   而 content_ctxdrift_track.py **硬依赖**它的产物 context_drift_outcomes.json
    #   (缺了直接 rc=2 报 'not found — run compute_context_drift_outcome.py first')。
    #   ⇒ D4 从来就跑不过 ctxdrift 这一 job(此前 D4 一直是 SKIP,所以没暴露)。
    #   顺序照一键脚本 run_eval_agentfault.sh 的权威口径:3 compute -> 4 tierA -> 5 infra -> 6 ctxdrift。
    agentfault_root = os.path.join(REPO, "scripts", "chaos", "agentfault")
    jobs = [("ctxdrift_outcome",
             [os.path.join(agentfault_root, "compute_context_drift_outcome.py")]),
            ("tierA", [os.path.join(base, "eval_agentfault_tierA.py")]),
            ("infra_neg", [os.path.join(base, "infra_negatives", "run_infra_negatives.py")]),
            ("ctxdrift", [os.path.join(base, "content_ctxdrift_track.py")])]
    if c.args.with_eval_paid:
        jobs.append(("whowhen", [os.path.join(base, "whowhen", "run_whowhen.py")]))
    out, bad = {}, []
    for name, argv in jobs:
        if not os.path.isfile(argv[0]):
            bad.append((name, "脚本不存在"))
            continue
        t0 = time.time()
        try:
            p = subprocess.run([c.python] + argv + ["--dataset-dir", c.tree],
                               capture_output=True, timeout=3600, cwd=REPO)
            rc = p.returncode
            tail = (p.stderr or b"").decode("utf-8", "replace")[-300:]
        except Exception as e:  # noqa: BLE001
            rc, tail = -1, repr(e)
        out[name] = {"rc": rc, "sec": round(time.time() - t0, 1), "stderr_tail": tail}
        if rc != 0:
            bad.append((name, "rc=%d" % rc))
    if bad:
        return R.bad("以下 eval 未零改跑通: %s" % bad, **out)
    return R.ok("%d 套 eval 全部零改跑通(同一命令只换 --dataset-dir)" % len(jobs), **out)


# ==========================================================================
# E 组 —— 跨批次语义可比(加列不删列 != 语义可比)
# ==========================================================================
@check("E1", "E", RECOLLECT, "csv", EV_CROSS, "PASS",
       "载体轮换:每 combo 的 carrier_seq_id 集合 == REF 同 combo")
def chk_E1(c):
    def by(rows):
        d = collections.defaultdict(set)
        for r in rows:
            d[r.get("group_id")].add((r.get("carrier_seq_id") or "").strip())
        return {k: sorted(v) for k, v in d.items()}
    rb, tb = by(c.ref_rows), by(c.rows)
    bad = [(k, tb[k], rb.get(k)) for k in tb if rb.get(k) != tb[k]]
    nums = {"ref": rb, "tree": tb, "diff": bad[:4]}
    if bad:
        return R.bad("carrier_seq_id 集合与 REF 不等(--runs 被砍 / 载体池换了): %s" % bad[:3], **nums)
    return R.ok("%d 个 combo 的 carrier_seq_id 集合与 REF 逐一相等" % len(tb), **nums)


@check("E2", "E", RECOLLECT, "journal+assets", EV_THIRD, "PASS",
       "载体池同源:journal 里的 item_sequence 逐字段命中 assets/carrier_pool.json")
def chk_E2(c):
    pool_p = os.path.join(REPO, "scripts", "chaos", "agentfault", "assets", "carrier_pool.json")
    if not os.path.isfile(pool_p):
        return R.bad("载体池不存在: %s" % pool_p)
    pool = json.load(_open(pool_p))
    seqs = {int(s["seq_id"]): list(s.get("history") or []) for s in pool.get("sequences") or []}
    jr = c.T.journals()
    bad, used = [], set()
    for cid, j in sorted(jr.items()):
        sid, ok = get_path(j, "probe.carrier_seq_id")
        seq, ok2 = get_path(j, "probe.item_sequence")
        if not ok or not ok2:
            continue
        try:
            sid = int(sid)
        except (TypeError, ValueError):
            continue
        used.add(sid)
        if seqs.get(sid) != list(seq or []):
            bad.append((cid, sid))
    nums = {"pool": os.path.relpath(pool_p, REPO), "pool_sha256": sha256_file(pool_p),
            "pool_size": len(seqs), "used_seq_ids": sorted(used),
            "mismatch": bad[:6], "n_mismatch": len(bad)}
    if not used:
        return R.bad("journal 里读不到 probe.carrier_seq_id(载体轮换没生效?)", **nums)
    if bad:
        return R.bad("%d 个 case 的 item_sequence 与载体池同 seq_id 不符" % len(bad), **nums)
    return R.ok("%d 个 case 的载体序列逐字段命中载体池(用到 %d 条 / 池 %d 条)"
                % (len(jr), len(used), len(seqs)), **nums)


@check("E3", "E", RECOLLECT, "journal", EV_CROSS, "PASS",
       "探针形状:probe.top_k 取值集合 == REF")
def chk_E3(c):
    def tk(tree):
        d = collections.Counter()
        for j in tree.journals().values():
            v, ok = get_path(j, "probe.top_k")
            if ok:
                d[v] += 1
        return d
    rd, td = tk(c.REF), tk(c.T)
    nums = {"ref": dict(rd), "tree": dict(td)}
    if not td:
        return R.bad("journal 里读不到 probe.top_k", **nums)
    if set(td) != set(rd):
        return R.bad("top_k 取值集合 %s != REF %s —— 候选面大小变了,跨批次不可比"
                     % (sorted(td), sorted(rd)), **nums)
    return R.ok("probe.top_k 取值集合 == REF %s" % sorted(rd), **nums)


@check("E4", "E", RECOLLECT, "live-k8s+spans", EV_THIRD, "SKIP",
       "★模型口径落在**请求侧**:pod 内 DEEPSEEK_MODEL == 仓库 .env 的 alias;"
       "span 里的 llm.model_name(API 返回的官方名)只作记录 + 断言单值")
def chk_E4(c):
    """★2026-07-27 改判 —— 原判据是**判据错**,不是数据错(改前:要求 span 的 `llm.model_name`
    取值集合 == REF 的 `['deepseek-chat','deepseek-v4-flash']`,B 档只有
    `['deepseek-v4-flash']` 964 条 → 判 BLOCK-RECOLLECT)。实测查明:

      · `llm.model_name` 是 openinference 从 **DeepSeek API 响应**里抄的 `model` 字段
        = **服务端返回的官方名**,不是请求侧填的 alias。`deepseek-chat` 这个 alias 已过期,
        它本来就路由到 `deepseek-v4-flash` —— **两个字符串指同一个模型**。
      · REF 那 82 条 `deepseek-chat` 逐条查过:**全部**出自 `(normal, ChatOpenAI)`。原因是
        v2 的零根臂当时**不装 observer**、走原生**流式**调用,而 faulted 臂经注入器强制
        **非流式**;两条路径 API 返回的 `model` 字段不同。
        ⇒ **REF 的"两个取值"是 v2 两臂不对称的产物,不是标准。**
      · B 档给零根臂也装了 observer(见 H1)⇒ 两臂调用模式对齐 ⇒ 模型名收敛成**单值** 964 条。
        这是**比 REF 更好**的一致性;拿 REF 的历史不对称当期望值 = 把 bug 当规范。
      ⚠️ 给后人:别再把 `['deepseek-chat','deepseek-v4-flash']` 这个集合当"应该长这样"。

    ⇒ 现判据分两层:
      主判据(PASS/FAIL)= **请求侧** pod 内 `DEEPSEEK_MODEL` == 仓库 `.env` 里的 alias。
        请求侧才是真正决定行为的那一侧:2026-07-22 实弹踩过 —— 换成官方参数名会**默认开
        thinking 且拒 tool_choice**,agent 流水线必须用 `deepseek-chat` 别名。
        span 里的官方名对这件事**没有分辨力**(两种写法都返回同一个官方名),所以它不该当闸。
      记录层(不判 FAIL)= span 的 `llm.model_name`,但断言**取值集合大小 == 1**:
        多于 1 个值 ⇒ 两臂调用模式不对称(v2 那样)⇒ WARN 并进 G3 披露清单。

    ⇒ 它现在还能因为什么而 FAIL / WARN(不是空闸):
        · pod 里 `DEEPSEEK_MODEL` 被改成官方参数名 / 别的模型 → FAIL(正是要挡的那个坑);
        · `.env` 与 `.env.example` 的 alias 不一致(请求侧口径自身不自洽)→ FAIL;
        · span 里一条 `llm.model_name` 都没有(openinference 埋点丢了)→ FAIL;
        · 取值多于 1 个(两臂不对称)→ WARN;
        · 单值但 REF 从没见过这个官方名(服务端换模型了,跨批次可比性需复核)→ WARN;
        · 没给 --live / 集群拿不到该 env → **SKIP = 未验收**(exit 6),不是通过。
    """
    def models(tree):
        d = collections.Counter()
        for sp in tree.spans().values():
            for rec in sp["recs"]:
                if rec.get("model"):
                    d[rec["model"]] += 1
        return d

    rm, tm = models(c.REF), models(c.T)
    dotenv = dotenv_values("DEEPSEEK_MODEL")
    nums = {"span_model_names_tree(记录,非判据)": dict(tm),
            "span_model_names_ref(v2 两臂不对称的产物,勿当标准)": dict(rm),
            "dotenv_DEEPSEEK_MODEL": dotenv}

    if not tm:
        return R.bad("被测树的 span 里一条 llm.model_name 都没有 —— openinference 埋点丢了", **nums)

    expect_src = ".env" if ".env" in dotenv else (".env.example" if ".env.example" in dotenv else None)
    if expect_src is None:
        return R.skip("仓根 .env / .env.example 都读不到 DEEPSEEK_MODEL —— 请求侧口径无依据,不可核",
                      **nums)
    expect = dotenv[expect_src]
    nums["expect_alias"] = expect
    nums["expect_from"] = expect_src
    if len(set(dotenv.values())) > 1:
        return R.bad("请求侧口径自身不自洽:%s —— 先把 .env 与 .env.example 对齐再验收" % dotenv,
                     **nums)

    if not c.args.live:
        return R.skip("未加 --live:请求侧 alias 只能问集群(pod env / deployment / envFrom secret),"
                      "span 里的官方名对『是否用了会默认开 thinking 的参数名』没有分辨力。"
                      "记录:span 官方名 = %s(期望 alias = %s,来自 %s)"
                      % (sorted(tm), expect, expect_src), **nums)

    got, source, detail = live_pod_env(c, "DEEPSEEK_MODEL")
    nums["live_source"] = source
    nums["live_value"] = got
    nums["live_detail"] = detail
    if got is None:
        return R.skip("集群侧取不到 DEEPSEEK_MODEL(source=%s)—— restore 回 stock 后 envFrom 已摘且"
                      " secret 也读不到 ⇒ 不可核 = 未验收(见 §6.3)" % source, **nums)
    if got != expect:
        return R.bad("请求侧模型 alias = %r(来自 %s)!= 仓库 %s 的 %r —— 2026-07-22 实弹踩过:"
                     "换成官方参数名会默认开 thinking 且拒 tool_choice,agent 流水线必须用别名"
                     % (got, source, expect_src, expect), **nums)

    if len(tm) > 1:
        return R.warn("请求侧 alias 正确(%r,来自 %s),但 span 里 API 返回的官方名有 %d 个取值 %s"
                      " —— 说明两臂调用模式不对称(v2 就是零根臂走流式、faulted 臂非流式),"
                      "**必须披露**" % (got, source, len(tm), dict(tm)), **nums)
    only = list(tm)[0]
    if only not in rm:
        return R.warn("请求侧 alias 正确(%r)且 span 官方名单值(%r,%d 条),但 REF 从没见过这个官方名"
                      "(REF: %s)—— 服务端把 alias 路由到了新模型,跨批次可比性需复核并披露"
                      % (got, only, tm[only], sorted(rm)), **nums)
    return R.ok("请求侧 DEEPSEEK_MODEL == %r(来自 %s,期望取自仓库 %s);span 里 API 返回的官方名"
                "**单值** %r × %d 条(REF 是 %s 两个值 = v2 两臂不对称的产物,B 档两臂已对齐)"
                % (got, source, expect_src, only, tm[only], sorted(rm)), **nums)


# ==========================================================================
# F 组 —— K8S 物理真实性(★"跑在真全栈里"必须有带内物理指纹,不能只靠自报)
# ==========================================================================
@check("F1", "F", RECOLLECT, "spans+csv", EV_CROSS, "FAIL",
       "★带内指纹(只看业务面):sasrec 的 /recommend 调用必须 100% 走集群 DNS;"
       "/health 探针显式排除并计数 + 反作弊断言 error_span_count 全 0")
def chk_F1(c):
    """本机隔离 harness 的指纹是 `127.0.0.1:8200`;K8S 全栈里 rec-agent 只能经 Service DNS 到
    sasrec。这条纯离线、不可伪造(除非连 span 一起编),是"赝品配方"(只真采 1 个 combo、
    其余 8 个从 v2 拷行)最先撞上的墙。REF 上按构造 FAIL —— 它正是判别式本身。

    ★2026-07-27 改判 —— 原判据把**探针面**和**业务面**混在一起数,把好数据判成 BLOCK-RECOLLECT。
    改前:统计所有 `:8200` 的 http.url,见到 `{'127.0.0.1:8200': 1012}` 就判"本机 harness 指纹"。
    按 **URL 路径**拆开一看,B 档的 1012 条 loopback **全部是 `/health`**:

      · 成因:`services/recommendation_agent/workflow.py:599` **硬编码**
        `requests.get("http://127.0.0.1:8200/health", timeout=5)`(在 `/recommend/health`
        端点里,**没读 `SASREC_API_URL`**)。K8S 的 readiness(10s)/liveness(20s)探针每次打
        `/recommend/health` 都触发它,pod 内没有 8200 ⇒ 这 1012 条 span **状态全 ERROR**。
      · 业务面完全正确:`sasrec:8200` 上 `/recommend` 133 条 + `/health` 22 条。
        pod 内 curl 实测 `http://sasrec:8200/health` = 200,`http://127.0.0.1:8200/health` = 拒绝。
      · **v2 也有这段硬编码**(同一份源码),只是 v2 在本机 sasrec 真在 8200 ⇒ v2 的 45 条
        `/health` 全 200 成功、不显形。**这是 v2 与 B 档共有的源码问题,不是 B 档的采集缺陷。**
      · 零污染已实证:`error_span_count` 在 **B 档与 v2 都是 108/108 全 0** —— 探针是独立 HTTP
        请求、trace_id 与业务 case 的 trace_id 不同,而该列按 case 的 trace_id 聚合
        ⇒ 探针 ERROR **天然隔离**,不进任何特征、不动任何 label/GT/评测。
        旁证(现算):每个 combo 的 `spans/<combo>.jsonl` 里有 99~203 个 trace_id,而 CSV 只认
        其中 12 个(= 该 combo 的 12 个 rep),多出来的 87~191 个全是探针 / warmup 的独立 trace。
        ⇒ 谁想拿"探针 span 在文件里"论证污染,先解释这 12 vs 99~203 的账。

    ⇒ 现判据:只看**业务面**(非 `/health` 的 sasrec 调用,实际就是 `/recommend`)。
      探针面**显式排除但必须计数并报出来**(不许静默忽略),再加一条反作弊断言:
      `error_span_count` 必须全 0 —— 它是"探针 ERROR 没进特征"的**可核证据**,
      少了它,"排除探针"就变成了一句自证。

    ⇒ 它现在还能因为什么而 FAIL(不是空闸):
        · 业务面(/recommend)出现任何 loopback → FAIL(REF 就是这样:129 条 /recommend 全走
          127.0.0.1)—— 拷 v2 的 span 依然当场现形;
        · 业务面用裸 IP(直连 Pod IP,下次 rollout 就失效)→ FAIL;
        · 一条业务面 sasrec 调用都没有(agent 根本没调到 sasrec / span 被截断)→ FAIL;
        · `error_span_count` 缺列或任何一行非 0 → FAIL(探针 ERROR 真的漏进了特征,
          或出现了真业务错误)。
    """
    def split_sasrec(tree):
        """→ (业务面 Counter[(netloc,path)], 探针面 Counter[(netloc,path)], 探针 ERROR 条数)"""
        biz, probe, perr = collections.Counter(), collections.Counter(), 0
        for sp in tree.spans().values():
            for rec in sp["recs"]:
                u = rec.get("url")
                if not u:
                    continue
                pr = urllib.parse.urlparse(u)
                if not pr.netloc.endswith(":8200"):
                    continue
                key = (pr.netloc, pr.path or "/")
                if (pr.path or "").rstrip("/").endswith("/health"):
                    probe[key] += 1
                    if rec.get("status") == "ERROR":
                        perr += 1
                else:
                    biz[key] += 1
        return biz, probe, perr

    biz, probe, probe_err = split_sasrec(c.T)
    ref_biz, ref_probe, _ = split_sasrec(c.REF)
    biz_loop = {k: v for k, v in biz.items() if is_loopback_netloc(k[0])}
    biz_ip = {k: v for k, v in biz.items()
              if not is_loopback_netloc(k[0]) and is_ip_literal_netloc(k[0])}
    probe_loop = {k: v for k, v in probe.items() if is_loopback_netloc(k[0])}
    n_biz = sum(biz.values())

    col = "error_span_count"
    esc = None
    if col in c.T.cols:
        esc = collections.Counter((r.get(col) or "").strip() for r in c.rows)
    nz = [] if esc is None else [(r.get("run_id"), r.get(col)) for r in c.rows
                                 if (r.get(col) or "0").strip() not in ("0", "")]

    def fmt(cnt):
        return {"%s%s" % k: v for k, v in sorted(cnt.items())}

    nums = {"business_sasrec(排除 /health)": fmt(biz), "n_business": n_biz,
            "business_loopback": fmt(biz_loop), "business_bare_ip": fmt(biz_ip),
            "probe_sasrec(/health,已排除)": fmt(probe),
            "n_probe_loopback_excluded": sum(probe_loop.values()),
            "n_probe_span_status_ERROR": probe_err,
            "probe_origin": "services/recommendation_agent/workflow.py:599 硬编码 "
                            "127.0.0.1:8200/health(未读 SASREC_API_URL),由 k8s readiness/"
                            "liveness 打 /recommend/health 触发",
            "ref_business_sasrec": fmt(ref_biz), "ref_probe_sasrec": fmt(ref_probe),
            "error_span_count_dist": (dict(esc) if esc is not None else None),
            "n_error_span_count_nonzero": len(nz), "error_span_count_sample": nz[:6]}

    excl = ("已排除 %d 条 workflow.py:599 硬编码探针 span(/health → loopback,其中 %d 条状态 ERROR)"
            % (sum(probe_loop.values()), probe_err))

    if esc is None:
        return R.bad("CSV 没有 %s 列 —— 无法证明探针 ERROR 没进特征,『排除探针』就成了自证。%s"
                     % (col, excl), **nums)
    if nz:
        return R.bad("%d 行 %s != 0 —— 探针 ERROR 漏进了 case 特征聚合(或出现真业务错误),"
                     "『排除 /health』的前提不成立: %s。%s" % (len(nz), col, nz[:3], excl), **nums)
    if not n_biz:
        return R.bad("业务面一条 sasrec 调用都没有(排除 /health 后为空)—— agent 根本没调到 "
                     "sasrec,或 span 被截断。%s" % excl, **nums)
    if biz_loop:
        return R.bad("业务面 sasrec 调用走 loopback %s(占业务面 %d/%d)= 本机 harness 指纹"
                     "(或从本机批次拷来的 span)。%s"
                     % (fmt(biz_loop), sum(biz_loop.values()), n_biz, excl), **nums)
    if biz_ip:
        return R.bad("业务面 sasrec 调用走裸 IP %s —— 不是集群 DNS(直连 Pod IP,下次 rollout 即失效,"
                     "不可复现)。%s" % (fmt(biz_ip), excl), **nums)
    return R.ok("业务面 %d 条 sasrec 调用 100%% 走集群 DNS %s(loopback 占比 0);%s;"
                "反作弊:%s 全部 %d/%d == 0,证明探针 ERROR 确实没进特征"
                % (n_biz, fmt(biz), excl, col, len(c.rows), len(c.rows)), **nums)


@check("F2", "F", RECOLLECT, "csv", EV_CROSS, "FAIL",
       "★rollout 物理:任意两个 combo 的 k8s_pod_name 集合不相交,且 distinct(pod) >= combo 数")
def chk_F2(c):
    """换 combo 必须 `kubectl set env` + rollout(strategy=Recreate)⇒ pod 换身是**物理必然**。
    拷贝行做不到这一点:8 个 combo 会共用同一个 pod 名。这条单独就打死赝品配方。
    REF 上按构造 FAIL(列不存在)。"""
    if "k8s_pod_name" not in c.T.cols:
        return R.bad("CSV 没有 k8s_pod_name 列 —— 不是 k8s 后端采的(REF 按构造如此)",
                     cols_tail=c.T.cols[-6:])
    by = collections.defaultdict(set)
    for r in c.rows:
        v = (r.get("k8s_pod_name") or "").strip()
        by[r.get("group_id")].add(v)
    empty = [k for k, v in by.items() if not v or "" in v]
    allpods = set()
    for v in by.values():
        allpods |= v
    overlap = []
    ks = sorted(by)
    for i in range(len(ks)):
        for j in range(i + 1, len(ks)):
            inter = by[ks[i]] & by[ks[j]]
            if inter:
                overlap.append((ks[i], ks[j], sorted(inter)[:2]))
    badfmt = sorted(p for p in allpods if p and not POD_RE.match(p))
    nums = {"pods_by_combo": {k: sorted(v) for k, v in by.items()},
            "n_distinct_pods": len(allpods), "n_combos": len(by),
            "overlap": overlap[:6], "bad_format": badfmt[:4], "empty_combos": empty}
    if empty:
        return R.bad("以下 combo 的 k8s_pod_name 有空值: %s" % empty[:4], **nums)
    if overlap:
        return R.bad("以下 combo 对共用 pod 名 %s —— rollout 没真发生(拷贝行的指纹)"
                     % overlap[:3], **nums)
    if len(allpods) < len(by):
        return R.bad("distinct pod %d < combo 数 %d" % (len(allpods), len(by)), **nums)
    if badfmt:
        return R.bad("pod 名不符合 ReplicaSet 命名形状: %s" % badfmt, **nums)
    return R.ok("%d 个 combo 用了 %d 个互不相交的 pod,命名形状合规" % (len(by), len(allpods)), **nums)


@check("F3", "F", RECOLLECT, "csv", EV_SELF, "FAIL",
       "k8s_pod_restarts 全 0(256Mi vs 411MB title cache 的 OOM 会静默截断 emptyDir)")
def chk_F3(c):
    if "k8s_pod_restarts" not in c.T.cols:
        return R.bad("CSV 没有 k8s_pod_restarts 列 —— 不是 k8s 后端采的(REF 按构造如此)")
    bad = [(r.get("run_id"), r.get("k8s_pod_restarts")) for r in c.rows
           if (r.get("k8s_pod_restarts") or "0").strip() not in ("0", "")]
    empt = [r.get("run_id") for r in c.rows if not (r.get("k8s_pod_restarts") or "").strip()]
    nums = {"n_restarted": len(bad), "sample": bad[:8], "n_empty": len(empt)}
    if empt:
        return R.bad("%d 行 k8s_pod_restarts 为空(取不到 = 不可核)" % len(empt), **nums)
    if bad:
        return R.bad("%d 行 pod 发生过重启 —— OOM 会截断 emptyDir 里的 span/台账且不报错,"
                     "涉及的 combo 必须重采" % len(bad), **nums)
    return R.ok("全部 %d 行 k8s_pod_restarts == 0" % len(c.rows), **nums)


@check("F4", "F", DISCLOSE, "csv", EV_SELF, "FAIL",
       "host 水位口径:host_cpu_pct 必须非常量(全 0 = 本次要消掉的那个 overclaim)+ 可分性披露")
def chk_F4(c):
    """★本条**有意偏离**通用的"三段式阈值门",理由:本次是 agent 语义故障,host_cpu_pct 若在
    faulted vs zero-root 之间显著可分,多半是**注入伪影**(副 LLM 额外调用),不是好信号。
    判定语义:
      · 常量列(distinct < 2)= **算不出** → FAIL(REF 108/108 恒 '0.0',按构造 FAIL);
      · 算得出则 p 是多少都不 FAIL,但 p<0.05 必须披露(WARN → G3 兜底)。
    检验钉死为:双侧 Mann-Whitney U + tie 校正正态近似(实现在本脚本内,不随 scipy 漂移)。"""
    col = "host_cpu_pct"
    if col not in c.T.cols:
        return R.bad("无 %s 列" % col)
    src = None
    if "host_metric_source" in c.T.cols:
        src = sorted({(r.get("host_metric_source") or "").strip() for r in c.rows})
    vals = [(r, fnum(r.get(col))) for r in c.rows]
    good = [(r, v) for r, v in vals if isinstance(v, float)]
    distinct = {v for _, v in good}
    nums = {"host_metric_source": src, "n_nonempty": len(good), "n_distinct": len(distinct),
            "min": min(distinct) if distinct else None, "max": max(distinct) if distinct else None}
    if len(distinct) < 2:
        return R.bad("%s 只有 %d 个不同取值(%s)—— 常量列 = 算不出 = 本机 harness 指纹未消除"
                     % (col, len(distinct), sorted(distinct)[:3]), **nums)
    if src and src == ["none"]:
        return R.bad("host_metric_source 全为 'none'(--k8s-host-metrics none)—— overclaim 原样保留",
                     **nums)
    a = [v for r, v in good if r.get("injected") == "1"]
    b = [v for r, v in good if r.get("injected") != "1"]
    mw = mannwhitney(a, b)
    nums.update({"n_faulted": len(a), "n_zero_root": len(b),
                 "test": "two-sided Mann-Whitney U, tie-corrected normal approx (in-script)"})
    if mw is None:
        return R.bad("可分性算不出(两臂样本不足或方差为 0)", **nums)
    p, z, u = mw
    nums.update({"p_value": round(p, 6), "z": round(z, 4), "U1": u})
    if p < 0.05:
        return R.warn("%s 在 faulted vs 零根之间可分(p=%.4g)—— 大概率是注入伪影(副 LLM 额外调用),"
                      "**必须披露**,不许当作『K8S 指标有信号』" % (col, p), **nums)
    return R.ok("%s 非常量(%d 个取值),两臂可分性 p=%.4g(不可分,符合 agent 语义故障预期)"
                % (col, len(distinct), p), **nums)


@check("F5", "F", RELEASE, "live-collector", EV_THIRD, "SKIP",
       "--live:随机抽 trace 在 collector 侧查得到(唯一的非自报证据)")
def chk_F5(c):
    if not c.args.live:
        return R.skip("未加 --live(Jaeger/Prometheus 保留期外 = 不可核,不许拖到过期再验收)")
    if not c.args.jaeger_url:
        return R.skip("未给 --jaeger-url")
    import random
    import urllib.request
    picks = random.sample(c.rows, min(5, len(c.rows)))
    out, bad = {}, []
    for r in picks:
        tid = r.get("trace_id")
        url = c.args.jaeger_url.rstrip("/") + "/api/traces/" + tid
        try:
            body = urllib.request.urlopen(url, timeout=15).read().decode("utf-8", "replace")
            obj = json.loads(body)
            n = len(((obj.get("data") or [{}])[0]).get("spans") or [])
        except Exception as e:  # noqa: BLE001
            n, body = -1, repr(e)
        out[r.get("run_id")] = {"trace_id": tid, "spans_in_collector": n}
        if n <= 0:
            bad.append(r.get("run_id"))
    if bad:
        return R.bad("以下 case 在 collector 侧查不到 trace(自报之外无第三方副本): %s" % bad, **out)
    return R.ok("抽查 %d 条 trace 在 collector 侧全部可查" % len(picks), **out)


# ==========================================================================
# G 组 —— 治理(阈值来源可信 + 披露必须机器可读)
# ==========================================================================
@check("G0", "G", RECOLLECT, "git", EV_THIRD, "PASS",
       "★REF 内容指纹:参考树 CSV 必须与 git 里的 blob 逐字节一致(否则所有阈值都可被调平)")
def chk_G0(c):
    rel = os.path.relpath(c.REF.csv_path, REPO).replace("\\", "/")
    try:
        p1 = subprocess.run(["git", "hash-object", c.REF.csv_path],
                            capture_output=True, timeout=60, cwd=REPO)
        p2 = subprocess.run(["git", "rev-parse", "HEAD:" + rel],
                            capture_output=True, timeout=60, cwd=REPO)
    except Exception as e:  # noqa: BLE001
        return R.bad("git 不可用,REF 指纹无法核对: %r" % e, ref_csv=rel)
    cur = (p1.stdout or b"").decode().strip()
    head = (p2.stdout or b"").decode().strip()
    nums = {"ref_csv": rel, "worktree_blob": cur, "head_blob": head}
    if p1.returncode or p2.returncode or not cur or not head:
        return R.bad("git 取不到 blob(REF 未入库?)—— 阈值来源不可信", **nums)
    if cur != head:
        return R.bad("REF 的 CSV 工作区版本与 HEAD 不一致 —— 改 REF 可以把所有阈值一次性调松,"
                     "本次结论全部作废", **nums)
    return R.ok("REF CSV 与 HEAD blob 一致(%s)" % cur[:12], **nums)


@check("G1", "G", RELEASE, "registry", EV_CROSS, "PASS",
       "REGISTRY 登记一致:新树必须在 datasets/REGISTRY.json 有条目且写着现算的 case 数")
def chk_G1(c):
    p = os.path.join(REPO, "datasets", "REGISTRY.json")
    if not os.path.isfile(p):
        return R.bad("REGISTRY.json 不存在: %s" % p)
    reg = json.load(_open(p))
    name = os.path.basename(os.path.normpath(c.tree))
    ent = None
    for x in reg.get("other_datasets") or []:
        if x.get("path") == name or x.get("id") == name:
            ent = x
            break
    nums = {"tree_name": name, "n_cases": len(c.rows), "entry_found": bool(ent)}
    if not ent:
        return R.bad("REGISTRY.other_datasets 里没有 %r 的条目 —— "
                     "『改一处 stale 漏同款』在本仓已犯过五次" % name, **nums)
    blob = json.dumps(ent, ensure_ascii=False)
    nums["entry_status"] = ent.get("status")
    if str(len(c.rows)) not in blob:
        return R.bad("REGISTRY 条目里找不到现算的 case 数 %d(登记与实物脱节)" % len(c.rows), **nums)
    return R.ok("REGISTRY 有 %r 条目(status=%s)且写着现算的 %d case"
                % (name, ent.get("status"), len(c.rows)), **nums)


@check("G2", "G", RELEASE, "docs", EV_SELF, "PASS",
       "随树文档存在:SUMMARY.md(是什么/怎么采)+ EVAL_NOTES.md(怎么评/局限)")
def chk_G2(c):
    need = ["SUMMARY.md", "EVAL_NOTES.md"]
    got = {n: os.path.getsize(os.path.join(c.tree, n))
           if os.path.isfile(os.path.join(c.tree, n)) else 0 for n in need}
    miss = [n for n, sz in got.items() if sz <= 0]
    if miss:
        return R.bad("缺随树文档(或为空): %s —— 缺 SUMMARY 不许当作『未声称任何东西』放行"
                     % miss, sizes=got)
    return R.ok("随树文档齐全: %s" % got, sizes=got)


@check("G3", "G", DISCLOSE, "limitations", EV_CROSS, "FAIL",
       "★披露必须机器可读:limitations.json 的 id 覆盖所有 WARN/DISCLOSE-FAIL 项,且数值逐位相等")
def chk_G3(c):
    """★把"写没写"升级成"写得对不对"。只要 SUMMARY 里写一句"本项存在局限"就能让所有 WARN
    转绿的自证循环必须堵死(trace_profile.json known_limitations[2] 那次事实性纠错就是它)。
    ⚠️ 本条**不扫自由文本**:仓内实测 `\\bdetector\\b` 命中不了 `observed_detector`(下划线是 \\w)、
    "检测器" 12 次里 6 次不带"结构"前缀、新采树根本没有 SUMMARY.md 时正则门会静默关闭。
    正则扫散文是假阴假阳温床,一律改判结构化文件。REF 按构造 FAIL(无该文件)。"""
    p = os.path.join(c.tree, "limitations.json")
    pending = c.args._pending_disclose or []
    if not os.path.isfile(p):
        return R.bad("无 limitations.json —— 需披露项 %s 无处可核(格式见规范 §7.3)"
                     % sorted(pending), path=os.path.relpath(p, REPO), pending=sorted(pending))
    try:
        obj = json.load(_open(p))
    except Exception as e:  # noqa: BLE001
        return R.bad("limitations.json 解析失败: %r" % e)
    items = obj.get("known_limitations") if isinstance(obj, dict) else obj
    if not isinstance(items, list):
        return R.bad("limitations.json 里没有 known_limitations 列表")
    ids = {str(x.get("id")) for x in items if isinstance(x, dict)}
    missing = sorted(set(pending) - ids)
    nums = {"declared_ids": sorted(ids), "pending": sorted(pending), "missing": missing}
    if missing:
        return R.bad("以下需披露项未在 limitations.json 声明: %s" % missing, **nums)
    return R.ok("limitations.json 覆盖全部 %d 个需披露项" % len(pending), **nums)


# ==========================================================================
# H 组 —— 观察器 / 内容轨(零根臂结构基线 + 副 LLM 归属)
# ==========================================================================
@check("H1", "H", RECOLLECT, "spans", EV_CROSS, "FAIL",
       "★零根臂必须 arm observer:normal 臂要有 agentfault.resolved_input span")
def chk_H1(c):
    """★这是**采集期物理条件**,采后补不回(与 pre 窗同级)。REF 的 spans/normal.jsonl 里
    resolved_input 计数 = 0(v2 采集时零根臂没 arm observer),故 REF 按构造 FAIL —— 本条属
    **新能力**判据,不拿 REF 当反例。
    ⇒ 规范 §8-P3 要求:开采前必须先跑 1-case smoke,断言 normal.jsonl 里该 span >= 1,
      不过就不许放量(否则烧完 108 case 的钱才发现,正是上一份 spec 翻车的姿势)。"""
    sp = c.T.spans()
    zero = sorted({r.get("group_id") for r in c.T.zero_root})
    detail = {k: sp.get(k, {}).get("names", collections.Counter()).get(
        "agentfault.resolved_input", 0) for k in zero}
    n_zero_cases = len(c.T.zero_root)
    nums = {"zero_root_combos": zero, "resolved_input_by_combo": detail,
            "n_zero_cases": n_zero_cases,
            "ref_detail": {k: c.REF.spans().get(k, {}).get("names", collections.Counter()).get(
                "agentfault.resolved_input", 0)
                for k in sorted({r.get("group_id") for r in c.REF.zero_root})}}
    if not zero:
        return R.bad("树里没有零根臂 combo", **nums)
    dead = [k for k, v in detail.items() if v <= 0]
    if dead:
        return R.bad("零根臂 %s 没有 agentfault.resolved_input span —— observer 未 arm,"
                     "结构检测器的误报率在本树**不可测量**(采后补不回)" % dead, **nums)
    return R.ok("零根臂 %s 共 %d 条 resolved_input span"
                % (zero, sum(detail.values())), **nums)


@check("H2", "H", RELEASE, "spans", EV_CROSS, "SKIP",
       "零根臂结构基线:按 content_ctxdrift_track 的 set 口径算签名偏离率 + 非退化断言")
def chk_H2(c):
    """★签名口径必须与**部署中的检测器**同源:`content_ctxdrift_track` 用的是
    `received_by_agent[agent] |= {n for n in names if n}`(非 null name 的 **set**)。
    用 msg_names **原串**做签名是另一回事:ReAct 多步会把 tool 消息追进列表,签名天然发散
    —— 在真实 K8S observe 树上实测,原串口径偏离率 0.455,set 口径 0.000。量错了东西的闸
    会把一份完美的零根臂判成有问题。

    ★另一个坑:PASS 条件若只写"偏离率 == 0",则 observer 半坏(emit 空 messages / 常量 content /
    _identify_agent 恒返同名)时偏离率**必然是 0** —— 坏得越彻底越绿。故本条不设偏离率阈值
    (禁-2 反愿望),只做**非退化断言**并把实测率原样报出去交给 G3 披露。"""
    if not any(c.T.spans().get(k, {}).get("names", collections.Counter()).get(
            "agentfault.resolved_input", 0) for k in {r.get("group_id") for r in c.T.zero_root}):
        return R.skip("零根臂无 resolved_input span(H1 未过)—— 本条无输入,不是通过")
    zero_combos = {r.get("group_id") for r in c.T.zero_root}
    n_zero_cases = len(c.T.zero_root)
    per_agent = collections.defaultdict(list)
    contentless = 0
    for cid in zero_combos:
        for rec in c.T.spans().get(cid, {}).get("recs", []):
            if rec["name"] != "agentfault.resolved_input":
                continue
            ag = rec.get("ri_agent")
            raw = rec.get("ri_names")
            if not ag:
                contentless += 1
                continue
            try:
                names = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:  # noqa: BLE001
                names = []
            sig = tuple(sorted({n for n in names if n}))
            per_agent[ag].append(sig)
    detail, dev_total, total = {}, 0, 0
    for ag, sigs in sorted(per_agent.items()):
        cnt = collections.Counter(sigs)
        modal, modal_n = cnt.most_common(1)[0]
        dev = len(sigs) - modal_n
        detail[ag] = {"n": len(sigs), "modal_n": modal_n, "deviation": dev,
                      "deviation_rate": round(dev / float(len(sigs)), 6),
                      "modal_sig": str(modal)[:100]}
        total += len(sigs)
        dev_total += dev
    rate = dev_total / float(total) if total else None
    modal_sigs = {tuple(sorted(collections.Counter(v).most_common(1)[0][0]))
                  for v in per_agent.values()}
    nums = {"per_agent": detail, "n_agents": len(per_agent), "n_zero_cases": n_zero_cases,
            "deviation_rate": round(rate, 6) if rate is not None else None,
            "n_distinct_modal_sigs": len(modal_sigs),
            "spans_without_agent_attr": contentless,
            "signature_semantics": "set(non-null message .name) — 与 content_ctxdrift_track 同源"}
    if rate is None:
        return R.bad("偏离率算不出(样本为空)", **nums)
    # ---- 非退化断言(挡"坏得越彻底越绿")----
    if len(per_agent) < 2:
        return R.bad("只识别出 %d 个 agent —— _identify_agent 退化(恒返同名)" % len(per_agent), **nums)
    if len(modal_sigs) < len(per_agent):
        return R.bad("%d 个 agent 只有 %d 个不同的 modal 签名 —— 签名退化,偏离率 0 无意义"
                     % (len(per_agent), len(modal_sigs)), **nums)
    if any(not m for m in modal_sigs):
        return R.bad("存在空 modal 签名(emit 到的 messages 为空)—— 偏离率 0 是假绿", **nums)
    if contentless:
        return R.bad("%d 条 resolved_input span 没有 .agent 属性(注入器两次独立 set_attribute "
                     "且异常吞掉,可分裂)" % contentless, **nums)
    return R.ok("零根臂 %d 条样本 / %d 个 agent,签名偏离率 %.6f(set 口径);非退化断言全过 —— "
                "该数必须原样写进 SUMMARY,不许被措辞掩盖" % (total, len(per_agent), rate), **nums)


@check("H3", "H", RECOLLECT, "csv+spans", EV_CROSS, "PASS",
       "副 LLM 归属:subllm_rewrite span 严格限于含该机制的族,且按 trace 覆盖每个 faulted case")
def chk_H3(c):
    """族名不硬编码 —— 从 REF 现算"哪些 kind 出现过 subllm_rewrite span"。
    ★计数口径必须按 **trace 覆盖**,不能按整 combo 的 span 条数:12 case 里 A 出 2 条、B 出 0 条
    也能凑够总数(REF 实测就是 14/13/13 条对 12 case,富余是真实存在的)。"""
    ref_sp, sp = c.REF.spans(), c.T.spans()
    ref_kind = c.REF.combo_kind()
    subllm_kinds = {ref_kind.get(k, "") for k, v in ref_sp.items()
                    if v["names"].get("agentfault.subllm_rewrite", 0) > 0}
    subllm_kinds.discard("")
    if not subllm_kinds:
        return R.bad("REF 未观测到 subllm_rewrite span,族归属无依据")
    kind = c.T.combo_kind()
    uncovered, cross = [], []
    detail = {}
    for cid in c.T.combos:
        traces = sp.get(cid, {}).get("by_name_trace", {}).get("agentfault.subllm_rewrite", set())
        f = [r for r in c.rows if r.get("group_id") == cid and r.get("injected") == "1"]
        detail[cid] = {"kind": kind.get(cid), "subllm_traces": len(traces), "faulted": len(f)}
        if kind.get(cid) in subllm_kinds:
            miss = [r.get("run_id") for r in f if r.get("trace_id") not in traces]
            if miss:
                uncovered.append((cid, miss[:3]))
        elif traces:
            cross.append((cid, len(traces)))
    # ---- overhead 列与 span 互证 ----
    # ★0.0 == "本行无 overhead",**不是**"越族证据"。runner 恒写 `_r2(... or 0.0)`,该列
    #   从不为空;上一版草案用 `v != ""` 判"有值",在参考树上误报 288 处(合格数据被判坏)。
    oh_cols = [x for x in c.T.cols if re.match(r"^span_.+_subllm_overhead_ms$", x)]
    off_family, nan_cells, oh_sum = [], [], 0.0
    for r in c.rows:
        for col in oh_cols:
            v = fnum(r.get(col))
            if v is None:
                continue
            if v == "NaN":
                nan_cells.append((r.get("run_id"), col))
                continue
            if v == 0.0:
                continue
            if r.get("kind") not in subllm_kinds:
                off_family.append((r.get("run_id"), col, v))
            else:
                oh_sum += v
    nums = {"subllm_kinds": sorted(subllm_kinds), "per_combo": detail,
            "overhead_cols": oh_cols, "overhead_sum_ms": round(oh_sum, 2),
            "off_family": off_family[:6], "nan_cells": nan_cells[:6],
            "uncovered": uncovered[:4], "cross_family_spans": cross[:4]}
    if cross:
        return R.bad("非 %s 族出现 subllm span: %s(注入器串族)" % (sorted(subllm_kinds), cross[:3]),
                     **nums)
    if uncovered:
        return R.bad("以下 faulted case 没有对应的 subllm span(corrected 列的扣除量无来源): %s"
                     % uncovered[:3], **nums)
    if nan_cells:
        return R.bad("%d 处 overhead 单元格不可解析" % len(nan_cells), **nums)
    if off_family:
        return R.bad("%d 处非零 overhead 落在非 %s 族行上" % (len(off_family), sorted(subllm_kinds)),
                     **nums)
    if oh_sum <= 0:
        return R.bad("overhead 合计 %.2f <= 0 —— corrected 列不可信(延迟伪影没被扣掉)"
                     % oh_sum, **nums)
    return R.ok("subllm span 严格限于 %s 族且按 trace 全覆盖;overhead 合计 %.2f ms"
                % (sorted(subllm_kinds), oh_sum), **nums)


# ==========================================================================
# --selftest 变异电池 —— 证明这些闸**有区分力**(翻不动的闸 = 空闸)
# ==========================================================================
def _mut_drop_rows(ctx):
    ctx.T.rows = ctx.T.rows[:-5]


def _mut_collapse_gt(ctx):
    for r in ctx.T.rows:
        if r.get("injected") == "1":
            r["injected"] = "0"
            r["ledger_status"] = "no_ledger_match"


def _mut_zero_candidates(ctx):
    t = ctx.T.tools()
    for i, k in enumerate(sorted(t["cand"])):
        if i < 4:
            t["cand"][k] = []


def _mut_placeholder(ctx):
    t = ctx.T.tools()
    for i, k in enumerate(sorted(t["cand"])):
        if i < 3 and t["cand"][k]:
            iid = t["cand"][k][0][0]
            t["cand"][k][0] = (iid, "Product_" + iid)


def _mut_fallback_title(ctx):
    """候选标题全变成 tools.py:126 的兜底串『未知商品』——
    这正是 B 档前两轮的真实形态(46 个 distinct 候选全是它),而当时 B1/B2/B3 全 PASS。
    本变异钉住 B9:兜底串必须翻 FAIL。同时钉住"B2 抓不到它"这件事仍然为真
    (故 expect 只写 B9;若哪天有人把 B2 改成也能抓,这条不会因此失败)。"""
    t = ctx.T.tools()
    for i, k in enumerate(sorted(t["cand"])):
        if i < 3 and t["cand"][k]:
            t["cand"][k] = [(iid, "未知商品") for iid, _ti in t["cand"][k]]


def _mut_truncate_spans(ctx):
    for combo, v in ctx.T.spans().items():
        keep = sorted(v["traces"])[:1]
        v["traces"] = collections.Counter({k: v["traces"][k] for k in keep})
        v["recs"] = [r for r in v["recs"] if r["trace_id"] in keep]
        v["by_name_trace"] = {n: (s & set(keep)) for n, s in v["by_name_trace"].items()}


def _mut_dirty_note(ctx):
    ctx.T.rows[0]["note"] = "no_ledger_match"


def _mut_break_journal(ctx):
    jr = ctx.T.journals()
    k = sorted(jr)[0]
    jr[k] = {"__parse_error__": "ValueError('injected by selftest')"}


MUTATIONS = [
    ("drop_rows        (剥掉 5 行 CSV = 掩盖坏行)", _mut_drop_rows, ["A2", "C1"]),
    ("collapse_gt      (整树 injected→0 = 台账随 pod 丢)", _mut_collapse_gt, ["C1", "C5"]),
    ("zero_candidates  (候选清零 = title cache 空)", _mut_zero_candidates, ["B1", "B3"]),
    ("placeholder      (占位符回潮 = 候选侧过滤失效)", _mut_placeholder, ["B2", "B3"]),
    # D2 是 2026-07-27 加进这一行的:它改判后主力落在"CSV total_span_count == spans/ 盘上实测"
    # 这条无阈值的跨面恒等上,本变异只动 span 面、不动 CSV ⇒ 必须翻 FAIL。
    # 把它钉在这里,是为了防止下一次"为了让某棵树过"再把 D2 放宽成空闸。
    ("truncate_spans   (每 combo 只剩 1 trace)", _mut_truncate_spans, ["D1", "D2"]),
    # ★2026-07-27 加:B 组曾漏掉"候选零语义"这一整类(B1/B2/B3 全 PASS 却毫无察觉),
    #   B9 是补的闸,这条变异是它的区分力证据。别删。
    ("fallback_title   (候选标题全变『未知商品』)", _mut_fallback_title, ["B9"]),
    ("dirty_note       (1 行 note 非空)", _mut_dirty_note, ["C5"]),
    ("break_journal    (1 个 journal 不可解析)", _mut_break_journal, ["A3"]),
]


def run_checks(ctx, only=None):
    results = []
    pending = []
    ctx.args._pending_disclose = pending
    for cid, group, level, plane, ev, ref_expect, title, fn in CHECKS:
        if only and cid.upper() not in only:
            continue
        try:
            r = fn(ctx)
        except Exception as e:  # noqa: BLE001
            import traceback
            r = R("ERROR", "校验器内部异常: %r" % e,
                  {"traceback": traceback.format_exc()[-1500:]})
        results.append({"id": cid, "group": group, "level": level, "plane": plane,
                        "evidence": ev, "ref_expect": ref_expect, "title": title,
                        "status": r.status, "msg": r.msg, "numbers": r.numbers})
        # WARN 与 DISCLOSE 级 FAIL 进"待披露"清单,交 G3 兜底核对
        if cid != "G3" and (r.status == "WARN" or (r.status == "FAIL" and level == DISCLOSE)):
            pending.append(cid)
    return results


def do_selftest(ctx):
    print("=" * 100)
    print(" --selftest 阶段 1:身份验证 —— 每条闸在 REF 上的实际状态 vs 声明的 ref_expect")
    print("=" * 100)
    res = run_checks(ctx)
    bad = []
    for r in res:
        okmark = "OK " if r["status"] == r["ref_expect"] else "!! "
        if r["status"] != r["ref_expect"]:
            bad.append((r["id"], r["ref_expect"], r["status"], r["msg"][:90]))
        print("%s%-4s expect=%-5s actual=%-5s  %s" %
              (okmark, r["id"], r["ref_expect"], r["status"], r["title"][:60]))
    if bad:
        print("\n[阶段 1 失败] 以下闸的实际状态与声明不符(判据写错了,或 REF 变了):")
        for x in bad:
            print("   %s: 声明 %s 实得 %s —— %s" % x)
    else:
        print("\n[阶段 1 通过] %d 条闸全部与声明一致" % len(res))

    print("\n" + "=" * 100)
    print(" --selftest 阶段 2:变异电池 —— 证明闸有区分力(翻不动 = 空闸)")
    print("=" * 100)
    mut_bad = []
    for name, fn, expect_fail in MUTATIONS:
        sub = Ctx(ctx.args, force_distinct=True)   # ★T 与 REF 必须是两个独立对象
        # 预热缓存后再变异(变异直接改缓存)
        sub.T.spans(); sub.T.tools(); sub.T.journals(); sub.T.ledgers()
        fn(sub)
        out = run_checks(sub, only={x.upper() for x in expect_fail})
        got = {r["id"]: r["status"] for r in out}
        notflip = [i for i in expect_fail if got.get(i) != "FAIL"]
        flag = "OK " if not notflip else "!! "
        print("%s%-46s 期望翻 FAIL: %-14s 实得: %s" %
              (flag, name, ",".join(expect_fail), got))
        if notflip:
            mut_bad.append((name, notflip))
    if mut_bad:
        print("\n[阶段 2 失败] 以下变异没能把闸翻成 FAIL(这些闸是空闸):")
        for x in mut_bad:
            print("   %s -> %s" % x)
    else:
        print("\n[阶段 2 通过] %d 组变异全部被对应的闸抓住" % len(MUTATIONS))

    a6_bad = _selftest_a6_evidence(ctx.REF.combos)
    return 0 if not bad and not mut_bad and not a6_bad else 1


def _selftest_a6_evidence(ref_combos):
    """阶段 3:A6 证据等级闸的区分力。

    ★为什么另起一个阶段而不是塞进 MUTATIONS:阶段 2 的变异全是**内存态**改 ctx 缓存,
    而 A6 是**直接读盘**(tree/provenance/invocations.json)。要覆盖它就得造真文件,
    而 REF 树是真数据集((archived) agentfault_v2)——不许往里写东西。故本阶段自己造临时树。

    验的是 2026-07-27 补的那道闸:采后人工补一份"全 false"的留痕不许让 A6 翻 PASS。
    """
    import shutil
    import tempfile

    class _RefStub(object):
        def __init__(self, combos):
            self.combos = combos

    class _CtxStub(object):
        def __init__(self, tree, combos):
            self.tree = tree
            self.REF = _RefStub(combos)

    CLEAN_LOG = ("  $ C:/py.exe D:/repo/collect/agentfault_runner.py --runs 12 "
                 "--out-dir datasets/agentfault_k8s --backend k8s\n")
    DIRTY_LOG = CLEAN_LOG.rstrip("\n") + " --k8s-allow-inject-residue\n"
    INVS = [{"skip_preflight": False, "k8s_skip_code_parity": False,
             "k8s_allow_inject_residue": False, "allow_mixed_tree": False,
             "warmup": 1, "combos": list(ref_combos)}]

    # (名字, 造树的 lambda, 期望状态)
    cases = [
        ("no_file          (压根没有留痕文件)", lambda d: None, "FAIL"),
        ("post_hoc         (采后人工补写,字段全 false)",
         lambda d: _a6_write(d, {"evidence_class": "post_hoc_reconstruction",
                                 "invocations": INVS}), "FAIL"),
        ("no_class         (缺 evidence_class 字段)",
         lambda d: _a6_write(d, {"invocations": INVS}), "FAIL"),
        ("log_missing      (声明有日志但文件不在)",
         lambda d: _a6_write(d, {"evidence_class": "contemporaneous_run_log",
                                 "run_log": "provenance/collect_run.log",
                                 "invocations": INVS}), "FAIL"),
        ("log_has_escape   (★日志原文里有逃生开关,但 JSON 自陈全 false)",
         lambda d: _a6_write(d, {"evidence_class": "contemporaneous_run_log",
                                 "run_log": "provenance/collect_run.log",
                                 "invocations": INVS}, log=DIRTY_LOG), "FAIL"),
        ("log_clean        (日志原文干净 + 字段一致)",
         lambda d: _a6_write(d, {"evidence_class": "contemporaneous_run_log",
                                 "run_log": "provenance/collect_run.log",
                                 "invocations": INVS}, log=CLEAN_LOG), "PASS"),
    ]

    print("\n" + "=" * 100)
    print(" --selftest 阶段 3:A6 证据等级闸 —— 证明『采后补一份留痕』翻不动它")
    print("=" * 100)
    bad = []
    for name, build, expect in cases:
        d = tempfile.mkdtemp(prefix="a6sel_")
        try:
            build(d)
            r = chk_A6(_CtxStub(d, ref_combos))
            got = r.status
        finally:
            shutil.rmtree(d, ignore_errors=True)
        flag = "OK " if got == expect else "!! "
        print("%s%-52s 期望 %-5s 实得 %-5s  %s" % (flag, name, expect, got, r.msg[:60]))
        if got != expect:
            bad.append((name, expect, got))
    if bad:
        print("\n[阶段 3 失败] A6 的证据等级闸没有区分力:")
        for x in bad:
            print("   %s: 期望 %s 实得 %s" % x)
    else:
        print("\n[阶段 3 通过] %d 组证据等级输入全部判对" % len(cases))
    return bad


def _a6_write(d, obj, log=None):
    """在临时树里造 provenance/invocations.json(+ 可选 run log)。"""
    p = os.path.join(d, "provenance")
    os.makedirs(p, exist_ok=True)
    with io.open(os.path.join(p, "invocations.json"), "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False))
    if log is not None:
        with io.open(os.path.join(p, "collect_run.log"), "w", encoding="utf-8") as f:
            f.write(log)


# ==========================================================================
# 主流程
# ==========================================================================
def build_argparser():
    ap = argparse.ArgumentParser(
        description="agent 语义故障 K8S 全栈重采(B 档)验收校验器 —— "
                    "规范 (project docs)/agentfault-k8s-recollect-20260727.md")
    ap.add_argument("--tree", default="datasets/agentfault_k8s", help="待验收树")
    ap.add_argument("--ref", default="(archived) agentfault_v2",
                    help="参考基线树 —— 所有阈值的唯一自家依据(脚本内零字面期望值)")
    ap.add_argument("--only", default=None, help="只跑这些 ID,逗号分隔(结论仅为 PARTIAL)")
    ap.add_argument("--selftest", action="store_true",
                    help="对 REF 自验:阶段1 比对 ref_expect + 阶段2 变异电池证区分力")
    ap.add_argument("--live", action="store_true", help="允许 kubectl / collector 实时比对(B8/F5)")
    ap.add_argument("--with-item-file", action="store_true", help="B7:扫 267MB 权威 item 表")
    ap.add_argument("--with-eval", action="store_true", help="D4:真跑免费的三套 eval(会写回被测树)")
    ap.add_argument("--with-eval-paid", action="store_true", help="D4:再加 whowhen(★调 LLM 花钱)")
    ap.add_argument("--rerun-log", default=None, help="A8:幂等重跑日志")
    ap.add_argument("--rerun-post-sha", default=None, help="A8:重跑后 CSV 的 sha256")
    ap.add_argument("--python", default=None, help="跑子进程用的 python(默认当前解释器)")
    ap.add_argument("--kubectl", default=None)
    ap.add_argument("--jaeger-url", default=None, help="F5:如 http://localhost:16686")
    ap.add_argument("--k8s-ns", default="recweb-chaos")
    ap.add_argument("--k8s-deploy", default="rec-agent")
    ap.add_argument("--k8s-container", default="rec-agent")
    ap.add_argument("--json-out", default=None, help="把完整结果(含全部现算数字)写成 JSON")
    return ap


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    args = build_argparser().parse_args(argv)
    args._pending_disclose = []
    t0 = time.time()

    valid = {cid for cid, _, _, _, _, _, _, _ in CHECKS}
    only = None
    if args.only:
        only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        unknown = sorted(only - valid)
        if unknown:
            print("[FATAL] --only 里有不存在的 ID: %s" % unknown)
            print("        合法 ID: %s" % " ".join(sorted(valid)))
            return 4

    if args.selftest:
        args.tree = args.ref
        try:
            ctx = Ctx(args)
        except CtxError as e:
            print("[FATAL] %s" % e)
            return 4
        except Exception as e:  # noqa: BLE001
            print("[FATAL] 上下文加载失败: %r" % e)
            return 4
        return do_selftest(ctx)

    try:
        ctx = Ctx(args)
    except CtxError as e:
        print("[FATAL] %s" % e)
        return 4
    except Exception as e:  # noqa: BLE001
        print("[FATAL] 上下文加载失败: %r" % e)
        return 4

    results = run_checks(ctx, only)

    reps = ctx.T.reps_per_combo()
    print("=" * 108)
    print(" 验收校验 · agent 语义故障 K8S 全栈重采(B 档)")
    print("   规范 : (project docs)/agentfault-k8s-recollect-20260727.md")
    print("   tree : %s" % os.path.relpath(ctx.tree, REPO))
    print("   ref  : %s   (所有期望值现算自此树,脚本内零字面期望值)"
          % os.path.relpath(ctx.ref, REPO))
    print("   现算 : cases=%d  faulted=%d  zero_root=%d  combos=%d  cols=%d(REF %d)"
          % (len(ctx.rows), len(ctx.T.faulted), len(ctx.T.zero_root),
             len(ctx.T.combos), len(ctx.T.cols), len(ctx.REF.cols)))
    print("   reps/combo(per-combo,非标量): %s" % dict(reps))
    if ctx.self_compare:
        print("   ⚠️ tree == ref:凡『与 REF 比对』的闸此刻**自比恒真**,PASS 不等于阈值被验证过。")
    print("=" * 108)
    print("%-4s %-5s %-16s %-16s %-6s %-6s %s" %
          ("ID", "GRP", "LEVEL", "PLANE", "EVID", "STATUS", "DETAIL"))
    print("-" * 108)
    for r in results:
        st = r["status"]
        shown = "SKIP*" if (st == "SKIP" and r["level"] != DISCLOSE) else st
        msg = (r["msg"] or "").replace("\n", " ")
        if len(msg) > 132:
            msg = msg[:129] + "..."
        print("%-4s %-5s %-16s %-16s %-6s %-6s %s" %
              (r["id"], r["group"], r["level"], r["plane"], r["evidence"], shown, msg))
    print("-" * 108)

    counts = collections.Counter(r["status"] for r in results)
    err = [r["id"] for r in results if r["status"] == "ERROR"]
    fail_re = [r["id"] for r in results if r["status"] == "FAIL" and r["level"] == RECOLLECT]
    fail_rl = [r["id"] for r in results if r["status"] == "FAIL" and r["level"] == RELEASE]
    fail_dc = [r["id"] for r in results if r["status"] == "FAIL" and r["level"] == DISCLOSE]
    skipped = [r["id"] for r in results if r["status"] == "SKIP" and r["level"] != DISCLOSE]
    warns = [r["id"] for r in results if r["status"] == "WARN"]

    print("统计: %s   耗时 %.1fs" % (dict(counts), time.time() - t0))
    if err:
        print("★ ERROR(校验器自身崩溃,状态未知,不得当作任何结论): %s" % err)
    if fail_re:
        print("★ BLOCK-RECOLLECT 失败(必须返工重采,改文档无效): %s" % fail_re)
    if fail_rl:
        print("★ BLOCK-RELEASE   失败(数据可留,不许发,补齐后重跑): %s" % fail_rl)
    if skipped:
        print("★ 未验收项(缺参数/缺证据,**不是通过**): %s" % skipped)
    if fail_dc:
        print("★ DISCLOSE 失败(必须写进 limitations.json): %s" % fail_dc)
    if warns:
        print("  WARN(需在 limitations.json 有对应条目,否则由 G3 兜底): %s" % warns)
    if only:
        print("★ PARTIAL:本次只跑了 %s —— 退出码不构成验收结论。" % sorted(only))

    if args.json_out:
        out = args.json_out if os.path.isabs(args.json_out) else os.path.join(REPO, args.json_out)
        d = os.path.dirname(out)
        if d:
            os.makedirs(d, exist_ok=True)
        payload = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "spec": "(project docs)/agentfault-k8s-recollect-20260727.md",
            "tree": os.path.relpath(ctx.tree, REPO), "ref": os.path.relpath(ctx.ref, REPO),
            "partial": sorted(only) if only else None,
            "live_counts": {"n_cases": len(ctx.rows), "n_faulted": len(ctx.T.faulted),
                            "n_zero_root": len(ctx.T.zero_root),
                            "n_combos": len(ctx.T.combos),
                            "reps_per_combo": dict(reps),
                            "n_cols": len(ctx.T.cols), "n_ref_cols": len(ctx.REF.cols)},
            "results": results,
            "verdict": {"error": err, "block_recollect": fail_re, "block_release": fail_rl,
                        "not_verified": skipped, "disclose": fail_dc, "warn": warns},
        }
        with io.open(out, "w", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, indent=1, default=str))
        print("报告已写: %s" % out)

    if err:
        return 5
    if fail_re:
        return 1
    if fail_rl:
        return 2
    if skipped:
        return 6
    if fail_dc:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
