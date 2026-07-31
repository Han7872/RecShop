# -*- coding: utf-8 -*-
"""backend 抽象改造的**离线**回归自检(不起服务、不调 API、不碰 kubectl、零花费)。

回答一个问题:凭什么说 `--backend local` 与改造前逐字节等价?
--------------------------------------------------------------------------------
[A] CSV 表头字节级不变
      local 后端 extra_csv_columns()==[] → runner.COLS 与 csv_columns() 逐字相同,
      且与已交付的 (upstream batch)dataset_agentfault.csv 表头**逐字节**一致。
[B] local seam 转调的是 ISM 的同一个函数(不是抄了一份)
      靠 monkeypatch 证明:替换 ISM.probe / ISM.wait_health / ISM.read_spans 后,
      LocalBackend 的同名方法立刻改道 → 说明它是运行时转调而非定义期冻死绑定。
      (这一条同时是 tests_dev/test_p0_2_runner.py test_c 能继续工作的前提。)
      外加:sync_ledger/extra_row_fields/summary_meta 在 local 上是 no-op/空。
[C] K8S 侧 env 白名单不外泄本机口径
      逐 combo 算 kubectl set env 的参数,断言:
        · AGENTFAULT_* 注入语义与 build_env 一致(注入语义单一真相源);
        · SASREC_API_URL / OTEL_* / PYTHONPATH / RECOMMENDATION_PORT / NACOS_ENABLED
          **一个都不出现** —— 这几样搬到 pod 上会直接把 B 档打废
          (尤其 SASREC_API_URL:本机 build_env 是 pop 掉它作护栏,搬过去 = pod 内
           tools.py 回落 127.0.0.1:8200 = 连自己 = 工具全失败)。
[D] 108 行离线 replay(最硬的一条)
      v2 的 spans/ ledgers/ raw/ journal/ 全在盘上 → 用**当前代码**把每个 case 的
      GT/内容轨/特征列重算一遍,与 dataset_agentfault.csv 逐字段 diff。
      探针时刻量(e2e/http/window/host_*/wallclock)无法离线重算,从 CSV 回填后排除比较。
      ★已知不可 replay 的一类:format_Recommendation_Synthesizer 的前 11 个 rep。
        原因 = 该 combo 是 per-rep-instance 模式,每次 _bring_up 都会在 warmup 后**清空
        台账文件**(FIX-A),所以盘上的 ledgers/format_*.jsonl 只剩最后一个 rep 的记录。
        这些 case 会被标 unreplayable 并跳过 GT 相关字段(不是失败)。
[E] 三份审查后新增守卫的回归(2026-07-27 第二轮)
      E1/E2 跨树守卫**双向**都拦得住 + local 不往冻结树写任何文件 + append_csv 的表头闸;
      E3 span 本地镜像按 combo 累积、offset 每 rep 推进(rollout 后不覆盖前面的 rep);
      E4 apiserver 5xx 与 rec-agent 5xx 的判据;E5 镜像守卫默认 tag。
      ★E1-E2 只在系统临时目录里造假树,**绝不碰 datasets/ 下任何一棵**。

跑法:
  PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/tests_dev/test_backend_parity.py
  # 只跑 A-C(没有 v2 树时):
  PYTHONIOENCODING=utf-8 python .../test_backend_parity.py --skip-replay
"""
import argparse
import csv
import json
import os
import shutil
import time
import sys

HERE = os.path.dirname(os.path.abspath(__file__))                 # .../agentfault/tests_dev
AGENTFAULT_DIR = os.path.dirname(HERE)                            # .../agentfault
COLLECT_DIR = os.path.join(AGENTFAULT_DIR, "collect")
INJECTOR_DIR = os.path.join(AGENTFAULT_DIR, "injector")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(AGENTFAULT_DIR)))
sys.path.insert(0, COLLECT_DIR)
sys.path.insert(0, INJECTOR_DIR)

import agentfault_runner as R          # noqa: E402
import backends as BK                  # noqa: E402
import injector_smoke as ISM           # noqa: E402

V2_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault_v2")

FAILS = []


def check(cond, msg):
    print(f"   [{'OK ' if cond else 'FAIL'}] {msg}")
    if not cond:
        FAILS.append(msg)


# ============================================================
# [A] CSV 表头字节级不变
# ============================================================
def test_a():
    print("\n[A] CSV 表头(local 后端一列不加)")
    local = BK.LocalBackend(build_env=R.build_env)
    check(local.extra_csv_columns() == [], "LocalBackend.extra_csv_columns() == []")
    check(R.COLS == R.csv_columns(),
          f"runner.COLS == csv_columns() ({len(R.COLS)} 列)")
    csv_path = os.path.join(V2_DIR, "dataset_agentfault.csv")
    if not os.path.exists(csv_path):
        print(f"   [SKIP] {csv_path} 不在,跳过与已交付表头的比对")
        return
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))
    check(header == R.COLS,
          f"与 agentfault_v2 表头逐字节一致(盘上 {len(header)} 列 / 代码 {len(R.COLS)} 列)")
    if header != R.COLS:
        only_disk = [c for c in header if c not in R.COLS]
        only_code = [c for c in R.COLS if c not in header]
        print(f"      盘上独有: {only_disk}\n      代码独有: {only_code}")

    # k8s 后端只在**末尾**追加,前缀必须与 local 完全相同
    k8s = BK.K8sBackend(build_env=R.build_env)
    k8s_cols = R.csv_columns() + k8s.extra_csv_columns()
    check(k8s_cols[:len(R.COLS)] == R.COLS,
          "k8s 追加列只在末尾(前缀与 local 表头逐字相同)")
    check(k8s.extra_csv_columns() == ["collect_backend", "host_metric_source",
                                      "k8s_pod_name", "k8s_pod_restarts"],
          f"k8s 追加 4 列口径/provenance 标签 (got {k8s.extra_csv_columns()})")


# ============================================================
# [B] local seam = 运行时转调 ISM(非定义期冻死绑定)
# ============================================================
def test_b():
    print("\n[B] LocalBackend seam 转调 + no-op 钩子")
    # ★runner 用 importlib 显式路径装载 backends(不再 sys.path.insert),但仍注册进
    #   sys.modules["backends"] —— 这里断言"外部 import 到的是同一个模块对象",
    #   否则本文件后面所有 monkeypatch 都会打在一个副本上,测了个寂寞。
    check(R.BK is BK, "runner.BK 与 `import backends` 是同一个模块对象(monkeypatch 有效)")
    check(R.BackendTransientError is BK.BackendTransientError,
          "BackendTransientError 也是同一个类(runner 的 except 才拦得住 backends 抛的)")
    local = BK.LocalBackend(build_env=R.build_env)

    seen = {}
    orig = (ISM.probe, ISM.wait_health, ISM.read_spans)
    try:
        ISM.probe = lambda port, seq=None, top_k=None: ("P", port, seq, top_k)
        ISM.wait_health = lambda port, timeout_s=ISM.HEALTH_TIMEOUT_S: ("H", port, timeout_s)
        ISM.read_spans = lambda span_file, tid: ("S", span_file, tid)
        check(local.probe(5131, seq=["x"], top_k=3) == ("P", 5131, ["x"], 3),
              "LocalBackend.probe -> ISM.probe(运行时转调,monkeypatch 生效)")
        check(local.wait_health(5131)[0] == "H",
              "LocalBackend.wait_health -> ISM.wait_health")
        check(local.read_spans("/f", "tid") == ("S", "/f", "tid"),
              "LocalBackend.read_spans -> ISM.read_spans")
        # runner 顶层 dispatcher 也必须走到同一处(BACKEND 默认就是 LocalBackend)
        seen["disp"] = R.BACKEND.probe(5199)
        check(seen["disp"][0] == "P", "runner.BACKEND.probe 走同一条转调链")
    finally:
        ISM.probe, ISM.wait_health, ISM.read_spans = orig

    check(local.sync_ledger("/nonexistent/ledger.jsonl") is None,
          "LocalBackend.sync_ledger 是 no-op(不碰盘、不抛)")
    check(local.extra_row_fields() == {}, "LocalBackend.extra_row_fields() == {}")
    check(local.summary_meta() is None,
          "LocalBackend.summary_meta() is None(run_summary.json 不多 key)")
    check(local.preflight() == [], "LocalBackend.preflight() == []")
    check(local.needs_phase1_venv is True and BK.K8sBackend.needs_phase1_venv is False,
          "phase1 venv FATAL 门只对 local 成立")

    # slot_ready 语义:local 端口占用 -> (False, 原 port_free 的拒绝理由)
    orig_req = ISM._req
    try:
        class _R:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        ISM._req = lambda url, timeout=2: _R()          # 端口"有人应答" = 不空
        ok, why = local.slot_ready(5131, "hallu_Product_Analyzer")
        # ★F2:必须与改造前 run_combo 里那句 raise 的文本**逐字节相同**
        want = "port 5131 not free; refuse to start temp instance for hallu_Product_Analyzer"
        check(ok is False and why == want,
              f"slot_ready 拒绝文案与改造前逐字节相同 (got {why!r})")
        ISM._req = lambda url, timeout=2: (_ for _ in ()).throw(OSError("refused"))
        ok, why = local.slot_ready(5131, "hallu_Product_Analyzer")
        check(ok is True, "slot_ready: 端口空 -> (True, '')")
    finally:
        ISM._req = orig_req


# ============================================================
# [C] K8S env 白名单:注入语义一致 + 本机口径零外泄
# ============================================================
_LOCAL_ONLY_KEYS = ("SASREC_API_URL", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "PYTHONPATH",
                    "RECOMMENDATION_PORT", "RECOMMENDATION_HOST", "NACOS_ENABLED",
                    "NO_PROXY", "no_proxy", "PATH")


def test_c():
    print("\n[C] K8sBackend env 白名单(离线,不碰 kubectl)")
    k8s = BK.K8sBackend(build_env=R.build_env)
    by_id = {c["id"]: c for c in R.build_combos()}

    # 预置 stale 旋钮:验证 pod 侧一定被显式 unset(pod 是长命对象,残留是最阴的坑)
    os.environ["AGENTFAULT_DEBUG"] = "1"
    try:
        for cid, combo in by_id.items():
            subtype, field = R.rep_subtype(combo, 1)
            sets, unsets, env = k8s.env_ops(combo, 5131, subtype=subtype, field=field)
            skeys = {s.split("=", 1)[0] for s in sets}
            ukeys = {u[:-1] for u in unsets}
            leaked = [k for k in _LOCAL_ONLY_KEYS if k in skeys]
            check(not leaked, f"{cid}: 本机专属 env 零外泄(leaked={leaked})")
            check(skeys | ukeys == set(k8s.ENV_WHITELIST),
                  f"{cid}: 白名单被全量覆盖(set∪unset == {len(k8s.ENV_WHITELIST)} 键)")
            check(not (skeys & ukeys), f"{cid}: 同一键不会既 set 又 unset")
            # ★R6:遗留黑盒钩子 AGENT_FAULT_<Name> / AGENT_FAULT_DELAY_MS(workflow.py L90/106
            #   真的会读)必须**恒 unset** —— 本机 build_env 是显式 pop 的,pod 侧不摘就是防护
            #   不对称:一个手工挂过的残留能给 agent 加 5s 延迟而 CSV 里毫无标记。
            legacy = {"AGENT_FAULT_DELAY_MS"} | {"AGENT_FAULT_" + a for a in R.AGENT_NAMES}
            check(legacy <= ukeys,
                  f"{cid}: 遗留 AGENT_FAULT_* 钩子全部显式 unset(缺: {sorted(legacy - ukeys)})")
            check("SPAN_FILE=/agentfault-data/spans.jsonl" in sets,
                  f"{cid}: SPAN_FILE 指 pod 内 emptyDir")
            check("AGENTFAULT_LEDGER=/agentfault-data/ledger.jsonl" in sets,
                  f"{cid}: AGENTFAULT_LEDGER 指 pod 内 emptyDir")
            check("AGENTFAULT_INSTRUMENT=1" in sets, f"{cid}: 内容层埋点常开")
            if combo["faulted"]:
                check("AGENTFAULT_INJECT=1" in sets, f"{cid}: INJECT=1")
                check("AGENTFAULT_OBSERVE" in ukeys, f"{cid}: OBSERVE 显式 unset")
                check(f"AGENTFAULT_KIND_{combo['agent']}={combo['kind']}" in sets,
                      f"{cid}: KIND_{combo['agent']}={combo['kind']}")
            else:
                check("AGENTFAULT_INJECT" in ukeys, f"{cid}: normal 臂 INJECT 显式 unset")
                check("AGENTFAULT_OBSERVE=1" in sets, f"{cid}: normal 臂 OBSERVE=1")
            # 继承自 driver shell 的 AGENTFAULT_DEBUG 也必须被显式下发/清除
            check("AGENTFAULT_DEBUG" in skeys or "AGENTFAULT_DEBUG" in ukeys,
                  f"{cid}: AGENTFAULT_DEBUG 在白名单内(不留残留)")
    finally:
        os.environ.pop("AGENTFAULT_DEBUG", None)

    # format 4 subtype 逐 rep 轮换都要能正确下发
    fmt = by_id["format_Recommendation_Synthesizer"]
    for i in (1, 2, 3, 4):
        st, fld = R.rep_subtype(fmt, i)
        sets, unsets, _ = k8s.env_ops(fmt, 5131, subtype=st, field=fld)
        check(f"AGENTFAULT_FORMAT_SUBTYPE={st}" in sets,
              f"format rep{i}: FORMAT_SUBTYPE={st}")
        if fld:
            check(f"AGENTFAULT_FORMAT_FIELD={fld}" in sets, f"format rep{i}: FIELD={fld}")
        else:
            check("AGENTFAULT_FORMAT_FIELD" in {u[:-1] for u in unsets},
                  f"format rep{i}: 无 field -> 显式 unset(不吃上一个 rep 的残留)")


# ============================================================
# [D] 108 行离线 replay
# ============================================================
# 探针时刻量:离线不可重算,从 CSV 回填后排除比较(它们与 backend 抽象无关)
_PROBE_TIME_COLS = {
    "window_start", "window_end", "e2e_latency_ms", "http_status", "http_success",
    "host_cpu_pct", "host_mem_pct", "wallclock_sanity_ok",
}


def _load_spans(span_file, trace_id):
    out = []
    try:
        with open(span_file, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    rec = json.loads(s)
                except Exception:
                    continue
                if rec.get("trace_id") == trace_id:
                    out.append(rec)
    except FileNotFoundError:
        pass
    return out


def _fmt(v):
    """按 csv.DictWriter 的口径把值字符串化,便于与盘上 CSV 逐字段比。"""
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(int(v))
    return str(v)


def test_d():
    print("\n[D] 108 行离线 replay(用当前代码重算 GT/内容轨/特征列)")
    csv_path = os.path.join(V2_DIR, "dataset_agentfault.csv")
    if not os.path.exists(csv_path):
        print(f"   [SKIP] 没有 {csv_path}")
        return
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rows = {r["run_id"]: r for r in csv.DictReader(f)}
    print(f"   CSV 行数 = {len(rows)}")

    by_id = {c["id"]: c for c in R.build_combos()}
    replayed = 0
    unreplayable = []
    mismatches = []

    for case_id, disk in sorted(rows.items()):
        jp = os.path.join(V2_DIR, "journal", f"{case_id}.json")
        rp = os.path.join(V2_DIR, "raw", f"{case_id}.json")
        if not (os.path.exists(jp) and os.path.exists(rp)):
            unreplayable.append((case_id, "缺 journal/raw"))
            continue
        with open(jp, "r", encoding="utf-8") as f:
            jr = json.load(f)
        with open(rp, "r", encoding="utf-8") as f:
            raw = json.load(f)
        combo = by_id.get(jr["combo_id"])
        if combo is None:
            unreplayable.append((case_id, f"combo {jr['combo_id']} 不在当前矩阵"))
            continue
        trace_id = jr.get("trace_id") or ""
        resp = raw.get("resp")
        span_file = os.path.join(V2_DIR, "spans", f"{combo['id']}.jsonl")
        ledger_file = os.path.join(V2_DIR, "ledgers", f"{combo['id']}.jsonl")
        spans = _load_spans(span_file, trace_id) if trace_id else []

        # ★per-rep-instance 的 format combo:台账每 rep 被清,盘上只剩最后一个 rep。
        #   这类 case 的 GT 无法离线复算,如实标 unreplayable 而不是假装通过。
        gt = R._determine_gt(combo, trace_id, ledger_file)
        if combo["faulted"] and str(disk.get("injected")) == "1" and not gt["faulted"]:
            unreplayable.append((case_id, f"台账已被后续 rep 清掉(ledger_status={gt['ledger_status']})"))
            continue
        if not spans and str(disk.get("total_span_count") or "0") != "0":
            unreplayable.append((case_id, "盘上 span 文件里没有该 trace_id"))
            continue

        by_span = {s["span_id"]: s for s in spans if s.get("span_id")}
        agg, total_spans, error_spans = R.aggregate_agent_spans(spans)
        quality = R.derive_quality(resp)
        subtype = jr.get("subtype")
        ct = R._content_track(combo, gt, resp, spans, by_span, quality, subtype=subtype)
        present = sum(1 for a in R.AGENT_NAMES if agg.get(a, {}).get("present"))
        span_matched = (present == len(R.AGENT_NAMES))
        note = ""
        if not trace_id:
            note = "no_trace_id_INVALID"
        elif combo["faulted"] and not gt["faulted"]:
            note = gt["ledger_status"]
        row = R.build_row(
            combo, case_id, trace_id, int(disk["http_status"] or 0),
            float(disk["e2e_latency_ms"] or 0), agg, total_spans, error_spans,
            quality, disk["host_cpu_pct"], disk["host_mem_pct"],
            disk["window_start"], disk["window_end"], span_matched,
            int(disk["wallclock_sanity_ok"] or 0), gt, ct, note=note,
            carrier_seq_id=(jr.get("probe") or {}).get("carrier_seq_id", ""))

        bad = []
        for col in R.csv_columns():
            if col in _PROBE_TIME_COLS:
                continue
            got, want = _fmt(row.get(col, "")), disk.get(col, "")
            if got != want:
                bad.append(f"{col}: replay={got!r} disk={want!r}")
        if bad:
            mismatches.append((case_id, bad))
        replayed += 1

    print(f"   replay 成功 {replayed} / CSV {len(rows)};unreplayable {len(unreplayable)}")
    for cid, why in unreplayable[:6]:
        print(f"      - {cid}: {why}")
    if len(unreplayable) > 6:
        print(f"      ... 另 {len(unreplayable) - 6} 个")
    for cid, bad in mismatches[:8]:
        print(f"   [DIFF] {cid}: " + "; ".join(bad[:5]))
    check(not mismatches,
          f"所有可 replay 的 case 逐字段一致({replayed} 个,{len(mismatches)} 个有 diff)")
    # unreplayable 只允许是 format(台账被清)这一类已知情况
    unexpected = [c for c, w in unreplayable if not c.startswith("format_")]
    check(not unexpected,
          f"unreplayable 只限 format_*(per-rep 清台账);意外的: {unexpected[:5]}")
    check(replayed >= 90, f"至少 90 个 case 可 replay(实得 {replayed})")


# ============================================================
# [E] 三份审查后新增守卫的回归(全离线,不碰 kubectl / 不碰真树)
# ============================================================
class _FakeHandle(object):
    """冒充 _PodHandle,只带 read_spans 用得到的两个字段。"""

    def __init__(self, span_offset=0):
        self.pod = "rec-agent-fake"
        self.span_offset = span_offset
        self.ledger_offset = 0
        self.log_path = ""
        self.combo_id = "fake"
        self.logs_dumped = False


def _span_line(trace_id, span_id, name="agent.Product_Analyzer"):
    return json.dumps({"trace_id": trace_id, "span_id": span_id, "name": name},
                      ensure_ascii=False)


def test_e(tmp_root):
    print("\n[E] 审查修正的回归守卫")

    # ---- E1(F1):跨树守卫必须**双向**拦得住,且 local 不往树里写任何文件 ----
    v2_header = None
    v2_csv = os.path.join(V2_DIR, "dataset_agentfault.csv")
    if os.path.exists(v2_csv):
        with open(v2_csv, "r", encoding="utf-8", newline="") as f:
            v2_header = next(csv.reader(f))
    if v2_header:
        local_tree = os.path.join(tmp_root, "as_local_tree")
        os.makedirs(local_tree, exist_ok=True)
        with open(os.path.join(local_tree, "dataset_agentfault.csv"), "w",
                  encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(v2_header)          # 82 列 local 表头
        # 当前 COLS 是 local 的 → 同后端必须放行,且**不能**留下 .collect_backend
        g = R._tree_backend_guard(local_tree, "local")
        check(g is None, "guard: local 后端进 local 树 -> 放行")
        check(not os.path.exists(os.path.join(local_tree, ".collect_backend")),
              "guard: local 后端**不写** .collect_backend(冻结树零新增文件)")
        # ★正路必须仍然通(新加的表头闸不能把正常追加也挡了)
        ok_append = True
        try:
            R.append_csv(os.path.join(local_tree, "dataset_agentfault.csv"),
                         {c: "" for c in R.COLS})
        except Exception as e:
            ok_append = False
            print(f"      append 正路失败: {e!r}")
        with open(os.path.join(local_tree, "dataset_agentfault.csv"),
                  "r", encoding="utf-8") as f:
            nline_ok = len([l for l in f if l.strip()])
        check(ok_append and nline_ok == 2,
              f"append_csv 正路(表头一致)照常追加,不被新闸误挡(行数={nline_ok})")
        # 伪装成 k8s 后端(把 COLS 临时换成 82+4 列)→ 必须拦住。这是 F1 原本漏掉的方向。
        k8s_cols = R.csv_columns() + BK.K8sBackend(build_env=R.build_env).extra_csv_columns()
        orig_cols = R.COLS
        try:
            R.COLS = k8s_cols
            g = R._tree_backend_guard(local_tree, "k8s")
            check(bool(g) and "表头" in g,
                  f"guard: k8s 后端进 local 树 -> 拦住 (got {str(g)[:60]!r})")
            check(not os.path.exists(os.path.join(local_tree, ".collect_backend")),
                  "guard: 被拦时不留标记文件")
            # ---- E2(F1 配套):append_csv 自己也要拦(树中途被换 / 直调 runner)----
            raised = ""
            try:
                R.append_csv(os.path.join(local_tree, "dataset_agentfault.csv"),
                             {c: "" for c in k8s_cols})
            except RuntimeError as e:
                raised = str(e)
            check("ragged" in raised or "表头" in raised,
                  f"append_csv: 表头列数不符 -> raise (got {raised[:60]!r})")
            # 追加被拒后文件行数必须**没变**(没写出 ragged 行)
            with open(os.path.join(local_tree, "dataset_agentfault.csv"),
                      "r", encoding="utf-8") as f:
                nline = len([l for l in f if l.strip()])
            check(nline == nline_ok, f"append_csv 被拒后文件未被写脏(行数 {nline_ok} -> {nline})")
        finally:
            R.COLS = orig_cols
    else:
        print("   [SKIP] 没有 v2 CSV,跳过 E1/E2")

    # ---- E3(R1/R3):span 本地镜像按 combo 累积 + offset 每 rep 推进 ----
    k8s = BK.K8sBackend(build_env=R.build_env)
    span_file = os.path.join(tmp_root, "spans", "format_fake.jsonl")
    pod_lines = []          # 冒充 pod 内 /agentfault-data/spans.jsonl 的全部行

    def fake_pull(handle, path, offset, what):
        return "\n".join(pod_lines[offset:])

    k8s._pull_since = fake_pull
    k8s._pod_line_count = lambda pod, path: len(pod_lines)

    # rep1:单实例,pod 里落 2 行
    k8s._cur = _FakeHandle(span_offset=0)
    k8s._span_prefix = k8s._read_local(span_file)          # 文件不存在 -> ""
    pod_lines = [_span_line("t1", "s1"), _span_line("t1", "s2")]
    got = k8s.read_spans(span_file, "t1")
    check(len(got) == 2, f"read_spans rep1 拿到 2 条 (got {len(got)})")
    check(k8s._cur.span_offset == 2, f"rep1 后 span_offset 推进到 2 (got {k8s._cur.span_offset})")

    # rep2:**同一个 pod**(单实例模式),只应拉到新增的 1 行,但本地文件必须有 3 行
    pod_lines.append(_span_line("t2", "s3"))
    got = k8s.read_spans(span_file, "t2")
    check(len(got) == 1, f"read_spans rep2 只拿本 rep 的 1 条 (got {len(got)}) —— O(n²) 已消除")
    n = len([l for l in open(span_file, encoding="utf-8") if l.strip()])
    check(n == 3, f"本地 spans 文件累积到 3 行 (got {n})")

    # rep3:模拟 **per-rep rollout**(format 族)—— pod 换身、行号归 0、本地前缀从盘上续
    pod_lines = [_span_line("t3", "s4")]
    k8s._cur = _FakeHandle(span_offset=0)
    k8s._span_prefix = k8s._read_local(span_file)          # ★start_instance 里做的那一步
    got = k8s.read_spans(span_file, "t3")
    check(len(got) == 1, f"rollout 后 rep3 拿到 1 条 (got {len(got)})")
    n = len([l for l in open(span_file, encoding="utf-8") if l.strip()])
    check(n == 4, f"★R1:rollout 后前 2 个 rep 的 span 没被覆盖(文件 4 行,got {n})")
    tids = [json.loads(l)["trace_id"] for l in open(span_file, encoding="utf-8") if l.strip()]
    check(tids == ["t1", "t1", "t2", "t3"], f"顺序与内容都对 (got {tids})")

    # ---- E4(R4):apiserver 5xx vs rec-agent 5xx 的判据 ----
    check(k8s._is_infra_5xx({"kind": "Status", "code": 503}, "") is True,
          "R4: apiserver Status 对象 -> 判基础设施 5xx(重试/作废,不入表)")
    check(k8s._is_infra_5xx(None, "no endpoints available") is True,
          "R4: 非 JSON 响应体 -> 判基础设施 5xx")
    check(k8s._is_infra_5xx(
        {"success": False, "message": "Recommendation failed: boom", "trace_id": "ab"}, "")
        is False,
        "R4: rec-agent 自己的 500(带 success/message/trace_id)-> 真实观测,原样入表")

    # ---- E5(R7):镜像守卫默认 tag 不能是会误命中 G1 旧镜像的子串 ----
    check(BK.K8sBackend(build_env=R.build_env).image_hint == "agentfault-v2",
          "R7: image_hint 默认 = agentfault-v2(不放行 G1 的旧 :agentfault)")

    # ---- E6(R5):"span 真的在写"硬闸 —— 必须抛 BackendFatalError,而且**不能**被
    #      runner 里那句 `except Exception -> [WARN] ledger truncate failed` 吞掉 ----
    k6 = BK.K8sBackend(build_env=R.build_env)
    k6.SPAN_WRITE_GRACE_S = 0        # 自检不等真实的 30s 宽限窗
    k6._cur = _FakeHandle(span_offset=7)
    k6._probes_since_start = 1                 # 发过 warmup 探针
    k6._running_pods = lambda: ["rec-agent-fake"]     # pod 没换(排除"瞬时"那一支)
    k6._pod_line_count = lambda pod, path: 7   # 行数纹丝不动 = exporter 没在写
    ledger_mirror = os.path.join(tmp_root, "ledgers", "fake.jsonl")
    raised = None
    t0 = time.time()
    try:
        k6.reset_ledger(ledger_mirror)
    except Exception as e:
        raised = e
    check(isinstance(raised, BK.BackendFatalError),
          f"R5: warmup 后 span 零增长 -> BackendFatalError (got {type(raised).__name__})")
    check(isinstance(raised, RuntimeError) and not isinstance(raised, BK.BackendTransientError),
          "R5: 是 Fatal 不是 Transient(重试没用,必须硬停整轮)")
    print(f"      (等待窗实测 {time.time() - t0:.0f}s;实跑时给 exporter 30s 余量)")
    # --warmup 0(一次探针都没发过)时必须**不**断言,否则必然误报
    k6b = BK.K8sBackend(build_env=R.build_env)
    k6b._cur = _FakeHandle(span_offset=0)
    k6b._probes_since_start = 0
    k6b._pod_line_count = lambda pod, path: 0
    ok_zero = True
    try:
        k6b.reset_ledger(os.path.join(tmp_root, "ledgers", "fake0.jsonl"))
    except Exception as e:
        ok_zero = False
        print(f"      误报: {e!r}")
    check(ok_zero, "R5: --warmup 0(零探针)时不做该断言(不误报)")
    # pod 在 warmup 期间被换掉 -> 是**瞬时**(作废本 combo 等 resume),不是硬停整轮
    k6c = BK.K8sBackend(build_env=R.build_env)
    k6c.SPAN_WRITE_GRACE_S = 0
    k6c._cur = _FakeHandle(span_offset=7)
    k6c._probes_since_start = 1
    k6c._running_pods = lambda: ["rec-agent-OTHER"]     # pod 换身
    k6c._pod_line_count = lambda pod, path: 0
    raised2 = None
    try:
        k6c.reset_ledger(os.path.join(tmp_root, "ledgers", "fake2.jsonl"))
    except Exception as e:
        raised2 = e
    check(isinstance(raised2, BK.BackendTransientError),
          f"R5: warmup 期间 pod 换身 -> Transient 不是 Fatal (got {type(raised2).__name__})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-replay", action="store_true")
    a = ap.parse_args()
    test_a()
    test_b()
    test_c()
    if not a.skip_replay:
        test_d()
    tmp_root = os.path.join(os.environ.get("TEMP") or "/tmp", "agentfault_parity_e")
    shutil.rmtree(tmp_root, ignore_errors=True)
    os.makedirs(tmp_root, exist_ok=True)
    try:
        test_e(tmp_root)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
    print("\n" + "=" * 64)
    if FAILS:
        print(f"BACKEND-PARITY FAILED: {len(FAILS)} check(s)")
        for m in FAILS:
            print(f"   - {m}")
        return 1
    print("BACKEND-PARITY PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
