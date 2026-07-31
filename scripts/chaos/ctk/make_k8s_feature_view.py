#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""K8S pilot 特征视图生成器 (M4 WF-1).

读 (native trees) <case>/metadata.json + raw/metrics/metrics_v2.jsonl,
产出 (case_id, fault_window_membership bucket) 粒度的特征矩阵 CSV.

两条铁律 (spec (project docs)/archive/TASK-K8S-M4-impl-spec.md sec5.3 / recon data-dict):
  (1) GROUND-TRUTH-NOT-IN-X   : root_cause_* / fault_* / affected_services /
      composition_type / interaction_pattern / path_relation / answer_type /
      root_count 是 LABEL-only, 绝不进 X. assert-disjoint.
  (2) off-graph root (host / mysql_items_lock) 的 feature 列(svc_host__* / svc_mysql__*)
      必须来自真实刮样 series: 一个 X 列若在 real/reg 行语料里没有任何非空观测 = 无 series =
      结构性 MISS(发空列无意义, WF-2 仍 scored MISS 还白占列空间)→ TRIP assert. 语义判据:
      要求该 col 的 service 在 real/reg 语料里 >=1 非空 cell(sparse-but-real 合法 —
      svc_mysql__items_lock_granted_count_p95 只在 db_lock 案 ~3-4 例非空, 其余 ~23 例空,
      这是 off-graph root 稀疏但真实的诚实信号, NOT ">=k 例"). 兼留 host__/mysql_(裸前缀)
      防御性 ban(发列命名应是 svc_host__/svc_mysql__, 裸前缀=命名错误).
      carrier-fingerprint *_isna 指示符亦绝不进 X (per-service *_isna 非空集是 case_id
      的确定性函数 = carrier 指纹). assert no *_isna col.

行粒度: 一个 feature row = (case_id, bucket), bucket in {F1_only, F2_only, overlap}.
  bucket 取自 metrics_v2.jsonl 的 fault_window_membership 字段 (实测, 非从
  composition_type 派生 -- spec sec5.1 桶规则对 m3b2 nested/fault_masking 错,
  实测 m3b2 只产 overlap 一行). baseline/recovery 不作 feature 行 (recovery 永不 emit).

列设计 (spec sec5.2):
  - ID/meta : case_id, window_id, group_id, fault_type, provenance, system
  - LABEL   : root_cause_services, root_cause_primary(DERIVED), root_cause_set(DERIVED),
              fault_type, fault_class, fault_category, composition_type,
              interaction_pattern, path_relation, answer_type, root_count, affected_services
  - FEATURE X = svc_<service>__<metric>_p95  (服务名 re-key: traffic-probe -> carrier)
              仅该 case 实际有遥测的服务有列; 缺 metric -> NaN (NOT 0); 无 *_isna.

group_id = metadata.config.fault (the --fault value, 防 same-fault-type repeats 跨 fold).
  config.fault=null (smoke_m1_dual02/03/04) -> fallback 派生占位
  (composition_type + sorted fault_types); provenance=smoke 行不进 CV 主报, fallback
  保证 all-included 副报里 group_id 仍无 None/NaN (assert no None in CV-eligible groups).

输出:
  features_k8s.csv      provenance==real only (~17 cases)
  features_k8s_all.csv  all 27 provenances (real/reg/smoke/fix)
  UTF-8, 列定序 (ID, LABEL, X 按服务名+指标名字典序).

用法:
  python scripts/chaos/ctk/make_k8s_feature_view.py
"""
import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dataset_registry as DR  # noqa: E402  (datasets/ 唯一真相源)

# Module-level defaults (back-compat: --pilot-dir default = this path). main() resolves
# the actual SRC_DIR from argv and threads it through; these constants are kept only as
# the default source of truth for the argparse default.
DEFAULT_SRC_DIR = str(DR.NATIVE_ROOT)

# 3 个不完整目录 (无 metadata.json + 无 metrics_v2.jsonl) -> 显式排除并记录
EXCLUDED_INCOMPLETE = {"m2a_netcfg_read03", "m2b2_podfail", "smoke_m1_dual01"}

# provenance 判定 (spec sec2.2): real=17 / reg=5 / smoke=3 / fix=2
# _reg_ 后缀 (或以 reg_ 开头) = reproducibility run; smoke_ 前缀=非正式; fix 前缀=debug.
REG_CASES = {"reg_m1", "reg_m2a", "reg_m2a_v2", "m3a_reg_netdelay", "m3b1_reg_svccpu"}
SMOKE_CASES = {"smoke_m1_dual02", "smoke_m1_dual03", "smoke_m1_dual04"}
FIX_CASES = {"fix01", "fix02"}

# FEATURE 行只认这三种 membership 桶 (baseline/recovery 不进 X 行).
FEATURE_BUCKETS = ("F1_only", "F2_only", "overlap")

# ★★ 2026-07-13 修复:traffic-probe 通道【整个排除】,不再 re-key。
#
# 原实现:SLI_REKEY_TO = "pricing",把 traffic-probe 的测量值改写成 pricing 的。
# 两个致命问题:
#   (1) 【硬编码 pricing】—— 载体不是恒为 pricing。--target-service 参数化之后,
#       载体探的是【目标服务自己】(order/cart/backend/...)。再 re-key 到 pricing,
#       就会把 order 的延迟异常【凭空记到 pricing 头上】,而 GT 写着 order
#       → BARO/RCD 被主动引向 pricing → 这批 case 全军覆没,还看起来像"方法不行"。
#   (2) 【它本来就是泄漏源】—— 载体是 per-fault 选的,"探谁 = 答案"。
#       实测(不读任何遥测数值,只看载体探了哪几个端口,留一法):
#           Hit@1 = 105/140 = 0.750  >  GT 频次先验 0.643
#       retarget 之后 carrier == 根因服务本身 → 会恶化成【1:1 完美 oracle】。
#
# 修法:整个排除。信号一点不少 —— 统一探针面板(probe-panel,11 端点 × 每 2s × 全 case 恒同)
#       本来就覆盖全部目标服务,而且它【不构成 fault-correlated 泄漏】(见下方 PANEL 注释)。
#       实测数据量:dense case 里 probe-panel 2970 条 / traffic-probe 仅 270 条 —— 丢掉几乎无损。
#       载体只保留它本来的职责:采集时的 gate 证据源(在 runner 里,不进特征矩阵)。
#
# ★这与 m9_adapter.py:16-18 的硬红线【口径一致】(它早就精确匹配排除了 traffic-probe)。
#   本文件此前与它不一致,是遗留 bug。
EXCLUDE_CARRIER = True          # 硬红线,不提供开关

# ★M9 统一探针面板 (service == "probe-panel", source=http_probe): 固定 11 目标服务, 每 2s 一轮,
#   【全 case / 全 fault 恒同】-> 不构成 fault-correlated 泄漏 (与 per-fault 选的 carrier 不同).
#   必须按 labels.target_service re-key 成【真服务名】; 否则 11 个目标的探针值会被压成同一列
#   (svc_probe-panel__*_p95) 取 p95 -> 面板意义销毁 + 一根混合废柱静默进 X.
#   列名带 panel_ 前缀, 与 carrier re-key 出的 svc_pricing__request_duration_ms_p95 物理分开.
PANEL_SERVICE = "probe-panel"
PANEL_COL_PREFIX = "panel_"

# 注入车辆 stressor (host_cpu 4 案 chaos StressChaos pod) 非业务服务 -> 不发列.
# (probe-panel 【不在】此集: 它 re-key 后发的是 11 个真服务的 panel 列.)
EXCLUDE_SERVICES = {"stressor"}

# LABEL 列名 (KEEP in CSV, NEVER in X). 顺序即 CSV 中出现顺序 (ID 之后).
# 注: fault_type 不在此列 -- 它是 ID/meta 列 (与 group_id 同源同值, 见 ID_COLS),
# 同时也是 spec sec5.1 的分组键. 一列物理上只能出现一次, 故 fault_type 只在 ID_COLS.
LABEL_COLS = [
    "root_cause_services", "root_cause_primary", "root_cause_set",
    "fault_class", "fault_category", "composition_type",
    "interaction_pattern", "path_relation", "answer_type", "root_count",
    "affected_services",
    # ★2026-07-13 新增. family = case 的 signal_class(逐 case 从 groundtruth 现算,
    #   见 dataset_registry.case_family); tree = 它属于哪棵采集树.
    #   为什么必须落到 csv 里: REGISTRY 定的"评测【必须按 family 分栏报】"这条规则,
    #   在评测链上得有个落点 —— 否则 root_local(送分题, Hit@1 0.98)会把
    #   propagation(0.08, 数据集真正的难核)在合并均值里稀释成一个漂亮但骗人的数.
    #   ★ 二者都是【标签, 禁入 X】(见 GT_FORBIDDEN_IN_X): family 由 fault_type 决定,
    #     当特征就是把答案喂给模型; tree 更是纯采集出身, 是 provenance 不是信号.
    "family", "tree",
]
# ID/meta 列 (CSV 最前). fault_type = config.fault 值 = group_id (spec sec5.1).
ID_COLS = ["case_id", "window_id", "group_id", "fault_type", "provenance", "system"]

# spec sec5.3 reviewer C 审重点: GT forbidden 进 X 的完整集合 (含 ID/meta 里与 GT 同名的也禁).
GT_FORBIDDEN_IN_X = {
    "root_cause_services", "root_cause_primary", "root_cause_set",
    "fault_type", "fault_class", "fault_category", "composition_type",
    "interaction_pattern", "path_relation", "answer_type", "root_count",
    "affected_services",
    "family", "tree",   # ★ family 由 fault_type 导出 = GT 的函数; tree = 采集出身. 均禁入 X.
}


def provenance_of(case_id):
    """real | reg | smoke | fix (spec sec2.2)."""
    if case_id in REG_CASES:
        return "reg"
    if case_id in SMOKE_CASES:
        return "smoke"
    if case_id in FIX_CASES:
        return "fix"
    return "real"


def p95(values):
    """numpy-free p95 (linear interpolation, matches numpy.quantile 0.95 default 'linear')."""
    nums = []
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isnan(f) or math.isinf(f):
            continue
        nums.append(f)
    if not nums:
        return None
    nums.sort()
    if len(nums) == 1:
        return round(nums[0], 6)
    # numpy linear interpolation: pos = q*(n-1); lo=floor, hi=ceil; frac.
    q = 0.95
    pos = q * (len(nums) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return round(nums[lo], 6)
    frac = pos - lo
    val = nums[lo] + (nums[hi] - nums[lo]) * frac
    return round(val, 6)


def load_metadata(case_dir):
    with open(os.path.join(case_dir, "metadata.json"), encoding="utf-8") as f:
        return json.load(f)


def load_metrics(case_dir):
    """Yield parsed records from metrics_v2.jsonl (skip blank lines)."""
    path = os.path.join(case_dir, "raw", "metrics", "metrics_v2.jsonl")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def derive_group_id(meta):
    """group_id = metadata.config.fault; null -> fallback (composition_type + sorted ftypes).

    smoke 行 provenance=smoke 不进主报 CV; fallback 仅保证 all-included 副报 group_id 无 None.
    """
    cfg = meta.get("config") or {}
    fault = cfg.get("fault")
    if isinstance(fault, str) and fault.strip():
        return fault.strip()
    # fallback: composition_type + sorted fault_types (adapter_notes 推荐).
    gt = meta.get("ground_truth") or {}
    comp = gt.get("composition_type") or meta.get("composition_type") or "unknown"
    ftypes = sorted({t for t in (gt.get("fault_types") or []) if isinstance(t, str)})
    # 短化 fault_class token (configuration -> config) 保持紧凑.
    comp_token = {"none": "none"}.get(comp, comp)
    ftype_tokens = [{"configuration": "config"}.get(t, t) for t in ftypes]
    suffix = "_".join(ftype_tokens) if ftype_tokens else "unknown"
    return f"{comp_token}_{suffix}"


def derive_labels(meta):
    """从 metadata.ground_truth (richer parent) 抽 LABEL 列值. 返回 dict (列名->str)."""
    gt = meta.get("ground_truth") or {}
    rc_services = gt.get("root_cause_services") or []
    # root_cause_primary = root_cause_services[0] (确定性 tie-break, spec sec5.2).
    # 多根 co-primary (如 m3d catalog/user) 取首列; off-graph (host/mysql) 同理取首列.
    primary = rc_services[0] if rc_services else ""
    # root_cause_set = 去重保序 (smoke_m1_dual02 catalog-gw x2 -> 单值).
    seen = []
    for s in rc_services:
        if s not in seen:
            seen.append(s)
    rc_set = "|".join(seen)
    rc_pipe = "|".join(rc_services)

    # fault_type (行级 label): spec sec5.1 "group_id = fault_type" (config.fault),
    # 即 fault_type col 与 group_id 同源同值 (config.fault 或 fallback). 这保证
    # eval port 里 df["label"]/df["group_id"] 一致, 且 SGKF5 分层与 LOGO 分组用同一键.
    # 注意: 这是 CASE 级组合故障名 (如 net_loss_single / net_delay_x_cfg_connect),
    # 非单根 fault_types 列表; per-component fault_types 可从 component_ground_truth 重取.
    ftype_label = derive_group_id(meta)

    # fault_class: per-component -> 行级 join (sorted dedup, | 分隔). 单根=单值.
    cgt = gt.get("component_ground_truth") or []
    fclasses = sorted({c.get("fault_class") for c in cgt if c.get("fault_class")})
    fault_class = "|".join(fclasses)

    fault_category = gt.get("fault_category") or meta.get("category") or ""
    composition_type = gt.get("composition_type") or meta.get("composition_type") or ""
    interaction_pattern = gt.get("interaction_pattern") or meta.get("interaction_pattern") or ""
    # path_relation 顶层 (recon MAJOR: 永远不在 ground_truth 块, 全 27 例非 null).
    path_relation = meta.get("path_relation") or ""
    answer_type = gt.get("answer_type") or ""
    root_count = gt.get("root_count")
    if root_count is None:
        root_count = meta.get("root_count")
    root_count = "" if root_count is None else str(root_count)
    affected = gt.get("affected_services") or []
    affected_pipe = "|".join(affected)

    return {
        "root_cause_services": rc_pipe,
        "root_cause_primary": primary,
        "root_cause_set": rc_set,
        "fault_type": ftype_label,
        "fault_class": fault_class,
        "fault_category": fault_category,
        "composition_type": composition_type,
        "interaction_pattern": interaction_pattern,
        "path_relation": path_relation,
        "answer_type": answer_type,
        "root_count": root_count,
        "affected_services": affected_pipe,
    }


def registry_index():
    """realpath(case_dir) -> {"family":…, "tree":…}  (从 registry 现算, 不猜)."""
    return {os.path.realpath(c["case_dir"]): {"family": c["family"], "tree": c["tree"]}
            for c in DR.cases()}


def family_tree_of(case_dir, reg_index):
    """case 的 (family, tree)。

    native case: 直接查 registry 索引。
    非 native 的扫描目标(如交付快照 --pilot-dir): registry 里没有这条路径 ->
      family 仍【逐 case 从它自己的 groundtruth.json 现算】(同一套 signal_class 规则),
      tree 标 "" (它不属于任何采集树 —— 别编一个出来).
    未知 fault_type -> dataset_registry.signal_class 抛 ValueError (fail-loud, 故意的):
      新增故障类型忘了登记, 这里炸, 而不是被静默塞进某一族污染分栏.
    """
    hit = reg_index.get(os.path.realpath(case_dir))
    if hit:
        return hit["family"], hit["tree"]
    return DR.case_family(case_dir), ""


def build_feature_rows(case_dir, meta):
    """Pivot metrics_v2 long -> wide; 一行 per FEATURE bucket.

    返回 (list_of_row_dicts_X_only, ordered_service_metric_pairs).
    每个 row dict 已含 (bucket, 列名 svc_<svc>__<metric>_p95 -> float/NaN占位).

    case_dir = full path of the discovered case dir (any depth under SRC_DIR),
    NOT os.path.join(SRC_DIR, case_id) — supports recursive scan-depth layout.
    """
    # 聚合: bucket -> (service, metric) -> [values]
    bucket_agg = defaultdict(lambda: defaultdict(list))
    # 服务有遥测的集合 (re-key 后, 排除 stressor)
    services_seen = set()
    metric_set = set()
    for r in load_metrics(case_dir):
        bucket = r.get("fault_window_membership")
        if bucket not in FEATURE_BUCKETS:
            continue
        svc = r.get("service")
        metric = r.get("metric")
        if not svc or not metric:
            continue
        # ★2026-07-13:traffic-probe 整个丢弃(原为 re-key 到硬编码的 "pricing" —— 见文件头长注释)。
        #   精确匹配, 勿改成 startswith(会把面板 probe-panel 一起吃掉)。
        if svc == "traffic-probe":
            continue
        # ★M9 面板 re-key: probe-panel -> labels.target_service (真服务名), metric 加 panel_ 前缀.
        #   无 target_service 标签 (不该发生) -> 丢弃, 绝不留混合列.
        elif svc == PANEL_SERVICE:
            target = (r.get("labels") or {}).get("target_service")
            if not target:
                continue
            svc = str(target)
            metric = PANEL_COL_PREFIX + metric
        # 排除注入车辆 / 非业务.
        if svc in EXCLUDE_SERVICES:
            continue
        bucket_agg[bucket][(svc, metric)].append(r.get("value"))
        services_seen.add(svc)
        metric_set.add(metric)

    rows = []
    for bucket in FEATURE_BUCKETS:
        if bucket not in bucket_agg:
            continue
        agg = bucket_agg[bucket]
        row = {"window_id": bucket}
        for (svc, metric), vals in agg.items():
            col = f"svc_{svc}__{metric}_p95"
            v = p95(vals)
            row[col] = v  # None -> 写空 (NaN)
        rows.append(row)
    return rows, services_seen, metric_set


def fmt_cell(v):
    """值 -> CSV 字符串. None/NaN -> 空 (pandas keep_default_na=False 读为 '' -> to_num 转 NaN)."""
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        # trim trailing zeros for readability; keep int-valued floats clean.
        if v == int(v):
            return str(int(v))
        return repr(v)
    return str(v)


def write_csv(path, rows, id_cols, label_cols, x_cols):
    """Write rows (list of dict) with column order: id_cols + label_cols + x_cols."""
    fieldnames = list(id_cols) + list(label_cols) + list(x_cols)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL,
                           extrasaction="ignore")
        w.writeheader()
        for row in rows:
            out = {}
            for c in fieldnames:
                out[c] = fmt_cell(row.get(c))
            w.writerow(out)


def _services_with_real_series(rows, x_cols):
    """返回在 real/reg 行语料里有 >=1 非空(非 None/非 NaN/有限数值)cell 的 service 集合。

    用于 assert(b) 语义判据: off-graph root 特征列(svc_host__*/svc_mysql__*) 必须有真实
    刮样 series 才合法 —— 一个 col 若在 real/reg 全语料里全空 = 无 series = 结构性 MISS。
    sparse-but-real 合法: svc_mysql__items_lock_granted_count_p95 只在 db_lock 案(~3-4 例)
    非空, 其余 ~23 例空, 这是 off-graph root 稀疏但真实的诚实信号(判据是 ">=1 cell" 非 ">=k 例")。
    """
    services = set()
    for col in x_cols:
        # col 形如 svc_<service>__<metric>_p95; 解析 service
        body = col[len("svc_"):-len("_p95")] if col.startswith("svc_") and col.endswith("_p95") else ""
        if "__" not in body:
            continue
        svc = body.split("__", 1)[0]
        for r in rows:
            if r.get("provenance") not in ("real", "reg"):
                continue
            v = r.get(col)
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if math.isnan(f) or math.isinf(f):
                continue
            services.add(svc)
            break  # 该 service 已有 >=1 实观测, 不必再扫该 col 其余行
    return services


def run_asserts(rows, x_cols, case_group_index):
    """Enforce 5 iron-rule asserts (raise AssertionError if violated)."""
    # (a) no GT/label col name in X col set.
    leaked = GT_FORBIDDEN_IN_X & set(x_cols)
    assert not leaked, f"ASSERT(a) GT label leaked into X cols: {sorted(leaked)}"

    # (b) off-graph root feature cols (svc_host__* / svc_mysql__*) must come from a REAL
    #     scraped series: 要求该 col 的 service 在 real/reg 行语料里有 >=1 非空 cell.
    #     sparse-but-real OK (svc_mysql__items_lock_granted_count_p95 只在 db_lock 案 ~3-4 例非空);
    #     全空 col = 无 series = 结构性 MISS = TRIP. 兼留 host__/mysql_ 裸前缀防御性 ban(命名错误).
    real_series_svcs = _services_with_real_series(rows, x_cols)
    bad_offgraph = []
    for c in x_cols:
        # 防御性裸前缀 ban(发列命名应是 svc_host__/svc_mysql__; 裸 host__/mysql_ = 命名错误)
        if c.startswith("host__") or c.startswith("mysql_"):
            bad_offgraph.append(c)
            continue
        if c.startswith("svc_host__") or c.startswith("svc_mysql__"):
            # 解析 service(svc_<svc>__<metric>_p95)并要求该 service 有真实刮样 series
            body = c[len("svc_"):-len("_p95")]
            svc = body.split("__", 1)[0] if "__" in body else ""
            if svc not in real_series_svcs:
                bad_offgraph.append(c)
    assert not bad_offgraph, (
        f"ASSERT(b) off-graph feature cols without real series: {bad_offgraph[:5]} "
        f"(services-with-real-series={sorted(real_series_svcs)})")

    # (c) no *_isna carrier-fingerprint col in X.
    bad_isna = [c for c in x_cols if c.endswith("_isna")]
    assert not bad_isna, f"ASSERT(c) carrier-fingerprint *_isna cols in X: {bad_isna[:5]}"

    # (d) all rows of same case_id share same group_id (multi-window same case -> same group).
    case_groups = defaultdict(set)
    for r in rows:
        case_groups[r["case_id"]].add(r["group_id"])
    viol = {c: g for c, g in case_groups.items() if len(g) > 1}
    assert not viol, f"ASSERT(d) same case_id split across groups: {viol}"

    # (e) no None/NaN/empty group_id for CV-eligible rows (provenance in real/reg).
    bad_groups = []
    for r in rows:
        if r["provenance"] in ("real", "reg"):
            g = r["group_id"]
            if g is None or (isinstance(g, float) and math.isnan(g)) or g == "":
                bad_groups.append((r["case_id"], r["window_id"], g))
    assert not bad_groups, f"ASSERT(e) None/empty group_id in CV-eligible rows: {bad_groups[:5]}"

    # (f) 每行都有合法 family(评测分栏的落点). 空/非法 = 分栏会静默错栏 -> TRIP.
    legal_fams = set(DR.family_names())
    bad_fam = sorted({(r["case_id"], r.get("family")) for r in rows
                      if r.get("family") not in legal_fams})
    assert not bad_fam, (
        f"ASSERT(f) rows with missing/illegal family: {bad_fam[:5]} "
        f"(legal={sorted(legal_fams)})")

    # sanity: window_id values all in FEATURE_BUCKETS.
    for r in rows:
        assert r["window_id"] in FEATURE_BUCKETS, \
            f"row bucket {r['window_id']} not in FEATURE_BUCKETS ({r['case_id']})"


def main(argv=None):
    ap = argparse.ArgumentParser(description="K8S pilot feature-view generator.")
    ap.add_argument(
        "--pilot-dir",
        default=DEFAULT_SRC_DIR,
        help="dataset root to scan (default: <output-root>/k8s_pilot; "
             "may point at k8s_pilot/{single,dual,triple}).",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="where to write features_k8s{,_all}.csv "
             "(default: (runtime) features -- see dataset_registry.FEATURES_DIR). "
             "NOTE: the historical default was in-place (= --pilot-dir = the NATIVE tree); "
             "that wrote derived artifacts into read-only source data and is now forbidden.",
    )
    args = ap.parse_args(argv)

    src_dir = args.pilot_dir
    # ★ 2026-07-13:默认输出不再是 src_dir(= native 采集树)。派生物一律落 _runtime/features/。
    #   --out-dir 覆盖仍保留,但同样过 assert_not_native —— 显式指向 native 也不行。
    out_dir = args.out_dir or str(DR.FEATURES_DIR)
    DR.assert_not_native(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    out_real = os.path.join(out_dir, "features_k8s.csv")
    out_all = os.path.join(out_dir, "features_k8s_all.csv")
    DR.assert_not_native(out_real)
    DR.assert_not_native(out_all)

    if not os.path.isdir(src_dir):
        print(f"[ERR] k8s_pilot dir not found: {src_dir}", file=sys.stderr)
        return 2

    # Recursive discovery: a "case dir" = dir containing BOTH metadata.json AND
    # raw/metrics/metrics_v2.jsonl, found at ANY depth under src_dir (supports future
    # cases/<root_cause>/<case_id>/ nesting, not just the current flat layout).
    # The <case>/mr2/ subdir has metadata.json but NO raw/metrics/metrics_v2.jsonl ->
    # the AND-filter excludes it (mr2/ must NOT be picked up as a case).
    # usable = list of (case_id, case_dir) tuples; case_id = basename of the case dir.
    skipped = []
    usable = []
    seen_ids = set()
    for dirpath, dirnames, filenames in os.walk(src_dir):
        if "metadata.json" not in filenames:
            continue
        case_dir = dirpath
        metrics_path = os.path.join(case_dir, "raw", "metrics", "metrics_v2.jsonl")
        if not os.path.exists(metrics_path):
            continue  # e.g. <case>/mr2/ : has metadata.json but no raw/metrics/ → not a case
        case_id = os.path.basename(case_dir)
        if case_id in EXCLUDED_INCOMPLETE:
            skipped.append((case_id, "excluded_incomplete (no metadata.json + no metrics_v2.jsonl)"))
            continue
        if case_id in seen_ids:
            # dup basename across depths would collide in case_id keying; skip later one.
            skipped.append((case_id, f"duplicate case_id (already seen at deeper/other path): {case_dir}"))
            continue
        seen_ids.add(case_id)
        usable.append((case_id, case_dir))
    # sort by case_id for stable ordering (matches old sorted() output on flat layout).
    usable.sort(key=lambda t: t[0])

    # soft WARN: never a hard mismatch (case count grows over time; 27 was a stale hardcode).
    if not usable:
        print(f"[WARN] 0 usable cases discovered under {src_dir}")
    else:
        print(f"[INFO] discovered {len(usable)} usable case dirs (recursive scan)")

    all_rows = []
    case_group_index = {}  # case_id -> group_id (for assert d)
    per_case_buckets = []  # (case_id, provenance, group_id, [buckets], n_services)
    global_x_cols = set()
    reg_index = registry_index()   # realpath(case_dir) -> {family, tree}

    for case_id, case_dir in usable:
        try:
            meta = load_metadata(case_dir)
        except Exception as e:
            skipped.append((case_id, f"metadata.json load failed: {e}"))
            continue

        # family/tree: registry 现算. groundtruth.json 缺失 -> skip(与 metadata 缺失同款);
        # 但【未知 fault_type 不 skip, 直接抛】—— 静默归族会污染分栏, 比崩掉危险.
        try:
            fam, tree = family_tree_of(case_dir, reg_index)
        except FileNotFoundError as e:
            skipped.append((case_id, f"groundtruth.json missing (cannot derive family): {e}"))
            continue

        # WF-1A abort gate: must read root_cause_services from ground_truth (spec sec11).
        gt = meta.get("ground_truth") or {}
        rc = gt.get("root_cause_services")
        if not rc:
            skipped.append((case_id, "WF-1A abort gate: ground_truth.root_cause_services empty/absent"))
            continue

        labels = derive_labels(meta)
        group_id = derive_group_id(meta)
        prov = provenance_of(case_id)
        case_group_index[case_id] = group_id

        feat_rows, services_seen, metric_set = build_feature_rows(case_dir, meta)
        for fr in feat_rows:
            row = {
                "case_id": case_id,
                "window_id": fr["window_id"],
                "group_id": group_id,
                "fault_type": labels["fault_type"],
                "provenance": prov,
                "system": meta.get("system", "recweb2"),
            }
            row.update(labels)
            row["family"] = fam    # ★ LABEL, 禁入 X (assert (a) 会核)
            row["tree"] = tree
            # X cols (svc_*__*_p95).
            for col, val in fr.items():
                if col == "window_id":
                    continue
                row[col] = val
                global_x_cols.add(col)
            all_rows.append(row)

        per_case_buckets.append((case_id, prov, group_id,
                                 sorted({r["window_id"] for r in feat_rows}),
                                 len(services_seen)))

    if not all_rows:
        print("[ERR] no feature rows produced", file=sys.stderr)
        return 2

    # X 列定序: 服务名字典序, 同服务按 metric 名字典序 (col = svc_<svc>__<metric>_p95).
    def x_sort_key(col):
        body = col[len("svc_"):-len("_p95")]  # <svc>__<metric>
        if "__" in body:
            svc, metric = body.split("__", 1)
        else:
            svc, metric = body, ""
        return (svc, metric)

    x_cols_ordered = sorted(global_x_cols, key=x_sort_key)

    # ---- asserts ----
    run_asserts(all_rows, x_cols_ordered, case_group_index)

    # ---- split real vs all ----
    real_rows = [r for r in all_rows if r["provenance"] == "real"]

    write_csv(out_real, real_rows, ID_COLS, LABEL_COLS, x_cols_ordered)
    write_csv(out_all, all_rows, ID_COLS, LABEL_COLS, x_cols_ordered)

    # ---- report ----
    print(f"[k8s-feature-view] usable cases: {len(usable)} | skipped: {len(skipped)}")
    if skipped:
        for c, why in skipped:
            print(f"  SKIP {c}: {why}")

    def dist(rows, col):
        d = defaultdict(int)
        for r in rows:
            d[r.get(col, "")] += 1
        return dict(sorted(d.items(), key=lambda kv: (-kv[1], kv[0])))

    n_real = len(real_rows)
    n_all = len(all_rows)
    n_x = len(x_cols_ordered)
    n_label = len(LABEL_COLS)
    n_id = len(ID_COLS)

    print(f"[features_k8s.csv]      {n_real} rows  (provenance==real only)")
    print(f"[features_k8s_all.csv]  {n_all} rows  (all provenances)")
    print(f"  total cols : {n_id + n_label + n_x}  (id/meta={n_id} label={n_label} X={n_x})")
    print(f"  X cols     : svc_<service>__<metric>_p95  (sorted by svc,metric)")

    print("  --- label distributions (all-included) ---")
    for col in ("family", "tree", "root_cause_primary", "fault_type", "fault_class", "group_id"):
        d = dist(all_rows, col)
        print(f"  {col} ({len(d)} distinct):")
        for k, v in d.items():
            print(f"      {k!r:40s} -> {v}")

    # provenance split.
    prov_dist = dist(all_rows, "provenance")
    print(f"  provenance: {prov_dist}")

    # per-case bucket summary (compact, ASCII).
    print("  --- per-case buckets (case | prov | group | buckets | n_svc_with_telemetry) ---")
    for case_id, prov, gid, buckets, n_svc in per_case_buckets:
        print(f"    {case_id:24s} | {prov:5s} | {gid:28s} | {','.join(buckets):20s} | svc={n_svc}")

    # assert outcomes (echo passed).
    print("  --- asserts (all PASSED) ---")
    print("    (a) GT/label cols disjoint from X                : OK")
    print("    (b) svc_host__/svc_mysql__ cols need real series : OK")
    print("    (c) no *_isna carrier-fingerprint col in X       : OK")
    print("    (f) every row has a legal family (eval 分栏落点)  : OK")
    print("    (d) same case_id -> same group_id (multi-window) : OK")
    print("    (e) no None/empty group_id in real/reg rows      : OK")

    print(f"[out] {out_real}")
    print(f"[out] {out_all}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
