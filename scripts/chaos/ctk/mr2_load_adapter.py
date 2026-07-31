#!/usr/bin/env python
"""mr2_load_adapter.py — RecWeb2 k8s_pilot → 上游 mr2 consumer 载入前置 adapter (M4b Phase 4, adapter #4).

目的(acceptance #4, 硬载入 gate, 非延后 align 项):
  上游 mr2 consumer 在我们的 raw 上 (a) filter `record['source']=='prometheus'` → 现返 0 行
  (我们 source 用 cadvisor/kube_state/otel/http_probe/nginx_config/mysql/host), 且
  (b) 直接索引 `record['quality']` / `record['container']` → 现 KeyError(我们两 emit 点没发)。
  本 adapter 把 per-case 的 raw 长格式 JSONL + metadata + quality 归一为 mr2 可载入形态,
  写到 <case>/mr2/ 子目录,**绝不变更 raw/**。

变换 spec((project docs)/archive/TASK-K8S-M4b-impl-spec.md §10 MAJOR/MINOR + §11 acceptance #4;
        AUTHORITATIVE target = (project docs)/REF-shijie-k8s-v2-sample-schema.md):

METRICS(raw/metrics/metrics_v2.jsonl,一条一 JSON 记录):
  - 每条 inject 'quality':'observed'(现缺)
  - 每条 inject 'container':labels.container 若有,否则从 entity/service/entity_type 派生(现缺)
  - alias 'source':{http_probe,nginx_config,cadvisor,kube_state,otel,mysql,prometheus,host,...}
           -> 'prometheus'(原始 source 保留在 labels.source_raw 作 provenance)
  - normalize 'unit':bool->boolean / count->attempts / per_second->periods_per_second /
           seconds->epoch_seconds;REF 已 attest 的 'milliseconds' 等保留;不在 上游 vocab 且
           REF silent 的(如 'code')保留原值 + 收集到 unmapped_units WARN 列表(绝不静默塞会
           破 consumer 的值,但也不丢)。

METADATA(metadata.json):
  - schema_version 'k8s.v2.1' -> 'v1.2'(上游样例实读;直接覆盖)
  - observation_stages:array{stage,window_start,window_end,seconds,...} -> object keyed by stage;
    内层 上游 确切 key 集(window_start_at/window_end_at/window_seconds/poll_interval_seconds
    + *_manifest path + *_filter{stage} + *_validation_status + gate_passed + status)
  - root_causes:array{service,...} -> string list(服务名)
  - component_fault_windows {F1:{start,end}} -> {F1:{start_time,end_time}}
  - overlap_window {start,end,duration_seconds} -> {start_time,end_time,duration_seconds}
    (顶层 + ground_truth.overlap_window 两处都归一)
  - validation_results:status token 'pass'->'passed';暴露 上游 8 canonical gate 子集
    (MR2_CANONICAL_GATES)
  - canonical 答案字段 root_cause_services(string list)已对齐 -> 不改

MANIFEST(raw/metrics/manifest.json -> mr2/manifest.json):
  - {schema_version:metrics.v2, record_count, stage_windows{start,end}, sources}
    -> 上游 结构({schema_version:metrics-manifest.v2.1, storage_layout:single_dir_stage_tagged,
    artifact_root:raw/metrics, files:[{stages,artifact,kind}], stage_windows{<与 observation_stages
    同构>}, validation{valid}})。stage_windows 每阶段复用 _norm_stage_dict。

QUALITY(raw/metrics/quality.json):
  - wrapper 'by_stage'->'stages';top key 集=[schema_version,stages](无 overall_valid);
    schema_version='metrics-quality.v2.1';per-stage 补 schema_version+stage key;
    per-stage 内 27-metric required_metrics list byte-identical 不动

GROUNDTRUTH(native groundtruth.json):
  - overlap_window {start,end,duration_seconds} -> {start_time,end_time,duration_seconds}
  - component_fault_windows 每 F 内层 {start,end} -> {start_time,end_time}
  - component_ground_truth[] 每 item:删 chaos_engine/crd/intensity(保留上游 10-key 集)
  - 顶层:删 run_id/affected_services/isolation_degraded/path_relation/
    root_metric_contract/sli_gate(保留上游 13-key 集)
  - **绝不变更答案值**(root_cause_services/target_component/role 等原样),只动 key 名 +
    删 RecWeb2 provenance 加性 key。

约束:
  - READ-ONLY on raw/(绝不改 raw/metrics|traces|logs 或现有 metadata.json/groundtruth.json)
  - ★ 2026-07-13 输出【搬出 native 树】:落 dataset_registry.runtime_dir("package", tag)/<case_id>/
    (metrics_v2.jsonl / metadata.json / quality.json / manifest.json / groundtruth.json)。
    旧行为是写 <case>/mr2/ —— 即往只读的采集树里拉派生物,后果:下游 4 个脚本得各写
    防御性 skip 才不至于把 mr2/ 当成 case 重复计数;更糟的是 stale mr2/ 会被"存在就复用",
    错 GT 就是这么进的交付包。现在 adapt_case() 进门先 assert_not_native(out)。
  - 保留 provenance(additive;原始放 namespaced key;不丢 RecWeb2 细节)
  - 幂等(重跑覆盖输出,raw 不变)
  - stdlib only(json/pathlib/argparse)
  - 风格承 scripts/chaos/ctk/retag_membership_recovery.py

用法: python mr2_load_adapter.py --in <case_dir>   # 单 case
      python mr2_load_adapter.py --in <root> --all  # root 下所有含 metadata.json 的 case
"""
import json
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dataset_registry as DR  # noqa: E402  (派生物落盘位置 + native 写入闸)

# ---------------------------------------------------------------------------
# REF-derived constants((project docs)/REF-shijie-k8s-v2-sample-schema.md)
# ---------------------------------------------------------------------------

# REF §1: metadata schema_version = v1.2(上游真样例实读)
MR2_META_SCHEMA_VERSION = "v1.2"
# quality.json schema_version = "metrics-quality.v2.1"(上游 raw/metrics/quality.json 实读,
# top + 每 stage 内都有同值)。
MR2_QUALITY_SCHEMA_VERSION = "metrics-quality.v2.1"
# raw/metrics/manifest.json schema_version = "metrics-manifest.v2.1"(上游实读)。
MR2_MANIFEST_SCHEMA_VERSION = "metrics-manifest.v2.1"
# metrics.v2 schema_version 沿用(REF §2 实读 metrics.v2,byte-一致 不动)

# source alias 目标:REF §2 line 27 source 域 = prometheus/http_probe/nginx_config(她真样例里
# prometheus=21 指标(cAdvisor+kube-state)主导)。consumer filter source=='prometheus' 故
# 我们所有源都 alias 到 'prometheus'(原始保留 labels.source_raw)。
MR2_SOURCE_ALIAS = "prometheus"

# unit normalize(spec §10 MAJOR 显式映射 + REF §4 实读值):
#   bool->boolean / count->attempts / per_second->periods_per_second / seconds->epoch_seconds
UNIT_NORMALIZE = {
    "bool": "boolean",
    "count": "attempts",
    "per_second": "periods_per_second",
    "seconds": "epoch_seconds",
}

# 上游 vocab(spec §10 给的 canonical unit 集合 + REF §4 实读 'milliseconds'):
#   REF §4 line 56 实读 request_duration_ms 的 unit="milliseconds" → milliseconds 为合法 mr2
#   unit(虽不在 spec 列举 vocab 内,但 REF 真实文件 attest)。其余不在该集合且 REF silent 的
#   unit 保留原值并进 unmapped_units WARN 列表(不静默塞坏值,也不丢)。
MR2_UNIT_VOCAB_KNOWN = {
    "boolean", "attempts", "replicas", "restarts", "events",
    "epoch_seconds", "periods_per_second", "seconds_per_second",
    "milliseconds",  # REF §4 实读 attest(request_duration_ms unit="milliseconds")
}

# REF §5 + spec §10 MINOR: 上游 8 canonical validation gate 子集
MR2_CANONICAL_GATES = [
    "component_injections_completed",
    "component_windows_recorded",
    "metric_stream_quality",
    "root_metric_contract",
    "each_root_signal_present",
    "trace_policy_satisfied",
    "three_stage_logs_complete",
    "recovery_confirmed",
]

# 上游 raw/metrics/manifest.json artifact path(单文件统一时序)。
MR2_METRICS_ARTIFACT = "raw/metrics/metrics_v2.jsonl"

# REF §6(上游样例 groundtruth.json 实读):顶层恰 13 key 的白名单(strict mirror,
# 不多不少)。删去我们 native 多出的 run_id/affected_services/isolation_degraded/
# path_relation/root_metric_contract/sli_gate 6 个加性 key(值属 RecWeb2 provenance,
# 不进 groundtruth 交付——保留在 native groundtruth.json 不丢)。
MR2_GT_TOP_KEYS = (
    "sample_id",
    "answer_type",
    "root_count",
    "fault_category",
    "composition_type",
    "interaction_pattern",
    "root_cause_services",
    "root_cause_instances",
    "fault_types",
    "injection_faults",
    "component_ground_truth",
    "component_fault_windows",
    "overlap_window",
)
# REF §6:component_ground_truth[] 每 item 恰 10 key 的白名单(strict mirror)。删去
# 我们 native 多出的 chaos_engine/crd/intensity 3 个加性 key(保留在 native)。
MR2_GT_CG_KEYS = (
    "fault_instance_id",
    "fault_class",
    "fault_type",
    "injection_fault",
    "target_component",
    "target_container",
    "role",
    "injected_at",
    "recovered_at",
    "status",
)
# 上游 observation_stages 内层确切 key 集(strict mirror,不多不少)。
#   window_start_at / window_end_at / window_seconds / poll_interval_seconds /
#   metrics_manifest / metrics_filter / traces_manifest / traces_filter /
#   logs_manifest / logs_filter / metrics_validation_status /
#   traces_validation_status / logs_validation_status / gate_passed / status
MR2_STAGE_MANIFEST_PATHS = {
    "metrics_manifest": "raw/metrics/manifest.json",
    "traces_manifest": "raw/traces/manifest.json",
    "logs_manifest": "raw/logs/manifest.json",
}


# ---------------------------------------------------------------------------
# metrics_v2 transform
# ---------------------------------------------------------------------------

def _derive_container(rec):
    """labels.container 若有则用;否则按 entity_type 派生:
    endpoint -> 'traffic-probe'(HTTP probe 无容器)/ service(回退);
    container -> service(cAdvisor,entity=service=容器逻辑名);
    pod/deployment -> service;service/otel -> service;config -> service。
    绝不返 None(consumer 直索引 record['container'] 不可 KeyError)。"""
    labels = rec.get("labels") or {}
    c = labels.get("container")
    if c is not None:
        return c
    et = rec.get("entity_type")
    svc = rec.get("service")
    # entity 通常是服务逻辑名(cadvisor/kube_state/otel/nginx_config);endpoint 例外
    if et == "endpoint":
        return svc or "traffic-probe"
    return svc or rec.get("entity") or "unknown"


def transform_metric_record(rec, stats):
    """单条 metrics_v2 记录 → mr2 形态。stats 累积 unmapped source/unit 统计(供 WARN)。"""
    out = dict(rec)  # 浅拷贝,原始不动
    labels = dict(rec.get("labels") or {})

    # (a) source alias → prometheus(原始保留 labels.source_raw)
    raw_source = rec.get("source")
    if raw_source != MR2_SOURCE_ALIAS:
        labels.setdefault("source_raw", raw_source)
        if raw_source is not None and raw_source not in stats["unmapped_sources"]:
            stats["unmapped_sources"].add(raw_source)
        out["source"] = MR2_SOURCE_ALIAS
    else:
        # source 已是 prometheus(如 host/mysql emit 走 prometheus source)→ 不动
        pass

    # (b) container inject(派生或取 labels.container)
    out["container"] = _derive_container({**rec, "labels": labels})

    # (c) quality inject(每条 'observed')
    out["quality"] = "observed"

    # (d) unit normalize(bool/count/per_second/seconds)或保留 + 收集 unmapped
    raw_unit = rec.get("unit")
    if raw_unit in UNIT_NORMALIZE:
        out["unit"] = UNIT_NORMALIZE[raw_unit]
    elif raw_unit in MR2_UNIT_VOCAB_KNOWN:
        out["unit"] = raw_unit  # 已合规(milliseconds 等 REF-attest)
    else:
        # REF silent + 不在 vocab:保留原值 + 收集 WARN(不静默塞坏值,也不丢)
        if raw_unit is not None and raw_unit not in stats["unmapped_units"]:
            stats["unmapped_units"].add(raw_unit)
        # out["unit"] 保持原值

    out["labels"] = labels
    return out


def transform_metrics_file(in_path, out_path, stats):
    """读 raw metrics_v2.jsonl,逐行 transform,写到 <case>/mr2/metrics_v2.jsonl。返回行数。"""
    n = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(in_path, encoding="utf-8") as fin, \
         open(out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            try:
                rec = json.loads(raw)
            except Exception:
                # 非 JSON 行原样透写(不应出现于 metrics_v2;保守)
                fout.write(raw + "\n")
                continue
            new_rec = transform_metric_record(rec, stats)
            fout.write(json.dumps(new_rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    return n


# ---------------------------------------------------------------------------
# metadata transform
# ---------------------------------------------------------------------------

def _norm_validation_results(vr):
    """validation_results:status token 'pass'->'passed'(余不变);暴露 8 canonical gate 子集;
    我们额外 RecWeb2 gate 保留在 _recweb2_validation_results。"""
    norm = []
    canonical_subset = []
    for item in vr or []:
        if not isinstance(item, dict):
            continue
        ni = dict(item)
        if ni.get("status") == "pass":
            ni["status"] = "passed"
        norm.append(ni)
        if ni.get("id") in MR2_CANONICAL_GATES:
            canonical_subset.append(dict(ni))
    return {
        "validation_results": canonical_subset,  # 上游 8 canonical gate 子集
        "_recweb2_validation_results": norm,      # 完整(additive,provenance)
    }


def _norm_stage_dict(s, observed_snapshots):
    """单阶段 dict -> 上游 observation_stages[stage] / manifest.stage_windows[stage] 确切
    key 集(strict mirror,不多不少)。输入 s 来自 raw observation_stages 项:
    {stage, window_start, window_end, seconds, poll_interval_seconds,
     expected_snapshots, observed_snapshots, gate_passed}。

    映射:
      window_start -> window_start_at
      window_end   -> window_end_at
      seconds      -> window_seconds
      poll_interval_seconds / gate_passed 保留(上游有)
      丢 stage / expected_snapshots / observed_snapshots(外层 key + quality.json 各持)
      补 *_manifest path + *_filter={"stage":<stage>} +
          metrics/traces/logs_validation_status + status

    status 诚实:observed_snapshots>0 -> "captured_ok" + 三模态 valid + gate_passed 透传;
                否则 -> "captured_failed" + 三模态 invalid(诚实,绝不假设)。
    """
    stage = s.get("stage")
    has_data = isinstance(observed_snapshots, int) and observed_snapshots > 0
    valid_tok = "valid" if has_data else "invalid"
    status_tok = "captured_ok" if has_data else "captured_failed"
    d = {
        "window_start_at": s.get("window_start"),
        "window_end_at": s.get("window_end"),
        "window_seconds": s.get("seconds"),
        "poll_interval_seconds": s.get("poll_interval_seconds"),
    }
    # manifest paths + stage filters(三模态)。
    for mk, mv in MR2_STAGE_MANIFEST_PATHS.items():
        d[mk] = mv
    d["metrics_filter"] = {"stage": stage}
    d["traces_filter"] = {"stage": stage}
    d["logs_filter"] = {"stage": stage}
    d["metrics_validation_status"] = valid_tok
    d["traces_validation_status"] = valid_tok
    d["logs_validation_status"] = valid_tok
    # gate_passed 透传(上游有此 key);raw 可能是 true/false/null,皆如实落。
    d["gate_passed"] = s.get("gate_passed")
    d["status"] = status_tok
    return d


def _norm_observation_stages(stages):
    """observation_stages:array{stage,window_start,window_end,...} -> object keyed by stage,
    每阶段值为 上游 确切 key 集(strict mirror)。原始数组保留在 _recweb2_observation_stages。"""
    if not isinstance(stages, list):
        return {}, stages
    obj = {}
    for s in stages:
        if not isinstance(s, dict) or "stage" not in s:
            continue
        obj[s["stage"]] = _norm_stage_dict(s, s.get("observed_snapshots"))
    return obj, stages


def _norm_root_causes(rcs):
    """root_causes:array{service,...} -> string list(服务名);完整对象保留 _recweb2_root_causes。"""
    if not isinstance(rcs, list):
        return [], rcs
    names = []
    for r in rcs:
        if isinstance(r, dict) and r.get("service"):
            names.append(r["service"])
        elif isinstance(r, str):
            names.append(r)
    return names, rcs


def _norm_component_fault_windows(cfw):
    """component_fault_windows {F1:{start,end}} -> {F1:{start_time,end_time}}。原始保留
    _recweb2_component_fault_windows(注意:顶层 component_fault_windows 在 raw metadata 是
    start/end,但我们 ground_truth.component_fault_windows 也在;两处都归一顶层键,gt 内不动
    因是 RecWeb2 provenance)。"""
    if not isinstance(cfw, dict):
        return {}
    out = {}
    for fid, w in cfw.items():
        if isinstance(w, dict):
            nw = {}
            for k, v in w.items():
                if k == "start":
                    nw["start_time"] = v
                elif k == "end":
                    nw["end_time"] = v
                else:
                    nw[k] = v
            out[fid] = nw
        else:
            out[fid] = w
    return out


def _norm_overlap_window(ow):
    """overlap_window {start,end,duration_seconds} -> {start_time,end_time,duration_seconds}
    (上游顶层 + ground_truth.overlap_window 实读 = start_time/end_time,无 start/end)。"""
    if not isinstance(ow, dict):
        return ow
    out = {}
    for k, v in ow.items():
        if k == "start":
            out["start_time"] = v
        elif k == "end":
            out["end_time"] = v
        else:
            out[k] = v
    return out


def _innermost_overlap_window(ows, native_single):
    """N-way innermost overlap window from overlap_windows(plural): 取 "F" 腿最多的 key
    (平手取 duration_seconds 最小)。overlap_windows 缺失/空则回退 native_single。
    值全取自输入,绝不重算/臆造(triple 命中 F1F2F3;dual/single 无复数键回退 native 2-way)。"""
    if isinstance(ows, dict) and ows:
        def _leg_rank(k):
            w = ows.get(k) or {}
            dur = w.get("duration_seconds")
            return (str(k).count("F"), -(dur if isinstance(dur, (int, float)) else 0.0))
        cand = ows.get(max(ows.keys(), key=_leg_rank))
        if isinstance(cand, dict):
            return cand
    return native_single


def _norm_manifest(raw_manifest, observation_stages_arr, quality):
    """raw/metrics/manifest.json({schema_version:metrics.v2,record_count,stage_windows,sources})
    -> 上游 结构({schema_version:metrics-manifest.v2.1, storage_layout:single_dir_stage_tagged,
    artifact_root:raw/metrics, files:[{stages,artifact,kind}], stage_windows:{...},
    validation:{valid}})。

    stage_windows 每阶段与 observation_stages 同构:复用 _norm_stage_dict(window_seconds 等取自
    observation_stages 项,因 raw manifest.stage_windows 仅 {start,end} 无 seconds)。
    validation.valid 取自 quality.json(若任一 stage invalid 则 false,否则 true)。
    """
    if not isinstance(raw_manifest, dict):
        return raw_manifest

    # 用 observation_stages(更完整)构造 stage_windows;manifest.stage_windows 仅作 start/end 回退。
    manifest_stage_windows = raw_manifest.get("stage_windows") if isinstance(
        raw_manifest.get("stage_windows"), dict) else {}
    obs_by_stage = {}
    if isinstance(observation_stages_arr, list):
        for s in observation_stages_arr:
            if isinstance(s, dict) and s.get("stage"):
                obs_by_stage[s["stage"]] = s

    # stages 顺序:优先 raw manifest.stage_windows 的 key 顺序,补 observation_stages 的额外。
    stage_order = list(manifest_stage_windows.keys())
    for st in obs_by_stage:
        if st not in stage_order:
            stage_order.append(st)

    stage_windows = {}
    for st in stage_order:
        obs_s = obs_by_stage.get(st)
        if obs_s:
            stage_windows[st] = _norm_stage_dict(obs_s, obs_s.get("observed_snapshots"))
        else:
            # 无 observation_stages 项:用 manifest.stage_windows[st] 的 {start,end} 兜底建一个
            # 最小 stage dict(window_start_at/end_at 来自 start/end,window_seconds=None)。
            mw = manifest_stage_windows.get(st, {})
            stage_windows[st] = _norm_stage_dict({
                "stage": st,
                "window_start": mw.get("start"),
                "window_end": mw.get("end"),
                "seconds": None,
                "poll_interval_seconds": None,
                "observed_snapshots": None,
                "gate_passed": None,
            }, None)

    # validation.valid:从 quality.json 推(stage 间 valid 字段全 true 才 true)。
    valid = True
    if isinstance(quality, dict):
        stages_q = quality.get("stages") or quality.get("by_stage") or {}
        if isinstance(stages_q, dict) and stages_q:
            valid = all(
                bool(sd.get("valid")) for sd in stages_q.values()
                if isinstance(sd, dict)
            )

    out = {
        "schema_version": MR2_MANIFEST_SCHEMA_VERSION,
        "storage_layout": "single_dir_stage_tagged",
        "artifact_root": "raw/metrics",
        "files": [{
            "stages": stage_order,
            "artifact": MR2_METRICS_ARTIFACT,
            "kind": "unified_timeseries",
        }],
        "stage_windows": stage_windows,
        "validation": {"valid": valid},
    }
    return out


def transform_metadata(meta):
    """metadata.json → mr2 形态(strict mirror 上游样例顶层 key 集)。返回新 dict(原始 meta 不动)。

    严格镜像:
      - schema_version -> v1.2(原 k8s.v2.1 直接覆盖,不再保留 _recweb2_* additive)
      - observation_stages -> object keyed by stage,内层 上游 确切 key 集
      - root_causes -> string list(服务名)
      - component_fault_windows -> start_time/end_time
      - overlap_window -> start_time/end_time/duration_seconds
      - validation_results -> 8 canonical gate 子集,status token pass->passed
    不再 emit _recweb2_* additive key(strict mirror = 与 上游 key 集一致)。
    """
    out = dict(meta)  # 浅拷贝顶层

    # schema_version: k8s.v2.1 -> v1.2(直接覆盖,strict mirror)
    out["schema_version"] = MR2_META_SCHEMA_VERSION

    # observation_stages: array -> object keyed by stage(上游 确切内层 key 集)
    stages_obj, _ = _norm_observation_stages(meta.get("observation_stages"))
    out["observation_stages"] = stages_obj

    # root_causes: object-list -> string-list
    rc_names, _ = _norm_root_causes(meta.get("root_causes"))
    out["root_causes"] = rc_names

    # component_fault_windows: start/end -> start_time/end_time
    out["component_fault_windows"] = _norm_component_fault_windows(
        meta.get("component_fault_windows"))

    # overlap_window + ground_truth 内层归一(上游 strict mirror):
    #   - overlap_window start/end -> start_time/end_time (顶层 + ground_truth.overlap_window)
    #   - ground_truth.component_fault_windows[Fx] start/end -> start_time/end_time (FIX: 旧版漏 gt 内层)
    #   - ground_truth.sample_id 补 (上游 ground_truth 包装内含 sample_id)
    # G1: 顶层 overlap_window 对齐上游 = N-way innermost(F1∩F2∩…∩FN),非 native 2-way F1F2 blob
    #   (真 innermost 在 overlap_windows[全腿 key],triple="F1F2F3";复数缺失回退 native)。
    out["overlap_window"] = _norm_overlap_window(
        _innermost_overlap_window(meta.get("overlap_windows"), meta.get("overlap_window")))
    gt = out.get("ground_truth")
    if isinstance(gt, dict):
        gt = dict(gt)
        gt["overlap_window"] = out["overlap_window"]  # G1-gt: gt.overlap_window == 顶层 innermost(上游 top==gt,不留 2-way 分歧)
        gt["component_fault_windows"] = _norm_component_fault_windows(gt.get("component_fault_windows"))
        if "sample_id" not in gt and out.get("sample_id"):
            gt["sample_id"] = out["sample_id"]
        out["ground_truth"] = gt

    # G4: traffic_error_stats.overlap 子对象(镜像上游 mr3 overlap 3-key {error_ratio,errors,total})。
    #   数据源 = runner gate evidence gw_overlap_error_ratio + gw_overlap_n(each_root_signal_present detail)。
    #   仅 CFG-gw-overlap 门(三-01/T1)产此信号;net/pod 门(T3/T4)无该统计 → overlap 自然缺省(不造数)。
    #   overlap 置首(镜像上游 traffic_error_stats key 序 overlap-first)。
    _tes = out.get("traffic_error_stats")
    if isinstance(_tes, dict) and "overlap" not in _tes:
        _gwr = _gwn = None
        for _e in (meta.get("validation_results") or []):
            _d = _e.get("detail") if isinstance(_e, dict) else None
            if isinstance(_d, dict) and "gw_overlap_error_ratio" in _d and "gw_overlap_n" in _d:
                _gwr, _gwn = _d.get("gw_overlap_error_ratio"), _d.get("gw_overlap_n")
                break
        if isinstance(_gwr, (int, float)) and isinstance(_gwn, int) and _gwn >= 0:
            _ov = {"error_ratio": _gwr,
                   "errors": int(round(_gwr * _gwn)),
                   "total": _gwn}
            out["traffic_error_stats"] = {"overlap": _ov, **_tes}

    # artifacts 桶 key 对齐上游: groundtruth -> ground_truth + 补 metadata 登记
    # + DIR-style 路径规范 (metrics/operations/logs/traces): 我方 native 这 4 key 可能是
    #   文件路径(metrics_v2.jsonl / injection.json)或目录+尾斜杠(raw/logs/) -> 上游 loader 按
    #   目录读(listdir/glob),必须统一成无尾斜杠的目录相对路径 "raw/<k>"。
    #   metadata/ground_truth 保持 FILE-style (上游 sample 同为 metadata.json / groundtruth.json)。
    art = out.get("artifacts")
    if isinstance(art, dict):
        art = dict(art)
        if "groundtruth" in art and "ground_truth" not in art:
            art["ground_truth"] = art.pop("groundtruth")
        if "metadata" not in art:
            art["metadata"] = "metadata.json"
        # DIR-style 规范 (幂等: 已是 "raw/<k>" 不变; 文件路径/尾斜杠/array 全归一)
        for k in ("metrics", "operations", "logs", "traces"):
            if k in art:
                art[k] = "raw/" + k
        out["artifacts"] = art

    # validation_results: pass->passed + 8 canonical gate 子集(strict mirror 上游 8 项)
    out["validation_results"] = _norm_validation_results(
        meta.get("validation_results"))["validation_results"]

    # root_metric_contract(顶层副本):若存在,保留原样(不属 上游 形状变换;不动)
    # canonical root_cause_services(string list)已对齐 -> 不改

    return out


# ---------------------------------------------------------------------------
# quality transform
# ---------------------------------------------------------------------------

def transform_quality(q):
    """quality.json -> 上游 形态(strict mirror)。top key 集 = [schema_version, stages]
    (无 overall_valid)。每 stage 内 key 集 = [schema_version, stage, expected_snapshots,
    observed_snapshots, coverage_ratio, max_gap_seconds, required_metrics,
    missing_required_metrics, null_required_value_records, record_count, metric_count, valid]
    (上游 raw/metrics/quality.json 实读;schema_version/stage 为补齐 key)。
    required_metrics 27-metric list byte-identical 不动。"""
    out = {}
    out["schema_version"] = MR2_QUALITY_SCHEMA_VERSION

    # 取 per-stage 块(by_stage 原生 / stages 已 mr2 形态)。
    src_stages = None
    if isinstance(q, dict):
        src_stages = q.get("by_stage")
        if src_stages is None:
            src_stages = q.get("stages")

    stages_out = {}
    if isinstance(src_stages, dict):
        for stage_name, sd in src_stages.items():
            if not isinstance(sd, dict):
                stages_out[stage_name] = sd
                continue
            nsd = dict(sd)  # 原生 27-metric block byte-identical
            # 补齐 上游 per-stage key:schema_version + stage(若无)。
            nsd.setdefault("schema_version", MR2_QUALITY_SCHEMA_VERSION)
            nsd.setdefault("stage", stage_name)
            stages_out[stage_name] = nsd
    out["stages"] = stages_out
    # overall_valid 丢弃(上游无);其余顶层键不透传(strict mirror = 仅 schema_version + stages)。
    return out


# ---------------------------------------------------------------------------
# groundtruth transform
# ---------------------------------------------------------------------------

def transform_groundtruth(gt):
    """native groundtruth.json -> 上游 形态(strict mirror,REF §6 真样例实读)。

    变换(只动 key 名 + 删加性 key;**绝不改任何答案值**):
      1. overlap_window {start,end,duration_seconds}
         -> {start_time,end_time,duration_seconds}(复用 _norm_overlap_window)
      2. component_fault_windows 每 F 内层 {start,end} -> {start_time,end_time}
         (复用 _norm_component_fault_windows)
      3. component_ground_truth[] 每 item:删 chaos_engine/crd/intensity 3 个加性 key,
         保留上游 10-key 集(MR2_GT_CG_KEYS 白名单)。注意只删加性 key,
         **不动其余内层值**(target_component/role/fault_instance_id 等答案原样保留)
      4. 顶层:删 run_id/affected_services/isolation_degraded/path_relation/
         root_metric_contract/sli_gate 6 个加性 key,保留上游 13-key 集
         (MR2_GT_TOP_KEYS 白名单)

    白名单过滤(strict mirror):仅保留 上游 key 集,key 顺序按白名单(与 上游 样例顺序一致)。
    若 native 缺白名单中某 key,该 key 缺失(不补 None——绝不臆造答案)。
    """
    if not isinstance(gt, dict):
        return gt

    out = {}
    for k in MR2_GT_TOP_KEYS:
        if k not in gt:
            continue  # 严格镜像:不补缺 key,绝不臆造答案值

        v = gt[k]

        if k == "overlap_window":
            # G1(gt-file 修): overlap_window = N-way innermost, 对齐 metadata + 上游
            #   (她 gt.overlap_window == metadata.overlap_window)。真 innermost 在 native
            #   gt.overlap_windows[全腿 key](非白名单键, 仅取值不输出);缺失(dual/single)回退
            #   native 2-way v。值全取自 native gt, 绝不臆造。
            out[k] = _norm_overlap_window(
                _innermost_overlap_window(gt.get("overlap_windows"), v))
        elif k == "component_fault_windows":
            out[k] = _norm_component_fault_windows(v)
        elif k == "component_ground_truth":
            # 每 item 白名单过滤到 10-key 集(删 chaos_engine/crd/intensity);
            # 不动其余内层值(答案原样保留)。
            items_out = []
            if isinstance(v, list):
                for item in v:
                    if not isinstance(item, dict):
                        items_out.append(item)
                        continue
                    ni = {}
                    for ck in MR2_GT_CG_KEYS:
                        if ck in item:
                            ni[ck] = item[ck]
                    items_out.append(ni)
            out[k] = items_out
        else:
            # 答案值原样(sample_id/root_cause_services/fault_types/... 不动)
            out[k] = v

    return out


# ---------------------------------------------------------------------------
# per-case driver
# ---------------------------------------------------------------------------

def adapt_case(case_dir, stats=None, out_dir=None, tag="default"):
    """归一单个 case 目录 → <out_dir>/。返回 (record_count, info_dict)。

    out_dir:派生物落地目录。不传则 = dataset_registry.runtime_dir("package", tag)/<case_id>。
    stats:可选共享 dict(unmapped_sources/units 集合);不传则建本地。

    ★ 2026-07-13:默认值【不再是 <case_dir>/mr2/】。
      旧默认把派生物写进 native 采集树里,后果实测有三:
        (a) 4 个下游脚本各自写防御性 "skip mr2/" 才不至于把它当成 case 重复计数;
        (b) mr2/ 是 stale 的也照样被复用 —— 错 GT 就是这么进的交付包;
        (c) native 树号称只读, 实际每次打包都在改它.
      现在 out 统一收口到 registry.runtime_dir(),并且【进函数第一件事就 assert_not_native】:
      再有人想往 (native trees)  里写,这里直接炸。
      (chaos_k8s_runner.py 是 native 的合法写入者 —— 它不走这条路径,不受影响。)
    """
    case_dir = Path(case_dir)
    meta_path = case_dir / "metadata.json"
    if not meta_path.exists():
        return 0, {"error": f"no metadata.json in {case_dir}"}

    # 单一收口:算出 out,并挡死 native。
    out = Path(out_dir) if out_dir else (DR.runtime_dir("package", tag) / case_dir.name)
    DR.assert_not_native(out)
    out.mkdir(parents=True, exist_ok=True)

    if stats is None:
        stats = {"unmapped_sources": set(), "unmapped_units": set()}

    # --- metrics ---
    raw_metrics = case_dir / "raw" / "metrics" / "metrics_v2.jsonl"
    n_metrics = 0
    if raw_metrics.exists():
        out_metrics = out / "metrics_v2.jsonl"
        n_metrics = transform_metrics_file(raw_metrics, out_metrics, stats)

    # --- metadata ---
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta_mr2 = transform_metadata(meta)
    out_meta = out / "metadata.json"
    out_meta.parent.mkdir(parents=True, exist_ok=True)
    out_meta.write_text(
        json.dumps(meta_mr2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # --- quality (read first so manifest validation can derive from it) ---
    raw_quality = case_dir / "raw" / "metrics" / "quality.json"
    out_quality = out / "quality.json"
    q_native = None
    if raw_quality.exists():
        q_native = json.loads(raw_quality.read_text(encoding="utf-8"))
        q_mr2 = transform_quality(q_native)
        out_quality.write_text(
            json.dumps(q_mr2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # --- manifest (raw/metrics/manifest.json -> mr2/manifest.json, 上游结构) ---
    raw_manifest = case_dir / "raw" / "metrics" / "manifest.json"
    if raw_manifest.exists():
        m_native = json.loads(raw_manifest.read_text(encoding="utf-8"))
        m_mr2 = _norm_manifest(
            m_native, meta.get("observation_stages"), q_native)
        out_manifest = out / "manifest.json"
        out_manifest.write_text(
            json.dumps(m_mr2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # --- groundtruth (native groundtruth.json -> mr2/groundtruth.json, 上游结构) ---
    # 非破坏 + 幂等(同 metadata/manifest/quality 模式):读 raw native,写 mr2 归一版,
    # 绝不改 native groundtruth.json。归一 = overlap_window/component_fault_windows
    # start->start_time/end->end_time + 删 chaos_engine/crd/intensity(每 item)+ 删
    # run_id/affected_services/isolation_degraded/path_relation/root_metric_contract/
    # sli_gate(顶层);答案值一字不动。
    raw_gt = case_dir / "groundtruth.json"
    if raw_gt.exists():
        gt_native = json.loads(raw_gt.read_text(encoding="utf-8"))
        gt_mr2 = transform_groundtruth(gt_native)
        out_gt = out / "groundtruth.json"
        out_gt.write_text(
            json.dumps(gt_mr2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return n_metrics, {
        "metrics_records": n_metrics,
        "unmapped_sources": sorted(stats["unmapped_sources"]),
        "unmapped_units": sorted(stats["unmapped_units"]),
        "out_dir": str(out),
    }


def main():
    ap = argparse.ArgumentParser(
        description="RecWeb2 k8s_pilot → mr2 load-time adapter (acceptance #4).")
    ap.add_argument("--in", dest="indir", required=True,
                    help="case dir (含 metadata.json) 或 --all 时的 root")
    ap.add_argument("--all", action="store_true",
                    help="对 <indir> 下所有含 metadata.json 的 case 逐个归一")
    ap.add_argument("--out", default=None,
                    help="派生物根目录(单 case 时 = 该 case 的输出目录;--all 时 = <out>/<case_id>/)。"
                         "缺省 = (runtime) package/<tag>/<case_id>/。"
                         "绝不允许指向 (native trees) (native 只读, assert_not_native 会挡)。")
    ap.add_argument("--tag", default="default",
                    help="缺省输出目录的 tag: (runtime) package/<tag>/")
    a = ap.parse_args()

    indir = Path(a.indir)
    out_root = Path(a.out) if a.out else None
    if out_root is not None:
        DR.assert_not_native(out_root)
    global_stats = {"unmapped_sources": set(), "unmapped_units": set()}

    if a.all:
        # Recursive discovery: keep parents of metadata.json that ALSO have
        # raw/metrics/metrics_v2.jsonl. This AND-filter excludes <case>/mr2/
        # false-positives (mr2/ has metadata.json but NO raw/metrics/metrics_v2.jsonl).
        # Recursive rglob supports future cases/<root_cause>/<case_id>/ nesting.
        cases = sorted(
            p.parent for p in indir.glob("**/metadata.json")
            if (p.parent / "raw" / "metrics" / "metrics_v2.jsonl").exists()
        )
        if not cases:
            print(f"[mr2-adapter] no cases under {indir}", file=sys.stderr)
            return 1
        total = 0
        for c in cases:
            n, info = adapt_case(c, global_stats,
                                 out_dir=(out_root / c.name) if out_root else None,
                                 tag=a.tag)
            total += n
            print(f"[mr2-adapter] {c.name}: {n} records -> {info['out_dir']}")
        print(f"[mr2-adapter] ALL done: {len(cases)} cases, {total} records total")
    else:
        if not (indir / "metadata.json").exists():
            print(f"[mr2-adapter] {indir} has no metadata.json", file=sys.stderr)
            return 1
        n, info = adapt_case(indir, global_stats, out_dir=out_root, tag=a.tag)
        print(f"[mr2-adapter] {indir.name}: {n} records -> {info['out_dir']}")

    # WARN list
    if global_stats["unmapped_sources"]:
        print("[mr2-adapter] WARN unmapped sources (aliased->prometheus, kept under "
              "labels.source_raw): " + ", ".join(sorted(global_stats["unmapped_sources"])))
    if global_stats["unmapped_units"]:
        print("[mr2-adapter] WARN unmapped units (REF silent, kept as-is, may need 上游 "
              "confirm): " + ", ".join(sorted(global_stats["unmapped_units"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
