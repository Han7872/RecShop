# -*- coding: utf-8 -*-
"""n5_lib.py — n=5 全量(140 case)BARO/RCD 打分的共享库。

设计红线:
  * datasets/ 与 third_party/ 只读;BARO/RCD 算法零改动(只改喂进去的列)。
  * MRCBench 四族 + GT 解析 直接 import m9_score(与在役打分器同一份代码,避免二次实现漂移)。
  * 字节→MB 归一 = 已确立的真 bug 修复,原样复制自 m9_score.py。
"""
from __future__ import annotations
import os, sys, json, glob, random, importlib.util
import numpy as np
import pandas as pd

REPO = "${REPO_DIR}"
CTK = os.path.join(REPO, "scripts", "chaos", "ctk")
sys.path.insert(0, os.path.join(REPO, "third_party", "_cl_patched"))
sys.path.insert(0, os.path.join(REPO, "third_party", "RCAEval"))
sys.path.insert(0, CTK)

from m9_adapter import build_wide, col_to_service           # noqa: E402
import m9_score as MS                                       # noqa: E402  (gt_roots / mrcbench / ranks_to_services)
import dataset_registry as DR                               # noqa: E402  (datasets/ 唯一真相源)


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(REPO, rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


_BARO = None
_RCD = None


def baro_fn():
    global _BARO
    if _BARO is None:
        _BARO = _load("third_party/RCAEval/RCAEval/e2e/baro.py", "n5_baro").baro
    return _BARO


def rcd_fn():
    global _RCD
    if _RCD is None:
        _RCD = _load("third_party/RCAEval/RCAEval/e2e/rcd.py", "n5_rcd").rcd
    return _RCD


# ---------------- 列宇宙 ----------------
KUBE_STATE = {"container_ready", "container_restart_count", "container_running",
              "container_start_time_seconds", "deployment_replicas_ready", "pod_ready"}
OFFGRAPH_METRICS = {"vm_cpu_saturation_ratio", "items_lock_granted_count",
                    "container_cpu_sum_cores", "container_throttled_sum"}


def suffix(c):
    return c.split("__", 1)[1] if "__" in c else c


def keep_resource(c):
    """resource 宇宙: container cpu/mem + kube_state + off-graph 伪节点。
    剔除: 所有 panel 延迟/错误、http_server_*(延迟/错误/请求数)、container_network_*。"""
    s = suffix(c)
    if s in OFFGRAPH_METRICS or s in KUBE_STATE:
        return True
    if s.startswith("container_cpu_") or s.startswith("container_memory_"):
        return True
    return False


COL_UNIVERSES = {
    "full": None,                 # adapter 默认全列
    "resource": keep_resource,
}


def prep(case_dir, gap_aware=True, bucket=2.0):
    """case_dir -> (df_full, inject, info);已做 stage 丢弃 + 字节→MB 归一(m9_score 同款)。"""
    df, inject, info = build_wide(case_dir, bucket=bucket, include_nginx=False, gap_aware=gap_aware)
    if df is None or inject is None or df.shape[0] < 4:
        return None, None, info
    if "stage" in df.columns:
        df = df.drop(columns=["stage"])
    byte_cols = [c for c in df.columns if c != "time" and "bytes" in c]
    if byte_cols:
        df = df.copy()
        for c in byte_cols:
            df[c] = df[c] / 1e6
    return df, inject, info


def subset(df, universe):
    fn = COL_UNIVERSES[universe]
    if fn is None:
        return df
    keep = ["time"] + [c for c in df.columns if c != "time" and fn(c)]
    return df[keep]


def run_baro(df, inject):
    try:
        r = baro_fn()(df.copy(), inject_time=inject, dataset="recshop").get("ranks", [])
        return MS.ranks_to_services(r), None
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def run_rcd(df, inject, seed):
    # 进程级随机状态残留防护:每次调用前显式重置全部 RNG(rcd 内部只 np.random.seed(seed))
    np.random.seed(seed)
    random.seed(seed)
    try:
        r = rcd_fn()(df.copy(), inject_time=inject, dataset="recshop", seed=seed,
                     gamma=5, localized=True, bins=5).get("ranks", [])
        return MS.ranks_to_services(r), None
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def all_cases(family=None, arity=None):
    """在役 case 全集(现为 195 = 140 dense + 55 single_spread)。

    ★ 2026-07-13:不再自己 glob 硬编码的树列表 —— 【一律走 dataset_registry】。
      硬编码 trees 列表就是双源:m9_eadro_adapter 那份漏了 single_spread,静默少算 55 个 case
      且不报错。加数据 = 改 datasets/REGISTRY.json 一处,不再改 N 个脚本。

    返回字段向后兼容(arity/type/case_id/case_dir 原样),另加 registry 的
    family/tree/fault_types/n_legs —— family 是【逐 case 从 groundtruth 现算】的
    signal_class,评测必须按它分栏(root_local 是送分题,合并报会稀释先验)。

    未知 family/arity -> ValueError(registry fail-loud),不会静默返回空表。
    """
    return DR.cases(family=family, arity=arity)
