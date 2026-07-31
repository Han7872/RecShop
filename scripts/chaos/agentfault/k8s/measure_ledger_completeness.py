# -*- coding: utf-8 -*-
"""量「随树同发的注入台账有多完整」—— C3 与台账类披露必须用数说话,不许用形容词。

为什么需要
--------------------------------------------------------------------------------
验收 C3 判 `journal` 里没有 `ground_truth.matched`(规范 §8-P2 未落地)⇒ "GT 事后不可复算"。
但**能复算多少**是个可测的量:凡是 `ledgers/<combo>.jsonl` 里还留着该 case trace_id 的,
都能用 `injector_smoke.read_ledger_entries(..., strict_trace=True)` 原样重算出来。

★2026-07-27 实测发现的结构性事实(**不是 K8S 特有,已发布的 agentfault_v2 同样如此**):
  `sync_ledger` 用 `_write_local()` **覆盖写**本地台账文件;
  per-rep combo(`format_*`,每个 rep 都换实例)每 rep 都重新 sync 一次
  ⇒ 后一个 rep 把前一个盖掉,整个 combo 最后只剩**最后一个 rep** 的行。
  补采同理(重跑该 combo 时又盖一次)。
  实测:B档首轮 format=1/12、hallu_Product=1/12(被补采覆盖)、hallu_Seq=1/12(同);
        v2 的 format 也是 **1/12**。
  ⇒ **GT 本身没问题**(采集当时算好写进 CSV/journal 了),缺的是**原始台账这个随包工件**。

用法
--------------------------------------------------------------------------------
    python scripts/chaos/agentfault/k8s/measure_ledger_completeness.py --tree datasets/agentfault_k8s
    python ... --tree <树> --json-out <path>      # 供 limitations.json 直接取用
    python ... --tree <树> --also <备份树1> ...    # 额外在别处找(如 .bak_pre_backfill/)
"""
from __future__ import unicode_literals

import argparse
import collections
import csv
import glob
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def _open(p):
    return io.open(p, "r", encoding="utf-8-sig", errors="replace")


def load_rows(tree):
    p = os.path.join(tree, "dataset_agentfault.csv")
    with _open(p) as f:
        return list(csv.DictReader(f))


def ledger_traces(tree):
    """{combo: {trace_id, ...}} —— 该树 ledgers/ 里实际留存的 trace。"""
    out = {}
    for p in glob.glob(os.path.join(tree, "ledgers", "*.jsonl")):
        combo = os.path.basename(p)[:-6]
        tr = set()
        with _open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line).get("trace_id")
                except Exception:  # noqa: BLE001
                    continue
                if t:
                    tr.add(t)
        out[combo] = tr
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tree", required=True)
    ap.add_argument("--also", nargs="*", default=[],
                    help="额外查找的树(如采前快照),用于区分『本树可查』与『只能从别处查』")
    ap.add_argument("--json-out", default=None)
    a = ap.parse_args()

    tree = a.tree if os.path.isabs(a.tree) else os.path.join(REPO, a.tree)
    rows = load_rows(tree)
    here = ledger_traces(tree)
    elsewhere = {}
    for t in a.also:
        tp = t if os.path.isabs(t) else os.path.join(REPO, t)
        if os.path.isdir(tp):
            for combo, tr in ledger_traces(tp).items():
                elsewhere.setdefault(combo, set()).update(tr)

    per = collections.OrderedDict()
    n_faulted = n_here = n_else = n_none = 0
    missing_ids = []
    for r in rows:
        if str(r.get("injected")) != "1":
            continue
        n_faulted += 1
        combo = r.get("group_id") or r.get("scenario_id") or ""
        tid = r.get("trace_id")
        d = per.setdefault(combo, {"faulted": 0, "in_tree": 0, "in_backup": 0, "nowhere": 0})
        d["faulted"] += 1
        if tid in here.get(combo, set()):
            d["in_tree"] += 1
            n_here += 1
        elif tid in elsewhere.get(combo, set()):
            d["in_backup"] += 1
            n_else += 1
        else:
            d["nowhere"] += 1
            n_none += 1
            missing_ids.append(r.get("run_id") or r.get("scenario_id"))

    res = {
        "tree": os.path.relpath(tree, REPO).replace("\\", "/"),
        "n_faulted": n_faulted,
        "recomputable_from_tree": n_here,
        "recomputable_only_from_backup": n_else,
        "nowhere_on_disk": n_none,
        "pct_recomputable_from_tree": (round(100.0 * n_here / n_faulted, 2)
                                       if n_faulted else None),
        "per_combo": per,
        "nowhere_sample": missing_ids[:12],
        "mechanism": ("sync_ledger 用 _write_local() 覆盖写;per-rep combo(format_*)每 rep 换实例、"
                      "每 rep 重新 sync ⇒ 只剩最后一个 rep 的行。补采同理。"
                      "★非 K8S 特有:已发布的 agentfault_v2 的 format_* 同样是 1/12。"
                      "GT 本身没问题(采集当时已写进 CSV/journal),缺的是原始台账这个随包工件。"),
    }

    print("tree = %s" % res["tree"])
    print("faulted case          : %d" % n_faulted)
    print("  本树 ledgers 可复算  : %-4d (%.1f%%)" % (n_here, res["pct_recomputable_from_tree"] or 0))
    if a.also:
        print("  仅备份树可复算       : %d" % n_else)
    print("  盘上任何位置都没有   : %d" % n_none)
    print()
    print("%-40s %8s %8s %8s %8s" % ("combo", "faulted", "本树", "备份", "都没有"))
    for combo, d in per.items():
        flag = "  <== per-rep" if combo.startswith("format") else ""
        print("%-40s %8d %8d %8d %8d%s"
              % (combo, d["faulted"], d["in_tree"], d["in_backup"], d["nowhere"], flag))

    if a.json_out:
        op = a.json_out if os.path.isabs(a.json_out) else os.path.join(REPO, a.json_out)
        with io.open(op, "w", encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False, indent=2))
        print("\njson -> %s" % op)
    return 0


if __name__ == "__main__":
    sys.exit(main())
