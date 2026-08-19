# -*- coding: utf-8 -*-
"""n5_run_strict51.py — strict51_20260819 交付树 × {gap_aware T/F} × {full,resource} × {BARO, RCD seed0-4} 打分。

与 n5_run.py 同一评分协议(逐 case 同进程布局、同 MRCBench 口径),唯一差异:
case 清单不走 dataset_registry(那是指向 v1 native 树的),改为枚举
datasets/_delivery/strict51_20260819/traditional/{single,dual,triple}/*/。

用法: python n5_run_strict51.py [--workers 8] [--out n5_raw_strict51.jsonl] [--limit N]
输出一行一 case(含全部 config 的排名 + MRCBench 四族),末尾打印 DATASHEET 口径摘要
(gap1 macro Hit@1, per-G 拆解, amended 拆分)。
"""
from __future__ import annotations
import argparse, json, os, sys, time, glob, traceback
import multiprocessing as mp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DELIVERY = os.path.join("datasets", "_delivery", "strict51_20260819", "traditional")
SEEDS = (0, 1, 2, 3, 4)
GAPS = (True, False)
UNIS = ("full", "resource")


def all_cases():
    out = []
    for tier in ("single", "dual", "triple"):
        for cd in sorted(glob.glob(os.path.join(DELIVERY, tier, "*") + os.sep)):
            if not os.path.isfile(os.path.join(cd, "groundtruth.json")):
                continue
            out.append({"case_id": os.path.basename(cd.rstrip("\\/")),
                        "case_dir": cd, "arity": tier})
    return out


def work(case):
    import n5_lib as L
    out = dict(case)
    out["gt_roots"] = L.MS.gt_roots(case["case_dir"])
    out["runs"] = {}
    t0 = time.time()
    try:
        for gap in GAPS:
            df, inject, info = L.prep(case["case_dir"], gap_aware=gap)
            if df is None:
                out["runs"][f"gap{int(gap)}"] = {"error": "NO_DATA"}
                continue
            g = {"n_rows": int(df.shape[0]), "n_cols": int(df.shape[1]) - 1,
                 "pre_points": info.get("pre_points"), "during_points": info.get("during_points")}
            for uni in UNIS:
                d = L.subset(df, uni)
                rec = {"n_cols": int(d.shape[1]) - 1}
                ranks, err = L.run_baro(d, inject)
                rec["baro"] = {"ranks": ranks, "err": err,
                               "m": L.MS.mrcbench(ranks, out["gt_roots"]) if ranks else None}
                rec["rcd"] = {}
                for s in SEEDS:
                    r, e = L.run_rcd(d, inject, s)
                    rec["rcd"][str(s)] = {"ranks": r, "err": e,
                                          "m": L.MS.mrcbench(r, out["gt_roots"]) if r else None}
                g[uni] = rec
            out["runs"][f"gap{int(gap)}"] = g
    except Exception:
        out["fatal"] = traceback.format_exc()[-1500:]
    out["secs"] = round(time.time() - t0, 1)
    return out


def summarize(rows, manifest_rows):
    """DATASHEET-comparable: macro Hit@1, per-(method, universe, G, amended)."""
    # map case folder -> (G, amended) via MANIFEST folder field
    f2meta = {r["folder"]: (r["distinct_root_services"], r["amended"]) for r in manifest_rows}

    def agg():
        acc = {}
        for row in rows:
            for uni in UNIS:
                b = row["runs"].get("gap1", {}).get(uni, {})
                bm = (b.get("baro") or {}).get("m")
                if bm:
                    acc.setdefault(("baro", uni), []).append(bm["hit@1"])
                rs = [(b.get("rcd", {}).get(str(s)) or {}).get("m")
                      for s in SEEDS]
                rs = [x["hit@1"] for x in rs if x]
                if rs:
                    acc.setdefault(("rcd", uni), []).append(sum(rs) / len(rs))
        return acc

    print("\n==== macro Hit@1 (gap1) — DATASHEET 口径 ====")
    for (meth, uni), vals in sorted(agg().items()):
        print("  %-4s / %-8s overall=%.3f (n=%d)" % (meth, uni, sum(vals) / len(vals), len(vals)))
    # per-G + amended split
    print("==== per-G / amended (baro & rcd, resource) ====")
    for meth in ("baro", "rcd"):
        for g in (1, 2, 3):
            for amd in (False, True):
                vals = []
                for row in rows:
                    meta = f2meta.get(row["case_id"])
                    if not meta or meta[0] != g or meta[1] != amd:
                        continue
                    b = row["runs"].get("gap1", {}).get("resource", {})
                    if meth == "baro":
                        m = (b.get("baro") or {}).get("m")
                        if m:
                            vals.append(m["hit@1"])
                    else:
                        rs = [((b.get("rcd", {}).get(str(s)) or {}).get("m") or {}).get("hit@1")
                              for s in SEEDS]
                        rs = [x for x in rs if x is not None]
                        if rs:
                            vals.append(sum(rs) / len(rs))
                if vals:
                    print("  %-4s G=%d amended=%-5s %.3f (n=%d)"
                          % (meth, g, amd, sum(vals) / len(vals), len(vals)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=os.path.join(HERE, "n5_raw_strict51.jsonl"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--summary-only", default=None,
                    help="reuse an existing n5_raw jsonl and just print the summary")
    a = ap.parse_args()

    if a.summary_only:
        rows = [json.loads(l) for l in open(a.summary_only, encoding="utf-8") if l.strip()]
        man = json.load(open(os.path.join("datasets", "_delivery", "strict51_20260819",
                                          "MANIFEST.json"), encoding="utf-8"))
        summarize(rows, man["case_index"])
        return

    sys.path.insert(0, HERE)
    cases = all_cases()
    if a.limit:
        cases = cases[: a.limit]
    print(f"[n5s51] {len(cases)} cases, workers={a.workers}", flush=True)

    done = 0
    with open(a.out, "w", encoding="utf-8") as f, \
            mp.Pool(a.workers, maxtasksperchild=1) as pool:
        for res in pool.imap_unordered(work, cases):
            f.write(json.dumps(res, ensure_ascii=False) + "\n")
            f.flush()
            done += 1
            print(f"[{done}/{len(cases)}] {res['case_id']} {res['secs']}s"
                  f"{' FATAL' if 'fatal' in res else ''}", flush=True)

    rows = [json.loads(l) for l in open(a.out, encoding="utf-8") if l.strip()]
    man = json.load(open(os.path.join("datasets", "_delivery", "strict51_20260819",
                                      "MANIFEST.json"), encoding="utf-8"))
    summarize(rows, man["case_index"])
    print("[n5s51] done ->", a.out, flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
