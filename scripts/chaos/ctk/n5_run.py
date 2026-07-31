# -*- coding: utf-8 -*-
"""n5_run.py — 140 case × {gap_aware T/F} × {full,resource} × {BARO, RCD seed0-4} 全量重打分。

★一 case 一进程(Pool maxtasksperchild=1)→ 进程级随机状态无法跨 case 残留。
★case 内每次 rcd 调用前 np.random.seed + random.seed 双重置。
输出: n5_raw.jsonl(一行一 case,含全部 config 的排名 + MRCBench 四族)

用法: python n5_run.py [--workers 8] [--out n5_raw.jsonl]
"""
from __future__ import annotations
import argparse, json, os, sys, time, traceback
import multiprocessing as mp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SEEDS = (0, 1, 2, 3, 4)
GAPS = (True, False)
UNIS = ("full", "resource")


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=os.path.join(HERE, "n5_raw.jsonl"))
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    sys.path.insert(0, HERE)
    import n5_lib as L
    cases = L.all_cases()
    if a.limit:
        cases = cases[: a.limit]
    print(f"[n5] {len(cases)} cases, workers={a.workers}", flush=True)

    done = 0
    with open(a.out, "w", encoding="utf-8") as f, \
            mp.Pool(a.workers, maxtasksperchild=1) as pool:
        for res in pool.imap_unordered(work, cases):
            f.write(json.dumps(res, ensure_ascii=False) + "\n")
            f.flush()
            done += 1
            print(f"[{done}/{len(cases)}] {res['case_id']} {res['secs']}s"
                  f"{' FATAL' if 'fatal' in res else ''}", flush=True)
    print("[n5] done ->", a.out, flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
