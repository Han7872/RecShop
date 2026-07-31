# -*- coding: utf-8 -*-
"""n5_progress.py — n5_run.py 全量评测的进度/ETA 显示器(只读, 不干扰跑批)。

用法:
    python scripts/chaos/ctk/n5_progress.py                       # 打一次快照
    python scripts/chaos/ctk/n5_progress.py --watch --every 60     # 持续盯(Ctrl-C 退出)
    python scripts/chaos/ctk/n5_progress.py --raw <path> --total 255

读什么:
  - raw jsonl 的行数 = 已完成 case 数(n5_run 每完成一个 flush 一行)
  - 每行的 secs 字段 = 该 case 耗时 → 算 per-case 均值/中位数
  - 文件 mtime + 起跑时刻 → 真实 wall-clock 速率(比 per-case 均值更准, 因为是并行)
  - tasklist 判 python.exe 是否还在(★Git-Bash 的 `ps` 看不见 Windows 原生进程, 别用它判死活)

ETA 两种口径都给:
  - wall 速率法: elapsed / done * remaining  (推荐, 已含并行度与调度开销)
  - per-case 法: median(secs) * remaining / workers  (workers 需手动给, 仅作对照)
"""
from __future__ import annotations
import argparse, json, os, statistics, subprocess, sys, time
from datetime import datetime, timedelta

DEFAULT_RAW = "${REPO_DIR}/logs/n5_raw_255.jsonl"


def python_alive() -> int:
    """还在跑的 python.exe 个数(Windows tasklist; MSYS ps 看不到原生进程)。"""
    for exe in (r"C:\Windows\System32\tasklist.exe", "tasklist"):
        try:
            out = subprocess.run([exe, "/FI", "IMAGENAME eq python.exe", "/NH"],
                                 capture_output=True, text=True, timeout=15).stdout
            return sum(1 for ln in out.splitlines() if "python.exe" in ln)
        except Exception:
            continue
    return -1   # 判不了


def read_raw(path):
    """→ (done, secs_list, per_tree Counter, first_mtime_hint)"""
    if not os.path.exists(path):
        return 0, [], {}, None
    secs, trees = [], {}
    done = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue          # 尾行可能正在写(半行) → 跳过不计
            done += 1
            if isinstance(r.get("secs"), (int, float)):
                secs.append(r["secs"])
            t = r.get("tree", "?")
            trees[t] = trees.get(t, 0) + 1
    return done, secs, trees, os.path.getmtime(path)


def fmt_dur(sec: float) -> str:
    if sec < 0 or sec != sec:
        return "?"
    return str(timedelta(seconds=int(sec)))


def bar(done: int, total: int, width: int = 32) -> str:
    frac = 0.0 if not total else min(1.0, done / total)
    fill = int(frac * width)
    return "[" + "#" * fill + "-" * (width - fill) + f"] {done}/{total} ({frac*100:.1f}%)"


def snapshot(raw: str, total: int, workers: int, t_start: float | None):
    done, secs, trees, mtime = read_raw(raw)
    now = time.time()
    print(bar(done, total))

    # 存活
    n_py = python_alive()
    alive = ("? (判不了)" if n_py < 0 else
             (f"{n_py} 个 python.exe 在跑" if n_py > 0 else "★无 python.exe —— 跑批已结束或中断"))
    idle = f"{now - mtime:.0f}s 前" if mtime else "从未"
    print(f"  存活: {alive} | raw 最后写入: {idle}")

    if done == 0:
        print("  (还没有 case 落盘; 首个 case 约需 2-3 分钟)")
        return done

    # per-case 耗时
    if secs:
        print(f"  per-case: median {statistics.median(secs):.0f}s  "
              f"mean {statistics.mean(secs):.0f}s  min {min(secs):.0f}s  max {max(secs):.0f}s")

    # ETA —— wall 速率法(推荐)
    if t_start:
        elapsed = now - t_start
        rate = done / elapsed                       # case/s (含并行)
        remain = total - done
        eta_wall = remain / rate if rate > 0 else float("nan")
        print(f"  已耗时 {fmt_dur(elapsed)} | 速率 {rate*3600:.1f} case/h | "
              f"★剩余约 {fmt_dur(eta_wall)} (预计 {(datetime.now()+timedelta(seconds=eta_wall)).strftime('%H:%M')} 完成)")
    # 对照: per-case 法
    if secs and workers:
        eta_pc = statistics.median(secs) * (total - done) / max(1, workers)
        print(f"  (对照 per-case 法: 剩余约 {fmt_dur(eta_pc)}, 假设 workers={workers})")

    if trees:
        print("  按树: " + " ".join(f"{k}={v}" for k, v in sorted(trees.items())))
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=DEFAULT_RAW)
    ap.add_argument("--total", type=int, default=255)
    ap.add_argument("--workers", type=int, default=6, help="仅用于 per-case 对照 ETA")
    ap.add_argument("--watch", action="store_true")
    ap.add_argument("--every", type=int, default=60)
    ap.add_argument("--start-ts", type=float, default=0.0,
                    help="起跑 unix 时刻(默认取 raw 文件 ctime; 若续跑请显式给)")
    a = ap.parse_args()

    t_start = a.start_ts or (os.path.getctime(a.raw) if os.path.exists(a.raw) else None)

    if not a.watch:
        snapshot(a.raw, a.total, a.workers, t_start)
        return

    while True:
        print(f"--- {datetime.now().strftime('%H:%M:%S')} ---")
        done = snapshot(a.raw, a.total, a.workers, t_start)
        if done >= a.total:
            print("✅ 全部完成")
            break
        if python_alive() == 0:
            print("⚠ 进程已退出但未跑满 —— 检查日志; 可用 n5_resume 续跑未完成的 case")
            break
        sys.stdout.flush()
        time.sleep(a.every)


if __name__ == "__main__":
    main()
