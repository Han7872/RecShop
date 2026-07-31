# -*- coding: utf-8 -*-
"""从**采集当时的运行日志**生成 A6 要的调用留痕件（`provenance/`）。

为什么需要这个脚本
--------------------------------------------------------------------------------
验收闸 A6 要证明"这批数据没开逃生开关、warmup>=1、combo 覆盖全集"。它只认两档证据：

  runner_emitted            采集器自己在采集时写的（**最强**，需规范 §8-P1 落地 —— 至今没落地）
  contemporaneous_run_log   采集当时的运行日志（第三方 = shell 实时写的），日志随树同发

本脚本产出**第二档**：把一键脚本 `run_collect_agentfault.sh` 的日志拷进树里，
再从日志原文解析出每次调用的 argv / 逃生开关 / warmup / combo，写成 `invocations.json`。

★这不是"采后补写的自陈"。区别在于：`invocations.json` 里的每个字段都能在随树同发的
`provenance/collect_run.log` 里逐字复核，而 **A6 会真的回去复核**（不信 JSON 自陈，
自己再 grep 一遍六种逃生开关拼法）。采后凭空编一份是过不了 A6 的
（见 `verify_recollect_acceptance.py --selftest` 阶段 3 的 `log_has_escape` 用例）。

★仍然弱于 runner_emitted：它证明"这条命令被发出过"，不证明"runner 内部没走别的分支"。
  这条局限必须进 `limitations.json`，别在文档里说成等价。

顺带解决的第二件事：`run_summary.json` 是**每次调用覆盖写**的
--------------------------------------------------------------------------------
所以补采一跑，全量那次的 9-combo 记录就被覆盖没了（首轮实证：归档树的
`run_summary.json` 最后只剩单个 combo）。本脚本按调用序把它快照成
`provenance/run_summary_<i>.json`，**在下一次调用覆盖它之前**。

  ⇒ 用法上的硬要求：**全量采完、补采之前**先跑一次本脚本；
    每次补采之后再跑一次（`--append` 语义靠日志本身是 append-only 保证）。

用法
--------------------------------------------------------------------------------
    python scripts/chaos/agentfault/k8s/make_collect_provenance.py \
        --tree datasets/agentfault_k8s \
        --run-log (collection logs)agentfault_k8s_run.log

★补采命令的输出**必须追加进同一个日志**（`... 2>&1 | tee -a <该日志>`），
  否则补采那次调用不在留痕里，A6 的 combo 并集会缺口。
"""
from __future__ import unicode_literals

import argparse
import io
import json
import os
import re
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))

# 逃生开关的两种拼法：runner 侧带 k8s- 前缀，一键脚本侧不带。两种都查。
ESCAPE_CLI = {
    "--skip-preflight": "skip_preflight",
    "--allow-mixed-tree": "allow_mixed_tree",
    "--k8s-skip-code-parity": "k8s_skip_code_parity",
    "--skip-code-parity": "k8s_skip_code_parity",
    "--k8s-allow-inject-residue": "k8s_allow_inject_residue",
    "--allow-inject-residue": "k8s_allow_inject_residue",
}

RE_ARGV = re.compile(r"agentfault_runner\.py")
# ★不能只看文件名:一键脚本的 banner 行 "── STEP 2: agentfault_runner.py  (LIVE DeepSeek)"
#   也含这个名字,会被当成一次调用(实测踩到:段 0 解析出 runs=None / combos=[])。
#   真正的 argv 行必然带 `--<flag>`;usage 里的 `[--only ...]` 用 `[--` 排除。
RE_HAS_FLAG = re.compile(r"--[a-z][a-z0-9-]+")
RE_DOLLAR = re.compile(r"^\s*\$\s*")
RE_HDR = re.compile(r"\[agentfault-runner\]\s+combos=(\d+)\s+runs=(\d+)\s+warmup=(\d+)")
RE_COMBO = re.compile(r"===\s+COMBO\s+([A-Za-z_][A-Za-z0-9_]*)\s")
RE_ONLY = re.compile(r"--only\s+(\S+)")
RE_RUNS = re.compile(r"--runs[= ](\d+)")
RE_WARMUP = re.compile(r"--warmup[= ](\d+)")


def _is_argv_line(ln):
    return (RE_ARGV.search(ln) is not None
            and RE_HAS_FLAG.search(ln) is not None
            and "[--" not in ln)


def _rel(p):
    """相对仓根;**跨盘符不抛异常**(os.path.relpath 在 C: vs D: 上会 ValueError)。"""
    try:
        return os.path.relpath(p, REPO).replace("\\", "/")
    except ValueError:
        return p.replace("\\", "/")


def parse_log(text):
    """把日志切成"每次 runner 调用一段"，逐段解析。

    切法：以含 `agentfault_runner.py` 的行为段首（一键脚本会把完整 argv 打出来）。
    段内再取 `[agentfault-runner] combos=.. runs=.. warmup=..` 与所有 `=== COMBO x`。
    """
    lines = text.splitlines()
    starts = [i for i, ln in enumerate(lines) if _is_argv_line(ln)]
    invs = []
    for k, i0 in enumerate(starts):
        i1 = starts[k + 1] if k + 1 < len(starts) else len(lines)
        argv = lines[i0].strip()
        seg = "\n".join(lines[i0:i1])

        flags = {v: False for v in set(ESCAPE_CLI.values())}
        hits = []
        for cli, key in ESCAPE_CLI.items():
            if cli in argv:
                flags[key] = True
                hits.append(cli)

        # warmup：优先取 runner 自己打的表头（是它真正用的值），退回 argv，再退回默认 1
        m = RE_HDR.search(seg)
        if m:
            n_combos_hdr, runs, warmup = int(m.group(1)), int(m.group(2)), int(m.group(3))
        else:
            n_combos_hdr = None
            mr = RE_RUNS.search(argv)
            runs = int(mr.group(1)) if mr else None
            mw = RE_WARMUP.search(argv)
            warmup = int(mw.group(1)) if mw else 1

        combos = []
        for c in RE_COMBO.findall(seg):
            if c not in combos:
                combos.append(c)

        mo = RE_ONLY.search(argv)
        inv = {
            "index": k,
            "argv": RE_DOLLAR.sub("", argv).strip(),
            "runs": runs,
            "warmup": warmup,
            "only": mo.group(1) if mo else None,
            "combos": combos,
            "n_combos_declared_by_runner": n_combos_hdr,
            "escape_flags_seen_in_argv": hits,
        }
        inv.update(flags)
        # 该段是否正常收尾（有 done / COLLECT-EXIT=0），供人工判读；A6 不读这个字段
        inv["segment_tail_ok"] = ("[agentfault-runner] done" in seg
                                  or "COLLECT-EXIT=0" in seg)
        invs.append(inv)
    return invs


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tree", required=True, help="数据集树根（如 datasets/agentfault_k8s）")
    ap.add_argument("--run-log", required=True, help="采集当时的运行日志（append-only）")
    ap.add_argument("--log-name", default="collect_run.log",
                    help="拷进树里的文件名（默认 collect_run.log）")
    a = ap.parse_args()

    tree = a.tree if os.path.isabs(a.tree) else os.path.join(REPO, a.tree)
    log = a.run_log if os.path.isabs(a.run_log) else os.path.join(REPO, a.run_log)
    if not os.path.isdir(tree):
        sys.stderr.write("FATAL 树不存在: %s\n" % tree)
        return 2
    if not os.path.isfile(log):
        sys.stderr.write("FATAL 日志不存在: %s\n" % log)
        return 2

    pdir = os.path.join(tree, "provenance")
    if not os.path.isdir(pdir):
        os.makedirs(pdir)

    # 1) 日志随树同发（A6 会回来读它复核）
    dst_log = os.path.join(pdir, a.log_name)
    shutil.copyfile(log, dst_log)

    text = io.open(log, "r", encoding="utf-8-sig", errors="replace").read()
    invs = parse_log(text)
    if not invs:
        sys.stderr.write("FATAL 日志里找不到任何 runner 调用行（应含 agentfault_runner.py）\n")
        return 3

    # 2) run_summary.json 快照（它是覆盖写的，不快照就会被下次调用抹掉）
    rs = os.path.join(tree, "run_summary.json")
    snap = None
    if os.path.isfile(rs):
        snap = "run_summary_snapshot_%02d.json" % (len(invs) - 1)
        shutil.copyfile(rs, os.path.join(pdir, snap))

    obj = {
        "_doc": ("A6 要的采集调用留痕。**证据等级 = contemporaneous_run_log**："
                 "每个字段都能在随树同发的 provenance/%s 里逐字复核，A6 会真的回去复核"
                 "（不信本文件的自陈，自己再 grep 一遍逃生开关）。"
                 "★仍弱于 runner_emitted：它证明'这条命令被发出过'，"
                 "不证明'runner 内部没走别的分支'——该局限须进 limitations.json。"
                 % a.log_name),
        "evidence_class": "contemporaneous_run_log",
        "run_log": "provenance/%s" % a.log_name,
        "generated_by": "scripts/chaos/agentfault/k8s/make_collect_provenance.py",
        "run_summary_snapshot": ("provenance/%s" % snap) if snap else None,
        "run_summary_note": ("run_summary.json 是每次调用**覆盖写**的（A5 判据：只作旁证）。"
                             "本快照是在下一次调用覆盖它之前留的。"),
        "escape_cli_checked": sorted(ESCAPE_CLI.keys()),
        "invocations": invs,
    }
    out = os.path.join(pdir, "invocations.json")
    with io.open(out, "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, indent=2))

    # ---- 打印给人看的核对表 ----
    allc = []
    for inv in invs:
        for c in inv["combos"]:
            if c not in allc:
                allc.append(c)
    bad = [(i["index"], i["escape_flags_seen_in_argv"]) for i in invs
           if i["escape_flags_seen_in_argv"]]
    low = [(i["index"], i["warmup"]) for i in invs if (i["warmup"] or 0) < 1]

    print("provenance -> %s" % _rel(out))
    print("  日志随树   : provenance/%s (%d bytes)" % (a.log_name, os.path.getsize(dst_log)))
    print("  调用次数   : %d" % len(invs))
    for i in invs:
        print("    [%d] runs=%s warmup=%s only=%s combos=%d tail_ok=%s"
              % (i["index"], i["runs"], i["warmup"], i["only"],
                 len(i["combos"]), i["segment_tail_ok"]))
    print("  combo 并集 : %d -> %s" % (len(allc), ",".join(allc)))
    print("  逃生开关   : %s" % ("无" if not bad else "★有! %s" % bad))
    print("  warmup<1   : %s" % ("无" if not low else "★有! %s" % low))
    if snap:
        print("  run_summary 快照: provenance/%s" % snap)
    if bad or low:
        print("\n★A6 会判 FAIL —— 上面那些开关/warmup 是从日志原文读出来的，改 JSON 没用。")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
