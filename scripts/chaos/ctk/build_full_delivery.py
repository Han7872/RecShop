# -*- coding: utf-8 -*-
"""build_full_delivery.py - assemble THE delivery: one tree, one README, one command.

    python scripts/chaos/ctk/build_full_delivery.py

That is the whole operation. Output lands in datasets/_delivery/RecShop_<tag>/
(tag defaults to today's date, override with --tag). Re-running with the same
--tag rebuilds in place.

================================ THE TREE ================================

  RecShop_<tag>/
    README.md                       what the whole thing is; read this first
    traditional/                    infra faults; entity space = SERVICE
      EVAL_NOTES_*.md               one per source batch (they differ!)
      single/   G=1 (1 root service; incl. compound: N>1 faults on 1 service)
      dual/     G=2 (2 distinct root services)
      triple/   G=3 (3 distinct root services; only 5 cases)
      -- buckets are by DISTINCT root services G (service-level), NOT by
         injected-fault count. Folder names keep their frozen mrN prefix
         (= injected faults); case_index records tier/G/injected/compound.
    agent/                          agent-semantic faults; entity space = AGENT
      SUMMARY.md / EVAL_NOTES.md / BASELINE_RESULTS.md / ...
      dataset_agentfault.csv + raw/ + spans/ + journal/ + ledgers/
      whowhen/                      96-case Who&When view (drop-in)
      baselines/                    our own baseline runs, for reference
      negative_controls/            infra faults seen from the AGENT layer
                                    (sourced from the 15 rec-agent cases)

  Agent faults have no dual/triple: exactly one agent is injected per case by
  construction. Said in the README rather than faked with empty directories.

============================= HOW IT IS BUILT =============================

Default (--mode copy): assemble from the three FROZEN traditional packages plus
the agent dataset. Nothing is re-derived, so every traditional case is
byte-identical to what was already sent and validated. This is the mode you
want; it takes minutes, not hours.

  datasets/_delivery/20260713_gtfix/{single,dual,triple}   140  sent 2026-07-13
  datasets/_delivery/single_spread_20260716/single_spread   55  sent 2026-07-16
  datasets/_delivery/single_recagent_20260722                15  packaged 07-22
  datasets/_archive/agentfault/agentfault_v2/ + datasets/_archive/agentfault/agentfault_v2_whowhen/ 108  agent side

--mode rebuild regenerates the traditional side from the native trees instead.
Only reach for it if a native GT changed; it takes ~1-2h and its output will NOT
be byte-identical to what shijie already holds.

===================== REPRODUCING A SOURCE PACKAGE ======================

If you ever need to rebuild one traditional batch from its native tree (this is
what --mode rebuild runs per batch), the recipe per batch is:

  python scripts/chaos/ctk/package_for_delivery.py \
      --pilot-dir datasets/k8s_pilot/<tree> --out <dest> \
      --bare --force --flat-traces --with-calltree --with-eval

  --flat-traces    raw/traces/ = flat projection (trace_id==span_id), the shape
                   shijie's loader already reads
  --with-calltree  raw/traces_calltree/ = untouched native call tree, WITH
                   cross-service edges. Both ship; they are not alternatives.
                   Needed by any trace-DAG method (Eadro et al).
  --with-eval      eval/data.csv wide table + inject_time.txt
  --bare           case folders flat under <dest>, no extra top level
  --force          always regenerate the adapter output (a cached adapter dir is
                   exactly how 15 stale-GT cases once shipped)

  single_recagent additionally needs its agent-layer views, which predate the
  packager and so live in a second script:

  python scripts/chaos/ctk/build_recagent_agent_views.py \
      --delivery-dir <dest> --pilot-dir datasets/k8s_pilot/single_recagent

  Native trees are READ-ONLY. package_for_delivery.py refuses to write into
  datasets/k8s_pilot/ and that guard is not to be worked around.
"""
import argparse
import collections
import csv
import datetime
import json
import os
import re
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dataset_registry as DR   # noqa: E402  (同目录; signal_class 对未登记 fault_type fail-loud)
# P0-8 release gate (HANDOFF §7 P0-8). The builder re-runs the gate INDEPENDENTLY
# rather than trusting that the upstream packager already filtered — a frozen
# delivery dir could have been built before the gate existed, or hand-edited.
from package_for_delivery import check_release_gate  # noqa: E402  (same dir)

# ★注入伪影族(DATASHEET §6 L15): 这两类不走 Chaos Mesh CRD, 而是 `kubectl set env` →
#   触发 deployment rollout → 起新 pod → 根因自己的 container_start_time_seconds 跳变 +
#   memory_rss 146→191MB。让根因"本地可见"的信号是【注入器指纹】而非【故障效应】,
#   真实生产的依赖延迟/运行时异常不会让容器重启 ⇒ 必须可一行过滤出来做敏感性分析。
ARTIFACT_FAULT_TYPES = frozenset({"dependency_latency", "runtime_exception"})

# 本脚本【不生成】、但必须留在交付树里的手工维护根级文件(重建时从 .bak 搬回)。
PRESERVE_FROM_BAK = ["DATASHEET.md", "FAULT_DESIGN.md", "CITATION.cff", "LICENSE.md"]
# 本脚本【会重新生成】的根级文件 —— 不在备份孤儿告警里报。
REGENERATED_ROOT_FILES = {"README.md", "MANIFEST.json",
                          "SHA256SUMS.txt"}   # SHA256SUMS 由 package_dist.py 在终态重算

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
PY = sys.executable

# (label, frozen delivery dir, native tree, arity bucket)
TRAD = [
    ("dense140_single", "datasets/_delivery/20260713_gtfix/single",
     "datasets/k8s_pilot/single_dense", "single"),
    ("spread55", "datasets/_delivery/single_spread_20260716/single_spread",
     "datasets/k8s_pilot/single_spread", "single"),
    ("recagent15", "datasets/_delivery/single_recagent_20260722",
     "datasets/k8s_pilot/single_recagent", "single"),
    ("dense140_dual", "datasets/_delivery/20260713_gtfix/dual",
     "datasets/k8s_pilot/dual_dense", "dual"),
    ("dense140_triple", "datasets/_delivery/20260713_gtfix/triple",
     "datasets/k8s_pilot/triple_dense", "triple"),
    # ★G2ext 扩充(2026-07-24 采完, 2026-07-25 打冻结包并入): 全异服务不坍缩,
    #   dual_ext 全 G=2 → dual 桶, triple_ext 全 G=3 → triple 桶(tier 均匀断言自校验)。
    ("dual_ext25", "datasets/_delivery/dual_ext_20260725/dual",
     "datasets/k8s_pilot/dual_ext", "dual"),
    ("triple_ext20", "datasets/_delivery/triple_ext_20260725/triple",
     "datasets/k8s_pilot/triple_ext", "triple"),
]

EVAL_NOTES = [
    # ★2026-07-28 精简: 交付树 doc 层只留 README.md + DATASHEET.md。原 per-batch EVAL_NOTES_*.md /
    #   BASELINES_255.md / COMPARISON_*.md 的关键指标已并入 DATASHEET §关键结果表;完整逐方法/逐服务
    #   表留仓内 docs/results/n5_255/(外部使用者可索取)。这里只搬【逐 case 分数 CSV】(数据,非文档)。
    ("docs/results/n5_255/per_case_scores.csv", "per_case_scores_255.csv"),
]

AGENT_SRC = "datasets/_archive/agentfault/agentfault_v2"
AGENT_WW_SRC = "datasets/_archive/agentfault/agentfault_v2_whowhen"
# ★2026-07-28 精简: 交付树 doc 层只留 README.md + DATASHEET.md(+ MANIFEST.json + 数据目录)。
#   原 per-track .md(SUMMARY/EVAL_NOTES/BASELINE_RESULTS/RESULTS_*)的关键指标已并入 DATASHEET
#   §关键结果表;完整逐方法分数留仓内 docs/results/。这里只搬【数据文件】,不搬【文档】。
AGENT_DOCS = ["limitations.json",
              "dataset_agentfault.csv",
              "run_summary.json", "context_drift_outcomes.json",
              "content_ctxdrift_results.json"]
AGENT_DATA = ["raw", "spans", "journal", "ledgers"]
AGENT_BASELINES = ["infra_negatives", "whowhen"]

# ★2026-07-28: the 12 zero-injection "normal" baseline cases are kept in the
#   SOURCE (datasets/agentfault_k8s) for false-alarm/specificity testing, but
#   are NOT shipped in the delivery tree. The headline localization denominator
#   is 96 faulted (matches existing agent-fault datasets and the traditional
#   line which has no normal arm). Two filters implement that at copy time:
#     - NORMAL_ARM_FILES: per-case + per-combo files excluded from the 4
#       AGENT_DATA dirs (raw/journal carry normal__r*.json; spans/ledgers carry
#       normal.jsonl + normal.serverlog). whowhen and infra_negatives already
#       contain only faulted cases, so they are NOT touched here.
#     - _write_faulted_agent_csv(): trims dataset_agentfault.csv to injected=="1".
NORMAL_ARM_FILES = shutil.ignore_patterns("normal__*", "normal.jsonl",
                                          "normal.serverlog")

# rec-agent batch's agent-layer views: they are produced by the traditional
# collection but belong, conceptually, to the agent tree.
NEGCTL_SRC = "datasets/_delivery/single_recagent_20260722"
NEGCTL_PARTS = ["agent_traces", "whowhen"]


def _const_baseline(root_sets):
    """Hit@1 of the best always-answer-X predictor over these cases.

    Ties (multiple services with the same top hit count) are broken
    alphabetically for determinism, and reported in `tied_with` so the README
    can name them honestly instead of picking one arbitrarily. Without this,
    the reported service flipped between runs (set iteration order = hash seed)
    on the triple tier where pricing/catalog/catalog-gw are a 3-way tie at 5/5.
    """
    cnt = collections.Counter()
    for s in root_sets:
        cnt.update(s)
    if not cnt:
        return {"service": None, "hit": 0, "n": 0, "rate": 0.0, "tied_with": []}
    top_count = cnt.most_common(1)[0][1]
    tied = sorted(s for s, c in cnt.items() if c == top_count)
    top = tied[0]
    hit = sum(1 for s in root_sets if top in s)
    return {"service": top, "hit": hit, "n": len(root_sets),
            "rate": round(hit / float(len(root_sets)), 3),
            "tied_with": tied[1:] if len(tied) > 1 else []}


def _negctl_summary(out):
    """Agent-layer behaviour per fault type, derived from the built artifacts."""
    base = os.path.join(out, "agent", "negative_controls")
    idx_p = os.path.join(base, "agent_traces", "index.json")
    ww_p = os.path.join(base, "whowhen", "cases_index.json")
    if not (os.path.exists(idx_p) and os.path.exists(ww_p)):
        return None
    with open(idx_p, encoding="utf-8") as f:
        idx = json.load(f)
    with open(ww_p, encoding="utf-8") as f:
        ww = json.load(f)
    runs = collections.Counter()
    for c in idx["cases"]:
        runs[c["fault_type"]] += c["pipeline_runs"]["during_fault"]
    grp = collections.defaultdict(collections.Counter)
    for c in ww["cases"]:
        grp[c["fault_type"]][c["group"]] += 1
    return {"spans": idx["totals"]["spans"], "runs": dict(runs),
            "groups": {k: dict(v) for k, v in grp.items()},
            "by_n_degraded": {g: m.get("by_n_degraded_agents", {})
                              for g, m in ww.get("groups", {}).items()}}


def p(*a):
    return os.path.join(ROOT, *a)


def run(cmd):
    print("  $ " + " ".join(cmd[1:]))
    r = subprocess.run(cmd, cwd=ROOT)
    if r.returncode:
        sys.exit("[ERR] command failed (%d)" % r.returncode)


def copytree(src, dst, ignore=None):
    shutil.copytree(src, dst, dirs_exist_ok=True, ignore=ignore)


def _write_faulted_agent_csv(src_csv, dst_csv):
    """Read the 108-row source dataset_agentfault.csv and write only the
    injected=="1" rows (96 faulted) to dst_csv. Returns (kept, total).

    The 12 zero-injection "normal" baselines stay in the source (for anyone
    who wants false-alarm testing) and are intentionally absent from the
    delivery tree, so the headline denominator (96) matches what is on disk.
    DictReader/DictWriter round-trip preserves every field; this is NOT a
    byte-copy because we drop rows.
    """
    with open(src_csv, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        fieldnames = rdr.fieldnames
        rows = list(rdr)
        kept = [r for r in rows if r.get("injected") == "1"]
    with open(dst_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in kept:
            w.writerow(r)
    return len(kept), len(rows)


def count_cases(d):
    return sum(1 for n in os.listdir(d)
               if n.startswith("mr") and os.path.isdir(os.path.join(d, n))) if os.path.isdir(d) else 0


def rebuild_traditional(native, dest, label):
    # Flags mirror what the three frozen packages were built with, so a rebuild
    # reproduces them (modulo byte-identity). --with-gt-distinct is required:
    # the README tells readers to read n_distinct_root_services for G, and
    # EVAL_NOTES_140's example code reads gt["n_distinct_root_services"]; without
    # it every rebuilt case is missing the field the partition is explained by.
    run([PY, "scripts/chaos/ctk/package_for_delivery.py",
         "--pilot-dir", native, "--out", dest,
         "--bare", "--force", "--flat-traces", "--with-calltree",
         "--with-eval", "--with-gt-distinct"])
    if label == "recagent15":
        run([PY, "scripts/chaos/ctk/build_recagent_agent_views.py",
             "--delivery-dir", dest, "--pilot-dir", native])


def main():
    global AGENT_SRC, AGENT_WW_SRC
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=datetime.date.today().strftime("%Y%m%d"))
    ap.add_argument("--mode", choices=("copy", "rebuild"), default="copy")
    ap.add_argument("--out-root", default="datasets/_delivery")
    ap.add_argument("--agent-src", default=AGENT_SRC,
                    help="agent-fault dataset root (default = agentfault_v2 REF). "
                         "For the B-档 (K8S) delivery: datasets/agentfault_k8s.")
    ap.add_argument("--agent-ww-src", default=AGENT_WW_SRC,
                    help="agent Who&When delivery view (default = "
                         "agentfault_v2_whowhen). For k8s: "
                         "datasets/agentfault_k8s_whowhen.")
    # P0-8 release gate (HANDOFF §7 P0-8). Default ON: the builder re-runs the
    # gate independently on every case it is about to copytree. --no-strict-gate
    # downgrades to warn-but-still-copy (legacy debug only).
    gate = ap.add_mutually_exclusive_group()
    gate.add_argument("--strict-gate", dest="strict_gate", action="store_true",
                      default=True,
                      help="(default) P0-8: reject cases whose release fields are "
                           "false/missing; they are excluded and NOT copied.")
    gate.add_argument("--no-strict-gate", dest="strict_gate", action="store_false",
                      help="legacy-debug ONLY: warn on release-field failures but "
                           "still copy. Do NOT use for real deliveries.")
    a = ap.parse_args()

    def _abs(x):
        return x if os.path.isabs(x) else p(x)
    AGENT_SRC = _abs(a.agent_src)
    AGENT_WW_SRC = _abs(a.agent_ww_src)
    print("[full] agent-src   = %s" % AGENT_SRC)
    print("[full] agent-ww-src= %s" % AGENT_WW_SRC)

    out = p(a.out_root, "RecShop_%s" % a.tag)
    print("[full] out=%s mode=%s" % (out, a.mode))
    if os.path.exists(out):
        # Never destroy first. A half-finished rmtree on Windows (file locked by
        # a zip tool or an editor) used to leave the old tree gutted and the new
        # one unwritten. Rename, build, then drop the backup only on success.
        # Anything hand-added to the tree (LICENSE, errata, a built zip) survives
        # in the backup instead of vanishing silently.
        bak = out + ".bak"
        if os.path.exists(bak):
            shutil.rmtree(bak)
        os.rename(out, bak)
        print("[full] existing tree -> %s (removed only after a clean build)"
              % os.path.basename(bak))
    else:
        bak = None
    for sub in ("traditional/single", "traditional/dual", "traditional/triple",
                "agent/whowhen", "agent/baselines", "agent/negative_controls"):
        os.makedirs(os.path.join(out, sub))

    stats = collections.Counter()
    case_index = []
    # P0-8 excluded ledger (HANDOFF §7 P0-8). The builder re-runs the gate
    # independently. Rejected cases are written to excluded_ledger.json at the
    # delivery root so a reader can see exactly what was held back and why.
    excluded = []  # list of {"source", "case", "reason"}

    # ---------------- traditional ----------------
    # Staging must start empty. Leftovers from a previous rebuild of the same tag
    # would be copied into the delivery as if they were current - that is exactly
    # the mechanical path by which 15 stale-GT cases once shipped.
    staging = p(a.out_root, "_rebuild_%s" % a.tag)
    if a.mode == "rebuild" and os.path.exists(staging):
        print("[full] clearing stale staging %s" % staging)
        shutil.rmtree(staging)
    # ★ Bucket by DISTINCT ROOT-CAUSE SERVICES (G), the service-level scoring
    #   unit, NOT by injected-fault count. A case with 2 faults on 1 service is a
    #   single-root case at service granularity and lands in single/. The folder
    #   NAME keeps its frozen mrN prefix (N = injected faults), so a single/
    #   folder named mr2_* is honestly "1 root service, 2 faults" - the compound
    #   flag in case_index makes that queryable. See README.
    G_BUCKET = {1: "single", 2: "dual", 3: "triple"}
    compound = []
    for label, frozen, native, arity in TRAD:
        if a.mode == "rebuild":
            tmp = os.path.join(staging, label)
            rebuild_traditional(native, tmp, label)
            src = tmp
        else:
            src = p(frozen)
            if not os.path.isdir(src):
                sys.exit("[ERR] frozen package missing: %s" % src)
        n = 0
        for name in sorted(os.listdir(src)):
            sp = os.path.join(src, name)
            if not (name.startswith("mr") and os.path.isdir(sp)):
                continue
            with open(os.path.join(sp, "groundtruth.json"), encoding="utf-8") as f:
                gt = json.load(f)
            g = len(set(gt["root_cause_services"]))
            nf = len(gt.get("fault_types") or [])
            bucket = G_BUCKET.get(g)
            if bucket is None:
                sys.exit("[ERR] unexpected G=%d in %s" % (g, name))
            dp = os.path.join(out, "traditional", bucket, name)
            if os.path.exists(dp):
                sys.exit("[ERR] case name collision: %s" % name)
            # ----- P0-8 release gate (HANDOFF §7 P0-8). Re-run INDEPENDENTLY here:
            #       the builder must not trust that the upstream frozen package
            #       was already filtered. A gate failure skips the copytree and
            #       records the case in excluded_ledger.json. -----
            gate_ok, gate_reason = check_release_gate(sp)
            if not gate_ok:
                if a.strict_gate:
                    print("[full] GATE-REJECT %s/%s: %s (excluded; not copied)"
                          % (label, name, gate_reason))
                    excluded.append({"source": label, "case": name,
                                     "tier": bucket, "reason": gate_reason})
                    continue
                else:
                    print("[full] GATE-WARN %s/%s: %s (--no-strict-gate: copied anyway)"
                          % (label, name, gate_reason))
            copytree(sp, dp)
            with open(os.path.join(dp, "metadata.json"), encoding="utf-8") as f:
                src_case = json.load(f).get("source_case_id")
            # source_package = the 3-value batch a reader splits by (dense140 /
            # spread55 / recagent15); `batch` keeps the fine-grained design-arity
            # label (e.g. dense140_dual) which != tier for compound cases.
            pkg = re.sub(r"_(single|dual|triple)$", "", label)
            # ★signal_class 走 dataset_registry.case_family() 【从 groundtruth 现算】,
            #   不从 tree/type 名反推(REGISTRY._provenance 明令), 也不抄 REGISTRY 的汇总数
            #   —— 那里各族的 n_cases/delta_z_hit1 仍是 195 期陈旧值(counts 才是 255 期)。
            #   未登记的 fault_type 会在 signal_class() 里 fail-loud, 不会静默污染分栏。
            sig = DR.case_family(dp)
            leg_types = [lg["fault_type"] for lg in gt["component_ground_truth"]]
            rec = {"folder": name, "tier": bucket, "distinct_root_services": g,
                   "injected_faults": nf, "source_arity": arity, "batch": label,
                   "source_package": pkg, "source_case_id": src_case,
                   "compound": nf > g,
                   "signal_class": sig,
                   "artifact_confounded": bool(set(leg_types) & ARTIFACT_FAULT_TYPES)}
            case_index.append(rec)
            if nf > g:
                compound.append(rec)
            stats[bucket] += 1
            n += 1
        print("[full] %-16s -> %d cases re-bucketed by G" % (label, n))
    for b in ("single", "dual", "triple"):
        print("[full] traditional/%-7s = %3d (by distinct root services)"
              % (b, stats[b]))

    for src, dst in EVAL_NOTES:
        shutil.copyfile(p(src), os.path.join(out, "traditional", dst))

    # ---------------- agent ----------------
    for req in (AGENT_SRC, AGENT_WW_SRC):
        if not os.path.isdir(p(req)):
            sys.exit("[ERR] agent source missing: %s" % p(req))
    for f in AGENT_DOCS:
        s = p(AGENT_SRC, f)
        if os.path.exists(s):
            if f == "dataset_agentfault.csv":
                # Ship only the 96 faulted cases; the 12 zero-injection normals
                # stay in the source. See _write_faulted_agent_csv().
                _n_kept, _n_src = _write_faulted_agent_csv(
                    s, os.path.join(out, "agent", os.path.basename(f)))
                print("[full] agent/dataset_agentfault.csv: kept %d faulted "
                      "rows (source has %d)" % (_n_kept, _n_src))
            else:
                shutil.copyfile(s, os.path.join(out, "agent", os.path.basename(f)))
    for d in AGENT_DATA:
        s = p(AGENT_SRC, d)
        if os.path.isdir(s):
            # Drop the normal-arm files (normal__r*.json / normal.jsonl /
            # normal.serverlog). whowhen + infra_negatives already skip normal.
            copytree(s, os.path.join(out, "agent", d), ignore=NORMAL_ARM_FILES)
    for d in AGENT_BASELINES:
        s = p(AGENT_SRC, d)
        if os.path.isdir(s):
            copytree(s, os.path.join(out, "agent", "baselines", d))
    copytree(p(AGENT_WW_SRC), os.path.join(out, "agent", "whowhen"))
    stats["agent_whowhen"] = len([x for x in os.listdir(
        os.path.join(out, "agent", "whowhen", "Injection-Generated"))
        if x.endswith(".json")])

    # negative controls (from the rec-agent traditional batch)
    ncs = p(NEGCTL_SRC) if a.mode == "copy" else os.path.join(staging, "recagent15")
    for part in NEGCTL_PARTS:
        s = os.path.join(ncs, part)
        if os.path.isdir(s):
            copytree(s, os.path.join(out, "agent", "negative_controls", part))
    ncd = os.path.join(out, "agent", "negative_controls", "whowhen")
    for g in ("Infra-Negative", "Infra-Negative-Degraded"):
        d = os.path.join(ncd, g)
        stats["negctl_" + g] = len(os.listdir(d)) if os.path.isdir(d) else 0

    # Per-fault agent-layer behaviour, READ BACK from the generated artifacts
    # rather than restated in prose. An earlier hand-written version of this
    # paragraph was wrong for two months of nobody noticing.
    negctl = _negctl_summary(out)

    # ---- constant baseline per G-tier, computed from the assembled tree ----
    # Published, not hidden: the multi-root tiers are heavily skewed toward the
    # high fan-in hub, because producing REAL propagation requires hitting it.
    # A reader who does not know this will mistake a constant predictor for a
    # working method.
    by_tier = collections.defaultdict(list)
    for r in case_index:
        by_tier[r["tier"]].append(r)
    const = {}
    for tier in ("single", "dual", "triple"):
        const[tier] = _const_baseline([set()]) if not by_tier[tier] else None
    # const per tier needs the actual root-service sets; recompute properly
    const = {}
    for tier in ("single", "dual", "triple"):
        sets = []
        for c in sorted(os.listdir(os.path.join(out, "traditional", tier))):
            with open(os.path.join(out, "traditional", tier, c, "groundtruth.json"),
                      encoding="utf-8") as f:
                sets.append(set(json.load(f)["root_cause_services"]))
        const[tier] = _const_baseline(sets)

    # Tiers are now bucketed BY G, so each must be uniform in G. That invariant
    # is asserted here: a non-uniform tier means the re-bucketing mislabeled
    # something and every downstream claim is suspect.
    tier_meta = {}
    for tier in ("single", "dual", "triple"):
        rows = by_tier[tier]
        gseen = collections.Counter(r["distinct_root_services"] for r in rows)
        nfseen = collections.Counter(r["injected_faults"] for r in rows)
        n_compound = sum(1 for r in rows if r["compound"])
        tier_meta[tier] = {
            "n_cases": len(rows),
            "distinct_root_services_G": dict(sorted(gseen.items())),
            "injected_faults_distribution": dict(sorted(nfseen.items())),
            "compound_cases": n_compound,
            "constant_baseline": const[tier],
        }
        tier_meta[tier]["signal_class"] = dict(sorted(
            collections.Counter(r["signal_class"] for r in rows).items()))
        tier_meta[tier]["artifact_confounded"] = sum(
            1 for r in rows if r["artifact_confounded"])
        if len(gseen) != 1:
            sys.exit("[ERR] tier %s not uniform in G: %s (re-bucketing bug)"
                     % (tier, dict(gseen)))

    # ---- signal_class 分层摘要(DATASHEET §6 L3/L15 的机器可读版) ----
    # 为什么必须随包发: REGISTRY 的 families.root_local.eval_note 原话 ——
    #   "合并进主表会稀释常量先验、制造『benchmark 变强了』的假象。必须单独一栏。"
    # 外部使用者拿不到 REGISTRY, 若包里没有这一栏, 他们【做不了这个分层】, 只能报总体分,
    # 而总体分会把"能定位 root_local"误读成"能做 RCA"。
    sig_ct = collections.Counter(r["signal_class"] for r in case_index)
    sig_x_art = collections.defaultdict(lambda: {"total": 0, "artifact_confounded": 0})
    for r in case_index:
        d = sig_x_art[r["signal_class"]]
        d["total"] += 1
        d["artifact_confounded"] += int(r["artifact_confounded"])
    signal_class_summary = {
        "_doc": ("每 case 的 signal_class 见 case_index。root_local=根因自身指标直接可见(送分); "
                 "propagation=只能靠传播推;off_graph=根因不是被埋点的服务(候选集需含伪节点); "
                 "mixed=多根因各腿分属不同族。报分必须按此分层,不可只报总体 Hit@1。"),
        "counts": dict(sorted(sig_ct.items())),
        "by_class": {k: dict(v) for k, v in sorted(sig_x_art.items())},
        "artifact_confounded_total": sum(1 for r in case_index if r["artifact_confounded"]),
        "_artifact_note": ("artifact_confounded=dependency_latency/runtime_exception 腿 —— "
                           "靠 kubectl set env 注入, 触发 rollout 起新 pod, 其『本地可见性』"
                           "是注入器指纹而非故障效应。见 DATASHEET §6 L15。"),
    }

    const["all"] = _const_baseline(
        [set(json.load(open(os.path.join(out, "traditional", t, c, "groundtruth.json"),
                          encoding="utf-8"))["root_cause_services"])
         for t in ("single", "dual", "triple")
         for c in sorted(os.listdir(os.path.join(out, "traditional", t)))])

    # agent case count from the FILTERED delivery CSV (96 faulted), NOT the
    # 108-row source — MANIFEST must match what is on disk in the delivery.
    with open(os.path.join(out, "agent", "dataset_agentfault.csv"),
              encoding="utf-8") as f:
        stats["agent_cases"] = sum(1 for _ in csv.DictReader(f))

    # Who&When headline (all_at_once overall + per-family) read from the agent
    # batch's OWN whowhen_results.json, so the README cites the numbers correct
    # for whichever batch is packaged (v2 = 0.406, k8s = 0.427 — they differ).
    # Falls back to None (rendered "N/A") if the batch has no scored whowhen yet.
    ww_headline = {"all_at_once": None, "hallucinate": None, "context_drift": None}
    ww_json = p(AGENT_SRC, "whowhen", "whowhen_results.json")
    if os.path.isfile(ww_json):
        try:
            with open(ww_json, encoding="utf-8") as f:
                wj = json.load(f)
            for r in wj.get("results", []):
                if r.get("method") == "all_at_once":
                    o = r.get("mrcbench_overall", {})
                    bf = r.get("mrcbench_by_family", {})
                    ww_headline["all_at_once"] = o.get("hit@1")
                    ww_headline["hallucinate"] = bf.get("hallucinate", {}).get("hit@1")
                    ww_headline["context_drift"] = bf.get("context_drift", {}).get("hit@1")
                    break
        except Exception as e:
            print("[full] WARN: whowhen headline unreadable at %s: %s" % (ww_json, e))

    # Agent-batch environment bullet for the README ⚠️ block. v2 (local harness,
    # host_cpu_pct all-zero) vs k8s (full cluster, prom_container, host_cpu_pct
    # nonzero) describe DIFFERENT environments — the README must match the batch
    # actually shipped. Detected from the CSV, not hardcoded.
    _hcpu_zero = True
    try:
        import csv as _csvm
        _rows = list(_csvm.DictReader(
            open(p(AGENT_SRC, "dataset_agentfault.csv"), encoding="utf-8")))
        _vals = [r.get("host_cpu_pct") for r in _rows]
        _hcpu_zero = bool(_vals) and all(
            (v in (None, "", "0", "0.0", "0.00")) or (float(v) == 0) for v in _vals)
    except Exception:
        _hcpu_zero = True
    _ac = stats["agent_cases"]
    if _hcpu_zero:
        agent_env_bullet = (
            "> * `agent/` %d case = **隔离 harness** —— 只起 `recommendation_agent` 单进程 + 真实\n"
            ">   `sasrec_api`(**共 2 个服务;本机运行,不在 K8S 上,无 Chaos Mesh**),**无 per-service\n"
            ">   基础设施指标**(`host_cpu_pct` 列恒 0 即此故),只有进程内 OTel span(含内容层属性)。" % _ac)
    else:
        agent_env_bullet = (
            "> * `agent/` %d case = **同一 K8S 集群**里的 `rec-agent` pod(集群 DNS 调真实\n"
            ">   `sasrec:8200` + DeepSeek,OTLP 到与 traditional 同一个 collector),host 水位 = prom_container\n"
            ">   **容器级**(`host_cpu_pct` 列非 0);Agent 故障由**注入器改写 LLM 消息**制造(非 Chaos Mesh)。\n"
            ">   只采 rec-agent 自己的进程内 span(含内容层属性)+ pod 容器水位,**非 25 服务三模态**。" % _ac)

    manifest = {
        "schema_version": "recshop-full-delivery.v1",
        "tag": a.tag,
        "build_mode": a.mode,
        "traditional": {"single": stats["single"], "dual": stats["dual"],
                        "triple": stats["triple"],
                        "total": stats["single"] + stats["dual"] + stats["triple"],
                        "_bucketing": "folders grouped by DISTINCT root-cause "
                                      "services (G = service-level scoring unit), "
                                      "NOT by injected-fault count; folder-name "
                                      "mrN prefix = injected faults (frozen)"},
        "tiers": tier_meta,
        "constant_baseline_hit_at_1": const,
        "signal_class_summary": signal_class_summary,
        "case_index": case_index,
        "agent": {"cases_csv_rows": stats["agent_cases"],
                  "whowhen_cases": stats["agent_whowhen"],
                  "whowhen_headline": ww_headline,
                  "env_bullet": agent_env_bullet,
                  "negative_controls": {
                      "Infra-Negative": stats["negctl_Infra-Negative"],
                      "Infra-Negative-Degraded": stats["negctl_Infra-Negative-Degraded"],
                      "detail": negctl}},
        # Where every byte came from. Without this, "rerun the command" is not a
        # reproduction claim - the sources are large, gitignored, and local.
        "sources": [{"label": lb, "path": (fz if a.mode == "copy" else nv),
                     "arity": ar, "mode": a.mode} for lb, fz, nv, ar in TRAD]
                   + [{"label": os.path.basename(os.path.normpath(AGENT_SRC)),
                       "path": AGENT_SRC},
                      {"label": os.path.basename(os.path.normpath(AGENT_WW_SRC)),
                       "path": AGENT_WW_SRC}],
        "rebuild_command": ("python scripts/chaos/ctk/build_full_delivery.py "
                            "--tag %s --mode %s --agent-src %s --agent-ww-src %s"
                            % (a.tag, a.mode, a.agent_src, a.agent_ww_src)),
    }
    with open(os.path.join(out, "MANIFEST.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    write_readme(out, manifest)

    # ---- P0-8 excluded ledger (HANDOFF §7 P0-8). Write excluded_ledger.json at
    #      the delivery root so a reader can audit exactly which cases the gate
    #      held back and why. Always written (even when empty) so its absence is
    #      itself a red flag that the gate did not run.
    ledger = {
        "schema": "p0_8_excluded_ledger/v1",
        "strict_gate": a.strict_gate,
        "n_excluded": len(excluded),
        "excluded": excluded,
    }
    with open(os.path.join(out, "excluded_ledger.json"), "w", encoding="utf-8") as f:
        json.dump(ledger, f, ensure_ascii=False, indent=2)
    if excluded:
        from collections import Counter as _Ctr
        reason_counts = _Ctr(e["reason"] for e in excluded)
        print("[full] GATE excluded %d case(s) by reason:" % len(excluded))
        for reason, cnt in sorted(reason_counts.items()):
            print("      %3d x %s" % (cnt, reason))
        for e in excluded:
            print("      - %s/%s: %s" % (e["source"], e["case"], e["reason"]))
    else:
        print("[full] GATE: 0 cases excluded (all passed release gate)")

    # ---- 把【手工维护的根级文件】从备份搬回新树 ----
    # ★2026-07-27 修的真 bug: L243 把旧树 rename 成 .bak, L481 又在成功后 rmtree 掉它,
    #   而 DATASHEET.md / LICENSE.md / score_baro.py 【本脚本不生成】 ⇒ 跑一次重建就把它们
    #   无声销毁。原注释"hand-added 会在备份里存活"是自欺 —— 备份随后就删了。
    #   这里显式搬回, 并把任何【未登记也未重新生成】的根级文件【大声列出来】, 不许静默丢。
    #   SHA256SUMS.txt 【故意不搬】: 它必须在终态重算(package_dist.py 负责), 搬回等于发陈旧哈希。
    if bak and os.path.exists(bak):
        for fn in PRESERVE_FROM_BAK:
            s, d = os.path.join(bak, fn), os.path.join(out, fn)
            if os.path.exists(s) and not os.path.exists(d):
                shutil.copyfile(s, d)
                print("[full] preserved hand-maintained %s" % fn)
        orphans = [fn for fn in sorted(os.listdir(bak))
                   if os.path.isfile(os.path.join(bak, fn))
                   and not os.path.exists(os.path.join(out, fn))
                   and fn not in REGENERATED_ROOT_FILES]
        if orphans:
            print("[full] ⚠ 备份里这些根级文件【没有】进新树, 确认是否该丢: %s" % ", ".join(orphans))
    if bak and os.path.exists(bak):
        shutil.rmtree(bak)
        print("[full] build clean -> backup dropped")
    print("[full] DONE %s" % json.dumps(manifest["traditional"]))
    print("[full] agent %s" % json.dumps(manifest["agent"]))
    print("[full] -> %s" % out)


def write_readme(out, m):
    t, ag = m["traditional"], m["agent"]
    cb = m["constant_baseline_hit_at_1"]
    tm = m["tiers"]
    _n = {"single": u"单根因", "dual": u"双根因", "triple": u"三根因"}

    def _nf_dist(d):
        return " · ".join("注入%d故障的 %d 个" % (int(k), v)
                          for k, v in sorted(d.items(), key=lambda kv: int(kv[0])))

    def _svc_cell(e):
        svcs = [e["service"]] + e["tied_with"]
        return " / ".join("`%s`" % s for s in svcs)

    tier_rows = "\n".join(
        "| `%s/` | %d | %s | %s | %d | %s **%.3f** |"
        % (k, tm[k]["n_cases"], _n[k], _nf_dist(tm[k]["injected_faults_distribution"]),
           tm[k]["compound_cases"], _svc_cell(cb[k]), cb[k]["rate"])
        for k in ("single", "dual", "triple"))

    rows = "\n".join(
        "| `%s` | %d | %s | %d/%d = **%.3f** |"
        % (k if k != "all" else u"三档合并", cb[k]["n"], _svc_cell(cb[k]),
           cb[k]["hit"], cb[k]["n"], cb[k]["rate"])
        for k in ("single", "dual", "triple", "all"))
    d = ag["negative_controls"].get("detail") or {}
    label = {"network_delay": u"网络延迟 450ms", "service_cpu_saturation": u"CPU 饱和",
             "service_unavailable": u"杀 pod"}
    obs = "\n".join(
        "| %s | %d | %s |" % (
            label.get(k, k), d.get("runs", {}).get(k, 0),
            (u"全部 4 个 Agent 产出真实分析,只是变慢"
             if d.get("groups", {}).get(k, {}).get("degraded", 0) == 0
             and d.get("runs", {}).get(k, 0) > 0 else
             u"Agent 层无任何遥测(pod 已被杀)" if d.get("runs", {}).get(k, 0) == 0 else
             u"%d 次正常, %d 次有 Agent 调用失败、该轮降级成占位文本"
             % (d["groups"][k].get("clean", 0), d["groups"][k].get("degraded", 0))))
        for k in ("network_delay", "service_cpu_saturation", "service_unavailable"))
    _ww = ag.get("whowhen_headline", {}) or {}
    _f3 = lambda v: ("%.3f" % v) if isinstance(v, (int, float)) else "N/A"
    txt = README_TMPL.format(
        tag=m["tag"], single=t["single"], dual=t["dual"], triple=t["triple"],
        trad_total=t["total"], agent_cases=ag["cases_csv_rows"],
        ww=ag["whowhen_cases"], const_rows=rows, obs_rows=obs,
        tier_rows=tier_rows,
        nc_spans="{:,}".format(d.get("spans", 0)),
        nc_clean=ag["negative_controls"]["Infra-Negative"],
        nc_deg=ag["negative_controls"]["Infra-Negative-Degraded"],
        nc_deg_dist=json.dumps(
            d.get("by_n_degraded", {}).get("Infra-Negative-Degraded", {}),
            ensure_ascii=False),
        nc_total=(ag["negative_controls"]["Infra-Negative"]
                  + ag["negative_controls"]["Infra-Negative-Degraded"]),
        ww_all_at_once=_f3(_ww.get("all_at_once")),
        ww_hallu=_f3(_ww.get("hallucinate")),
        ww_ctxdrift=_f3(_ww.get("context_drift")),
        agent_env_bullet=ag.get("env_bullet", ""))
    with open(os.path.join(out, "README.md"), "w", encoding="utf-8") as f:
        f.write(txt)


README_TMPL = u"""# RecShop 故障数据集({tag})

**一个数据集,两个层、两类根因。** 同一个真实电商推荐系统(SASRec 序列推荐 + 4 Agent LangGraph 协作 + LLM 重排,25 微服务 + K8S):`traditional/` 注入基础设施故障({trad_total} case)、`agent/` 注入 Agent 语义故障({agent_cases} case),各自给出机器可核验的根因真值;另带一组跨层阴性对照(`agent/negative_controls/` {nc_total} 条)。

> **定位与价值**:跑在**真实 25 微服务电商系统**上(K8S + Chaos Mesh 注入、全栈 OTel 三模态、机器可核验根因真值)—— 数据集**扎实、根因可定位**(BARO/resource 0.608、Who&When `all_at_once` 0.427、契约 oracle 1.000 均显著超常量基线),适合作 metric / log / 多模态 RCA 方法的**泛化补充与方法可分性测试床**(TrainTicket 之外第二拓扑);诚实局限见下文。

## 包里有什么

```
traditional/single · dual · triple    按 G(去重根因服务数)分档,共 {trad_total} case
  case 目录:metadata/groundtruth/eval/data.csv + raw/(metrics·traces 扁平投影+原生调用树·logs)/scripts/
agent/  ({agent_cases} faulted case)
  data:dataset_agentfault.csv · limitations.json · run_summary/context_drift_outcomes/content_ctxdrift_results.json
       whowhen/({ww} 交付视图) · baselines/ · raw/ · spans/ · journal/ · ledgers/ · negative_controls/({nc_total} 条)
DATASHEET.md · FAULT_DESIGN.md · MANIFEST.json
```

| 目录 | case | G | 注入故障数分布 | 复合 case* | 常量基线 Hit@1 |
|---|---|---|---|---|---|
{tier_rows}

\* **复合 case** = 注入故障数 > 根因服务数(多个故障打在同一服务上)。**判档看 `single/dual/triple` 目录,不看目录名的 `mrN` 前缀**(= 冻结前的注入故障数,保留作溯源)。`MANIFEST.json` 的 `case_index` 给每条标了 `tier / distinct_root_services / injected_faults / compound`。**5 个来源批次**(dense140 / spread55 / recagent15 / dual_ext25 / triple_ext20)同目录混排、按 `source_package` 区分,特征面板与密度不同,concat 需批次感知切分(细则见 `DATASHEET.md`)。

> `agent/negative_controls/` {nc_total} 条 = 把 `traditional/single/` 里打在 `rec-agent` 上的 15 个 case,在零 Agent 注入下重采的 Agent 流水线轨迹(真值统一是"根因在基础设施",点名任一 Agent 即误报)。明细见 `DATASHEET.md` §2.2。

> **故障设计**(逐 combo 完整表:8 个单腿基础故障机制 + 21 个双故障腿设计 combo + 8 个三故障腿设计 combo + 4 个 agent 族)见 **`FAULT_DESIGN.md`**。这里的“双/三故障腿”描述设计态注入腿数，不是服务级 G；交付判档仍以去重根因服务数为准。

## 怎么评

- **traditional**:服务级,GT = 被注入的服务(即使级联也不标下游),R 取所在档 G(single=1 / dual=2 / triple=3)。入口 = 每个 case 的 `eval/data.csv`;跑前必读 `DATASHEET.md` §2 与 §3(切分防泄漏、特征面板差异、`inject_time` 锚点等坑 —— 按来源批次,原 per-batch EVAL_NOTES 内部的 single / dual / triple 是**注入故障数**,与本目录 G 分档不再一一对应)。
- **agent**:候选 = 4 个进程内 Agent(单根),分母 = 96(零注入基线不参与定位 macro);**Hit@1 是唯一有判别力的头条**(4 候选 ⇒ Hit@5 恒 1.000)。入口 = `agent/whowhen/` + `agent/dataset_agentfault.csv`。

## ★ 关键结果

**规则(全包适用,只说一次)**:每个分数都并排它所在档的**常量基线**(永远答同一个最热门服务 / Agent 拿到的分)。不高于基线 = 没有定位能力,只是先验。

### traditional/(常量基线 = "永远答最热门服务")

| 档 | n | 最优常答 | 常量基线 Hit@1 |
|---|---|---|---|
{const_rows}

BARO/resource macro Hit@1 = **0.608**(140 版 0.500,配对 Δ +0.235 [0.145, 0.322]);**RCD/resource 仅 0.216**(结构性只吐 ~2 候选)。注:ext45 子组 BARO/resource = 1.000(45/45 近饱和)推高了合并数,朴素 delta_z/resource 0.816 仍居首 —— ext45(45)+ spread/recagent(70)两批叠加把 dual / triple 的中枢倾斜从 1.000 / 0.800 稀释到 0.600 / 0.600(逐方法 + 分池见 `DATASHEET.md` §关键结果表 A(逐方法/逐服务完整表可索取))。`single/` 常量基线最低(0.192)、判别力最强,是定位能力的主战场。

### agent/(常量基线 = "永远答 `Recommendation_Synthesizer`" = **0.375**,36/96)

被注入的 case **恰好注一个 Agent**,无单 / 双 / 三根因之分。四类故障:hallucinate 36 / context_drift 36 / wrong_item_pick 12 / format_violation 12(= **96 faulted**)。本交付只发这 96 个 faulted case;另 12 个零注入干净基线(normal 臂)留源仓供误报率/特异性测试,**不在本交付**(定位分母仍是 96,与历史 agent-fault 数据集口径一致)。

| 方法 | overall Hit@1 | hallu(n=36) | wrong_pick(n=12) | format(n=12) | ctx_drift(n=36) |
|---|---|---|---|---|---|
| Who&When `all_at_once` | **{ww_all_at_once}** | **{ww_hallu}** | 0.833 | 0.417 | **{ww_ctxdrift}** |
| 常量基线 | 0.375 | 0.000 | 1.000 | 1.000 | 0.333 |

判官 = DeepSeek;逐方法 / Hit@K / MRR / 解析率见 `DATASHEET.md` §关键结果表 B(逐方法/Hit@K/MRR 可索取)。**GLM-5.2 跨族复核**:all_at_once **0.240**(hallucinate 0.361 / context_drift 0.000)—— 跨族确认 hallucinate 仍可定位、context_drift 跨族仍盲;GLM 分低于 DeepSeek ⇒ 同族膨胀未见。

### 三条互不重叠的"只有 X 能定位 Y"

1. **结构化故障(wrong_item_pick / format_violation)**:确定性内容信号能定位,但该族先验本身就是 1.000 —— Who&When 的 0.833 / 0.417 反而**低于先验**,该族先验即 1.000,方法分未超出先验。
2. **hallucinate**:**只有内容感知判官能定位**(`all_at_once` {ww_hallu},先验 0.000 ⇒ 真定位);但方法间差异极大(`binary_search` 仅 0.083)。
3. **context_drift**:**近乎对所有输出阅读型方法不能定位**(3/4 低于先验 0.333;binary_search 0.389 勉强超但 n=36 不显著);**只有轨迹结构信号能定位**(`agentfault.resolved_input` 观察型检测器 36/36 = 1.000,见 `DATASHEET.md` §关键结果表 B)。

## 已知局限(详见 `DATASHEET.md` §3)

- **多根因两档 Hit@1 几乎只反映先验**(常量基线 0.600):报定位能力看 `single/`(基线 0.192);dual 报 FullHit@2 / Recall@2,triple 报 FullHit@3 / Recall@3(n={triple} 标注)。
- **只 1 个 Agent 系统**:4 个 Agent 是同一 LangGraph 流水线内的角色,泛化到其他多 agent 拓扑需新数据。
- **完整局限**(C3 GT 不可逐 case 重推导、内容检测器底噪、Eadro erratum 等)见 `DATASHEET.md` §3 + `agent/limitations.json`。

## 钻取入口(按需深入)

- **`DATASHEET.md`** —— 完整方法学 / §关键结果表(指标汇总) / threats-to-validity / GT 字段陷阱 / anti-trivial 细则 / per-batch 评测须知的汇总。**doc 层有它 + 本 README + `FAULT_DESIGN.md`**。
- **`MANIFEST.json`** —— 机器可读账目:`case_index` {trad_total} 条 `folder → tier / G / injected / compound / source_package / signal_class / artifact_confounded`、`constant_baseline_hit_at_1`、`signal_class_summary`、agent `whowhen_headline`、`sources`。
- **包外仓内**(非交付件,需时索取)—— 逐方法 / 逐服务 / 逐 case 的完整分数表(传统基线、agent 逐方法、per-batch EVAL_NOTES)均留源仓、可索取。
"""


if __name__ == "__main__":
    main()
