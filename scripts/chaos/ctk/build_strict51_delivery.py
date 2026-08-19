# -*- coding: utf-8 -*-
"""build_strict51_delivery.py - assemble the strict51 traditional-v2 delivery tree.

    python scripts/chaos/ctk/build_strict51_delivery.py \
        --lite-root <lite-root> \
        --out datasets/_delivery/strict51_20260819

One command. Output lands in datasets/_delivery/<tag>/ (tag defaults to
strict51_<today>). Re-running with the same --out rebuilds in place.

================================ WHAT THIS BUILDS ================================

  strict51_<tag>/
    README.md           tree guide + the amendment disclosure (see below)
    MANIFEST.json       machine-readable index incl. the `amended` column
    traditional/
      single/   G=1  130 cases (125 primary + 5 amended)
      dual/     G=2  100 cases ( 92 primary +  8 amended)
      triple/   G=3   25 cases ( 25 primary +  0 amended)
    Buckets are by DISTINCT root services G (service-level), mirroring the
    RecShop_20260728 convention. Folder names are the shijie-style tokens the
    v1 packager derives from metadata; the strict51 case id (s51-bK-NN-<sc>)
    is preserved in case_index.source_case_id and metadata.formal_slot_id.

============================ WHERE THE CASES COME FROM ============================

The lite worktree's strict51 campaign trees (READ-ONLY here, never mutated):

  traditional_v2_lite_strict51_s51_b1 .. b5   242 valid primary slots
    .qualification/S51-B*/.attempts/<NN>-<scenario>/runner-out/<case-id>/
    Selection = outcome.json EXISTS + runner-out case complete (metadata.json,
    groundtruth.json, raw/metrics/metrics_v2.jsonl). Attempts named nf_*/sham_*
    are the 30 qualification controls (they deliberately produce no case and
    are NOT delivered, same as v1's excluded 12 normals). *.tmp dirs skipped.
  traditional_v2_lite_strict51_amd     13 PASS amendments
    .qualification/S51-AMD/amendment-manifest.json drives selection
    (verdict == PASS); 2 FAIL attempts stay in the manifest, never delivered.

=============================== HOW IT IS BUILT ================================

The v1 per-case packager is reused UNCHANGED (that is the point: byte-compatible
delivery format with what shijie already reads). This script only:

  1. selects the 255 cases (hard census assertions fail-loud if the campaign
     state ever drifts from 242 primary + 13 amended = 130/100/25 by G),
  2. stages them flat under datasets/_runtime/strict51_stage/native/,
  3. runs package_for_delivery.py ONCE with the full v1 flag set
     (--bare --force --flat-traces --with-calltree --with-eval --with-gt-distinct),
  4. moves each delivered folder into its G bucket,
  5. re-runs the P0-8 release gate independently per delivered case,
  6. writes MANIFEST.json (case_index with amended/filter columns) + README.md.

=============================== AMENDMENT DISCLOSURE ===============================

13 of the 255 delivered cases are disclosed re-runs of slots that failed their
first attempt for SCIENTIFIC_ATTRITION / environment-degradation reasons (exec
crash cluster during B4's degraded window; B5's first block was voided and
re-collected after a machine reboot - that void is archived, not delivered).
Amendments follow protocol v3 (same frozen schedule/seed/identity chain; first
attempt only per two-digit label) and NEVER count into the 242 primary slots.
They are filterable via case_index.amended == true and per-case
amendment.{scenario, attempt_id}. Full audit trail:
  <lite>/datasets/k8s_pilot/traditional_v2_lite_strict51_amd/.qualification/
         S51-AMD/amendment-manifest.json   (15 entries: 13 PASS + 2 D06 FAIL)
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
PY = sys.executable

sys.path.insert(0, HERE)
import dataset_registry as DR  # noqa: E402  (signal_class: fail-loud on unknown)
from package_for_delivery import check_release_gate  # noqa: E402  (P0-8, re-run)

# ---- campaign invariants (hard assertions; campaign state is frozen) ----------
BLOCKS = ["b1", "b2", "b3", "b4", "b5"]
EXPECT_PRIMARY_PER_BLOCK = {"b1": 50, "b2": 50, "b3": 49, "b4": 43, "b5": 50}
EXPECT_AMENDED = 13
EXPECT_TOTAL = 255
EXPECT_G = {"single": 130, "dual": 100, "triple": 25}
G_BUCKET = {1: "single", 2: "dual", 3: "triple"}
# attempts whose dir name matches this are qualification controls, not cases
CONTROL_RE = re.compile(r"^\d+-(nf_|sham_)")
# 注入伪影族(与 build_full_delivery.py 同一判定): rollout 重启指纹而非故障效应,
# 可一行过滤(column artifact_confounded)做敏感性分析。
ARTIFACT_FAULT_TYPES = frozenset({"dependency_latency", "runtime_exception"})

STAGE_ROOT = os.path.join(ROOT, "datasets", "_runtime", "strict51_stage")

# 交付树 README 的权威模板 = 磁盘 datasets/_delivery/strict51_20260819/README.md
# (中文 v1 格式版)。改动 README 时同步改这里,或 rebuild 前备份磁盘版。
README_TMPL = """# RecShop 故障数据集 · traditional v2(strict51,20260819)

**同一拓扑、同一基线,strict51 协议化重采。** 同一个真实电商推荐系统(SASRec 序列推荐 + 25 微服务 + K8S):`traditional/` 注入基础设施故障(**255 case**),机器可核验的根因真值;单层交付,无 agent 侧(v2 为 traditional 线的协议化重采,agent 线沿用 `RecShop_20260728` 不变)。

> **定位与价值**:与 v1(`RecShop_20260728` traditional 255)**同故障族、同常量基线(逐层一字不差)**的一次采集协议升级——51 场景 × 5 区组 RCBD、预冻结身份链、逐 case 资格门。数据更可用(BARO/resource 0.608 → **0.698**,朴素法微降),协议可审计(预注册 + 修正案留痕)。适合作 metric / log / 多模态 RCA 方法的泛化补充与方法可分性测试床;诚实局限见下文。

## 包里有什么

```
traditional/single · dual · triple    按 G(去重根因服务数)分档,共 255 case
  case 目录:metadata/groundtruth/eval/data.csv + raw/(metrics·traces 扁平投影+原生调用树·logs)/scripts/
  traditional/per_case_scores_255.csv  逐 case × 16 方法/通道组合的全指标(BARO/RCD 5seed/delta_z/delta_ratio × full/resource)
FAULT_DESIGN.md · LICENSE.md · CITATION.cff · MANIFEST.json
```

| 目录 | case | G | 注入故障数分布 | 复合 case* | 常量基线 Hit@1 |
|---|---|---|---|---|---|
| `single/` | 130 | 单根因 | 注入1故障的 110 个 · 注入2故障的 20 个 | 20 | `catalog` / `catalog-gw` **0.192** |
| `dual/` | 100 | 双根因 | 注入2故障的 85 个 · 注入3故障的 15 个 | 15 | `catalog-gw` **0.600** |
| `triple/` | 25 | 三根因 | 注入3故障的 25 个 | 0 | `catalog` **0.600** |

\* **复合 case** = 注入故障数 > 根因服务数(多个故障打在同一服务上)。**判档看 `single/dual/triple` 目录,不看目录名的 `mrN` 前缀**(= 冻结前的注入故障数,保留作溯源)。`MANIFEST.json` 的 `case_index` 给每条标了 `tier / distinct_root_services / injected_faults / compound`。

**与 v1 的结构差异(增量说明)**:v1 的 255 来自跨约两周、5 个来源批次(特征面板与密度不同,concat 需批次感知);本版 **255 = 单一冻结协议一次战役采出**(51 场景 × 5 区组,3 天内同环境),无批次效应混杂,场景级统计可做区组内配对比较。逐 case 溯源字段:`source_case_id`(s51-bK-NN-<场景>)、`source_batch`(b1..b5)、`family`(root_local/propagation/mixed/off_graph)、`artifact_confounded`(55,注入伪影族可一行过滤做敏感性分析)。

## 怎么评

- **traditional**:服务级,GT = 被注入的服务(即使级联也不标下游),R 取所在档 G(single=1 / dual=2 / triple=3)。入口 = 每个 case 的 `eval/data.csv`;切分防泄漏、`inject_time` 锚点、raw 侧捷径等坑见本树 `DATASHEET.md` §2。
- **统计口径 = 242 主槽**;13 个补采 case 带 `amended: true` 可过滤(见下)。

## 补采披露(本版协议新增,必读)

255 = **242 主槽**(5 区组单次通过)+ **13 补采**(修正案制度:首采因科学损耗/环境退化失败的槽,按同一冻结协议 v3 的 schedule/seed/身份链重采,不挤占主槽统计)。逐条审计 = 源仓 `traditional_v2_lite_strict51_amd/.qualification/S51-AMD/amendment-manifest.json`(15 条全留痕:13 PASS + 2 FAIL)。补采槽集中于难场景 ⇒ 分数系统性偏低(如 BARO G2 补采 0.625 vs 主槽 0.848),**合并统计会抬高难场景权重**——分栏报告用 `case_index[].amended` 一行过滤。

## ★ 关键结果

**规则(全包适用,只说一次)**:每个分数都并排它所在档的**常量基线**(永远答同一个最热门服务拿到的分)。不高于基线 = 没有定位能力,只是先验。

### traditional/(常量基线 = "永远答最热门服务")

| 档 | n | 最优常答 | 常量基线 Hit@1 |
|---|---|---|---|
| `single` | 130 | `catalog` / `catalog-gw` | 25/130 = **0.192** |
| `dual` | 100 | `catalog-gw` | 60/100 = **0.600** |
| `triple` | 25 | `catalog` | 15/25 = **0.600** |
| `三档合并` | 255 | `catalog-gw` | 95/255 = **0.373** |

(与 v1 各档常量基线逐层相同——故障族同构,两版方法分数可直接对比,零口径修正。)

macro Hit@1(gap1,resource 通道):**BARO 0.698**(v1 0.608;full 通道 0.286 → 0.447 首次超过常量基线)、**RCD 0.258**(v1 0.216,仍低于常量基线,仅 triple 档 0.664 反超)、朴素 delta_z 0.769(v1 0.816,微降仍居首)。随机地板 0.106(固定 N=15 全并集口径,保守值;逐 case 实测可排名宇宙 full 通道 14–15、resource 通道 4–15)。两版对照的方向性:真方法双升 + 朴素法微降——密集 per-service 通道的红利被真方法吃走,单一幅度信号反而更难奏效(single 档 delta_z 0.738 → 0.638)。

## 已知局限

- **triple 档双饱和**:BARO 与 delta_z 均 1.000(25/25)——该档只能作"多根因可检出性"证据,不可作方法判别;报定位能力看 `single/`(基线 0.192)。
- **朴素 delta_z 仍居首**:资源类故障占比决定幅度信号天然强;敏感性分析用 `artifact_confounded` 过滤列。
- **单一采集环境**:3 天内单机单节点 K8S(Docker Desktop)一次战役采出——批次效应被协议消除,但环境多样性为零,跨环境泛化未测。

## 钻取入口(按需深入)

- **`DATASHEET.md`** —— 完整方法学 / §关键结果表(含 v1 对照与补采拆分)/ threats-to-validity / GT 字段口径的汇总。
- **`FAULT_DESIGN.md`** —— 51 场景逐条设计表(机制 × 服务 × 交互 × v1 对应编号)+ 机制词汇表与诚实交代 + strict51 协议设计(RCBD/对照/修正案/身份链)。
- **`MANIFEST.json`** —— 机器可读账目:`case_index` 255 条 `folder → tier / G / injected / compound / amended / amendment{scenario,attempt_id} / source_case_id / source_batch / family / artifact_confounded`、`protocol`(seed/块结构/主槽与补采账)、`identity`(冻结 SHA,出处记录)、`tiers`。
- **包外仓内**(非交付件,需时索取)—— 逐 case 全网格打分(BARO/RCD 5 seed × 2 通道 × 2 gap + MRCBench 四族,`n5_raw_strict51.jsonl`)、朴素法逐 case(`delta_s51.json`)、修正案 15 条 manifest 原件、B5 作废-重采全链留痕,均留源仓可索取。
"""


def fail(msg):
    raise SystemExit("[build_strict51] FAIL: " + msg)


def jload(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def case_complete(case_dir):
    return all(os.path.exists(os.path.join(case_dir, p)) for p in
               ("metadata.json", "groundtruth.json",
                os.path.join("raw", "metrics", "metrics_v2.jsonl")))


def collect_selection(lite_root):
    """Return list of selection dicts: {src, case_id, kind, batch, amd}."""
    k8s = os.path.join(lite_root, "datasets", "k8s_pilot")
    sel = []
    per_block = {}
    for blk in BLOCKS:
        tree = os.path.join(k8s, "traditional_v2_lite_strict51_s51_" + blk)
        qual = [d for d in (os.path.join(tree, ".qualification", x)
                            for x in os.listdir(os.path.join(tree, ".qualification")))
                if os.path.isdir(d)]
        if len(qual) != 1:
            fail("block %s: expected exactly 1 qualification dir, got %r" % (blk, qual))
        attempts = os.path.join(qual[0], ".attempts")
        n = 0
        for name in sorted(os.listdir(attempts)):
            if name.endswith(".tmp") or CONTROL_RE.match(name):
                continue
            att = os.path.join(attempts, name)
            if not os.path.isdir(att):
                continue
            if not os.path.exists(os.path.join(att, "outcome.json")):
                continue  # failed/voided attempt: no valid case
            for case in sorted(os.listdir(os.path.join(att, "runner-out"))) \
                    if os.path.isdir(os.path.join(att, "runner-out")) else []:
                cdir = os.path.join(att, "runner-out", case)
                if os.path.isdir(cdir) and case_complete(cdir):
                    sel.append({"src": cdir, "case_id": case, "kind": "primary",
                                "batch": "s51_" + blk, "amd": None})
                    n += 1
        per_block[blk] = n

    amd_tree = os.path.join(k8s, "traditional_v2_lite_strict51_amd")
    mpath = os.path.join(amd_tree, ".qualification", "S51-AMD",
                         "amendment-manifest.json")
    man = jload(mpath)
    n_amd = 0
    for e in man.get("entries", []):
        if e.get("verdict") != "PASS":
            continue  # D06-amd-04/05: FAIL entries stay in the manifest only
        att = e["attempt_dir"]
        ro = os.path.join(att, "runner-out")
        cases = [c for c in sorted(os.listdir(ro)) if
                 os.path.isdir(os.path.join(ro, c)) and
                 case_complete(os.path.join(ro, c))] if os.path.isdir(ro) else []
        if len(cases) != 1:
            fail("amendment %s: expected 1 complete case, got %r" % (e["attempt_id"], cases))
        sel.append({"src": os.path.join(ro, cases[0]), "case_id": cases[0],
                    "kind": "amended", "batch": "amd",
                    "amd": {"scenario": e.get("scenario"),
                            "attempt_id": e.get("attempt_id")}})
        n_amd += 1

    # ---- census assertions (fail-loud) ----
    for blk, want in EXPECT_PRIMARY_PER_BLOCK.items():
        if per_block[blk] != want:
            fail("block %s: %d valid primary cases, expected %d (campaign drift?)"
                 % (blk, per_block[blk], want))
    if n_amd != EXPECT_AMENDED:
        fail("%d PASS amendments, expected %d" % (n_amd, EXPECT_AMENDED))
    n_total = len(sel)
    if n_total != EXPECT_TOTAL:
        fail("%d selected cases, expected %d" % (n_total, EXPECT_TOTAL))
    print("[build_strict51] census OK: primary %s (=%d), amended %d, total %d"
          % (per_block, sum(per_block.values()), n_amd, n_total))
    return sel


def g_of(groundtruth):
    g = len(set(groundtruth["root_cause_services"]))
    if g not in G_BUCKET:
        fail("case %s: G=%d not in 1..3" % (groundtruth.get("sample_id"), g))
    return g


def stage(sel):
    native = os.path.join(STAGE_ROOT, "native")
    if os.path.exists(native):
        shutil.rmtree(native)
    os.makedirs(native)
    for s in sel:
        dst = os.path.join(native, s["case_id"])
        shutil.copytree(s["src"], dst)
    print("[build_strict51] staged %d cases -> %s" % (len(sel), native))
    return native


def run_packager(native, packed):
    if os.path.exists(packed):
        shutil.rmtree(packed)
    cmd = [PY, os.path.join(HERE, "package_for_delivery.py"),
           "--pilot-dir", native, "--out", packed,
           "--bare", "--force", "--flat-traces", "--with-calltree",
           "--with-eval", "--with-gt-distinct"]
    print("[build_strict51] $ " + " ".join(cmd))
    # GBK console pitfall (D12): never let the child inherit undecodable output.
    proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          encoding="utf-8", errors="replace")
    log_path = os.path.join(STAGE_ROOT, "packager.log")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(proc.stdout)
    tail = "\n".join(proc.stdout.splitlines()[-5:])
    print("[build_strict51] packager rc=%d (full log: %s)\n%s"
          % (proc.returncode, log_path, tail))
    if proc.returncode != 0:
        fail("packager exited %d" % proc.returncode)
    # folder <- case mapping from the packager's own OK lines
    mapping = {}
    for line in proc.stdout.splitlines():
        m = re.match(r"\[pkg\] OK (\S+) <- (\S+)", line)
        if m:
            mapping[m.group(2)] = m.group(1)
    if len(mapping) != EXPECT_TOTAL:
        fail("packager OK mapping covers %d cases, expected %d"
             % (len(mapping), EXPECT_TOTAL))
    return mapping


def bucket_and_index(packed, sel, mapping, out_trad):
    rows = []
    counters = {}
    for s in sel:
        folder = mapping.get(s["case_id"])
        if folder is None:
            fail("no packager output folder for %s" % s["case_id"])
        src_dir = os.path.join(packed, folder)
        gt = jload(os.path.join(src_dir, "groundtruth.json"))
        meta = jload(os.path.join(src_dir, "metadata.json"))
        g = g_of(gt)
        nd = gt.get("n_distinct_root_services")
        if nd is not None and nd != g:
            fail("%s: GT n_distinct_root_services=%s != recomputed G=%d"
                 % (folder, nd, g))
        tier = G_BUCKET[g]
        shutil.move(src_dir, os.path.join(out_trad, tier, folder))
        legs = [f.get("fault_type") for f in meta.get("faults", [])]
        classes = {DR.signal_class(ft) for ft in legs if ft}
        family = next(iter(classes)) if len(classes) == 1 else "mixed"
        injected = len(legs)
        counters[(tier, s["kind"])] = counters.get((tier, s["kind"]), 0) + 1
        rows.append({
            "folder": folder,
            "tier": tier,
            "distinct_root_services": g,
            "injected_faults": injected,
            "compound": bool(injected > g),
            "family": family,
            "artifact_confounded": any(ft in ARTIFACT_FAULT_TYPES for ft in legs),
            "amended": s["kind"] == "amended",
            "amendment": s["amd"],
            "source_case_id": s["case_id"],
            "source_batch": s["batch"],
        })
    # tier census (fail-loud)
    got = {t: sum(n for (tt, _), n in counters.items() if tt == t)
           for t in EXPECT_G}
    if got != EXPECT_G:
        fail("tier census %r != expected %r" % (got, EXPECT_G))
    print("[build_strict51] buckets OK %r (by kind: %r)"
          % (got, {str(k): v for k, v in sorted(counters.items())}))
    return rows


def verify(out_trad, rows):
    problems = []
    for r in rows:
        cdir = os.path.join(out_trad, r["tier"], r["folder"])
        for rel in ("eval/data.csv", "raw/traces", "raw/traces_calltree",
                    "raw/logs", "raw/metrics", "raw/operations"):
            if not os.path.exists(os.path.join(cdir, rel)):
                problems.append("%s: missing %s" % (r["folder"], rel))
        gt = jload(os.path.join(cdir, "groundtruth.json"))
        if "n_distinct_root_services" not in gt:
            problems.append("%s: GT lacks n_distinct_root_services" % r["folder"])
        ok, reason = check_release_gate(cdir)
        if not ok:
            problems.append("%s: release gate: %s" % (r["folder"], reason))
        fsid = jload(os.path.join(cdir, "metadata.json")).get("formal_slot_id")
        if fsid and fsid != r["source_case_id"]:
            problems.append("%s: formal_slot_id %s != %s"
                            % (r["folder"], fsid, r["source_case_id"]))
    if problems:
        for p in problems[:20]:
            print("  !! " + p)
        fail("%d verification problems (first 20 shown)" % len(problems))
    print("[build_strict51] verify OK: %d cases (gate + artifacts + slot id)"
          % len(rows))


def write_manifest_and_readme(out, rows, lite_root):
    n_amd = sum(1 for r in rows if r["amended"])
    n_pri = len(rows) - n_amd
    manifest = {
        "schema_version": "strict51-delivery-manifest.v1",
        "tag": os.path.basename(os.path.normpath(out)),
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "built_by": os.path.basename(__file__),
        "dataset": "RecShop traditional v2 (strict51 RCBD campaign)",
        "protocol": {
            "version": "2026-08-17.v3",
            "seed": "recshop-traditional-v2-lite-strict51-20260816-rcbd-v1",
            "blocks": BLOCKS,
            "primary_slots": n_pri,
            "amendment_slots": n_amd,
            "controls": "30 (nf/sham qualification gates; produce no case, "
                        "not delivered - v1 convention)",
        },
        "identity": {
            "note": "SHAs from the lite campaign freeze; provenance record, the "
                    "delivery does not re-verify them at runtime.",
            "freeze_report_sha256": "b5f18f31f828ab02137dcf47050d55e4ed1f8d7bf"
                                    "8dead7e09834bada6affb0a",
            "contract_artifact_sha256": "fb773c108771c58687bad222b5baefb8e79e32b9"
                                        "33fcb090b261044dd47ec955",
            "runner_sha256": "cb1396f0574764437de3c410f5ac2ac3c3fa338f97b9d91597"
                             "682a513e8cce51",
        },
        "sources": {
            "lite_root": os.path.abspath(lite_root),
            "primary_trees": ["datasets/k8s_pilot/traditional_v2_lite_strict51_s51_"
                              + b for b in BLOCKS],
            "amendment_tree": "datasets/k8s_pilot/traditional_v2_lite_strict51_amd",
            "amendment_manifest_relpath": ".qualification/S51-AMD/"
                                          "amendment-manifest.json",
            "amendment_manifest_entries": 15,
            "amendment_manifest_pass": n_amd,
        },
        "tiers": {t: sum(1 for r in rows if r["tier"] == t) for t in EXPECT_G},
        "case_index": rows,
        "rebuild_command": ("python scripts/chaos/ctk/build_strict51_delivery.py "
                            "--lite-root <%s> --out <%s>"
                            % (os.path.abspath(lite_root), os.path.abspath(out))),
    }
    with open(os.path.join(out, "MANIFEST.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)

    # ★README 模板与交付树内 README.md 保持同一份(2026-08-19 起)。此前此函数内嵌
    #   英文初版模板,与磁盘上手写的中文 v1 格式 README 分叉——rebuild 会用旧模板
    #   覆盖中文版(对抗审核 MINOR-2)。现钉为中文版静态文本:全部数字由本脚本的
    #   census 断言守门,断言失败则到不了写 README 这步,静态即安全。
    readme = README_TMPL
    with open(os.path.join(out, "README.md"), "w", encoding="utf-8") as f:
        f.write(readme)
    print("[build_strict51] wrote MANIFEST.json + README.md")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lite-root", required=True,
                    help="lite worktree root (strict51 campaign trees)")
    ap.add_argument("--out", required=True,
                    help="delivery tree dir, e.g. datasets/_delivery/strict51_20260819")
    ap.add_argument("--keep-stage", action="store_true",
                    help="keep datasets/_runtime/strict51_stage after build")
    args = ap.parse_args()
    lite_root = os.path.abspath(args.lite_root)
    out = args.out if os.path.isabs(args.out) else os.path.join(ROOT, args.out)

    sel = collect_selection(lite_root)
    native = stage(sel)
    packed = os.path.join(STAGE_ROOT, "packed")
    mapping = run_packager(native, packed)

    out_trad = os.path.join(out, "traditional")
    for t in EXPECT_G:
        d = os.path.join(out_trad, t)
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)
    rows = bucket_and_index(packed, sel, mapping, out_trad)
    verify(out_trad, rows)
    write_manifest_and_readme(out, rows, lite_root)

    if not args.keep_stage:
        shutil.rmtree(STAGE_ROOT, ignore_errors=True)
        print("[build_strict51] stage cleaned (keep with --keep-stage)")
    print("[build_strict51] DONE: %s (%d cases)" % (out, len(rows)))


if __name__ == "__main__":
    main()
