# -*- coding: utf-8 -*-
"""
build_whowhen_format_delivery.py
================================

Build a **Who&When-schema-compatible delivery view** of our agent-fault dataset
by AUGMENTING the already-scored Who&When cases with the auxiliary fields the
official Who&When schema carries. This is a pure OFFLINE transform — no API, no
services, no re-collection.

INPUT (read-only)
  - Scored cases : (upstream batch)whowhen/cases/case_001.json .. case_NNN.json
                   (the EXACT inputs the Who&When / A2P baselines scored on).
                   Each has 6 fields: question, ground_truth, history,
                   mistake_agent, mistake_step, _provenance.
  - Main CSV     : (upstream batch)dataset_agentfault.csv
                   (injection ground truth: kind, format_subtype,
                    context_drift_dropped_agent, ...).
  - Prompts      : services/recommendation_agent/agents/prompts.py
                   (4 <Agent>_system_prompt = \"\"\"...\"\"\" definitions).

OUTPUT (new)
  - (upstream whowhen view)
      Injection-Generated/1.json .. N.json    (integer filenames, official layout)
      cases_index.json                         (int file -> case_id + kind + mistake_agent)
      README.md

FIELD POLICY (honest augmentation; do NOT fabricate)
  Core 5 (method-consumed) copied VERBATIM from the scored case, byte-identical:
      question, ground_truth, history, mistake_agent, mistake_step
  Added auxiliary fields:
      question_ID   = opaque sha256(case_id)[:16] (real case_id stays in _provenance)
      mistake_reason= derived from injection config, clearly prefixed as auto-derived
      system_prompt = {canonical_agent_name: verbatim prompt} from prompts.py
  Omitted (NOT fabricated):
      level         = we have no difficulty grading.
      is_correct    = Who&When's is_correct=false means "task finally FAILED" (their
                      set is naturally-occurring failures). Our cases are CONTROLLED
                      injections that frequently do NOT change the outcome
                      (context_drift 27/36 recovered, EVAL_NOTES §4b), on an
                      OPEN-ENDED task with no reference answer. Task-level
                      correctness is undefined -> omitted rather than asserted.
  Kept extra:
      _provenance   = traceability (case_id, kind, format_subtype, trace_id)

  Per-case output schema = 9 fields: question, question_ID, ground_truth, history,
  mistake_agent, mistake_step, mistake_reason, system_prompt, _provenance.

  NOTE on mistake_step type: the official Algorithm-Generated set stores mistake_step
  as a STRING ("0"). Our scored cases store it as an INT, and that int is one of the
  five byte-identical core fields the baselines actually ran on. We therefore KEEP it
  as int (do NOT coerce to str) — changing it would break the "same data the baselines
  scored" guarantee. The official method loaders never type-cast mistake_step, so int
  is accepted as-is. See README for details.

Run from repo root (default = agentfault_v2 REF; override --dataset-dir for any
other batch that ships whowhen/cases/ + dataset_agentfault.csv):
    PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/whowhen/build_whowhen_format_delivery.py
    # B 档 (agentfault_k8s) → its own delivery view:
    PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/whowhen/build_whowhen_format_delivery.py \
        --dataset-dir datasets/agentfault_k8s --out-root datasets/agentfault_k8s_whowhen
"""

import argparse
import ast
import collections
import csv
import glob
import hashlib
import json
import os
import sys

# --------------------------------------------------------------------------- #
# Paths (all relative to repo root; script is meant to run from repo root)
# --------------------------------------------------------------------------- #
# Defaults below = the v2 batch, so a bare invocation still reproduces the REF
# view byte-for-byte. `main()` overrides them from CLI args (--dataset-dir /
# --out-root), so the SAME builder serves any agent-fault batch that ships
# whowhen/cases/case_*.json + dataset_agentfault.csv — e.g. agentfault_v2 (REF,
# local harness) and agentfault_k8s (B 档, full 25-svc cluster).
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
DEFAULT_DATASET_DIR = os.path.join(
    REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault_v2")
DEFAULT_OUT_ROOT = os.path.join(
    REPO_ROOT, "datasets", "_archive", "agentfault", "agentfault_v2_whowhen")
PROMPTS_PY = os.path.join(
    REPO_ROOT, "services", "recommendation_agent", "agents", "prompts.py"
)
# Set by main() from CLI args; kept as module globals so build()/validate() and
# write_readme() read them uniformly. (None until main() resolves them.)
SCORED_CASES_GLOB = None
MAIN_CSV = None
OUT_ROOT = None
OUT_CASES_DIR = None
DATASET_REL = None   # repo-relative dataset path, for the generated README text
DATASET_NAME = None  # basename, for the generated README title

# The 5 fields the Who&When / A2P methods consume; must stay byte-identical.
CORE_FIELDS = ("question", "ground_truth", "history", "mistake_agent", "mistake_step")

# Canonical agent execution order (matches the 4-agent recommendation pipeline).
CANONICAL_AGENTS = (
    "Sequence_Recommender",
    "User_Behavior_Analyzer",
    "Product_Analyzer",
    "Recommendation_Synthesizer",
)

MISTAKE_REASON_PREFIX = "[auto-derived from injection ground truth — not human-annotated]"
WRONG_PICK_SENTINEL = "B00EKWZK5E"


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def load_system_prompts(prompts_path):
    """Extract the 4 <Agent>_system_prompt = \"\"\"...\"\"\" string literals VERBATIM.

    Uses ast (no import side effects). Variable name minus the '_system_prompt'
    suffix IS the canonical agent name, so the mapping is unambiguous.
    Returns dict {canonical_agent_name: verbatim_prompt_string}.
    """
    with open(prompts_path, "r", encoding="utf-8") as f:
        src = f.read()
    tree = ast.parse(src)
    prompts = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id.endswith("_system_prompt"):
                # Python 3.8+: string literal is ast.Constant
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    agent = target.id[: -len("_system_prompt")]
                    prompts[agent] = node.value.value
    return prompts


def load_csv_by_keys(csv_path):
    """Return (by_run_id, by_trace_id) dicts of the main dataset CSV rows."""
    by_run_id, by_trace_id = {}, {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            by_run_id[row["run_id"]] = row
            by_trace_id[row["trace_id"]] = row
    return by_run_id, by_trace_id


def load_scored_cases(glob_pattern):
    """Return list of (filename, dict) for scored cases, sorted by filename."""
    out = []
    for p in sorted(glob.glob(glob_pattern)):
        with open(p, "r", encoding="utf-8") as f:
            out.append((os.path.basename(p), json.load(f)))
    return out


# --------------------------------------------------------------------------- #
# Derivations
# --------------------------------------------------------------------------- #
def opaque_question_id(case_id):
    """Stable, opaque, non-revealing id derived from the case_id."""
    return hashlib.sha256(case_id.encode("utf-8")).hexdigest()[:16]


def derive_mistake_reason(kind, mistake_agent, csv_row):
    """Derive a short, factual mistake_reason from the injection config.

    Clearly prefixed as auto-derived (not human-annotated). This is metadata; the
    Who&When methods do NOT read it (they read only question/ground_truth/history),
    so it cannot leak into scoring — but we still label it honestly.
    """
    if kind == "hallucinate":
        body = "semantic hallucination injected at {}".format(mistake_agent)
    elif kind == "context_drift":
        dropped = (csv_row or {}).get("context_drift_dropped_agent") or "<unknown>"
        body = "upstream message from {} deleted from {}'s input".format(
            dropped, mistake_agent
        )
    elif kind == "wrong_item_pick":
        body = "recommended ASIN forced to sentinel {}".format(WRONG_PICK_SENTINEL)
    elif kind == "format_violation":
        subtype = (csv_row or {}).get("format_subtype") or "<unknown>"
        body = "output contract violated ({})".format(subtype)
    else:
        body = "faulted run (kind={})".format(kind)
    return "{} {}".format(MISTAKE_REASON_PREFIX, body)


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def build():
    print("== BUILD: Who&When-format delivery view ==")
    system_prompts = load_system_prompts(PROMPTS_PY)
    by_run_id, by_trace_id = load_csv_by_keys(MAIN_CSV)
    scored = load_scored_cases(SCORED_CASES_GLOB)
    print("scored cases loaded : {}".format(len(scored)))
    print("system prompts found: {} -> {}".format(len(system_prompts), sorted(system_prompts)))

    os.makedirs(OUT_CASES_DIR, exist_ok=True)

    index = []  # cases_index.json rows
    for i, (fname, case) in enumerate(scored, start=1):
        prov = case["_provenance"]
        case_id = prov["case_id"]
        kind = prov["kind"]
        mistake_agent = case["mistake_agent"]

        # match to CSV row (case_id == run_id is primary; trace_id is fallback)
        csv_row = by_run_id.get(case_id) or by_trace_id.get(prov.get("trace_id", ""))

        # Assemble augmented case in official field order.
        # BOTH `level` and `is_correct` are omitted (not fabricated):
        #   - level      : we have no difficulty grading.
        #   - is_correct : Who&When's is_correct=false means "the task finally
        #                  FAILED" (their set is naturally-occurring failures). Our
        #                  cases are CONTROLLED injections that frequently do NOT
        #                  change the outcome (context_drift 27/36 recovered per
        #                  EVAL_NOTES §4b), on an OPEN-ENDED task with no reference
        #                  answer (ground_truth = "N/A"). Task-level correctness is
        #                  therefore undefined; asserting is_correct=false for all
        #                  would contradict our own GT ("注入在 X ≠ 失败由 X 造成").
        aug = {
            "question": case["question"],            # core, verbatim
            "question_ID": opaque_question_id(case_id),
            "ground_truth": case["ground_truth"],    # core, verbatim
            "history": case["history"],              # core, verbatim
            "mistake_agent": case["mistake_agent"],  # core, verbatim
            "mistake_step": case["mistake_step"],    # core, verbatim (kept as int)
            "mistake_reason": derive_mistake_reason(kind, mistake_agent, csv_row),
            "system_prompt": {a: system_prompts[a] for a in CANONICAL_AGENTS},
            "_provenance": case["_provenance"],       # kept for traceability
        }

        out_path = os.path.join(OUT_CASES_DIR, "{}.json".format(i))
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(aug, f, ensure_ascii=False, indent=2)

        index.append(
            {
                "file": "Injection-Generated/{}.json".format(i),
                "case_id": case_id,
                "kind": kind,
                "mistake_agent": mistake_agent,
                "question_ID": aug["question_ID"],
            }
        )

    with open(os.path.join(OUT_ROOT, "cases_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print("wrote {} case files to {}".format(len(index), OUT_CASES_DIR))
    print("wrote cases_index.json")
    # README.md is HAND-MAINTAINED (the Chinese delivery README that actually ships
    # to 上游: it carries the per-family baseline table + 评测须知 this builder has no
    # source for). Never clobber it — only generate when the file is absent, and
    # otherwise print the counts that must be synced into it by hand.
    readme_path = os.path.join(OUT_ROOT, "README.md")
    if os.path.exists(readme_path):
        kinds = collections.Counter(e["kind"] for e in index)
        print("README.md exists (hand-maintained) -> NOT overwritten. "
              "Sync these numbers manually if they changed: n_cases={}, per-family={}"
              .format(len(index), dict(sorted(kinds.items()))))
    else:
        write_readme(index, system_prompts)
        print("wrote README.md (generated; it is hand-maintained from now on)")
    return scored, index, system_prompts


# --------------------------------------------------------------------------- #
# Validation (offline)
# --------------------------------------------------------------------------- #
def validate(scored, index, system_prompts):
    print("\n== VALIDATION ==")
    ok = True

    # 1. count == len(scored) (DERIVED, never hardcoded — the case set grows when
    #    the dataset is backfilled), valid JSON, 8 official-derived target fields present
    #    (official 10 minus BOTH `level` and `is_correct`), plus `_provenance`.
    target_fields = {
        "question", "question_ID", "ground_truth", "history",
        "mistake_agent", "mistake_step", "mistake_reason", "system_prompt",
    }
    omitted_fields = ("level", "is_correct")
    out_files = sorted(
        glob.glob(os.path.join(OUT_CASES_DIR, "*.json")),
        key=lambda p: int("".join(filter(str.isdigit, os.path.basename(p))) or 0),
    )
    print("[1] output file count: {}".format(len(out_files)))
    n_expected = len(scored)
    if len(out_files) != n_expected:
        ok = False
        print("    !! expected {} (= scored case count)".format(n_expected))
    loaded = {}
    for p in out_files:
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)
        loaded[os.path.basename(p)] = d
        missing = target_fields - set(d.keys())
        if missing:
            ok = False
            print("    !! {} missing fields {}".format(os.path.basename(p), missing))
        if "_provenance" not in d:
            ok = False
            print("    !! {} missing _provenance".format(os.path.basename(p)))
        for fld in omitted_fields:
            if fld in d:
                ok = False
                print("    !! {} unexpectedly contains omitted field '{}'".format(
                    os.path.basename(p), fld))
    print("[1] all files valid JSON with 8 target fields + _provenance present, "
          "level & is_correct omitted: {}".format("PASS" if ok else "FAIL"))

    # 2. core 5 fields byte-identical to the scored case (json canonical dump equality).
    core_ok = True
    for i, (fname, case) in enumerate(scored, start=1):
        out = loaded["{}.json".format(i)]
        for fld in CORE_FIELDS:
            a = json.dumps(case[fld], sort_keys=True, ensure_ascii=False)
            b = json.dumps(out[fld], sort_keys=True, ensure_ascii=False)
            if a != b:
                core_ok = False
                ok = False
                print("    !! MISMATCH {} field '{}' (case_id={})".format(
                    fname, fld, case["_provenance"]["case_id"]))
    print("[2] core 5 fields byte-identical to scored cases: {}".format(
        "PASS" if core_ok else "FAIL"))

    # 3. question_ID uniqueness across all cases.
    qids = [loaded["{}.json".format(i)]["question_ID"] for i in range(1, len(out_files) + 1)]
    uniq = len(set(qids)) == len(qids)
    if not uniq:
        ok = False
    print("[3] question_ID unique across {} cases: {}".format(
        len(qids), "PASS" if uniq else "FAIL"))

    # 4. system_prompt: 4 agents mapped; print keys + first 60 chars each.
    sp_ok = set(system_prompts.keys()) == set(CANONICAL_AGENTS) and len(system_prompts) == 4
    if not sp_ok:
        ok = False
    print("[4] system_prompt: 4 agents mapped: {}".format("PASS" if sp_ok else "FAIL"))
    sample = loaded["1.json"]["system_prompt"]
    for a in CANONICAL_AGENTS:
        first60 = sample[a][:60].replace("\n", "\\n")
        print("      {:<26} -> {!r}".format(a, first60))

    # 5. official method loader can enumerate the folder; history well-typed.
    #    Reuse the vendored numeric-sort loader semantics + field-shape checks.
    def _sorted_json_files(directory):
        files = [f for f in os.listdir(directory) if f.endswith(".json")]
        return sorted(files, key=lambda x: int("".join(filter(str.isdigit, x)) or 0))

    enum = _sorted_json_files(OUT_CASES_DIR)
    loader_ok = len(enum) == n_expected
    for fn in enum:
        with open(os.path.join(OUT_CASES_DIR, fn), "r", encoding="utf-8") as f:
            d = json.load(f)
        hist = d.get("history")
        if not isinstance(hist, list) or not hist:
            loader_ok = False
            print("    !! {} history not a non-empty list".format(fn))
            continue
        for entry in hist:
            # non-handcrafted loader uses index_agent='name'
            if not (isinstance(entry, dict) and "name" in entry and "content" in entry):
                loader_ok = False
                print("    !! {} history entry missing name/content".format(fn))
                break
        if "mistake_agent" not in d or "mistake_step" not in d:
            loader_ok = False
            print("    !! {} missing mistake_agent/mistake_step".format(fn))
    if not loader_ok:
        ok = False
    ms_types = set(type(loaded["{}.json".format(i)]["mistake_step"]).__name__
                   for i in range(1, len(out_files) + 1))
    print("[5] vendored loader can enumerate + history is [{{name,content}}] + "
          "mistake_agent/step present: {}".format("PASS" if loader_ok else "FAIL"))
    print("    mistake_step type in our view: {} (official uses str; kept as-is "
          "to preserve byte-identical core field)".format(sorted(ms_types)))

    print("\n== VALIDATION {} ==".format("ALL PASS" if ok else "HAD FAILURES"))
    return ok


# --------------------------------------------------------------------------- #
# README
# --------------------------------------------------------------------------- #
def write_readme(index, system_prompts):
    from collections import Counter
    kinds = Counter(r["kind"] for r in index)
    kinds_line = ", ".join("{}={}".format(k, kinds[k]) for k in sorted(kinds))

    readme = """# {dataset_name} — Who&When-format delivery view

**What this is.** A Who&When-schema-compatible *view* of
`{dataset_rel}/` (the trajectory-attribution slice of our agent-fault
dataset). Each file mirrors the official
[Who&When](https://github.com/ag2ai/Agents_Failure_Attribution) case schema so
the official Who&When / A2P failure-attribution methods run on this folder
**as-is** (point their loader at `Injection-Generated/`, `is_handcrafted=False`).

- **{n} cases**, one per faulted multi-agent recommendation run.
- Kind breakdown: {kinds_line}.
- Filenames are integers (`1.json` .. `{n}.json`) like the official set. The
  subfolder is named **`Injection-Generated`** (honest: our cases are produced by
  fault *injection* into a live 4-agent pipeline — not algorithm-generated and
  not hand-crafted).
- `cases_index.json` maps each integer file back to its original `case_id`,
  `kind`, and `mistake_agent` for traceability.

## Provenance guarantee (same data the baselines scored)

The five method-consumed fields are copied **byte-identical** from the already
scored cases at `{dataset_rel}/whowhen/cases/case_*.json` — the exact
inputs the Who&When / A2P baselines ran on:

> `question`, `ground_truth`, `history`, `mistake_agent`, `mistake_step`

The build script re-verifies byte-identity (canonical JSON dump equality) for all
{n} cases on every run. This view only **adds** auxiliary metadata; it never
regenerates or edits the scored data. The originals are treated as read-only.

## Field mapping vs official Who&When (Algorithm-Generated schema, 10 fields)

| Official field  | In this view | How |
|-----------------|--------------|-----|
| `question`      | identical    | copied verbatim from scored case (byte-identical core field) |
| `ground_truth`  | identical    | copied verbatim (byte-identical core field) |
| `history`       | identical    | copied verbatim — list of `{{name, content}}` (byte-identical core field) |
| `mistake_agent` | identical    | copied verbatim (byte-identical core field) |
| `mistake_step`  | identical    | copied verbatim — **kept as `int`** (see note below) (byte-identical core field) |
| `question_ID`   | **added**    | opaque, deterministic `sha256(case_id)[:16]`. Non-revealing; the real `case_id` lives only in `_provenance` (not judge-visible) |
| `mistake_reason`| **added (auto-derived)** | derived from the injection ground truth and prefixed `"{prefix}"`. Content per kind: hallucinate → "semantic hallucination injected at &lt;agent&gt;"; context_drift → "upstream message from &lt;dropped_agent&gt; deleted from &lt;agent&gt;'s input"; wrong_item_pick → "recommended ASIN forced to sentinel {sentinel}"; format_violation → "output contract violated (&lt;subtype&gt;)" |
| `system_prompt` | **added**    | dict `{{agent_name: verbatim prompt}}` for all 4 pipeline agents, extracted verbatim from `services/recommendation_agent/agents/prompts.py` |
| `level`         | **omitted**  | we have no difficulty grading; omitted rather than fabricated |
| `is_correct`    | **omitted**  | task-level correctness is undefined for our cases — see the semantic-mismatch note below |
| —               | `_provenance`| **extra** (ours): `{{case_id, kind, format_subtype, trace_id}}` for traceability |

### ⚠️ Why `is_correct` is omitted — a semantic mismatch readers must understand

Who&When cases are **observed, naturally-occurring failures**: their
`is_correct` is meaningful because each recorded run genuinely failed the task.
**Our cases are a different kind of object.** They are **controlled fault
injections** into a live pipeline, and an injected fault **frequently does NOT
change the outcome** — our own `EVAL_NOTES §4b` measured `context_drift` at
**27 recovered / 9 silent_wrong** out of 36 (27 injections left the output
effectively unchanged). On top of that the recommendation task is **open-ended
with no reference answer** (that is why `ground_truth = "N/A"`), so there is no
oracle for task-level correctness at all.

Setting `is_correct=false` for all {n} would assert "all {n} tasks failed," which
**contradicts our own measurements** and our ground-truth iron rule *"注入在 X ≠
失败由 X 造成"* (injecting at X ≠ the failure was caused by X). We therefore
**omit `is_correct` entirely** rather than fabricate it. What we DO know per case
— which agent was injected, and how — is preserved honestly in `mistake_agent`,
`mistake_step`, `mistake_reason`, and `_provenance`. Readers should treat this
folder as an **injection-attribution** set, not an observed-failure set.

### `mistake_step` type note (important)

The official Algorithm-Generated set stores `mistake_step` as a **string**
(e.g. `"0"`, `"12"`). Our scored cases store it as an **int**, and that int is one
of the five byte-identical core fields the baselines actually scored on. We
therefore **keep `mistake_step` as `int`** and do NOT coerce it to `str` — coercing
would break the "same data the baselines ran on" guarantee. The vendored method
loaders (`Automated_FA/Lib/utils.py`, `evaluate.py`) never type-cast
`mistake_step`, so an int is consumed without issue.

### `mistake_reason` cannot leak into scoring

Verified against the vendored methods: `all_at_once`, `step_by_step`, and
`binary_search` read only `history`, `question`, and `ground_truth` from each
file; `evaluate.py` additionally reads `mistake_agent` / `mistake_step`. **None**
read `mistake_reason`, `system_prompt`, `question_ID`, `level`, or `is_correct`.
So the auto-derived `mistake_reason` is inert metadata — it is labeled honestly
but cannot contaminate the method predictions.

## ⚠️ What `mistake_agent` / `mistake_step` mean HERE (read before interpreting labels)

In the official Who&When set, `mistake_agent` / `mistake_step` mean **"the agent
(and step) that *made the mistake*"** — a causal attribution of a real error.

**In this injection-based dataset the same fields mean something different:** they
name **the agent at which the fault was INJECTED / where it manifests — NOT "the
agent that committed an error."** This is our ground-truth iron rule:

> **注入在 X ≠ 失败由 X 造成** — *injecting at X is NOT the same as the failure
> being caused by X.* (See `{dataset_rel}/EVAL_NOTES.md` §1.)

The clearest case is **`context_drift`**: the labeled `mistake_agent` **did NOT
err**. The injection **deleted an upstream message from that agent's input**, so
the agent simply never received some information — it behaved reasonably given the
(tampered) input it saw. Labeling it as the `mistake_agent` marks *where the fault
enters the trajectory*, not a cognitive failure by that agent.

Readers and downstream methods must therefore **not** read `mistake_agent` as a
causal "this agent failed." It is an **injection-site / manifestation label**. The
attribution question this dataset poses is "can a method localize the injection
site from the trajectory + telemetry?", which is deliberately harder and different
from "which agent visibly blundered?".

## 🔒 Field classification — INPUT vs GROUND TRUTH / METADATA (leak prevention)

Every field in each case JSON is exactly one of two classes. Anyone running
attribution **must feed a judge ONLY the INPUT fields**. (The vendored harness
already enforces this — it reads only the three INPUT fields — but this table
makes the contract explicit for any re-user.)

| Field | Class | Rule |
|-------|-------|------|
| `question`      | **INPUT (judge-visible)** | may be shown to the judge |
| `ground_truth`  | **INPUT (judge-visible)** | may be shown to the judge (here it is `"N/A"` — open-ended task) |
| `history`       | **INPUT (judge-visible)** | may be shown to the judge |
| `mistake_agent` | **GROUND TRUTH — never feed a judge** | this is the answer key |
| `mistake_step`  | **GROUND TRUTH — never feed a judge** | this is the answer key |
| `mistake_reason`| **METADATA — never feed a judge** | auto-derived answer-key rationale |
| `question_ID`   | **METADATA — never feed a judge** | bookkeeping id |
| `system_prompt` | **METADATA — never feed a judge** | context aid, not a method input |
| `_provenance`   | **METADATA — never feed a judge** | traceability; **see the sharp edge below** |

**Sharp edge — `_provenance.case_id` literally spells out the answer.** Case ids
are human-readable and encode the fault type + target agent, e.g.
`ctxdrift_prod_from_ub__r1` (= *context_drift, Product_Analyzer, dropped from
User_Behavior_Analyzer*) or `hallu_Sequence_Recommender__r1`. **Dumping a whole
case JSON into an LLM judge would hand it the ground truth.** Verified method
consumption: `all_at_once` / `step_by_step` / `binary_search` read only
`history`, `question`, `ground_truth`; `evaluate.py` reads `mistake_agent` /
`mistake_step` **for scoring only, never inside a prompt**. Pass only the three
INPUT fields to any judge.

## ⚠️ This view carries ONLY the conversation-attribution modality

This Who&When view contains the **conversation trajectory** signal only. The
infra / metric / trace-structure signals of the full dataset —
per-agent OTel **span durations**, child **http counts**, and the
`agentfault.resolved_input` **structural signal** that localizes `context_drift`
by showing which upstream message was dropped — live in the FULL dataset at
`{dataset_rel}/`, **NOT here**. Consequently this view **alone cannot
reproduce the context_drift structural-detector result**; use the full dataset for
metric / trace / multimodal methods.

## How to run the official methods on this folder

Point the vendored Who&When methods (and our harness) at
`Injection-Generated/` with `is_handcrafted=False` (our history uses the `name`
key, matching the non-handcrafted loader path):

- Vendored methods : `third_party/reference/whowhen/Automated_FA/` (read-only).
- Our harness      : `scripts/chaos/agentfault/eval/whowhen/`
  (`make_whowhen_cases.py`, `run_whowhen.py`, `score_whowhen.py`, `SPEC.md`).

## Reproducibility

This folder is regenerated (and self-validated) by:

    scripts/chaos/agentfault/eval/whowhen/build_whowhen_format_delivery.py

Run from repo root:

    PYTHONIOENCODING=utf-8 python scripts/chaos/agentfault/eval/whowhen/build_whowhen_format_delivery.py

The script copies the 5 core fields byte-identical, adds the auxiliary fields as
described above, writes `Injection-Generated/*.json` + `cases_index.json` + this
README, then runs the offline validation suite (count, byte-identity, question_ID
uniqueness, system_prompt extraction, vendored-loader enumeration).
""".format(
        n=len(index),
        kinds_line=kinds_line,
        prefix=MISTAKE_REASON_PREFIX,
        sentinel=WRONG_PICK_SENTINEL,
        dataset_rel=DATASET_REL,
        dataset_name=DATASET_NAME,
    )

    with open(os.path.join(OUT_ROOT, "README.md"), "w", encoding="utf-8") as f:
        f.write(readme)


# --------------------------------------------------------------------------- #
def main():
    global SCORED_CASES_GLOB, MAIN_CSV, OUT_ROOT, OUT_CASES_DIR, PROMPTS_PY
    global DATASET_REL, DATASET_NAME

    ap = argparse.ArgumentParser(
        description="Build a Who&When-format delivery view of an agent-fault batch.")
    ap.add_argument(
        "--dataset-dir", default=DEFAULT_DATASET_DIR,
        help="agent-fault dataset root (must contain whowhen/cases/case_*.json + "
             "dataset_agentfault.csv). Default = agentfault_v2 (REF).")
    ap.add_argument(
        "--out-root", default=DEFAULT_OUT_ROOT,
        help="output Who&When-format delivery dir. Default = agentfault_v2_whowhen.")
    ap.add_argument(
        "--prompts", default=PROMPTS_PY,
        help="prompts.py with the 4 *_system_prompt literals. Default = rec-agent prompts.py.")
    a = ap.parse_args()

    def _abs(x):
        return x if os.path.isabs(x) else os.path.join(REPO_ROOT, x)

    ds = _abs(a.dataset_dir)
    PROMPTS_PY = _abs(a.prompts)
    OUT_ROOT = _abs(a.out_root)
    SCORED_CASES_GLOB = os.path.join(ds, "whowhen", "cases", "case_*.json")
    MAIN_CSV = os.path.join(ds, "dataset_agentfault.csv")
    OUT_CASES_DIR = os.path.join(OUT_ROOT, "Injection-Generated")
    try:
        DATASET_REL = os.path.relpath(ds, REPO_ROOT).replace("\\", "/")
    except ValueError:                       # cross-drive relpath (rare)
        DATASET_REL = ds.replace("\\", "/")
    DATASET_NAME = os.path.basename(os.path.normpath(ds))

    print("dataset-dir : %s" % DATASET_REL)
    print("out-root    : %s" % OUT_ROOT)
    scored, index, system_prompts = build()
    ok = validate(scored, index, system_prompts)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
