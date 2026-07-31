# -*- coding: utf-8 -*-
"""score_whowhen.py — score Who&When / A2P outputs on agentfault (SPEC §1.3). Offline.

Parsing = byte-for-byte the regex protocol of whowhen evaluate.py
(read_predictions): block regex `Prediction for ([^:]+\\.json):(.*?)(?=Prediction
for|\\Z)` (DOTALL) + `Agent Name:\\s*([\\w_]+)` + `Step Number:\\s*(\\d+)`
(IGNORECASE). step_by_step's "no error found" cases produce no prediction block
-> recorded as missing.

Three side-by-side readings (SPEC §1.3, never replacing one another):
  1. Who&When native: agent accuracy + step accuracy, denominator = total case
     files (64), substring match (actual in predicted) — identical to their
     evaluate.py (method fidelity).
  2. MRCBench (m9_score.mrcbench, entity-agnostic): single-prediction method ->
     ranked = [pred] + remaining AGENT_NAMES (canonical order, dedup);
     missing/unparsable -> ranked = [] (mrcbench yields all-zero on empty).
     Only K=1 is discriminative (1 prediction, 4 candidates; @3/@5 are ceiling
     artifacts) — all listed, footnoted.
  3. Normalized Hit@1: judge may emit name variants ("Product Analyzer" — the
     native [\\w_]+ regex only captures "Product" -> counted wrong). Normalization
     = case-insensitive, space/underscore equivalent, full-name or unique-prefix
     match to the 4 canonical names; where the native regex misses a block
     entirely (markdown bold, e.g. 'Agent Name: **X**'), a tolerant extraction
     recovers the name for this reading ONLY (native protocol untouched).
     Divergence vs native is reported, not substituted.

A constant "always-Synthesizer" baseline row goes through the same scoring
path: the GT prior is skewed toward Recommendation_Synthesizer, so per-family
numbers must be read against it (footnote [g], computed from the actual GT —
v1: 40/64 = 0.625; v2: 36/96 = 0.375, because the context_drift family spreads
its roots over three different downstream agents. The number is NEVER hardcoded;
any narrative quoting 0.625/40-of-64 is a v1 statement and must be recomputed).

PARAMETRIZED (mirrors eval_agentfault_tierA.py / infra_negatives): `--dataset-dir
DIR` (or --out-root) points it at any agentfault tree; with NO flags it scores v1
((v1)whowhen) exactly as before. The FAULT FAMILY LIST IS DERIVED
from cases_index.json at runtime — v1 yields 3 families, v2 yields 4 (the new one
being context_drift) with no code change and no hardcoded counts.

context_drift (v2) reporting: it gets a normal per-family column, and footnote [h]
states plainly that a LOW score on that family is the EXPECTED, INTENDED result,
not a harness failure. The injection deletes an upstream message from the
DOWNSTREAM agent's INPUT while all four OUTPUTS stay present and normal, so an
output-reading LLM judge has no visible symptom to attribute; measuring that
blindness is the experiment. Its GT is the downstream target whose input was
stripped.

Output: <out-root>/whowhen_results.json (per method x per fault family x overall
+ per-case detail + cases build fingerprint) + markdown table on stdout with the
honesty footnotes of SPEC §2.

Multi-judge (--tag, default "deepseek"): scores outputs/<method>_<tag>.txt as
produced by run_whowhen.py --tag <tag>. The default tag keeps the historical
inputs and writes the historical whowhen_results.json; any other tag writes
whowhen_results_<tag>.json so per-judge results never overwrite each other.
The constant always-Synthesizer baseline row and all footnote logic are
tag-independent (they depend only on the GT).

Usage:
  PYTHONIOENCODING=utf-8 python3 \
      scripts/chaos/agentfault/eval/whowhen/score_whowhen.py
  ... --dataset-dir (archived) agentfault_v2   # score the v2 (96-case, 4-family) run
  ... --tag glm      # score outputs/<method>_glm.txt -> whowhen_results_glm.json
  ... --self-test    # offline unit test of parsing + MRCBench path on crafted
                     # fake output fragments (temp files in system temp, cleaned)
"""
import argparse
import glob
import io
import json
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", ".."))
CTK = os.path.join(REPO, "scripts", "chaos", "ctk")
sys.path.insert(0, CTK)

# SPEC §1.3: import mrcbench exactly like eval_agentfault_tierA.py L55-80 —
# m9_score re-wraps sys.stdout at import (GC-closes the shared buffer), so we
# reopen an independent stream on a dup of fd 1 right after.
from m9_score import mrcbench, K_LIST  # noqa: E402

try:
    sys.stdout = io.TextIOWrapper(io.FileIO(os.dup(1), "w"), encoding="utf-8", errors="replace")
except Exception:
    pass

DEFAULT_OUT_ROOT = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "whowhen")
DEFAULT_TAG = "deepseek"


def output_filename(method, tag=DEFAULT_TAG):
    """outputs/<method>_<tag>.txt — the default tag keeps the historical names."""
    return "%s_%s.txt" % (method, tag)


def results_filename(tag=DEFAULT_TAG):
    """Default tag keeps the historical whowhen_results.json; any other tag
    writes whowhen_results_<tag>.json so judges never overwrite each other."""
    if tag == DEFAULT_TAG:
        return "whowhen_results.json"
    return "whowhen_results_%s.json" % tag


AGENT_NAMES = ["Sequence_Recommender", "User_Behavior_Analyzer",
               "Product_Analyzer", "Recommendation_Synthesizer"]
METHODS = ("all_at_once", "step_by_step", "binary_search", "a2p")

# Canonical family display order. The ACTIVE `FAMILIES` is DERIVED in main() (and
# in self_test()) by filtering this to the families actually present in
# cases_index.json, then appending any unknown family found in the data — so v1
# reports 3 families unchanged while v2 gains the context_drift column, with no
# hardcoded family count anywhere.
CANON_FAMILY_ORDER = ("hallucinate", "wrong_item_pick", "format_violation", "context_drift")
FAMILIES = CANON_FAMILY_ORDER[:3]   # v1 default; reassigned from data in main()

# Family whose expected outcome is INVERTED: see footnote [h]. A low score here is
# the intended finding (judge blindness to a silent input-side fault), not a bug.
BLIND_BY_DESIGN_FAMILY = "context_drift"


def derive_families(families_map):
    """Active family tuple, DERIVED from the cases_index.json kind values:
    canonical order first (only the ones actually present), then any unexpected
    family, so a future family shows up in the report instead of vanishing."""
    present = set(families_map.values())
    ordered = [f for f in CANON_FAMILY_ORDER if f in present]
    ordered += sorted(f for f in present if f not in CANON_FAMILY_ORDER)
    return tuple(ordered)

# ---- whowhen evaluate.py regexes, byte-identical (SPEC §1.3) ----
BLOCK_RE = re.compile(r"Prediction for ([^:]+\.json):(.*?)(?=Prediction for|\Z)", re.DOTALL)
AGENT_RE = re.compile(r"Agent Name:\s*([\w_]+)", re.IGNORECASE)
STEP_RE = re.compile(r"Step Number:\s*(\d+)", re.IGNORECASE)

# Fix #5: tolerant "Agent Name" line matcher, used ONLY by the normalized
# reading and ONLY where the native AGENT_RE failed on a block. deepseek-chat
# frequently emits markdown bold ('Agent Name: **Product_Analyzer**' or
# '**Agent Name:** Product_Analyzer'), which makes the native regex miss
# entirely; without a fallback both readings record 'missing' and the
# normalized column loses its recovery purpose. The native protocol is never
# touched — divergence stays visible.
TOLERANT_AGENT_LINE_RE = re.compile(r"Agent\s*Name\b[^:\n]{0,8}:\s*([^\n]+)", re.IGNORECASE)
_MD_WRAP_CHARS = "*_`\"' \t"


def tolerant_agent_extract(block):
    """Fallback extraction for the NORMALIZED reading (fix #5): find an
    'Agent Name:' line even when wrapped in markdown, strip * _ ` \" ' wrapping,
    allow space-separated names, then map through normalize_pred to one of the
    4 canonical names. Returns canonical name or None. Never used for the
    native Who&When protocol."""
    if not block:
        return None
    for m in TOLERANT_AGENT_LINE_RE.finditer(block):
        cand = m.group(1).strip().strip(_MD_WRAP_CHARS)
        # cut at sentence-ish punctuation so 'Product Analyzer, because...'
        # still yields the name part
        cand = re.split(r"[.;,:(\[]", cand)[0].strip().strip(_MD_WRAP_CHARS)
        norm = normalize_pred(cand)
        if norm:
            return norm
    return None

FOOTNOTES = [
    "[a] Same-family judge bias: judge LLM = DeepSeek = the hallucinate-injection "
    "sub-LLM's family -> may inherit blind spots or, conversely, familiarity with "
    "the rewrite style (SPEC §2; EVAL_NOTES §4e). A cross-family re-test with ONE "
    "outside judge (GLM-5.2, tag glm52) has since been run and showed no sign of "
    "same-family inflation (hallucinate scores went UP, not down, in the normalized "
    "reading); one judge family is not enough to call the bias falsified.",
    "[c] w/o ground-truth-answer setting: ground_truth is an N/A sentence (open-ended "
    "task, no unique answer) — harder than Who&When's code default of pasting the "
    "true answer into the prompt; aligns with their w/o-GT-answer setting.",
    "[d] Single-prediction methods: for MRCBench the ranked list is [pred] + the "
    "remaining canonical names as padding, so @3/@5/@R, mrr AND ndcg@K are all "
    "canonical-padding artifacts with no discriminative power (1 prediction, "
    "4 candidates); ONLY hit@1 (K=1) is discriminative and should coincide with "
    "native agent accuracy (dual-protocol cross-check).",
    "[e] binary_search ambiguous-answer tie-break is random.randint; harness fixes "
    "random.seed(0) before each run (vendored code untouched).",
    "[f] Normalized Hit@1 is a diagnostic side reading, never a substitute for the "
    "native protocol: case-insensitive, space/underscore-equivalent, full-name or "
    "unique-prefix matching; where the native regex misses a block entirely (e.g. "
    "markdown-bold 'Agent Name: **X**'), a tolerant extraction (strip */_/`/quote "
    "wrapping, allow spaces in names) recovers the name for THIS column only "
    "(per-method recovery count reported as normalized_tolerant_recoveries).",
]


def sentinel_footnote(build_info):
    """[b] wrong_item_pick sentinel conspicuity — DERIVED from the sentinel the
    adapter recorded in cases_index.json '_build'. v1's 'B00000FAULT' literally
    spells FAULT, which makes that family's LLM-judge score an upper bound; v2's
    'B00EKWZK5E' is catalogue-shaped and carries no such giveaway, so the caveat
    genuinely weakens. Stating the wrong one would be a (previously hardcoded)
    honesty bug, hence the derivation."""
    sent = (build_info or {}).get("wrongpick_sentinel") or ""
    if not sent:
        return ("[b] wrong_item_pick sentinel: not recorded in this cases build "
                "(rebuild with the current make_whowhen_cases.py). If the sentinel "
                "string contains a giveaway token such as 'FAULT', that family's "
                "LLM-judge score is an UPPER BOUND (sentinel artifact, not covert "
                "wrong-pick localizability).")
    if "FAULT" in sent.upper():
        return ("[b] wrong_item_pick sentinel is conspicuous: '%s' literally contains "
                "'FAULT' -> LLM-judge scores on that family are an UPPER BOUND "
                "(sentinel artifact, not covert wrong-pick localizability)." % sent)
    return ("[b] wrong_item_pick sentinel is '%s' — catalogue-shaped, with no giveaway "
            "token (unlike the v1 sentinel 'B00000FAULT', which spelled FAULT). The "
            "family's score is therefore NOT inflated by a conspicuous marker; the "
            "judge must notice the picked ASIN is inconsistent with the upstream "
            "candidates. Still an easier target than a silent fault: the wrong pick "
            "IS visible in the transcript." % sent)


def blind_family_footnote(families, gt, fam_map):
    """[h] context_drift = blind-by-design family. A LOW score is the EXPECTED,
    INTENDED result and the point of including the family; it is NOT a harness
    failure. Emitted only when the family is present, so v1 reports are unchanged."""
    if BLIND_BY_DESIGN_FAMILY not in families:
        return None
    n = sum(1 for f in fam_map.values() if f == BLIND_BY_DESIGN_FAMILY)
    roots = {}
    for fname, g in gt.items():
        if fam_map.get(fname) == BLIND_BY_DESIGN_FAMILY:
            roots[g["mistake_agent"]] = roots.get(g["mistake_agent"], 0) + 1
    return ("[h] %s (n=%d) is BLIND BY DESIGN for every method in this table: the "
            "injection deletes an upstream agent's message from the DOWNSTREAM agent's "
            "INPUT, while all four agents' OUTPUTS remain present, well-formed and "
            "normal-looking. An output-reading LLM judge is shown a transcript with no "
            "visible symptom, so a LOW score here is the EXPECTED, INTENDED result — "
            "it is the evidence that output-reading attribution cannot see silent "
            "context faults, NOT a defect of the harness or of the method. Nothing in "
            "the case JSON hints at the deletion (adapter checks 8a-8c: no metadata "
            "field, no leak vocabulary, identical structure across families). GT = the "
            "downstream target whose input was stripped; its roots spread over %s, "
            "which is also why the always_synthesizer prior drops relative to a "
            "context_drift-free dataset (see [g])."
            % (BLIND_BY_DESIGN_FAMILY, n,
               ", ".join("%s x%d" % (a, c) for a, c in sorted(roots.items()))))


def gt_prior_footnote(gt, families):
    """Fix #3(b): dynamic honesty footnote about the skewed GT prior. Computed
    from the actual GT so the numbers can never drift from the data."""
    total = len(gt)
    syn = "Recommendation_Synthesizer"
    n_syn = sum(1 for g in gt.values() if g["mistake_agent"] == syn)
    fam_tot, fam_syn = {}, {}
    for fname, g in gt.items():
        fam = families.get(fname, "?")
        fam_tot[fam] = fam_tot.get(fam, 0) + 1
        if g["mistake_agent"] == syn:
            fam_syn[fam] = fam_syn.get(fam, 0) + 1
    per = ", ".join(
        "%s %d/%d (%.0f%%)" % (f, fam_syn.get(f, 0), fam_tot[f],
                               100.0 * fam_syn.get(f, 0) / fam_tot[f])
        for f in FAMILIES if fam_tot.get(f))
    return ("[g] GT prior is heavily skewed: %d/%d (%.1f%%) of all GT agents are "
            "Recommendation_Synthesizer; per family: %s. Any method's score on the "
            "families where the prior is (near-)100%% MUST be read against the "
            "always_synthesizer constant baseline row, which achieves those numbers "
            "by prior alone with zero localization ability."
            % (n_syn, total, 100.0 * n_syn / total if total else 0.0, per))


BASELINE_AGENT = "Recommendation_Synthesizer"


def constant_baseline_result(gt, families):
    """Fix #3(a): 'always-Synthesizer' constant-prediction baseline. Predicts
    Recommendation_Synthesizer (and its execution-order step) for every case
    and goes through the exact same dual-protocol scoring path as the real
    methods, so the table rows are directly comparable."""
    step = str(AGENT_NAMES.index(BASELINE_AGENT))
    preds = {fname: {"predicted_agent": BASELINE_AGENT,
                     "predicted_step": step,
                     "raw_block": "<constant baseline: always predicts %s>" % BASELINE_AGENT}
             for fname in gt}
    return score_from_preds("always_synthesizer (const baseline)",
                            "<none: constant prediction, no LLM output>",
                            preds, {}, gt, families)


def read_predictions(eval_file):
    """Parse one outputs/*.txt with the exact whowhen evaluate.py protocol
    (SPEC §1.3). Returns (predictions, blocks):
      predictions = {case_fname: {'predicted_agent','predicted_step'}} — native
        protocol, a block counts only if BOTH AGENT_RE and STEP_RE match
        (byte-identical to their evaluate.py);
      blocks = {case_fname: raw block text} for EVERY block found, including
        natively unparsable ones — consumed only by the normalized reading's
        tolerant fallback (fix #5), never by the native protocol."""
    with open(eval_file, "r", encoding="utf-8") as f:
        data = f.read()
    predictions = {}
    blocks = {}
    for block in BLOCK_RE.finditer(data):
        idx = block.group(1).strip()
        content = block.group(2).strip()
        blocks[idx] = content
        am = AGENT_RE.search(content)
        sm = STEP_RE.search(content)
        if am and sm:
            predictions[idx] = {
                "predicted_agent": am.group(1),
                "predicted_step": sm.group(1),
                "raw_block": content,
            }
    return predictions, blocks


def normalize_pred(pred):
    """Normalized matching (SPEC §1.3 reading #3): case-insensitive,
    space/underscore equivalent, full name or unique prefix -> canonical name
    or None."""
    if not pred:
        return None
    p = re.sub(r"[\s_]+", "_", str(pred).strip()).strip("_").lower()
    if not p:
        return None
    canon = {a.lower(): a for a in AGENT_NAMES}
    if p in canon:
        return canon[p]
    prefix_hits = [a for a in AGENT_NAMES if a.lower().startswith(p)]
    if len(prefix_hits) == 1:
        return prefix_hits[0]
    return None


def ranked_of(pred_agent):
    """Single prediction -> ranked list for mrcbench (SPEC §1.3):
    [pred] + remaining AGENT_NAMES in canonical order, dedup; missing -> []."""
    if pred_agent is None:
        return []
    ranked = [pred_agent]
    for a in AGENT_NAMES:
        if a != pred_agent:
            ranked.append(a)
    return ranked


def load_gt(cases_dir):
    """GT from cases/case_NNN.json (mistake_agent / mistake_step, SPEC §1.3)."""
    gt = {}
    for path in sorted(glob.glob(os.path.join(cases_dir, "*.json"))):
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        gt[os.path.basename(path)] = {
            "mistake_agent": str(d["mistake_agent"]),
            "mistake_step": str(d["mistake_step"]),
        }
    return gt


def load_families(out_root):
    """(case fname -> fault family, build info) from cases_index.json
    (SPEC §1.3). Keys starting with '_' are metadata (the top-level '_build'
    fingerprint written by make_whowhen_cases.py, fix #6), not cases."""
    idx_path = os.path.join(out_root, "cases_index.json")
    with open(idx_path, "r", encoding="utf-8") as f:
        idx = json.load(f)
    families = {fname: rec["kind"] for fname, rec in idx.items()
                if not fname.startswith("_")}
    return families, (idx.get("_build") or {})


def _macro(dicts):
    """Mean of a list of mrcbench dicts (macro over cases). Empty -> {}."""
    if not dicts:
        return {}
    keys = dicts[0].keys()
    return {k: sum(d[k] for d in dicts) / float(len(dicts)) for k in keys}


def score_method(method, eval_file, gt, families):
    """One method's output file -> per-case detail + native/normalized/MRCBench
    aggregates (SPEC §1.3 dual protocol + normalized column)."""
    preds, blocks = read_predictions(eval_file)
    return score_from_preds(method, eval_file, preds, blocks, gt, families)


def score_from_preds(method, eval_file, preds, blocks, gt, families):
    """Shared scoring path (fix #3: the constant baseline goes through exactly
    this code, so its numbers are comparable to every method's)."""
    total = len(gt)
    per_case = []
    native_agent_ok = native_step_ok = norm_ok = norm_tolerant = 0
    mrc_rows, mrc_by_family = [], {f: [] for f in FAMILIES}

    for fname in sorted(gt.keys()):
        g = gt[fname]
        fam = families.get(fname, "?")
        p = preds.get(fname)
        pred_agent = p["predicted_agent"] if p else None
        pred_step = p["predicted_step"] if p else None
        # native (whowhen evaluate.py): substring match, denominator = total files
        agent_ok = bool(p) and (g["mistake_agent"] in pred_agent)
        step_ok = bool(p) and (g["mistake_step"] in pred_step)
        # normalized reading; fix #5: when the native token is absent or does
        # not normalize, fall back to tolerant extraction on the raw block
        # (markdown-wrapped names). Native numbers above are untouched.
        norm_agent = normalize_pred(pred_agent)
        via_tolerant = False
        if norm_agent is None:
            t = tolerant_agent_extract(blocks.get(fname))
            if t is not None:
                norm_agent = t
                via_tolerant = True
        n_ok = norm_agent == g["mistake_agent"]
        # MRCBench: raw predicted string as rank head (SPEC: missing -> [])
        mrc = mrcbench(ranked_of(pred_agent), [g["mistake_agent"]])
        native_agent_ok += agent_ok
        native_step_ok += step_ok
        norm_ok += n_ok
        norm_tolerant += via_tolerant
        mrc_rows.append(mrc)
        if fam in mrc_by_family:
            mrc_by_family[fam].append(mrc)
        per_case.append({
            "case": fname, "family": fam,
            "gt_agent": g["mistake_agent"], "gt_step": g["mistake_step"],
            "pred_agent": pred_agent, "pred_step": pred_step,
            "missing": p is None,
            "native_agent_correct": agent_ok, "native_step_correct": step_ok,
            "normalized_agent": norm_agent, "normalized_hit1": n_ok,
            "normalized_via_tolerant": via_tolerant,
            "mrc_hit1": mrc["hit@1"],
        })

    n_missing = sum(1 for c in per_case if c["missing"])
    res = {
        "method": method,
        "eval_file": eval_file,
        "total_cases": total,
        "parsed_predictions": total - n_missing,
        "missing_predictions": n_missing,
        "native": {
            "agent_accuracy": native_agent_ok / float(total) if total else 0.0,
            "step_accuracy": native_step_ok / float(total) if total else 0.0,
        },
        "normalized_hit1": norm_ok / float(total) if total else 0.0,
        "normalized_tolerant_recoveries": norm_tolerant,
        "mrcbench_overall": _macro(mrc_rows),
        "mrcbench_by_family": {f: _macro(v) for f, v in mrc_by_family.items() if v},
        "native_by_family": {},
        "per_case": per_case,
    }
    for fam in FAMILIES:
        rows = [c for c in per_case if c["family"] == fam]
        if rows:
            res["native_by_family"][fam] = {
                "n": len(rows),
                "agent_accuracy": sum(c["native_agent_correct"] for c in rows) / float(len(rows)),
                "step_accuracy": sum(c["native_step_correct"] for c in rows) / float(len(rows)),
                "normalized_hit1": sum(c["normalized_hit1"] for c in rows) / float(len(rows)),
            }
    return res


def markdown_report(results, footnotes, build_info=None, tag=DEFAULT_TAG):
    """stdout markdown tables (later merged into BASELINE_RESULTS.md, SPEC §1.3).
    Footnote letters in the headers must exist in `footnotes` (fix #4).
    The header names the judge model tag so per-judge tables stay attributable
    once several judges' reports are merged into one document."""
    n_cases = results[0]["total_cases"] if results else 0
    lines = ["## Who&When / A2P on agentfault (%d faulted cases, %d fault famil%s)"
             % (n_cases, len(FAMILIES), "y" if len(FAMILIES) == 1 else "ies"), "",
             "judge model tag: %s" % tag, ""]
    if build_info:
        lines.append("cases build: fingerprint=%s, generated_at=%s, n_cases=%s"
                     % (build_info.get("fingerprint", "?"),
                        build_info.get("generated_at", "?"),
                        build_info.get("n_cases", "?")))
        lines.append("")
    hdr = ("| method | parsed/total | native agent acc | native step acc | "
           "normalized Hit@1 [f] | MRC hit@1 | MRC hit@3 [d] | MRC hit@5 [d] | MRC mrr [d] |")
    lines += [hdr, "|" + "---|" * 9]
    for r in results:
        mo = r["mrcbench_overall"] or {}
        lines.append("| %s | %d/%d | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f |" % (
            r["method"], r["parsed_predictions"], r["total_cases"],
            r["native"]["agent_accuracy"], r["native"]["step_accuracy"],
            r["normalized_hit1"], mo.get("hit@1", 0.0), mo.get("hit@3", 0.0),
            mo.get("hit@5", 0.0), mo.get("mrr", 0.0)))
    lines.append("")
    lines.append("### per fault family (native agent acc / normalized Hit@1) [g]")
    # the blind-by-design family carries its own footnote marker in the header so a
    # reader cannot mistake its (expected) low numbers for a broken run
    hdr_fams = ["%s [h]" % f if f == BLIND_BY_DESIGN_FAMILY else f for f in FAMILIES]
    lines.append("| method | " + " | ".join(hdr_fams) + " |")
    lines.append("|" + "---|" * (1 + len(FAMILIES)))
    for r in results:
        cells = []
        for fam in FAMILIES:
            fr = r["native_by_family"].get(fam)
            cells.append("%.3f / %.3f (n=%d)" % (fr["agent_accuracy"], fr["normalized_hit1"], fr["n"])
                         if fr else "-")
        lines.append("| %s | %s |" % (r["method"], " | ".join(cells)))
    lines.append("")
    lines.append("Footnotes (honesty items, SPEC §2):")
    for fn in footnotes:
        lines.append("- " + fn)
    return "\n".join(lines)


def main():
    global FAMILIES
    ap = argparse.ArgumentParser(description="Score Who&When/A2P outputs (SPEC §1.3, offline). "
                                             "Defaults reproduce the v1 scoring run exactly.")
    ap.add_argument("--dataset-dir", default=None,
                    help="agentfault tree; sets --out-root to <dir>/whowhen unless that "
                         "flag is given. Default = (archived) agentfault (v1). "
                         "v2: (archived) agentfault_v2")
    ap.add_argument("--out-root", default=None,
                    help="whowhen dataset root (contains cases/, outputs/, cases_index.json); "
                         "overrides --dataset-dir")
    ap.add_argument("--tag", default=DEFAULT_TAG,
                    help="judge model tag: score outputs/<method>_<tag>.txt as produced by "
                         "run_whowhen.py --tag <tag>; default '%s' keeps the historical "
                         "filenames and result path" % DEFAULT_TAG)
    ap.add_argument("--self-test", action="store_true",
                    help="offline unit test on 3 crafted fake output fragments")
    args = ap.parse_args()

    if args.self_test:
        return self_test()

    # path resolution: every default falls back to the v1 constant, so running
    # with NO flags scores v1 exactly as before (v2 must never overwrite v1)
    out_root = args.out_root
    if out_root is None and args.dataset_dir:
        base = args.dataset_dir if os.path.isabs(args.dataset_dir) \
            else os.path.join(REPO, args.dataset_dir)
        out_root = os.path.join(base, "whowhen")
    if out_root is None:
        out_root = DEFAULT_OUT_ROOT

    cases_dir = os.path.join(out_root, "cases")
    outputs_dir = os.path.join(out_root, "outputs")
    if not os.path.isdir(cases_dir) or not glob.glob(os.path.join(cases_dir, "*.json")):
        print("ERROR: no cases in %s — run make_whowhen_cases.py first." % cases_dir)
        return 1
    gt = load_gt(cases_dir)
    families, build_info = load_families(out_root)
    # families DERIVED from the data: v1 -> 3, v2 -> 4 (adds context_drift)
    FAMILIES = derive_families(families)
    print("[families] %d derived from cases_index.json: %s"
          % (len(FAMILIES), ", ".join("%s(n=%d)" % (f, sum(1 for v in families.values() if v == f))
                                      for f in FAMILIES)))

    results, missing_files = [], []
    for method in METHODS:
        eval_file = os.path.join(outputs_dir, output_filename(method, args.tag))
        if not os.path.isfile(eval_file):
            missing_files.append(eval_file)
            continue
        results.append(score_method(method, eval_file, gt, families))

    for mf in missing_files:
        print("[warn] output file missing (method not run yet?): %s" % mf)
    if not results:
        print("ERROR: no output files found in %s — run run_whowhen.py first." % outputs_dir)
        return 1

    # Fix #3: constant always-Synthesizer baseline, same scoring path, so the
    # skewed GT prior (see footnote [g]) is visible right in the table.
    results.append(constant_baseline_result(gt, families))

    footnotes = ([FOOTNOTES[0], sentinel_footnote(build_info)] + list(FOOTNOTES[1:])
                 + [gt_prior_footnote(gt, families)])
    blind = blind_family_footnote(FAMILIES, gt, families)
    if blind:
        footnotes.append(blind)
    out_json = os.path.join(out_root, results_filename(args.tag))
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"results": results, "footnotes": footnotes,
                   "judge_tag": args.tag,
                   "families": list(FAMILIES),
                   "cases_build": build_info,
                   "k_list": list(K_LIST)}, f, ensure_ascii=False, indent=2)
    print(markdown_report(results, footnotes, build_info, tag=args.tag))
    print("\n[written] %s" % out_json)
    return 0


# ------------------------------- self test -----------------------------------
def self_test():
    """Task (c): crafted fake output fragments through the full parse+score
    path — all_at_once normal / step_by_step with a no-prediction case /
    space-variant + markdown-bold agent names (fix #5) / constant baseline
    (fix #3) / footnote-reference integrity (fix #4) / v2 additions: family list
    DERIVED from the data (4 families incl. context_drift), the blind-by-design
    [h] footnote, and the data-derived sentinel [b] footnote in both of its
    branches. Temp files only, nothing left in datasets/."""
    global FAMILIES
    tmp = tempfile.mkdtemp(prefix="whowhen_selftest_")
    cases_dir = os.path.join(tmp, "cases")
    outputs_dir = os.path.join(tmp, "outputs")
    os.makedirs(cases_dir)
    os.makedirs(outputs_dir)

    # 4 fake cases, incl. one context_drift whose GT is a DOWNSTREAM agent
    # (Product_Analyzer) — mirrors the v2 family mix
    gt_spec = [("case_001.json", "Product_Analyzer", 2, "hallucinate"),
               ("case_002.json", "Recommendation_Synthesizer", 3, "format_violation"),
               ("case_003.json", "Product_Analyzer", 2, "hallucinate"),
               ("case_004.json", "Product_Analyzer", 2, "context_drift")]
    index = {"_build": {"fingerprint": "deadbeef0123", "generated_at": "t", "n_cases": 4,
                        "wrongpick_sentinel": "B00EKWZK5E"}}
    for fname, agent, step, kind in gt_spec:
        with open(os.path.join(cases_dir, fname), "w", encoding="utf-8") as f:
            json.dump({"question": "q", "ground_truth": "n/a",
                       "history": [{"name": a, "content": "x"} for a in AGENT_NAMES],
                       "mistake_agent": agent, "mistake_step": step}, f)
        index[fname] = {"case_id": fname, "kind": kind, "mistake_agent": agent}
    with open(os.path.join(tmp, "cases_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f)

    # fragment 1: all_at_once, canonical name, correct -> native+norm+mrc all hit
    with open(os.path.join(outputs_dir, "all_at_once_deepseek.txt"), "w", encoding="utf-8") as f:
        f.write("Prediction for case_001.json:\n"
                "Agent Name: Product_Analyzer\nStep Number: 2\nReason for Mistake: xx\n\n"
                + "=" * 50 + "\n\n"
                "Prediction for case_002.json:\n"
                "Agent Name: Recommendation_Synthesizer\nStep Number: 1\nReason for Mistake: yy\n\n"
                + "=" * 50 + "\n\n"
                "Prediction for case_003.json:\n"
                "Agent Name: Sequence_Recommender\nStep Number: 0\nReason for Mistake: zz\n\n"
                + "=" * 50 + "\n\n"
                # context_drift case: judge sees a clean transcript and falls back
                # to the Synthesizer -> wrong. This is the EXPECTED shape of the
                # blind-by-design family (footnote [h]).
                "Prediction for case_004.json:\n"
                "Agent Name: Recommendation_Synthesizer\nStep Number: 3\n"
                "Reason for Mistake: nothing looks wrong, blaming the last agent\n\n"
                + "=" * 50 + "\n")
    # fragment 2: step_by_step, case_002 finds error, case_001/003 -> no prediction block
    with open(os.path.join(outputs_dir, "step_by_step_deepseek.txt"), "w", encoding="utf-8") as f:
        f.write("--- Analyzing File: case_001.json ---\n"
                "No decisive errors found by step-by-step analysis in file case_001.json\n\n"
                "--- Analyzing File: case_002.json ---\n"
                "Prediction for case_002.json: Error found.\n"
                "Agent Name: Recommendation_Synthesizer\nStep Number: 3\n"
                "Reason provided by LLM: bad json\n\n" + "=" * 50 + "\n\n"
                "--- Analyzing File: case_003.json ---\n"
                "No decisive errors found by step-by-step analysis in file case_003.json\n\n"
                "--- Analyzing File: case_004.json ---\n"
                "No decisive errors found by step-by-step analysis in file case_004.json\n")
    # fragment 3: a2p — three native-regex failure shapes the normalized reading
    # must handle: space variant (native catches only "Product"), markdown bold
    # around the value, markdown bold around the label (both -> native miss
    # entirely; tolerant extraction recovers them, fix #5)
    with open(os.path.join(outputs_dir, "a2p_deepseek.txt"), "w", encoding="utf-8") as f:
        f.write("Prediction for case_001.json:\n"
                "Agent Name: Product Analyzer\nStep Number: 2\nReason for Mistake: variant\n\n"
                + "=" * 50 + "\n\n"
                "Prediction for case_002.json:\n"
                "Agent Name: **Recommendation_Synthesizer**\nStep Number: 3\n"
                "Reason for Mistake: bold value\n\n" + "=" * 50 + "\n\n"
                "Prediction for case_003.json:\n"
                "**Agent Name:** Product_Analyzer\nStep Number: 2\n"
                "Reason for Mistake: bold label\n\n" + "=" * 50 + "\n\n"
                "Prediction for case_004.json:\n"
                "Agent Name: Product_Analyzer\nStep Number: 2\n"
                "Reason for Mistake: canonical\n")

    gt = load_gt(cases_dir)
    families, build_info = load_families(tmp)
    FAMILIES = derive_families(families)   # v2 path: DERIVED, not hardcoded
    checks = [("load_families skips _build and returns it",
               set(families) == {f for f, _, _, _ in gt_spec}
               and build_info.get("fingerprint") == "deadbeef0123")]
    checks.append(("derive_families -> 4 families in canonical order incl. context_drift",
                   FAMILIES == ("hallucinate", "format_violation", "context_drift")
                   or FAMILIES == ("hallucinate", "wrong_item_pick", "format_violation",
                                   "context_drift")))
    checks.append(("derive_families keeps canonical order (context_drift last)",
                   FAMILIES[-1] == "context_drift"))

    r1 = score_method("all_at_once", os.path.join(outputs_dir, "all_at_once_deepseek.txt"),
                      gt, families)
    checks.append(("aao parsed 4/4", r1["parsed_predictions"] == 4))
    checks.append(("aao native agent acc == 2/4", abs(r1["native"]["agent_accuracy"] - 0.5) < 1e-9))
    checks.append(("aao native step acc == 1/4", abs(r1["native"]["step_accuracy"] - 0.25) < 1e-9))
    checks.append(("aao mrc hit@1 == native agent acc", abs(r1["mrcbench_overall"]["hit@1"] - r1["native"]["agent_accuracy"]) < 1e-9))
    checks.append(("aao mrc hit@5 == 1.0 (ceiling artifact)", r1["mrcbench_overall"]["hit@5"] == 1.0))
    # blind-by-design family gets its own per-family row and scores 0 here
    checks.append(("aao context_drift family row present, n=1, agent acc 0.0 (expected miss)",
                   r1["native_by_family"].get("context_drift", {}).get("n") == 1
                   and r1["native_by_family"]["context_drift"]["agent_accuracy"] == 0.0))
    checks.append(("aao context_drift family present in mrcbench_by_family",
                   "context_drift" in r1["mrcbench_by_family"]))

    r2 = score_method("step_by_step", os.path.join(outputs_dir, "step_by_step_deepseek.txt"),
                      gt, families)
    checks.append(("sbs missing == 3", r2["missing_predictions"] == 3))
    checks.append(("sbs native agent acc == 1/4 (denominator=total)", abs(r2["native"]["agent_accuracy"] - 0.25) < 1e-9))
    c1 = next(c for c in r2["per_case"] if c["case"] == "case_001.json")
    checks.append(("sbs missing case -> mrc all-zero", c1["mrc_hit1"] == 0.0 and c1["missing"]))

    r3 = score_method("a2p", os.path.join(outputs_dir, "a2p_deepseek.txt"), gt, families)
    c1 = next(c for c in r3["per_case"] if c["case"] == "case_001.json")
    checks.append(("space variant: native regex captured 'Product'", c1["pred_agent"] == "Product"))
    checks.append(("space variant: native substring -> wrong", not c1["native_agent_correct"]))
    checks.append(("space variant: normalized recovers Product_Analyzer",
                   c1["normalized_agent"] == "Product_Analyzer" and c1["normalized_hit1"]))
    checks.append(("space variant: mrc raw-pred hit@1 == 0", c1["mrc_hit1"] == 0.0))

    # fix #5: markdown-bold blocks — native protocol misses them (byte-fidelity
    # to whowhen evaluate.py), tolerant extraction recovers them for the
    # normalized column only
    c2 = next(c for c in r3["per_case"] if c["case"] == "case_002.json")
    c3 = next(c for c in r3["per_case"] if c["case"] == "case_003.json")
    checks.append(("bold value: native missing (protocol untouched)",
                   c2["missing"] and c2["pred_agent"] is None and not c2["native_agent_correct"]))
    checks.append(("bold value: tolerant recovers Recommendation_Synthesizer",
                   c2["normalized_agent"] == "Recommendation_Synthesizer"
                   and c2["normalized_via_tolerant"] and c2["normalized_hit1"]))
    checks.append(("bold label: native missing, tolerant recovers Product_Analyzer",
                   c3["missing"] and c3["normalized_agent"] == "Product_Analyzer"
                   and c3["normalized_via_tolerant"] and c3["normalized_hit1"]))
    checks.append(("tolerant recovery count == 2", r3["normalized_tolerant_recoveries"] == 2))
    checks.append(("bold blocks: mrc stays raw (missing -> 0)", c2["mrc_hit1"] == 0.0))

    # fix #3: always-Synthesizer constant baseline through the same scoring path
    rb = constant_baseline_result(gt, families)
    checks.append(("baseline parsed 4/4, no missing",
                   rb["parsed_predictions"] == 4 and rb["missing_predictions"] == 0))
    checks.append(("baseline native agent acc == 1/4 (GT prior of the fake set)",
                   abs(rb["native"]["agent_accuracy"] - 0.25) < 1e-9))
    checks.append(("baseline native step acc == 1/4",
                   abs(rb["native"]["step_accuracy"] - 0.25) < 1e-9))
    checks.append(("baseline format_violation family == 1.0, hallucinate == 0.0",
                   rb["native_by_family"]["format_violation"]["agent_accuracy"] == 1.0
                   and rb["native_by_family"]["hallucinate"]["agent_accuracy"] == 0.0))
    # context_drift roots are DOWNSTREAM agents, not always the Synthesizer -> the
    # constant baseline does NOT get a free pass on this family (this is exactly
    # why the v2 always-Synthesizer prior drops vs v1)
    checks.append(("baseline context_drift family == 0.0 (prior does not cover it)",
                   rb["native_by_family"]["context_drift"]["agent_accuracy"] == 0.0))
    checks.append(("baseline mrc hit@1 == native agent acc",
                   abs(rb["mrcbench_overall"]["hit@1"] - rb["native"]["agent_accuracy"]) < 1e-9))

    # fix #3(b)+#4: footnote set — GT-prior footnote computed from data; every
    # bracketed letter referenced in the tables must exist as a footnote
    footnotes = ([FOOTNOTES[0], sentinel_footnote(build_info)] + list(FOOTNOTES[1:])
                 + [gt_prior_footnote(gt, families)])
    blind = blind_family_footnote(FAMILIES, gt, families)
    if blind:
        footnotes.append(blind)
    gt_fn = next(fn for fn in footnotes if fn.startswith("[g]"))
    checks.append(("gt prior footnote carries computed numbers",
                   "1/4 (25.0%)" in gt_fn and "always_synthesizer" in gt_fn
                   and "format_violation 1/1 (100%)" in gt_fn))
    checks.append(("gt prior footnote includes the context_drift family",
                   "context_drift 0/1 (0%)" in gt_fn))
    # [b] sentinel footnote is DERIVED from _build, both branches
    b_fn = next(fn for fn in footnotes if fn.startswith("[b]"))
    checks.append(("[b] sentinel footnote names the v2 sentinel, no 'upper bound' claim",
                   "B00EKWZK5E" in b_fn and "UPPER BOUND" not in b_fn))
    b_v1 = sentinel_footnote({"wrongpick_sentinel": "B00000FAULT"})
    checks.append(("[b] v1 sentinel branch still warns UPPER BOUND",
                   "B00000FAULT" in b_v1 and "UPPER BOUND" in b_v1))
    b_none = sentinel_footnote({})
    checks.append(("[b] missing sentinel -> explicit 'not recorded', never silent",
                   "not recorded" in b_none))
    # [h] blind-by-design footnote
    checks.append(("[h] blind footnote emitted, states EXPECTED/INTENDED + n",
                   blind is not None and blind.startswith("[h]")
                   and "EXPECTED, INTENDED" in blind and "n=1" in blind))
    checks.append(("[h] absent when the family is absent (v1 reports unchanged)",
                   blind_family_footnote(("hallucinate", "format_violation"), gt, families) is None))
    report = markdown_report([r1, r2, r3, rb], footnotes, build_info)
    checks.append(("report header derives case + family counts",
                   "(4 faulted cases, 3 fault families)" in report))
    checks.append(("report marks the blind family column with [h]",
                   "context_drift [h]" in report))
    body = report.split("Footnotes")[0]
    refs = set(re.findall(r"\[([a-z])\]", body))
    defined = {fn[1] for fn in footnotes if fn.startswith("[")}
    checks.append(("all footnote refs in tables exist (%s ⊆ %s)"
                   % (sorted(refs), sorted(defined)), refs <= defined))
    checks.append(("report carries build fingerprint", "deadbeef0123" in report))
    checks.append(("baseline row present in report", "always_synthesizer" in report))

    ok = all(passed for _, passed in checks)
    print("=== score_whowhen self-test ===")
    for name, passed in checks:
        print("  [%s] %s" % ("PASS" if passed else "FAIL", name))
    print("=== %s (%d/%d) ===" % ("ALL PASS" if ok else "FAILURES",
                                  sum(1 for _, p in checks if p), len(checks)))
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
