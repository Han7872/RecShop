# -*- coding: utf-8 -*-
"""make_whowhen_cases.py — agentfault raw case -> Who&When JSON adapter (SPEC §1.1).

Pure offline, no API. Reads <dataset-dir>/raw/*.json, keeps row.injected==1 and
row.ledger_status=='injected' (the clean `normal` reps and any injection that did
not diverge / inject_failed are excluded: Who&When presupposes a FAILED task,
SPEC §1.1). Writes <dataset-dir>/whowhen/cases/case_NNN.json + cases_index.json.

PARAMETRIZED (mirrors eval_agentfault_tierA.py / infra_negatives/run_infra_negatives.py):
`--dataset-dir DIR` (or --raw-dir/--spans-dir/--csv/--out-root) point it at any
agentfault tree. With NO flags it reproduces v1 ((archived) agentfault, 72 rows ->
64 faulted) byte-for-byte. EVERY count/expectation is DERIVED at runtime — from
raw/*.json for the build and, independently, from dataset_agentfault.csv for the
expectation side of the self-checks (two sources must agree). Nothing about
64/96/72/108, the family list, or the wrong_item_pick sentinel is hardcoded:
  * families            -> derived from the CSV's `kind` column
  * per-family counts   -> derived from the CSV, asserted against what was built
  * sentinel ASIN       -> derived as the response_asin of the wrong_item_pick
                           rows whose response_asin_is_sentinel is truthy
                           (v1 -> B00000FAULT, v2 -> B00EKWZK5E; the injector's
                           current constant is deliberately NOT used, because it
                           tracks the newest dataset and would break v1)

Design decisions (each traced to SPEC §1.1):
  * execution order   = injector_smoke.AGENT_NAMES (canonical underscore names);
    raw resp.conversation keys are camel (no underscore) and alphabetically
    ordered, so we remap + reorder.
  * history entry "name" = canonical underscore name (matches GT string;
    whowhen evaluate.py regex [\\w_]+ + substring match; the 4 names are not
    substrings of each other).
  * Synthesizer content gets a tool-call args block appended for ALL cases
    (uniformly, not only format_violation — otherwise presence/absence of the
    block itself leaks the fault family; and format_violation corrupts the raw
    Synthesize_Recommendation arguments string, which lives only in spans).
    Missing capture -> placeholder "<no tool-call captured>" + counted; if >=75%
    of the without-args cases concentrate in one kind, that IS a leak -> exit 2.
  * question / ground_truth byte-identical across all cases (leak-proof static
    statements; ground_truth = the N/A sentence = Who&When w/o GT-answer
    setting, SPEC §1.1 + §2).
  * output filenames opaque: case_001..case_NNN by case_id (run_id) lexicographic
    order (raw filenames contain the GT agent name).
  * mistake_step = 0-based index of injected agent in execution order (4 agents
    speak once each -> step localization ≡ agent localization).

context_drift (NEW in v2) — "strategy A": the case is handed to the judge EXACTLY
like every other family, i.e. the complete, unmodified 4-agent conversation, with
NO hint that anything was deleted. Concretely, context_drift_dropped_agent /
context_drift_dropped_chars / context_drift_outcome / carrier_seq_id NEVER appear
anywhere in the case JSON (asserted, check 8). Rationale: the deletion happens in
the DOWNSTREAM agent's INPUT — all four agents still produce present, well-formed,
normal-looking OUTPUTS — so an output-reading judge has no visible symptom to find.
Recording that the judge therefore FAILS on this family is the experimental point,
not a defect: it is the evidence that output-reading attribution is blind to silent
context faults. Adding any marker would manufacture the signal we are trying to
show is absent. GT for a context_drift case = row.root_cause_set = the DOWNSTREAM
TARGET agent whose input was stripped (NOT the upstream agent whose message was
dropped); check 8e asserts index(dropped)+1 == index(root) in execution order.

Judge-visible payload: the vendored methods read ONLY `history`, `question` and
`ground_truth` (verified in-code by check 0 against
third_party/reference/{whowhen,a2p}/Automated_FA/Lib/utils.py). `_provenance` is
offline bookkeeping and never reaches an LLM; the leak scans below are run
against the reconstructed judge-visible payload, exactly as all_at_once builds it.

Usage:
  # v1 (default — identical to the original hardcoded run):
  PYTHONIOENCODING=utf-8 python3 \
      scripts/chaos/agentfault/eval/whowhen/make_whowhen_cases.py
  # v2 (108-row dataset incl. the context_drift family -> 96 faulted cases):
  PYTHONIOENCODING=utf-8 python3 \
      scripts/chaos/agentfault/eval/whowhen/make_whowhen_cases.py \
      --dataset-dir (archived) agentfault_v2
Exit: 0 = built + all hard checks pass; 1 = assertion/build failure;
      2 = tool-call capture gap concentrated (>=75%) in one kind (leak, stop and
      report). Rebuild is destructive-clean: cases/*.json + cases_index.json are
      removed first so stale files from an older raw set can never survive;
      cases_index.json carries a top-level "_build" fingerprint.
"""
import argparse
import csv
import datetime
import glob
import hashlib
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))                    # .../eval/whowhen
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", ".."))
INJECTOR_DIR = os.path.join(REPO, "scripts", "chaos", "agentfault", "injector")

# v1 defaults — running with NO flags must reproduce the original build exactly.
DEFAULT_DATASET_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault")

VENDORED_UTILS = [
    os.path.join(REPO, "third_party", "reference", "whowhen", "Automated_FA", "Lib", "utils.py"),
    os.path.join(REPO, "third_party", "reference", "a2p", "Automated_FA", "Lib", "utils.py"),
]

# SPEC §1.1: import injector_smoke by sys.path (top-level import is lightweight;
# only AGENT_NAMES / read_spans / synthesizer_toolcall_arg_strings are used).
# NOTE: injector_smoke.WRONG_ASIN is deliberately NOT used — it tracks the newest
# dataset (v2: B00EKWZK5E) and would silently break the v1 self-check
# (v1 sentinel: B00000FAULT). The sentinel is derived from the data instead.
sys.path.insert(0, INJECTOR_DIR)
import injector_smoke  # noqa: E402

AGENT_NAMES = injector_smoke.AGENT_NAMES
NO_ARGS_PLACEHOLDER = "<no tool-call captured>"

# SPEC §1.1: static, byte-identical across all cases (anti-leak).
QUESTION = (
    "A multi-agent e-commerce recommendation pipeline with 4 agents collaborating "
    "sequentially (Sequence_Recommender -> User_Behavior_Analyzer -> Product_Analyzer "
    "-> Recommendation_Synthesizer) must produce one personalized product "
    "recommendation with an explanation, based on the user's historical interactions. "
    "The pipeline produced an incorrect or invalid recommendation."
)
GROUND_TRUTH = "N/A (open-ended recommendation task; no single reference answer)"

# Case-JSON schema, fixed and identical for every family (structural uniformity is
# itself an anti-leak property: a context_drift case must not be distinguishable by
# shape). Asserted by check 8a.
CASE_TOP_KEYS = ("question", "ground_truth", "history", "mistake_agent",
                 "mistake_step", "_provenance")
PROVENANCE_KEYS = ("case_id", "kind", "format_subtype", "trace_id")
HISTORY_ENTRY_KEYS = ("name", "content")

# Injection-metadata field names that must never appear ANYWHERE in a case JSON
# (check 8b). The context_drift_* / carrier_seq_id columns are v2 additions; the
# older ones are listed too so the guard stays valid if families are added later.
FORBIDDEN_META_FIELDS = (
    "context_drift_dropped_agent", "context_drift_dropped_chars",
    "context_drift_outcome", "carrier_seq_id",
    "dropped_agent", "dropped_chars", "root_cause_set", "ledger_status",
    "divergent_needle", "fault_type_set", "injected",
)

# Vocabulary that would betray the context_drift injection to an output-reading
# judge (check 8c). Scanned case-insensitively over the JUDGE-VISIBLE payload.
# Empirically ZERO occurrences across all 96 v2 faulted cases, so a hit is loud
# and rare; it may still be an innocent natural-language utterance by an agent —
# the failure message says so and names the case for manual adjudication.
LEAK_WORDS = ("dropped", "removed", "stripped", "omitted", "deleted",
              "context_drift", "context drift", "truncat", "was not provided",
              "missing context")


def conv_key_of(agent_name):
    """canonical underscore name -> raw resp.conversation camel key (SPEC §1.1:
    mapping = canonical name with underscores removed)."""
    return agent_name.replace("_", "")


def toolcall_args_block(arg_strings):
    """Uniform tool-call args block appended to Synthesizer content (SPEC §1.1).
    Multiple captured strings -> numbered list; none -> placeholder."""
    head = "\n\n[Tool call] Synthesize_Recommendation(arguments):"
    if not arg_strings:
        return head + " " + NO_ARGS_PLACEHOLDER
    if len(arg_strings) == 1:
        return head + " " + arg_strings[0]
    return head + "".join(
        "\n({}) {}".format(i, a) for i, a in enumerate(arg_strings, 1)
    )


def judge_visible_payload(case):
    """Reconstruct EXACTLY what the vendored methods put in front of the LLM:
    question + ground_truth + '\\n'.join(f'{name}: {content}') over history
    (whowhen utils.py all_at_once L61-73; step_by_step/binary_search/a2p build the
    same chat_content from the same three keys — see check 0). `_provenance` is
    NOT part of it. All leak scans run on this string, never on the raw file, so
    they measure what the judge can actually see."""
    chat = "\n".join("%s: %s" % (e.get("name", "Unknown Agent"), e.get("content", ""))
                     for e in case.get("history", []))
    return "%s\n%s\n%s" % (case.get("question", ""), case.get("ground_truth", ""), chat)


def vendored_read_keys():
    """check 0: prove the judge-visible contract instead of assuming it. Greps the
    read-only vendored utils.py files for `data.get("<key>"` and returns the set of
    case-JSON keys the methods actually consume. If this ever grows beyond
    {history, question, ground_truth}, every leak argument in this file needs
    re-deriving — so it fails loudly rather than silently. Missing vendored files
    -> (None, []) and the check is reported as SKIPPED, never silently passed."""
    keys, seen = set(), []
    for path in VENDORED_UTILS:
        if not os.path.isfile(path):
            return None, seen
        seen.append(path)
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
        keys |= set(re.findall(r"data\.get\(\s*[\"']([^\"']+)[\"']", src))
        keys |= set(re.findall(r"data\[\s*[\"']([^\"']+)[\"']\s*\]", src))
    return keys, seen


def load_faulted_cases(raw_dir):
    """raw/*.json -> list of (case_id, raw_dict), filtered per SPEC §1.1
    (injected==1 and ledger_status=='injected'), sorted by case_id."""
    cases = []
    for path in glob.glob(os.path.join(raw_dir, "*.json")):
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        row = d["row"]
        if not (row.get("injected") == 1 and row.get("ledger_status") == "injected"):
            continue
        case_id = row.get("run_id") or os.path.splitext(os.path.basename(path))[0]
        cases.append((case_id, d))
    cases.sort(key=lambda t: t[0])   # lexicographic by case_id -> stable numbering
    return cases


def csv_expectations(csv_path):
    """INDEPENDENT expectation source for the self-checks: the dataset CSV, read
    with the same faulted filter. Returns (per-kind counts, sentinel ASIN or None,
    total rows). Two sources (raw/*.json vs CSV) must agree — that is the check,
    instead of a hardcoded literal that silently rots."""
    if not os.path.isfile(csv_path):
        return None, None, 0
    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    faulted = [r for r in rows
               if str(r.get("injected", "")).strip() == "1"
               and str(r.get("ledger_status", "")).strip() == "injected"]
    counts = {}
    for r in faulted:
        k = str(r.get("kind", "")).strip()
        counts[k] = counts.get(k, 0) + 1
    # sentinel = the ASIN the wrong_item_pick rows were forced to, flagged by the
    # dataset's own response_asin_is_sentinel column (v1 B00000FAULT / v2 B00EKWZK5E)
    sentinels = {str(r.get("response_asin", "")).strip() for r in faulted
                 if str(r.get("kind", "")).strip() == "wrong_item_pick"
                 and str(r.get("response_asin_is_sentinel", "")).strip() == "1"}
    sentinel = sentinels.pop() if len(sentinels) == 1 else None
    return counts, sentinel, len(rows)


def build_one(case_id, d, spans_dir):
    """One raw case -> (whowhen_json, meta). meta carries self-check facts."""
    row = d["row"]
    conv = d["resp"]["conversation"]
    mistake_agent = str(row["root_cause_set"]).strip()   # single-root by design
    assert mistake_agent in AGENT_NAMES, (
        "root_cause_set not a canonical agent: %r (case %s)" % (mistake_agent, case_id))

    # tool-call raw args from per-combo spans file, filtered by this trace_id (SPEC §1.1)
    span_file = os.path.join(spans_dir, row["scenario_id"] + ".jsonl")
    spans = injector_smoke.read_spans(span_file, row["trace_id"])
    arg_strings = injector_smoke.synthesizer_toolcall_arg_strings(spans)

    history = []
    for agent in AGENT_NAMES:                             # fixed execution order
        content = conv.get(conv_key_of(agent), "")
        if agent == "Recommendation_Synthesizer":         # uniform args block, all cases
            content = content + toolcall_args_block(arg_strings)
        history.append({"name": agent, "content": content})

    case = {
        "question": QUESTION,
        "ground_truth": GROUND_TRUTH,
        "history": history,
        "mistake_agent": mistake_agent,
        "mistake_step": AGENT_NAMES.index(mistake_agent),  # 0-based (SPEC §1.1)
        "_provenance": {
            "case_id": case_id,
            "kind": row["kind"],
            "format_subtype": row.get("format_subtype", ""),
            "trace_id": row["trace_id"],
        },
    }
    meta = {
        "case_id": case_id,
        "kind": row["kind"],
        "format_subtype": row.get("format_subtype", ""),
        "mistake_agent": mistake_agent,
        "divergent_needle": row.get("divergent_needle", "") or "",
        # context_drift bookkeeping — kept in META ONLY (never written into the
        # case JSON); used exclusively by the anti-leakage checks 8c/8d/8e.
        "ctx_dropped_agent": str(row.get("context_drift_dropped_agent", "") or ""),
        "ctx_dropped_chars": str(row.get("context_drift_dropped_chars", "") or ""),
        "ctx_outcome": str(row.get("context_drift_outcome", "") or ""),
        "n_args": len(arg_strings),
        "arg_strings": arg_strings,
        "history": history,
    }
    return case, meta


def _parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="agentfault raw -> Who&When case JSON adapter (SPEC §1.1, offline). "
                    "Defaults reproduce the v1 64-case build exactly.")
    ap.add_argument("--dataset-dir", default=None,
                    help="agentfault tree holding raw/, spans/ and dataset_agentfault.csv; "
                         "cases are written to <dir>/whowhen/. "
                         "Default = (archived) agentfault (v1). v2: (archived) agentfault_v2")
    ap.add_argument("--raw-dir", default=None, help="explicit raw/ dir (overrides --dataset-dir)")
    ap.add_argument("--spans-dir", default=None, help="explicit spans/ dir (overrides --dataset-dir)")
    ap.add_argument("--csv", default=None,
                    help="explicit dataset_agentfault.csv (independent expectation source "
                         "for the self-checks; overrides --dataset-dir)")
    ap.add_argument("--out-root", default=None,
                    help="explicit whowhen output root, i.e. where cases/ and "
                         "cases_index.json go (overrides --dataset-dir). Use a temp dir "
                         "to verify a build without touching committed outputs.")
    return ap.parse_args(argv)


def _abs(p):
    return p if os.path.isabs(p) else os.path.join(REPO, p)


def main(argv=None):
    args = _parse_args(argv)
    base = _abs(args.dataset_dir) if args.dataset_dir else DEFAULT_DATASET_DIR
    raw_dir = _abs(args.raw_dir) if args.raw_dir else os.path.join(base, "raw")
    spans_dir = _abs(args.spans_dir) if args.spans_dir else os.path.join(base, "spans")
    csv_path = _abs(args.csv) if args.csv else os.path.join(base, "dataset_agentfault.csv")
    out_root = _abs(args.out_root) if args.out_root else os.path.join(base, "whowhen")
    cases_dir = os.path.join(out_root, "cases")

    if not os.path.isdir(raw_dir):
        print("ERROR: raw dir not found: %s" % raw_dir)
        return 1
    os.makedirs(cases_dir, exist_ok=True)

    # Fix #6: rebuilds must not leave stale artifacts behind. If the raw set
    # shrinks/renames, an old case_NNN.json would otherwise survive and be
    # silently consumed downstream (run_whowhen globs cases/*.json). Delete
    # cases/*.json and the old cases_index.json before writing anything.
    stale = glob.glob(os.path.join(cases_dir, "*.json"))
    for path in stale:
        os.remove(path)
    old_idx = os.path.join(out_root, "cases_index.json")
    if os.path.exists(old_idx):
        os.remove(old_idx)
    if stale:
        print("[clean] removed %d stale case json(s) from %s" % (len(stale), cases_dir))

    cases = load_faulted_cases(raw_dir)
    if not cases:
        print("ERROR: no faulted cases found in %s" % raw_dir)
        return 1
    # derived once, used by check1/check5 AND recorded in cases_index for the scorer
    exp_counts, sentinel, csv_rows = csv_expectations(csv_path)

    index = {}
    metas = []
    for i, (case_id, d) in enumerate(cases, 1):
        case, meta = build_one(case_id, d, spans_dir)
        fname = "case_%03d.json" % i
        with open(os.path.join(cases_dir, fname), "w", encoding="utf-8") as f:
            json.dump(case, f, ensure_ascii=False, indent=2)
        index[fname] = {
            "case_id": case_id,
            "kind": meta["kind"],
            "format_subtype": meta["format_subtype"],
            "mistake_agent": meta["mistake_agent"],
        }
        meta["fname"] = fname
        meta["case"] = case
        metas.append(meta)

    # Fix #6: build fingerprint = sha256 of the sorted case_id list (first 12 hex)
    # + generation time, written as a top-level "_build" field so scorer reports
    # can pin exactly which build they scored. The fingerprint is stable across
    # rebuilds of the same raw set; generated_at is informational only.
    fingerprint = hashlib.sha256(
        "\n".join(sorted(cid for cid, _ in cases)).encode("utf-8")).hexdigest()[:12]
    index_out = {"_build": {
        "fingerprint": fingerprint,
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "n_cases": len(cases),
        # recorded so score_whowhen.py can state the ACTUAL sentinel in its
        # honesty footnote instead of hardcoding v1's "B00000FAULT" (whose
        # literal 'FAULT' substring made that family an upper bound; v2's
        # catalogue-shaped B00EKWZK5E does not carry that giveaway).
        "wrongpick_sentinel": sentinel or "",
    }}
    index_out.update(index)
    with open(os.path.join(out_root, "cases_index.json"), "w", encoding="utf-8") as f:
        json.dump(index_out, f, ensure_ascii=False, indent=2)
    print("[build] fingerprint=%s (sha256 of sorted case_id list, first 12 hex)" % fingerprint)

    # ---------------- build self-checks (SPEC §1.1 + v2 anti-leakage) ----------------
    print("=== make_whowhen_cases build report ===")
    print("dataset dir: %s" % base)
    print("raw dir    : %s" % raw_dir)
    print("spans dir  : %s" % spans_dir)
    print("cases dir  : %s" % cases_dir)

    n = len(metas)
    kind_counts = {}
    for m in metas:
        kind_counts[m["kind"]] = kind_counts.get(m["kind"], 0) + 1

    # (0) judge-visible contract, proven against the vendored sources
    vkeys, vfiles = vendored_read_keys()
    if vkeys is None:
        print("[check0] SKIPPED: vendored utils.py not found (%s) — judge-visible key "
              "contract unverified this run." % ", ".join(VENDORED_UTILS))
    else:
        extra = vkeys - {"history", "question", "ground_truth"}
        print("[check0] vendored methods read case-JSON keys %s from %d file(s) "
              "(expect exactly {question, ground_truth, history}; _provenance is NOT "
              "judge-visible)" % (sorted(vkeys), len(vfiles)))
        assert not extra, (
            "vendored methods now read case-JSON key(s) %s beyond "
            "{question, ground_truth, history} — every anti-leak argument in this "
            "adapter (notably that _provenance never reaches the LLM) must be "
            "re-derived before trusting this build" % sorted(extra))

    # (1) counts — derived from raw, cross-checked against the CSV (never hardcoded)
    if exp_counts is None:
        print("[check1] case count = %d; per-family = %s  (CSV %s absent -> "
              "cross-check SKIPPED)" % (n, kind_counts, csv_path))
    else:
        exp_total = sum(exp_counts.values())
        print("[check1] case count = %d (CSV-derived expectation %d, from %d CSV rows); "
              "per-family = %s (CSV-derived %s)"
              % (n, exp_total, csv_rows, kind_counts, exp_counts))
        assert n == exp_total, (
            "built %d cases but the CSV's faulted filter yields %d — raw/ and CSV disagree"
            % (n, exp_total))
        assert kind_counts == exp_counts, (
            "per-family counts disagree between raw/ (%s) and CSV (%s)"
            % (kind_counts, exp_counts))

    # (2) question / ground_truth byte-identical
    qs = {m["case"]["question"] for m in metas}
    gts = {m["case"]["ground_truth"] for m in metas}
    assert len(qs) == 1 and len(gts) == 1, "question/ground_truth not uniform"
    print("[check2] distinct question strings = %d, distinct ground_truth strings = %d (both must be 1)"
          % (len(qs), len(gts)))

    # (3) history shape/order
    for m in metas:
        h = m["case"]["history"]
        assert len(h) == 4, "%s: history len != 4" % m["fname"]
        assert [e["name"] for e in h] == AGENT_NAMES, "%s: history order != AGENT_NAMES" % m["fname"]
    print("[check3] all %d cases: history has exactly 4 entries in AGENT_NAMES order (assert PASS)" % n)

    # (4) hallucinate needle present in mistake_agent content
    hallu = [m for m in metas if m["kind"] == "hallucinate"]
    empty_needle = [m["fname"] for m in hallu if not m["divergent_needle"]]
    hit = 0
    miss = []
    for m in hallu:
        if not m["divergent_needle"]:
            continue
        content = next(e["content"] for e in m["case"]["history"]
                       if e["name"] == m["mistake_agent"])
        if m["divergent_needle"] in content:
            hit += 1
        else:
            miss.append(m["fname"])
    print("[check4] hallucinate needle in mistake_agent content: %d/%d; "
          "empty needle: %d %s; missing: %s"
          % (hit, len(hallu), len(empty_needle), empty_needle, miss))

    # (5) wrongpick sentinel in Synthesizer content (sentinel DERIVED from the CSV)
    wp = [m for m in metas if m["kind"] == "wrong_item_pick"]
    if sentinel is None:
        print("[check5] SKIPPED: sentinel ASIN not derivable from CSV "
              "(need wrong_item_pick rows with response_asin_is_sentinel==1)")
    else:
        wp_hit = 0
        for m in wp:
            synth = next(e["content"] for e in m["case"]["history"]
                         if e["name"] == "Recommendation_Synthesizer")
            if sentinel in synth:
                wp_hit += 1
        print("[check5] wrong_item_pick sentinel %s (derived from CSV) in Synthesizer "
              "content: %d/%d (expect %d/%d)" % (sentinel, wp_hit, len(wp), len(wp), len(wp)))

    # (6) malformed_json cases: captured raw args string must NOT json.loads
    mal = [m for m in metas if m["format_subtype"] == "malformed_json"]
    mal_bad = 0
    for m in mal:
        def _unparsable(s):
            try:
                json.loads(s)
                return False
            except Exception:
                return True
        if m["arg_strings"] and any(_unparsable(a) for a in m["arg_strings"]):
            mal_bad += 1
    print("[check6] malformed_json cases with non-json.loads-able args: %d/%d (expect %d/%d)"
          % (mal_bad, len(mal), len(mal), len(mal)))

    # (7) tool-call args capture rate + leak concentration check
    with_args = [m for m in metas if m["n_args"] > 0]
    without = [m for m in metas if m["n_args"] == 0]
    wo_by_kind = {}
    for m in without:
        wo_by_kind[m["kind"]] = wo_by_kind.get(m["kind"], 0) + 1
    print("[check7] tool-call args captured: with-args=%d, without-args=%d (rate %.1f%%); "
          "without-by-kind=%s; multi-args cases=%d"
          % (len(with_args), len(without), 100.0 * len(with_args) / n,
             wo_by_kind, sum(1 for m in metas if m["n_args"] > 1)))
    # Fix #7: skew trigger, not only the exclusive (single-kind) case. If >=75%
    # of the without-args cases fall into one kind, the placeholder is already a
    # strong fault-family signal even though it is not exclusive.
    if without:
        dom_kind, dom_n = max(wo_by_kind.items(), key=lambda kv: kv[1])
        dom_ratio = dom_n / float(len(without))
        if dom_ratio >= 0.75:
            print("!!! LEAK WARNING: %d/%d (%.0f%%) of without-args cases concentrate "
                  "in kind=%s (threshold 75%%) -> the placeholder itself becomes a "
                  "fault-family signal. STOP and report."
                  % (dom_n, len(without), 100.0 * dom_ratio, dom_kind))
            return 2

    # ---------------- (8) context_drift anti-leakage (NEW, v2) ----------------
    # Strategy A: a context_drift case must be INDISTINGUISHABLE, to the judge,
    # from any other case except through the conversation content itself. Four
    # independent guards, all hard asserts.
    ctx = [m for m in metas if m["kind"] == "context_drift"]
    print("[check8] context_drift anti-leakage (%d context_drift case(s) of %d)"
          % (len(ctx), n))

    # 8a: structural uniformity — key sets identical across EVERY case/family, so
    # shape alone can never single out a context_drift case.
    top_shapes = {tuple(m["case"].keys()) for m in metas}
    prov_shapes = {tuple(m["case"]["_provenance"].keys()) for m in metas}
    hist_shapes = {tuple(e.keys()) for m in metas for e in m["case"]["history"]}
    assert top_shapes == {CASE_TOP_KEYS}, (
        "case top-level key sets are not uniform/expected: %s" % sorted(top_shapes))
    assert prov_shapes == {PROVENANCE_KEYS}, (
        "_provenance key sets are not uniform/expected: %s" % sorted(prov_shapes))
    assert hist_shapes == {HISTORY_ENTRY_KEYS}, (
        "history entry key sets are not uniform/expected: %s" % sorted(hist_shapes))
    print("  [8a] structural uniformity: 1 top-level shape %s, 1 _provenance shape %s, "
          "1 history-entry shape %s across all %d cases (assert PASS)"
          % (list(CASE_TOP_KEYS), list(PROVENANCE_KEYS), list(HISTORY_ENTRY_KEYS), n))

    # 8b: no injection-metadata field name anywhere in the serialized case JSON
    # (context_drift_dropped_agent / _dropped_chars / _outcome / carrier_seq_id ...).
    meta_hits = []
    for m in metas:
        blob = json.dumps(m["case"], ensure_ascii=False)
        for field in FORBIDDEN_META_FIELDS:
            if field in blob:
                meta_hits.append((m["fname"], m["kind"], field))
    assert not meta_hits, (
        "injection-metadata field name(s) leaked into case JSON: %s" % meta_hits[:10])
    print("  [8b] none of the %d forbidden injection-metadata field names appear in any "
          "case JSON (incl. context_drift_dropped_agent/_dropped_chars/_outcome, "
          "carrier_seq_id) (assert PASS)" % len(FORBIDDEN_META_FIELDS))

    # 8c: leak vocabulary absent from the JUDGE-VISIBLE payload. NOTE on the
    # dropped-agent NAME: it is deliberately NOT scanned as a bare string, because
    # all four canonical agent names appear in EVERY case (history[i].name and the
    # question) — uniformly, hence carrying zero information. What must be absent
    # is any statement ABOUT a deletion; that is what LEAK_WORDS tests.
    word_hits = []
    per_family_hits = {}
    for m in metas:
        payload = judge_visible_payload(m["case"]).lower()
        for w in LEAK_WORDS:
            if w in payload:
                word_hits.append((m["fname"], m["kind"], w))
                per_family_hits[m["kind"]] = per_family_hits.get(m["kind"], 0) + 1
    assert not word_hits, (
        "LEAK-VOCABULARY HIT in the judge-visible payload: %s ... (first 10 shown). "
        "This may be an innocent natural-language utterance by an agent rather than a "
        "harness leak — inspect the named case(s) and adjudicate manually before "
        "trusting this build." % word_hits[:10])
    print("  [8c] leak vocabulary %s: 0 occurrences in the judge-visible payload "
          "(question + ground_truth + history) of all %d cases (assert PASS)"
          % (list(LEAK_WORDS), n))

    # 8d: positive evidence of judge blindness — for every context_drift case all
    # four agent OUTPUTS are present and non-empty. The deletion lives in the
    # downstream agent's INPUT, so nothing in the transcript is visibly wrong.
    if ctx:
        empty_out = [(m["fname"], e["name"]) for m in ctx for e in m["case"]["history"]
                     if not str(e["content"]).strip()]
        assert not empty_out, (
            "context_drift case(s) with an EMPTY agent output: %s — that would be a "
            "visible symptom and would break the 'silent fault' claim" % empty_out[:10])
        lens = [len(e["content"]) for m in ctx for e in m["case"]["history"]]
        print("  [8d] all %d context_drift cases: 4/4 agent outputs present and non-empty "
              "(content len min=%d max=%d) -> no visible symptom for an output-reading "
              "judge; a LOW score on this family is the EXPECTED, INTENDED result "
              "(assert PASS)" % (len(ctx), min(lens), max(lens)))

        # 8e: GT semantics — the root is the DOWNSTREAM target whose input was
        # stripped, i.e. index(dropped_agent) + 1 == index(root), never the
        # upstream agent whose message was dropped.
        bad = []
        pairs = {}
        for m in ctx:
            dropped, root = m["ctx_dropped_agent"], m["mistake_agent"]
            pairs["%s -> %s" % (dropped or "?", root)] = pairs.get(
                "%s -> %s" % (dropped or "?", root), 0) + 1
            if dropped not in AGENT_NAMES or root == dropped or \
                    AGENT_NAMES.index(dropped) + 1 != AGENT_NAMES.index(root):
                bad.append((m["fname"], dropped, root))
        assert not bad, (
            "context_drift GT is not the immediate DOWNSTREAM target of the dropped "
            "agent: %s" % bad[:10])
        print("  [8e] context_drift GT = downstream target (index(dropped)+1 == "
              "index(root)) for all %d cases; dropped->GT pairs: %s (assert PASS)"
              % (len(ctx), pairs))
    else:
        print("  [8d/8e] no context_drift cases in this dataset (v1) — SKIPPED")

    print("=== build OK: %d cases -> %s ===" % (n, cases_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
