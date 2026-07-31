# -*- coding: utf-8 -*-
"""eval_k8s_supervised -- K8S pilot RCA *supervised group-aware* classification
baseline (port of eval_baseline.py), adapted per WF-1 carry-forward (#1-#5) and
the M4 impl spec section 6.2.

Reads the WF-1 feature view ((native trees) features_k8s.csv = REAL-only
primary, and features_k8s_all.csv = ALL-provenance secondary) and runs group-aware
cross-validation to demonstrate that the telemetry carries classifiable signal and
to expose how small the pilot is (motivates M4b batch collection).

================================================================================
CRITICAL PORT CHANGES vs eval_baseline.py (carry-forward #1-#5, MUST honor):
  #1 (S CRITICAL) build_X here does NOT add `*_isna` indicator columns into X.
     K8S iron rule: per-service `*_isna` is a carrier fingerprint (the non-NA set
     is a deterministic function of case_id / carrier path). eval_baseline.py put
     them in X (lines ~64-65); this port DELETES that. Empty cells -> numeric NaN
     filled with 0 as a plain placeholder, NO indicator columns emitted.
  #2 X_COLS = [c for c in df.columns if c.startswith('svc_') and c.endswith('_p95')]
     (124 cols over 5 observed services). targets = fault_type + root_cause_primary.
     group = df['group_id'] (= fault_type). assert no None/empty/nan in groups.
  #3 SGKF guard: n_splits = min(5, n_groups). distinct fault_type=10(real)/12(all),
     many with 1-2 repeats -> least-populated-class warning is expected; we suppress
     it and degrade gracefully (guard prevents requesting more splits than groups).
  #4 Report REALIZED label domain: root_cause_primary realized = {catalog, host,
     mysql_items_lock} = 3 in REAL-only; =4 in ALL (adds catalog-gw). user is a
     legal candidate but never primary in the pilot (m3d co-primary picks catalog
     as first column). fault_class realized: real=5 distinct (incl pipe-combos);
     the realized implemented subset of the advisor 6-class taxonomy is 4/6
     (RES/NET/LIF/CFG; no runtime/dependency). macro-F1/Top@k computed over the
     classes that actually appear (sklearn classes_ semantics).
  #5 Three CV schemes, reported together honestly:
       - SGKF5 (min(5,n_groups)): primary. each fold stratifies on y, groups stay
         intact. tests "known fault-type family, unseen concrete window".
       - LOGO(group=fault_type): leaves one fault-type out -> held-out type has
         ZERO training samples -> that type's Top@k / F1 = 0. This is a
         DATA-STRUCTURE FLOOR, not a model defect (documented as such).
       - case_id lenient comparison: group=case_id. DENSE, near-leaky upper bound
         because same-case multi-windows (m3d F1_only/F2_only) can split across
         train/test under case_id grouping. Labeled explicitly as a leaky upper
         bound, NOT a fair evaluation.
  #6 Not applicable here (that is the U/ranking port); this script is S only.
  Both: do NOT git commit (main loop commits). ASCII-only python prints (Windows
  GBK). origin/main untouched.

Determinism: fixed SEEDS list, no unseeded randomness. SGKF uses random_state=0
for the (single) split generation per target; per-seed randomness only enters via
the estimator's random_state.

Usage (Windows, conda env recweb2):
    PYTHONIOENCODING=utf-8 python3 \
        scripts/chaos/ctk/eval_k8s_supervised.py
    # ALL-provenance secondary only:
    ... eval_k8s_supervised.py --csv all
"""
import argparse
import io
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.exceptions import UndefinedMetricWarning
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import LeaveOneGroupOut, StratifiedGroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# ---- determinism ----
SEEDS = [0, 1, 2, 3, 4]   # >=5 seeds
SGKF_K = 5                # requested k; guarded to min(5, n_groups) at runtime

# ---- paths ----
# ★ 2026-07-13: 数据根取自 dataset_registry(datasets/REGISTRY.json = 唯一真相源),
#   不再自己拼路径。--pilot-dir 参数保留。
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
import dataset_registry as DR  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
# ★ 2026-07-13 事故修复:本脚本【不再有】任何默认指向 native 的路径。
#   曾经 PILOT = DR.NATIVE_ROOT 既是输入根又是输出根,配上 finally 里的无条件写盘,
#   一次 --help 就把 usage 文本写进了 (native trees) BASELINE_RESULTS_supervised.txt。
#   现在:输入 CSV 走 DR.feature_csv()(找不到就炸),输出走 DR.runtime_dir("scores")。


# ============================================================
# feature / label construction (carry-forward #1, #2)
# ============================================================
def na(v) -> bool:
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def x_columns(df: pd.DataFrame):
    """X = all svc_<service>__<metric>_p95 columns. Carrier-fingerprint *_isna
    columns are NEVER added to X (K8S iron rule, carry-forward #1)."""
    cols = [c for c in df.columns if c.startswith("svc_") and c.endswith("_p95")]
    return cols


def build_X(df: pd.DataFrame, x_cols):
    """Numeric cast of the svc_*_p95 columns. Empty -> NaN -> filled with 0.0 as a
    plain placeholder. NO *_isna indicator columns (carry-forward #1: those are a
    carrier fingerprint and are forbidden in X).

    NOTE: 0.0 placeholder is honest -- with no indicator the model cannot recover
    'was this missing'; the missingness signal is intentionally discarded to avoid
    the carrier-fingerprint leak. This is the conservative, leak-safe choice.
    """
    raw = df[x_cols].copy()
    feats = {}
    for c in x_cols:
        as_str = raw[c].astype(str).str.strip()
        isna = (as_str == "") | (as_str.str.lower() == "nan")
        num = pd.to_numeric(as_str.where(~isna, other=np.nan), errors="coerce")
        feats[c] = num.fillna(0.0).astype(float)
        # deliberately NOT adding feats[c + "_isna"] here (carry-forward #1)
    return pd.DataFrame(feats, index=df.index)


def fault_class_primary(token):
    """fault_class is a pipe-delimited combo for multi-root cases
    (e.g. 'configuration|network'). For a single-label classification target we
    take the FIRST token (deterministic). The full multi-label combo is reported
    separately as honest context, but single-label macro-F1 needs one y."""
    if na(token):
        return token
    return token.split("|")[0].strip()


def make_models():
    """{name: (factory(seed), supports_proba)}. LR is marked sample-limited.

    RandomForest uses n_estimators=100 (not the eval_baseline 300): the K8S pilot
    has only 21 (real) / 39 (all) rows, so 100 trees is already ample and actually
    better-generalizing than 300 on this tiny N; determinism is preserved via the
    fixed random_state=s (the tree count is not a determinism lever). It also keeps
    the 27-fold case_id LOGO x 5-seed x 3-target sweep tractable."""
    return {
        "Dummy(most_frequent)": (lambda s: DummyClassifier(strategy="most_frequent"), False),
        "RandomForest": (lambda s: RandomForestClassifier(
            n_estimators=100, random_state=s, n_jobs=1), True),
        # LR with StandardScaler. max_iter high; sample-limited -> reference only.
        "LogReg(scaled)": (lambda s: Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=5000, random_state=s)),
        ]), True),
    }


# ============================================================
# metrics (global unweighted macro-F1 over realized classes)
# ============================================================
def macro_f1(y_true, y_pred):
    """macro-F1, global unweighted, over the union of realized labels in y_true/y_pred.
    Equivalent to sklearn's f1_score(average='macro') over seen classes (no
    reliance on a fixed labels= order)."""
    labels = sorted(set(list(y_true)) | set(list(y_pred)))
    f1s = []
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    for lab in labels:
        tp = int(np.sum((yp == lab) & (yt == lab)))
        fp = int(np.sum((yp == lab) & (yt != lab)))
        fn = int(np.sum((yp != lab) & (yt == lab)))
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
        f1s.append(f1)
    return float(np.mean(f1s)) if f1s else 0.0


def accuracy(y_true, y_pred):
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    return float(np.mean(yt == yp))


# ============================================================
# CV runners (out-of-fold aggregation)
# ============================================================
def run_cv_classify(X, y, splitter, model_factory, seed):
    """Classification CV -> (accuracy, macroF1) over OOF predictions."""
    y = np.asarray(y)
    oof = np.empty(len(y), dtype=object)
    for tr, te in splitter:
        if len(set(y[tr])) < 2:
            # N=12 degenerate fold: single-class training (e.g. LOGO over fault_type leaves
            # only 'resource' when pod_failure is the held-out 'lifecycle' group). sklearn
            # LogReg/RF raise on 1-class fit -> predict the lone class for honest (poor)
            # group-CV score instead of crashing. (case_id-leaky CV with 12 groups never
            # hits this; only the grouped fault_type/fault_class CV does.)
            oof[te] = y[tr][0]
            continue
        est = model_factory(seed)
        est.fit(X.iloc[tr], y[tr])
        oof[te] = est.predict(X.iloc[te])
    return accuracy(y, oof), macro_f1(y, oof)


def run_cv_topk(X, y, splitter, model_factory, seed, supports_proba):
    """Localization CV -> (Top@1, Top@3). Top@1 = hard-pred accuracy. Top@3 needs
    predict_proba (Dummy(most_frequent) has no proba -> Top@3 degrades to Top@1,
    same convention as eval_baseline.py:279)."""
    y = np.asarray(y)
    n = len(y)
    oof = np.empty(n, dtype=object)
    top3_hits = np.zeros(n, dtype=float)
    has_top3 = np.zeros(n, dtype=bool)
    for tr, te in splitter:
        if len(set(y[tr])) < 2:
            # N=12 degenerate fold (single-class training): predict lone class, Top@3
            # degrades to Top@1 for these rows. Honest poor score, no sklearn crash.
            oof[te] = y[tr][0]
            continue
        est = model_factory(seed)
        est.fit(X.iloc[tr], y[tr])
        oof[te] = est.predict(X.iloc[te])
        if supports_proba and hasattr(est, "predict_proba"):
            proba = est.predict_proba(X.iloc[te])
            classes = list(est.classes_)
            pos = {c: i for i, c in enumerate(classes)}
            order = np.argsort(-proba, axis=1)[:, :3]
            for li, gi in enumerate(te):
                has_top3[gi] = True
                yt = y[gi]
                if yt in pos and yt in {classes[j] for j in order[li]}:
                    top3_hits[gi] = 1.0
    top1 = accuracy(y, oof)
    if has_top3.any():
        top3 = float(np.mean(top3_hits[has_top3]))
    else:
        top3 = top1  # no-proba model: Top@3 degrades to hard hit
    return top1, top3


def aggregate(vals):
    a = np.asarray(vals, dtype=float)
    return float(a.mean()), float(a.std(ddof=0))


def fmt(mean, std):
    return f"{mean:.3f}+/-{std:.3f}"


# ============================================================
# split construction (carry-forward #3 SGKF guard; carry-forward #5 three schemes)
# ============================================================
def logo_splits(X, y, groups):
    """LeaveOneGroupOut over group=fault_type. Independent of y content."""
    logo = LeaveOneGroupOut()
    return list(logo.split(X, y, groups))


def sgkf_splits(X, y, groups, n_groups):
    """StratifiedGroupKFold, k = min(SGKF_K, n_groups). Guarded: never request more
    splits than groups. Suppresses the 'least populated class' warning (expected:
    many fault_types have 1-2 repeats in this small pilot).

    Returns (splits, actual_k) where splits is a list of (train, test) index arrays
    or None if SGKF is infeasible even at k=2; actual_k is the k that succeeded
    (None when splits is None). Combined/multi never raise -> actual_k == SGKF_K
    and the output is byte-identical to before this hardening.

    Robustness: sklearn's StratifiedGroupKFold feasibility check (n_splits vs the
    per-class member count, modulated by group structure) can raise ValueError even
    when k <= n_groups. When that happens we degrade gracefully by reducing k one
    step at a time (k-1) and retrying until k=2. If still infeasible at k=2 we
    return (None, None) and the caller skips the SGKF row. This ValueError path
    only triggers on small/awkward y distributions (e.g. single dataset: 6
    fault_types x 3 members each)."""
    k = min(SGKF_K, n_groups)
    last_err = None
    while k >= 2:
        sgkf = StratifiedGroupKFold(n_splits=k, shuffle=True, random_state=0)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return list(sgkf.split(X, y, groups)), k
        except ValueError as e:
            last_err = e
            # sklearn deems this k infeasible for this y/group structure: step down.
            print(f"[SGKF] requested k={k} infeasible (sklearn ValueError: {e}); "
                  f"reducing to k={k-1}")
            k -= 1
    print(f"[SGKF] infeasible even at k=2 for this y (last error: {last_err}); "
          f"SGKF skipped (report LOGO/case_id instead).")
    return None, None


def logo_caseid_splits(X, y, case_groups):
    """case_id lenient comparison: LeaveOneGroupOut over group=case_id. This is a
    DENSE / near-leaky upper bound: same-case multi-windows (m3d F1_only/F2_only)
    can land on opposite sides of train/test. Reported as a leaky reference, NOT a
    fair evaluation (carry-forward #5)."""
    logo = LeaveOneGroupOut()
    return list(logo.split(X, y, case_groups))


# ============================================================
# main eval driver
# ============================================================
def evaluate_csv(df, tag):
    """Run all CV schemes x targets x models on one DataFrame. Returns results dict
    and prints a summary."""
    x_cols = x_columns(df)
    X = build_X(df, x_cols)

    # ---- labels ----
    y_ft = df["fault_type"].to_numpy()                 # group-aligned, multi-class
    y_fc = np.array([fault_class_primary(t) for t in df["fault_class"].to_numpy()])
    y_rc = df["root_cause_primary"].to_numpy()

    # ---- groups ----
    groups = df["group_id"].to_numpy()                 # = fault_type
    case_groups = df["case_id"].to_numpy()             # for lenient comparison
    # carry-forward #2: assert no None/empty/nan in groups
    bad = [g for g in groups if na(g)]
    assert not bad, f"empty/nan group_id found ({len(bad)} rows); SGKF would raise"
    assert (groups == df["fault_type"].to_numpy()).all(), "group_id must equal fault_type"

    n_rows = len(df)
    n_groups = len(set(groups))
    n_cases = len(set(case_groups))
    n_ft = len(set(y_ft))
    n_fc = len(set(y_fc))
    n_rc = len(set(y_rc))

    print(f"\n########## {tag} ##########")
    print(f"rows={n_rows}  svc_*_p95 X cols={len(x_cols)}  (no *_isna in X)")
    print(f"fault_type realized classes = {n_ft} : {sorted(set(y_ft))}")
    print(f"fault_class(primary-token) realized = {n_fc} : {sorted(set(y_fc))}")
    print(f"root_cause_primary realized = {n_rc} : {sorted(set(y_rc))}")
    print(f"groups (group_id=fault_type) = {n_groups} ; distinct case_id = {n_cases}")
    # provenance + per-group supports (honesty context)
    prov = df["provenance"].value_counts().to_dict() if "provenance" in df.columns else {}
    print(f"provenance: {prov}")
    rc_grp = df.groupby("root_cause_primary")["group_id"].nunique().to_dict()
    print("root_cause_primary per-class group support: "
          + ", ".join(f"{k}={v}" for k, v in sorted(rc_grp.items(), key=lambda kv: -kv[1])))
    ft_grp = df.groupby("fault_type")["group_id"].nunique().to_dict()
    print("fault_type per-class group support: "
          + ", ".join(f"{k}={v}" for k, v in sorted(ft_grp.items(), key=lambda kv: -kv[1])))

    models = make_models()

    # ---- pre-build splits per target/scheme ----
    # classification target = fault_type (also runs fault_class below)
    # sgkf_* may be None when SGKF is infeasible even at k=2 (small/awkward y);
    # the CV loops then mark the SGKF row as infeasible instead of running models.
    # actual k per target is surfaced in the printed scheme label (e.g. SGKF3 when
    # k was reduced from 5 to 3) so the reader knows the realized fold count.
    sgkf_ft, sgkf_ft_k = sgkf_splits(X, y_ft, groups, n_groups)
    logo_ft = logo_splits(X, y_ft, groups)
    case_ft = logo_caseid_splits(X, y_ft, case_groups)

    sgkf_fc, sgkf_fc_k = sgkf_splits(X, y_fc, groups, n_groups)
    logo_fc = logo_splits(X, y_fc, groups)
    case_fc = logo_caseid_splits(X, y_fc, case_groups)

    # localization target = root_cause_primary
    sgkf_rc, sgkf_rc_k = sgkf_splits(X, y_rc, groups, n_groups)
    logo_rc = logo_splits(X, y_rc, groups)
    case_rc = logo_caseid_splits(X, y_rc, case_groups)

    # scheme label reflects the realized SGKF k (per-target, since y_ft/y_fc/y_rc
    # can each have different feasibility). When no reduction happened the label is
    # the historical "SGKF5" so combined/multi output stays byte-identical.
    def _sgkf_label(actual_k):
        if actual_k is None:
            return "SGKF(n/a)"
        return "SGKF5" if actual_k == SGKF_K else f"SGKF{actual_k}"

    sgkf_ft_lbl = _sgkf_label(sgkf_ft_k)
    sgkf_fc_lbl = _sgkf_label(sgkf_fc_k)
    sgkf_rc_lbl = _sgkf_label(sgkf_rc_k)

    schemes_classify = {
        sgkf_ft_lbl: sgkf_ft, "LOGO(ft)": logo_ft, "case_id(leaky)": case_ft,
    }
    schemes_classify_fc = {
        sgkf_fc_lbl: sgkf_fc, "LOGO(ft)": logo_fc, "case_id(leaky)": case_fc,
    }
    schemes_locate = {
        sgkf_rc_lbl: sgkf_rc, "LOGO(ft)": logo_rc, "case_id(leaky)": case_rc,
    }

    # results[tag][task][scheme][model] = {metric:(mean,std)}
    out = {"fault_type": {}, "fault_class": {}, "rootcause": {}}

    for mname, (factory, sup) in models.items():
        # ---- y1a: fault_type classification ----
        for sch, splits in schemes_classify.items():
            if splits is None:
                # SGKF infeasible for y_ft: mark the row unavailable, skip scoring.
                # _infeasible is set both on the per-model dict (so _fmt_cell can
                # detect it) and on the scheme dict (so _print_interpretation can).
                sch_d = out["fault_type"].setdefault(sch, {"_infeasible": True})
                sch_d["_infeasible"] = True
                sch_d[mname] = {
                    "_infeasible": True,
                    "acc": (float("nan"), float("nan")),
                    "macroF1": (float("nan"), float("nan")),
                }
                continue
            accs, f1s = [], []
            for s in SEEDS:
                a, f = run_cv_classify(X, y_ft, splits, factory, s)
                accs.append(a); f1s.append(f)
            out["fault_type"].setdefault(sch, {})[mname] = {
                "acc": aggregate(accs), "macroF1": aggregate(f1s),
            }
        # ---- y1b: fault_class(primary token) classification ----
        for sch, splits in schemes_classify_fc.items():
            if splits is None:
                sch_d = out["fault_class"].setdefault(sch, {"_infeasible": True})
                sch_d["_infeasible"] = True
                sch_d[mname] = {
                    "_infeasible": True,
                    "acc": (float("nan"), float("nan")),
                    "macroF1": (float("nan"), float("nan")),
                }
                continue
            accs, f1s = [], []
            for s in SEEDS:
                a, f = run_cv_classify(X, y_fc, splits, factory, s)
                accs.append(a); f1s.append(f)
            out["fault_class"].setdefault(sch, {})[mname] = {
                "acc": aggregate(accs), "macroF1": aggregate(f1s),
            }
        # ---- y2: root_cause_primary localization Top@1/Top@3 ----
        for sch, splits in schemes_locate.items():
            if splits is None:
                sch_d = out["rootcause"].setdefault(sch, {"_infeasible": True})
                sch_d["_infeasible"] = True
                sch_d[mname] = {
                    "_infeasible": True,
                    "top1": (float("nan"), float("nan")),
                    "top3": (float("nan"), float("nan")),
                }
                continue
            t1s, t3s = [], []
            for s in SEEDS:
                t1, t3 = run_cv_topk(X, y_rc, splits, factory, s, sup)
                t1s.append(t1); t3s.append(t3)
            out["rootcause"].setdefault(sch, {})[mname] = {
                "top1": aggregate(t1s), "top3": aggregate(t3s),
            }

    _print_tables(out, models)
    _print_interpretation(out, models, n_rows, n_groups, n_cases,
                          y_ft, y_rc, tag, prov)
    return out


def _print_tables(out, models):
    # scheme keys come from insertion order in out (SGKF first, then LOGO, then
    # case_id). When SGKF k was reduced the key is e.g. "SGKF3"; when infeasible
    # it is "SGKF(n/a)". Iterating out[*] keeps the historical 3-row layout for
    # combined/multi (SGKF5 -> identical output).
    def _fmt_cell(d, keys):
        # d is the per-model dict; keys is (acc_key, other_key) or (top1_key, top3_key)
        if d.get("_infeasible"):
            return f"{'infeasible':17s} {'infeasible':17s}"
        a, b = keys
        return f"{fmt(*d[a]):17s} {fmt(*d[b]):17s}"

    print("\n=== y1: fault_type classification (acc / macroF1) ===")
    print(f"  {'scheme':18s} {'model':22s} {'acc':17s} {'macroF1':17s}")
    for sch in out["fault_type"]:
        for m in models:
            d = out["fault_type"][sch][m]
            print(f"  {sch:18s} {m:22s} "
                  f"{_fmt_cell(d, ('acc', 'macroF1'))}")

    print("\n=== y1b: fault_class (primary token) classification (acc / macroF1) ===")
    print(f"  {'scheme':18s} {'model':22s} {'acc':17s} {'macroF1':17s}")
    for sch in out["fault_class"]:
        for m in models:
            d = out["fault_class"][sch][m]
            print(f"  {sch:18s} {m:22s} "
                  f"{_fmt_cell(d, ('acc', 'macroF1'))}")

    print("\n=== y2: root_cause_primary localization (Top@1 / Top@3) ===")
    print(f"  {'scheme':18s} {'model':22s} {'Top@1':17s} {'Top@3':17s}")
    for sch in out["rootcause"]:
        for m in models:
            d = out["rootcause"][sch][m]
            print(f"  {sch:18s} {m:22s} "
                  f"{_fmt_cell(d, ('top1', 'top3'))}")
    print("  [note] Dummy(most_frequent) has no predict_proba -> its Top@3 degrades "
          "to Top@1 (hard hit), reference only.")


def _print_interpretation(out, models, n_rows, n_groups, n_cases, y_ft, y_rc, tag, prov):
    # Resolve the realized SGKF scheme key per task (first inserted key in each dict
    # is always the SGKF scheme). It may be "SGKF5" (combined/multi, no reduction),
    # "SGKF<k>" (reduced), or "SGKF(n/a)" (infeasible). Using the live label keeps
    # combined/multi output byte-identical (label == "SGKF5" there).
    sgkf_ft_lbl = next(iter(out["fault_type"]))
    sgkf_fc_lbl = next(iter(out["fault_class"]))
    sgkf_rc_lbl = next(iter(out["rootcause"]))
    sgkf_ft_infeasible = out["fault_type"][sgkf_ft_lbl].get("_infeasible", False)
    sgkf_rc_infeasible = out["rootcause"][sgkf_rc_lbl].get("_infeasible", False)

    rf_rc_logo_t1 = out["rootcause"]["LOGO(ft)"]["RandomForest"]["top1"]
    rf_rc_case_t3 = out["rootcause"]["case_id(leaky)"]["RandomForest"]["top3"]
    n_rc = len(set(y_rc))
    n_ft = len(set(y_ft))

    print(f"\n--- interpretation ({tag}) ---")
    if sgkf_ft_infeasible:
        print(f"  {sgkf_ft_lbl} fault_type macroF1: SGKF INFEASIBLE for this y even "
              f"at k=2 (sklearn StratifiedGroupKFold); report LOGO(ft)/case_id rows "
              f"above instead.")
    else:
        rf_ft_sgkf = out["fault_type"][sgkf_ft_lbl]["RandomForest"]["macroF1"]
        dmf_ft_sgkf = out["fault_type"][sgkf_ft_lbl]["Dummy(most_frequent)"]["macroF1"]
        print(f"  {sgkf_ft_lbl} fault_type macroF1: RF = {fmt(*rf_ft_sgkf)} vs "
              f"Dummy(most_frequent) = {fmt(*dmf_ft_sgkf)} (lower bound).")
    if sgkf_rc_infeasible:
        print(f"  {sgkf_rc_lbl} root_cause_primary (realized {n_rc} classes): "
              f"SGKF INFEASIBLE for this y even at k=2; report LOGO(ft)/case_id rows "
              f"above instead.")
    else:
        rf_rc_sgkf_t1 = out["rootcause"][sgkf_rc_lbl]["RandomForest"]["top1"]
        rf_rc_sgkf_t3 = out["rootcause"][sgkf_rc_lbl]["RandomForest"]["top3"]
        print(f"  {sgkf_rc_lbl} root_cause_primary (realized {n_rc} classes): "
              f"RF Top@1 = {fmt(*rf_rc_sgkf_t1)}, Top@3 = {fmt(*rf_rc_sgkf_t3)}.")
    print(f"  LOGO(ft) root_cause_primary RF Top@1 = {fmt(*rf_rc_logo_t1)} -- LEAVES "
          f"one fault-type OUT (held-out type has 0 train samples -> its Top@k=0). "
          f"This is a DATA-STRUCTURE FLOOR (small N, {n_groups} fault-types many "
          f"singletons), NOT a pure model defect.")
    print(f"  case_id(leaky) RF Top@3 = {fmt(*rf_rc_case_t3)} -- DENSE upper bound "
          f"(group=case_id; same-case multi-windows m3d F1_only/F2_only can split "
          f"across train/test). LEAKY REFERENCE, not a fair evaluation.")
    print(f"  honesty: N={n_rows} rows, {n_groups} fault-types, {n_cases} cases, "
          f"provenance={prov}. macro-F1/Top@k computed over REALIZED classes only "
          f"(fault_type={n_ft}, root_cause_primary={n_rc}).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", choices=["real", "all", "both"], default="both",
                    help="which feature view to evaluate (default both).")
    ap.add_argument("--features-dir", "--pilot-dir", dest="features_dir", default=None,
                    help="dir holding features_k8s[_all].csv "
                         "(default: (runtime) features -- see dataset_registry.feature_csv; "
                         "native (native trees)  is read-only and no longer holds derived CSVs).")
    args = ap.parse_args()

    which = args.csv
    if which in ("real", "both"):
        csv_real = DR.feature_csv("features_k8s.csv", search_dir=args.features_dir)
        df_real = pd.read_csv(csv_real, dtype=str, keep_default_na=False)
        evaluate_csv(df_real, "REAL-only (features_k8s.csv)")
    if which in ("all", "both"):
        csv_all = DR.feature_csv("features_k8s_all.csv", search_dir=args.features_dir)
        df_all = pd.read_csv(csv_all, dtype=str, keep_default_na=False)
        evaluate_csv(df_all, "ALL-provenance (features_k8s_all.csv, secondary)")


class _Tee:
    """Tee stdout to both the real stream and a captured buffer. Robust against
    Windows console encoding / background-redirection buffering: the buffer is
    always flushed to a results file at the end so output is never lost even if
    the wrapped stdout never flushes."""

    def __init__(self, stream):
        self._stream = stream
        self.buf = []

    def write(self, s):
        try:
            self._stream.write(s)
        except Exception:
            pass
        self.buf.append(s)

    def flush(self):
        try:
            self._stream.flush()
        except Exception:
            pass

    def isatty(self):
        try:
            return self._stream.isatty()
        except Exception:
            return False


if __name__ == "__main__":
    # silence sklearn's per-split 'least populated class' warning (expected &
    # documented: many fault_types have 1-2 repeats in this small pilot).
    warnings.simplefilter("ignore")
    tee = _Tee(sys.stdout)
    sys.stdout = tee
    # ★ 2026-07-13:produced 是这次事故的核心修复。
    #   以前 finally: 无条件写盘 -> --help / argparse 报错 / 任何提前异常都把当时的 stdout
    #   当成"基线结果"落盘(SystemExit 也走 finally),于是 usage 文本冒充了结果。
    #   现在只有 main() 真的跑完(= 真出了结果)才落盘。
    produced = False
    try:
        main()
        produced = True
    finally:
        sys.stdout = tee._stream
        try:
            tee._stream.flush()
        except Exception:
            pass
        if produced:
            # durable copy of all output, ASCII/utf-8 safe. 输出根【永远不是 native】。
            out_file = DR.runtime_dir("scores") / "BASELINE_RESULTS_supervised.txt"
            DR.assert_not_native(out_file)
            out_file.write_text("".join(tee.buf), encoding="utf-8")
            print(f"\n[written] {out_file}", flush=True)
