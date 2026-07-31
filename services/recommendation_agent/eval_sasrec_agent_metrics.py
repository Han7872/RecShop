import argparse
import csv
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests


@dataclass
class Sample:
    user_id: str
    history: List[str]
    label: str
    split: str


def ndcg_at_k_single(label: str, pred: Sequence[str], k: int) -> float:
    """Single-positive-label NDCG@k."""
    try:
        rank0 = list(pred[:k]).index(label)
    except ValueError:
        return 0.0
    rank = rank0 + 1
    return 1.0 / math.log2(rank + 1)


def recall_at_k_single(label: str, pred: Sequence[str], k: int) -> float:
    """Single-positive-label Recall@k (a.k.a HitRate@k)."""
    return 1.0 if label in pred[:k] else 0.0


def call_sasrec_topk(
    api_url: str,
    history: Sequence[str],
    top_k: int,
    exclude_history: bool = True,
    timeout: int = 30,
) -> List[Dict]:
    resp = requests.post(
        f"{api_url.rstrip('/')}/recommend",
        json={
            "item_sequence": list(history),
            "top_k": int(top_k),
            "exclude_history": bool(exclude_history),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(data.get("message", "SASRec recommend failed"))
    return data.get("recommendations", [])


def is_token_valid_for_model(api_url: str, item_id: str, timeout: int = 10) -> bool:
    """Best-effort check whether an item token is recognized by the model.

    The SASRec service returns HTTP 400 when *all* items in the input sequence are invalid.
    So we call /recommend with a single-item history. If it succeeds, the token is valid.
    """
    try:
        _ = call_sasrec_topk(
            api_url=api_url,
            history=[item_id],
            top_k=1,
            exclude_history=False,
            timeout=timeout,
        )
        return True
    except requests.exceptions.HTTPError:
        return False
    except Exception:
        return False


def to_item_list(recs: Sequence[Dict]) -> List[str]:
    return [r.get("item_id") for r in recs if r.get("item_id")]


def fetch_test_sequences_from_api(
    api_url: str,
    split: str = "test",
    max_users: int = 5000,
    timeout: int = 120,
) -> List["Sample"]:
    """Fetch LS-split sequences directly from the SASRec API server.

    The server reconstructs sequences from the RecBole-preprocessed dataset,
    guaranteeing every item in history and label is in the model vocabulary.
    This avoids the OOV-sparse-history problem caused by reading the raw inter file.
    """
    url = f"{api_url.rstrip('/')}/dataset/test_sequences"
    resp = requests.get(url, params={"split": split, "max_users": max_users}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError("API returned success=false from /dataset/test_sequences")
    samples = []
    for s in data.get("sequences", []):
        samples.append(Sample(
            user_id=str(s["user_id"]),
            history=list(s["history"]),
            label=str(s["label"]),
            split=str(s["split"]),
        ))
    return samples


def call_sasrec_score_sampled(
    api_url: str,
    history: Sequence[str],
    target_item: str,
    num_negatives: int = 99,
    exclude_history: bool = True,
    timeout: int = 30,
    return_candidates: bool = False,
) -> Dict:
    """Call /score/sampled on the SASRec API.

    Returns a dict with target_rank, target_score, num_candidates, target_valid.
    When return_candidates=True, also includes 'candidates' list.
    """
    resp = requests.post(
        f"{api_url.rstrip('/')}/score/sampled",
        json={
            "item_sequence": list(history),
            "target_item": target_item,
            "num_negatives": int(num_negatives),
            "exclude_history": bool(exclude_history),
            "return_candidates": bool(return_candidates),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def call_real_agent(
    agent_url: str,
    history: Sequence[str],
    agent_topk: int = 10,
    timeout: int = 120,
) -> Optional[str]:
    """Call the real LangGraph multi-agent recommendation workflow.

    POST {agent_url}/recommend with item_sequence and top_k.
    Returns the single recommended item_id chosen by the agent, or None on failure.
    """
    resp = requests.post(
        f"{agent_url.rstrip('/')}/recommend",
        json={
            "item_sequence": list(history),
            "top_k": int(agent_topk),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(data.get("message", "Agent recommend failed"))
    rec = data.get("recommendation", {})
    return rec.get("recommended_product") or None


def call_agent_from_candidates(
    agent_url: str,
    history: Sequence[str],
    candidates: Sequence[Dict],
    timeout: int = 120,
) -> Optional[str]:
    """Call the multi-agent system with pre-computed sampled candidates.

    POST {agent_url}/recommend/from_candidates with item_sequence and candidates.
    The agent picks the best item from the same candidate pool as the baseline.
    Returns the recommended item_id, or None on failure.
    """
    resp = requests.post(
        f"{agent_url.rstrip('/')}/recommend/from_candidates",
        json={
            "item_sequence": list(history),
            "candidates": list(candidates),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(data.get("message", "Agent from_candidates failed"))
    rec = data.get("recommendation", {})
    return rec.get("recommended_product") or None


def read_samples_jsonl(path: str) -> List[Sample]:
    samples: List[Sample] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            user_id = str(obj.get("user_id", f"line_{line_no}"))
            history = obj.get("history")
            label = obj.get("label")
            split = str(obj.get("split", "test"))

            if not isinstance(history, list) or not history:
                raise ValueError(f"Invalid history at line {line_no}")
            if not isinstance(label, str) or not label:
                raise ValueError(f"Invalid label at line {line_no}")

            samples.append(Sample(user_id=user_id, history=history, label=label, split=split))
    return samples


def _detect_delimiter(header_line: str) -> str:
    if "\t" in header_line:
        return "\t"
    return ","


def _normalize_colname(name: str) -> str:
    """Normalize RecBole-style column names.

    Examples:
    - user_id:token -> user_id
    - timestamp:float -> timestamp
    """
    if not name:
        return name
    return name.split(":", 1)[0].strip()


def build_samples_from_interactions(
    interaction_file: str,
    split: str,
    user_col: str = "user_id",
    item_col: str = "item_id",
    ts_col: str = "timestamp",
    max_users: Optional[int] = None,
) -> List[Sample]:
    """Build valid/test samples based on time-ordered user sequences.

    Follows the README definition:
    - valid history = seq[:-2], valid label = seq[-2]
    - test  history = seq[:-1], test  label = seq[-1]

    interaction_file can be .csv/.tsv/.inter as long as it has 3 columns.
    """
    with open(interaction_file, "r", encoding="utf-8") as f:
        first = f.readline()
        if not first:
            raise ValueError("Empty interaction file")
        delim = _detect_delimiter(first)
        f.seek(0)

        reader = csv.DictReader(f, delimiter=delim)
        fieldnames = list(reader.fieldnames or [])
        norm_fieldnames = [_normalize_colname(n) for n in fieldnames]
        norm_to_raw: Dict[str, str] = {}
        for raw, norm in zip(fieldnames, norm_fieldnames):
            # keep first occurrence
            norm_to_raw.setdefault(norm, raw)

        missing = [c for c in (user_col, item_col, ts_col) if c not in norm_to_raw]
        if missing:
            raise ValueError(
                f"Missing columns: {missing}. Got: {fieldnames}. "
                f"Normalized: {sorted(norm_to_raw.keys())}"
            )

        raw_user_col = norm_to_raw[user_col]
        raw_item_col = norm_to_raw[item_col]
        raw_ts_col = norm_to_raw[ts_col]

        by_user: Dict[str, List[Tuple[float, str]]] = {}
        for row in reader:
            uid = str(row.get(raw_user_col, "")).strip()
            iid = str(row.get(raw_item_col, "")).strip()
            ts_raw = str(row.get(raw_ts_col, "")).strip()
            if not uid or not iid or not ts_raw:
                continue
            try:
                ts = float(ts_raw)
            except ValueError:
                continue
            by_user.setdefault(uid, []).append((ts, iid))

    samples: List[Sample] = []
    for uid, events in by_user.items():
        if max_users is not None and len(samples) >= max_users:
            break

        events.sort(key=lambda x: x[0])
        seq = [iid for _, iid in events]
        if len(seq) < 3:
            continue

        if split == "valid":
            history = seq[:-2]
            label = seq[-2]
        elif split == "test":
            history = seq[:-1]
            label = seq[-1]
        else:
            raise ValueError("split must be 'valid' or 'test'")

        if not history or not label:
            continue
        samples.append(Sample(user_id=uid, history=history, label=label, split=split))

    return samples


def evaluate(
    samples: Sequence[Sample],
    api_url: str,
    k_eval: int = 10,
    topk_call: int = 20,
    exclude_history: bool = True,
    timeout: int = 30,
    show_progress_every: int = 50,
    debug_samples: int = 0,
    debug_label_check: bool = False,
    run_agent: bool = False,
    agent_url: str = "http://127.0.0.1:5001",
    agent_topk: int = 10,
    agent_timeout: int = 120,
    eval_mode: str = "sampled",
    num_negatives: int = 99,
) -> Dict[str, float]:
    """Evaluate SASRec baseline and (optionally) the real LangGraph Agent.

    eval_mode='sampled' (default, recommended):
        Calls /score/sampled: scores target vs num_negatives random negatives.
        Replicates RecBole's uni-N sampled-ranking protocol.
        Metrics: sasrec_recall@K and sasrec_ndcg@K based on target's rank
        among (num_negatives+1) candidates.

    eval_mode='full':
        Calls /recommend with full ranking over all 433K items.
        Performance is ~0 for small sample sizes because the model was
        optimized for sampled-ranking (uni100), not full ranking.
    """
    n = 0
    skipped = 0
    num_label_in_history = 0
    num_target_invalid = 0
    error_counter: Counter[str] = Counter()

    sasrec_ndcg_sum = 0.0
    sasrec_recall_sum = 0.0

    # sampled-mode diagnostics
    target_rank_sum = 0
    eff_hist_len_sum = 0
    sasrec_ndcg1_sampled_sum = 0.0
    sasrec_recall1_sampled_sum = 0.0

    # full-mode extras
    sasrec_ndcg1_sum = 0.0
    sasrec_recall1_sum = 0.0
    sasrec_recall20_sum = 0.0
    num_label_in_top20 = 0

    # agent
    agent_skipped = 0
    agent_n = 0
    agent_error_counter: Counter[str] = Counter()
    agent_ndcg1_sum = 0.0
    agent_recall1_sum = 0.0

    for i, s in enumerate(samples, start=1):
        if s.label in s.history:
            num_label_in_history += 1

        # ── SAMPLED mode: /score/sampled ─────────────────────────────
        if eval_mode == "sampled":
            try:
                score_result = call_sasrec_score_sampled(
                    api_url=api_url,
                    history=s.history,
                    target_item=s.label,
                    num_negatives=num_negatives,
                    exclude_history=exclude_history,
                    timeout=timeout,
                    return_candidates=run_agent,
                )
            except requests.exceptions.HTTPError as e:
                msg = str(e)
                try:
                    if e.response is not None:
                        msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                except Exception:
                    pass
                error_counter[msg] += 1
                skipped += 1
                if show_progress_every > 0 and i % show_progress_every == 0:
                    print(f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}", file=sys.stderr)
                continue
            except Exception as e:
                error_counter[f"{type(e).__name__}: {str(e)[:200]}"] += 1
                skipped += 1
                if show_progress_every > 0 and i % show_progress_every == 0:
                    print(f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}", file=sys.stderr)
                continue

            if not score_result.get("target_valid", False):
                num_target_invalid += 1
                skipped += 1
                if show_progress_every > 0 and i % show_progress_every == 0:
                    print(f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}", file=sys.stderr)
                continue

            target_rank = score_result.get("target_rank")
            num_cands = score_result.get("num_candidates", num_negatives + 1)

            if debug_samples > 0 and i <= debug_samples:
                print("=" * 60, file=sys.stderr)
                print(f"[DEBUG sample {i}] user_id={s.user_id}", file=sys.stderr)
                print(f"history_len={len(s.history)} label={s.label}", file=sys.stderr)
                print(f"target_rank={target_rank}/{num_cands} score={score_result.get('target_score')}", file=sys.stderr)

            # Build a synthetic pred list of length num_cands for metric helpers
            # (label is at position target_rank-1, rest are placeholders)
            if target_rank is not None:
                pred_list = ["__neg__"] * num_cands
                pred_list[target_rank - 1] = s.label
            else:
                pred_list = ["__neg__"] * num_cands

            sasrec_ndcg_sum += ndcg_at_k_single(s.label, pred_list, k_eval)
            sasrec_recall_sum += recall_at_k_single(s.label, pred_list, k_eval)
            sasrec_ndcg1_sampled_sum += ndcg_at_k_single(s.label, pred_list, 1)
            sasrec_recall1_sampled_sum += recall_at_k_single(s.label, pred_list, 1)
            if target_rank is not None:
                target_rank_sum += target_rank
            eff_hist_len_sum += score_result.get("effective_history_length", 0)
            n += 1

        # ── FULL mode: /recommend full ranking ───────────────────────
        else:
            try:
                recs20 = call_sasrec_topk(
                    api_url=api_url,
                    history=s.history,
                    top_k=topk_call,
                    exclude_history=exclude_history,
                    timeout=timeout,
                )
            except requests.exceptions.HTTPError as e:
                msg = str(e)
                try:
                    if e.response is not None:
                        msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                except Exception:
                    pass
                error_counter[msg] += 1
                skipped += 1
                if show_progress_every > 0 and i % show_progress_every == 0:
                    print(f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}", file=sys.stderr)
                continue
            except Exception as e:
                error_counter[f"{type(e).__name__}: {str(e)[:200]}"] += 1
                skipped += 1
                if show_progress_every > 0 and i % show_progress_every == 0:
                    print(f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}", file=sys.stderr)
                continue

            sasrec_pred20 = to_item_list(recs20)
            sasrec_pred10 = sasrec_pred20[:k_eval]
            sasrec_pred1 = sasrec_pred20[:1]

            if debug_samples > 0 and i <= debug_samples:
                print("=" * 60, file=sys.stderr)
                print(f"[DEBUG sample {i}] user_id={s.user_id} split={s.split}", file=sys.stderr)
                print(f"history_len={len(s.history)} label={s.label}", file=sys.stderr)
                if debug_label_check:
                    valid = is_token_valid_for_model(api_url, s.label, timeout=min(timeout, 10))
                    print(f"label_token_valid_for_model={valid}", file=sys.stderr)
                print(f"sasrec_top20={sasrec_pred20}", file=sys.stderr)
                print(f"sasrec_top10={sasrec_pred10}", file=sys.stderr)

            sasrec_ndcg1_sum += ndcg_at_k_single(s.label, sasrec_pred1, 1)
            sasrec_recall1_sum += recall_at_k_single(s.label, sasrec_pred1, 1)
            sasrec_ndcg_sum += ndcg_at_k_single(s.label, sasrec_pred10, k_eval)
            sasrec_recall_sum += recall_at_k_single(s.label, sasrec_pred10, k_eval)
            sasrec_recall20_sum += recall_at_k_single(s.label, sasrec_pred20, topk_call)
            if s.label in sasrec_pred20:
                num_label_in_top20 += 1
            n += 1

        # ── Real Agent call (optional, expensive) ────────────────────
        if run_agent:
            try:
                # In sampled mode, pass pre-computed candidates to agent
                # so it picks from the SAME 100 candidates as baseline
                if eval_mode == "sampled" and score_result.get("candidates"):
                    agent_pick = call_agent_from_candidates(
                        agent_url=agent_url,
                        history=s.history,
                        candidates=score_result["candidates"],
                        timeout=agent_timeout,
                    )
                else:
                    agent_pick = call_real_agent(
                        agent_url=agent_url,
                        history=s.history,
                        agent_topk=agent_topk,
                        timeout=agent_timeout,
                    )
                if debug_samples > 0 and i <= debug_samples:
                    print(f"agent_pick={agent_pick}", file=sys.stderr)
                # Verbose agent diagnostics: show pick rank & hit/miss for ALL samples
                if agent_pick and eval_mode == "sampled" and score_result.get("candidates"):
                    cands = score_result["candidates"]
                    pick_rank = next((c["rank"] for c in cands if c["item_id"] == agent_pick), "NOT_IN_LIST")
                    target_r = score_result.get("target_rank", "?")
                    hit = "HIT" if agent_pick == s.label else "miss"
                    print(f"  [agent {i}] pick={agent_pick} pick_rank={pick_rank} "
                          f"target={s.label} target_rank={target_r} → {hit}", file=sys.stderr)
                if agent_pick:
                    agent_ndcg1_sum += ndcg_at_k_single(s.label, [agent_pick], 1)
                    agent_recall1_sum += recall_at_k_single(s.label, [agent_pick], 1)
                    agent_n += 1
                else:
                    agent_skipped += 1
            except Exception as e:
                agent_error_counter[f"{type(e).__name__}: {str(e)[:120]}"] += 1
                agent_skipped += 1

        if show_progress_every > 0 and i % show_progress_every == 0:
            msg = f"[Progress] {i}/{len(samples)} ok={n} skip={skipped}"
            if run_agent:
                msg += f" | agent ok={agent_n} skip={agent_skipped}"
            print(msg, file=sys.stderr)

    if n == 0:
        return {
            "eval_mode": eval_mode,
            "num_samples": 0.0,
            "num_skipped": float(skipped),
            "num_target_invalid": float(num_target_invalid),
            "label_in_history_rate": 0.0,
            f"sasrec_ndcg@{k_eval}": 0.0,
            f"sasrec_recall@{k_eval}": 0.0,
            "agent_ndcg@1": None,
            "agent_recall@1": None,
            "top_errors": [{"error": k, "count": v} for k, v in error_counter.most_common(3)],
        }

    result: Dict = {
        "eval_mode": eval_mode,
        "num_negatives": num_negatives if eval_mode == "sampled" else None,
        "num_samples": float(n),
        "num_skipped": float(skipped),
        "num_target_invalid": float(num_target_invalid),
        "label_in_history_rate": num_label_in_history / n,
        f"sasrec_ndcg@{k_eval}": sasrec_ndcg_sum / n,
        f"sasrec_recall@{k_eval}": sasrec_recall_sum / n,
        "top_errors": [{"error": k, "count": v} for k, v in error_counter.most_common(5)],
    }

    if eval_mode == "sampled":
        result["sasrec_ndcg@1"] = sasrec_ndcg1_sampled_sum / n
        result["sasrec_recall@1"] = sasrec_recall1_sampled_sum / n
        result["avg_target_rank"] = target_rank_sum / n
        result["avg_effective_history_len"] = eff_hist_len_sum / n
        result["random_baseline_recall"] = k_eval / (num_negatives + 1)

    if eval_mode == "full":
        result["label_in_top20_rate"] = num_label_in_top20 / n
        result[f"sasrec_ndcg@1"] = sasrec_ndcg1_sum / n
        result[f"sasrec_recall@1"] = sasrec_recall1_sum / n
        result[f"sasrec_recall@{topk_call}"] = sasrec_recall20_sum / n

    if run_agent:
        result["agent_num_samples"] = float(agent_n)
        result["agent_num_skipped"] = float(agent_skipped)
        result["agent_ndcg@1"] = agent_ndcg1_sum / agent_n if agent_n > 0 else None
        result["agent_recall@1"] = agent_recall1_sum / agent_n if agent_n > 0 else None
        result["agent_errors"] = [{"error": k, "count": v} for k, v in agent_error_counter.most_common(3)]
    else:
        result["agent_ndcg@1"] = None
        result["agent_recall@1"] = None

    return result


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate SASRec with an agent-layer Top20->Top10 filtering policy and compute NDCG@10/Recall@10. "
            "Input can be a JSONL samples file or an interaction file with user_id,item_id,timestamp columns."
        )
    )

    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="SASRec API base url")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--samples-jsonl", help="Path to JSONL samples. Each line: {user_id, history, label, split}")
    group.add_argument(
        "--interaction-file",
        help="Path to interaction file (.csv/.tsv/.inter) with columns user_id,item_id,timestamp",
    )
    group.add_argument(
        "--test-from-api",
        action="store_true",
        default=False,
        help="Fetch clean test sequences directly from the SASRec API (/dataset/test_sequences). "
             "Recommended: avoids OOV-sparse-history problem of reading raw inter file.",
    )

    parser.add_argument("--split", default="test", choices=["valid", "test"], help="Split used when building from interaction file")
    parser.add_argument("--max-users", type=int, default=None, help="Limit number of users when building from interaction file")

    parser.add_argument("--exclude-history", action="store_true", default=True, help="Exclude history items in SASRec recommend")
    parser.add_argument("--include-history", dest="exclude_history", action="store_false", help="Do not exclude history items")

    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout for SASRec API")
    parser.add_argument("--progress", type=int, default=50, help="Print progress every N samples; 0 disables")

    parser.add_argument(
        "--debug-samples",
        type=int,
        default=0,
        help="Print first N samples' history/label/top20 for debugging (printed to stderr)",
    )
    parser.add_argument(
        "--debug-label-check",
        action="store_true",
        default=False,
        help="For debug samples, probe if label token is valid for the model (extra API calls)",
    )

    # Evaluation mode
    parser.add_argument(
        "--eval-mode",
        default="sampled",
        choices=["sampled", "full"],
        help="sampled (default): /score/sampled replicates uni100 protocol, gives meaningful metrics. "
             "full: /recommend full-ranking over 433K items, near-zero metrics for small samples.",
    )
    parser.add_argument(
        "--num-negatives",
        type=int,
        default=99,
        help="Number of random negatives for sampled mode (default: 99 → uni100)",
    )

    # Real agent options
    parser.add_argument(
        "--run-agent",
        action="store_true",
        default=False,
        help="Also call the real LangGraph Agent (POST /recommend on agent Flask app) and compute agent_recall@1 / agent_ndcg@1. WARNING: each call involves multiple LLM API calls — use a small --max-users (e.g. 50-100) to limit cost.",
    )
    parser.add_argument(
        "--agent-url",
        default="http://127.0.0.1:5001",
        help="Base URL of the recommendation agent Flask app (default: http://127.0.0.1:5001)",
    )
    parser.add_argument(
        "--agent-topk",
        type=int,
        default=10,
        help="Number of SASRec candidates the agent sees before picking 1 item (default: 10)",
    )
    parser.add_argument(
        "--agent-timeout",
        type=int,
        default=120,
        help="HTTP timeout in seconds for agent calls (default: 120, LLM can be slow)",
    )

    args = parser.parse_args()

    if args.samples_jsonl:
        samples = read_samples_jsonl(args.samples_jsonl)
        samples = [s for s in samples if s.split == args.split]
    elif args.test_from_api:
        print(f"[INFO] Fetching test sequences from {args.api_url}/dataset/test_sequences ...", file=sys.stderr)
        samples = fetch_test_sequences_from_api(
            api_url=args.api_url,
            split=args.split,
            max_users=args.max_users or 5000,
            timeout=120,
        )
        print(f"[INFO] Got {len(samples)} sequences from API.", file=sys.stderr)
    else:
        samples = build_samples_from_interactions(
            interaction_file=args.interaction_file,
            split=args.split,
            max_users=args.max_users,
        )

    print(
        f"[INFO] eval_mode={args.eval_mode}"
        + (f", num_negatives={args.num_negatives}" if args.eval_mode == "sampled" else ""),
        file=sys.stderr,
    )
    if args.run_agent:
        print(
            f"[INFO] --run-agent enabled: will call {args.agent_url}/recommend for each sample.\n"
            f"       Agent sees SASRec top-{args.agent_topk} before picking 1 item.\n"
            f"       Each agent call may take 10-60s. Evaluating {len(samples)} users.",
            file=sys.stderr,
        )

    metrics = evaluate(
        samples=samples,
        api_url=args.api_url,
        exclude_history=args.exclude_history,
        timeout=args.timeout,
        show_progress_every=args.progress,
        debug_samples=args.debug_samples,
        debug_label_check=args.debug_label_check,
        run_agent=args.run_agent,
        agent_url=args.agent_url,
        agent_topk=args.agent_topk,
        agent_timeout=args.agent_timeout,
        eval_mode=args.eval_mode,
        num_negatives=args.num_negatives,
    )

    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
