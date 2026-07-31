# -*- coding: utf-8 -*-
"""run_whowhen.py — Who&When / A2P method harness on the agentfault cases (SPEC §1.2).

Vendored method code runs UNCHANGED (fidelity):
  * third_party/reference/whowhen/Automated_FA/Lib/utils.py  -> ww_utils
    (all_at_once / step_by_step / binary_search)
  * third_party/reference/a2p/Automated_FA/Lib/utils.py      -> a2p_utils
    (all_at_once with a2p=True scaffolding prompt)
  Both are loaded by importlib.util.spec_from_file_location because their module
  names collide (Lib.utils); their inference.py is NOT imported (torch deps).

Client: openai.OpenAI(api_key=<value of the env var named by --api-key-env>,
base_url=<--api-base>). Defaults reproduce the original DeepSeek setup from
repo-root .env (python-dotenv: DEEPSEEK_API_KEY / DEEPSEEK_API_BASE; model =
DEEPSEEK_MODEL, default deepseek-chat). whowhen code only calls
client.chat.completions.create(...) -> duck-typed swap of AzureOpenAI, zero
method edits — so ANY OpenAI-compatible endpoint works as the judge. A counting
wrapper (harness-side, SPEC §1.2) tallies API calls. Proxy env untouched (the
judge endpoint is external; NO_PROXY rule only applies to local service
injection).

Per-run stdout is redirected to
(v1)whowhen/outputs/<method>_<tag>.txt (utf-8; --tag defaults
to "deepseek", keeping the historical filenames byte-identical) — same
redirect_stdout pattern as whowhen's own inference.py; tqdm writes to stderr so
the log stays clean. The file is written as <name>.part and atomically
os.replace()'d on completion, so an interrupted run never leaves a final-named
partial file that the skip-if-exists guard (or the scorer) would trust; on
interruption the .part is kept for inspection and a loud warning is printed.
Smoke runs (--max-cases N) write to outputs/_smoke_firstN/ — never to the
full-run filenames, so a smoke artifact cannot block or be scored as a full
run — and the temp case-subset dir is removed after the run.
random.seed(0) is set before each run because
binary_search resolves ambiguous LLM answers by random.randint (SPEC §2, seed
recorded, vendored code untouched).

max_tokens: 1024 for the 3 Who&When methods; 4096 for A2P (paper default is
large; deepseek-chat output cap is 8k -> 4096, recorded honestly, SPEC §1.2).

PARAMETRIZED (mirrors eval_agentfault_tierA.py / infra_negatives): --dataset-dir
DIR is a convenience that sets --cases-dir <DIR>/whowhen/cases and --out-dir
<DIR>/whowhen/outputs; both remain individually overridable and both default to
v1 ((v1)whowhen/...), so a flagless invocation is byte-identical
to the original harness. METHOD INVOCATION SEMANTICS ARE UNCHANGED — the vendored
Who&When / A2P functions are still called exactly as before, zero modification;
only paths moved.

Usage:
  PYTHONIOENCODING=utf-8 python3 \
      scripts/chaos/agentfault/eval/whowhen/run_whowhen.py --method all
  ... --dataset-dir (archived) agentfault_v2   # v2 tree (96 faulted cases, 4 families)
  ... --dry-run          # offline self-check: vendored import + client + case count
  ... --max-cases 2      # smoke: copy first N cases to a temp dir, run on those
                         # (outputs land in outputs/_smoke_firstN/, not outputs/)
  ... --force            # re-run even if the output file exists (default: skip)

Multi-judge usage (any OpenAI-compatible endpoint; the API key is passed as the
NAME of an environment variable via --api-key-env — resolved from repo-root
.env / os.environ — never the key itself on the command line):
  # GLM (Zhipu, OpenAI-compatible endpoint):
  ... run_whowhen.py --method all --tag glm --model glm-4.6 \
      --api-base https://open.bigmodel.cn/api/paas/v4 --api-key-env GLM_API_KEY
  # Anthropic (OpenAI compatibility layer):
  ... run_whowhen.py --method all --tag claude --model claude-opus-4-8 \
      --api-base https://api.anthropic.com/v1/ --api-key-env ANTHROPIC_API_KEY
Outputs land in outputs/<method>_<tag>.txt (skip-if-exists and .part atomic
publish apply per tag) and are scored with score_whowhen.py --tag <tag>.
Without the new flags, behavior/filenames are identical to the original
DeepSeek-only harness.
"""
import argparse
import contextlib
import glob
import importlib.util
import json
import os
import random
import re
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", ".."))
WW_UTILS_PATH = os.path.join(REPO, "third_party", "reference", "whowhen",
                             "Automated_FA", "Lib", "utils.py")
A2P_UTILS_PATH = os.path.join(REPO, "third_party", "reference", "a2p",
                              "Automated_FA", "Lib", "utils.py")
DEFAULT_CASES_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "whowhen", "cases")
DEFAULT_OUT_DIR = os.path.join(REPO, "datasets", "_archive", "agentfault", "agentfault", "whowhen", "outputs")

RANDOM_SEED = 0   # SPEC §2: fixes binary_search's ambiguous-answer coin flip

METHODS = ("all_at_once", "step_by_step", "binary_search", "a2p")


def load_vendored(path, mod_name):
    """Load a vendored utils.py by file path (SPEC §1.2: both libs are named
    Lib.utils -> path-based load avoids the module-name collision; never import
    their inference.py which pulls torch/transformers)."""
    if not os.path.isfile(path):
        raise FileNotFoundError("vendored module missing: %s" % path)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class CountingClient:
    """Duck-typed client wrapper: exposes .chat.completions.create and counts
    calls (SPEC §1.2: count API calls harness-side, never edit method code).

    Retry: vendored _make_api_call has NO retry — one transient connection blip
    cascades (first full run: step_by_step lost 64/64, all_at_once 23/64,
    binary_search 14/64 cases to 'Connection error'). Harness-side exponential
    backoff heals transients; after exhaustion re-raise so vendored except path
    behaves exactly as upstream wrote it. Retry logs go to STDERR only (stdout
    is the redirect-captured prediction stream the scorer parses)."""

    MAX_ATTEMPTS = 6
    BACKOFF_BASE_S = 1.0  # 1,2,4,8,16 s between the 6 attempts

    def __init__(self, inner):
        self._inner = inner
        self.calls = 0
        self.retries = 0
        # token 会计(OpenAI 兼容 usage:prompt/completion/total;思考型模型的 reasoning
        # token 计入 completion_tokens 或 total)。usage 缺失时静默跳过(不阻断)。
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.total_tokens = 0
        outer = self

        class _Completions:
            def create(self, *args, **kwargs):
                outer.calls += 1
                last_exc = None
                for attempt in range(outer.MAX_ATTEMPTS):
                    try:
                        resp = outer._inner.chat.completions.create(*args, **kwargs)
                        try:
                            u = getattr(resp, "usage", None)
                            if u is not None:
                                outer.prompt_tokens += int(getattr(u, "prompt_tokens", 0) or 0)
                                outer.completion_tokens += int(getattr(u, "completion_tokens", 0) or 0)
                                outer.total_tokens += int(getattr(u, "total_tokens", 0) or 0)
                        except Exception:
                            pass   # token 会计失败绝不阻断评测
                        return resp
                    except Exception as exc:  # connection/timeout/rate-limit transients
                        last_exc = exc
                        if attempt == outer.MAX_ATTEMPTS - 1:
                            break
                        outer.retries += 1
                        wait = outer.BACKOFF_BASE_S * (2 ** attempt)
                        print("[retry %d/%d after %.0fs] %s: %s"
                              % (attempt + 1, outer.MAX_ATTEMPTS - 1, wait,
                                 type(exc).__name__, str(exc)[:120]),
                              file=sys.stderr, flush=True)
                        time.sleep(wait)
                raise last_exc

        class _Chat:
            completions = _Completions()

        self.chat = _Chat()


def build_client(api_base=None, api_key_env="DEEPSEEK_API_KEY", model=None,
                 allow_missing_key=False):
    """OpenAI-compatible client (defaults: DeepSeek from repo-root .env, SPEC §1.2).

    api_key_env is the NAME of the environment variable holding the key (looked
    up after loading repo-root .env — the key itself never crosses the CLI).
    allow_missing_key (set by --dry-run) downgrades a missing key to a stderr
    warning: no API request is ever sent on that path, while a real run still
    hard-fails exactly as before. Returns (CountingClient, model, base_url)."""
    import dotenv
    import openai
    env_path = os.path.join(REPO, ".env")
    dotenv.load_dotenv(env_path)
    api_key = os.environ.get(api_key_env)
    base = api_base or os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
    mdl = model or os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
    if not api_key:
        if allow_missing_key:
            print("[warn] env var %s not set (checked %s and os.environ) — "
                  "acceptable for --dry-run only; a real run requires it."
                  % (api_key_env, env_path), file=sys.stderr)
            api_key = "MISSING-KEY-DRY-RUN-ONLY"
        else:
            raise RuntimeError("%s not found in %s" % (api_key_env, env_path))
    client = openai.OpenAI(api_key=api_key, base_url=base)
    return CountingClient(client), mdl, base


def list_cases(cases_dir):
    return sorted(glob.glob(os.path.join(cases_dir, "*.json")))


def smoke_subset(cases_dir, n):
    """--max-cases: copy the first N case files into a throwaway dir under the
    whowhen dataset area (SPEC §1.2: never touch the official cases dir)."""
    files = list_cases(cases_dir)[:n]
    sub = os.path.join(os.path.dirname(cases_dir.rstrip("/\\")), "_tmp_first%d_cases" % n)
    if os.path.isdir(sub):
        shutil.rmtree(sub)
    os.makedirs(sub)
    for f in files:
        shutil.copy2(f, sub)
    return sub


def run_method(method, client, model, cases_dir, out_dir, force, ww_utils, a2p_utils,
               tag="deepseek", max_tokens=None):
    """Dispatch one method run with stdout redirected to its output file
    (SPEC §1.2 run matrix, incl. max_tokens per method). Output filename is
    <method>_<tag>.txt — the default tag "deepseek" keeps the historical names.

    Completeness guarantee: output is written to <out_file>.part and only
    os.replace()'d to the final name after the method returns. A run that is
    interrupted (Ctrl-C / crash / machine sleep) leaves only the .part file,
    so the skip-if-exists guard can never mistake a partial run for a done
    one — for step_by_step a truncated file would otherwise be scored with
    silent 'missing' cases indistinguishable from genuine 'no error found'."""
    out_file = os.path.join(out_dir, "%s_%s.txt" % (method, tag))
    if os.path.exists(out_file) and not force:
        print("[skip] %s exists (use --force to re-run): %s" % (method, out_file))
        return
    os.makedirs(out_dir, exist_ok=True)
    part_file = out_file + ".part"
    calls_before = client.calls
    tok_before = client.total_tokens
    ptok_before = client.prompt_tokens
    ctok_before = client.completion_tokens
    random.seed(RANDOM_SEED)   # SPEC §2: binary_search tie-break determinism
    try:
        # max_tokens: defaults 1024 (whowhen) / 4096 (a2p) reproduce the original
        # DeepSeek runs; --max-tokens overrides BOTH for thinking-type judges
        # (e.g. glm-5.2 spends the budget on reasoning tokens and returns EMPTY
        # visible content at 1024 — observed live: smoke case_001 'Failed to get
        # prediction' with no API error line == empty content, not a failure).
        ww_mt = max_tokens or 1024
        a2p_mt = max_tokens or 4096
        with open(part_file, "w", encoding="utf-8") as f, contextlib.redirect_stdout(f):
            if method == "all_at_once":
                ww_utils.all_at_once(client, cases_dir, is_handcrafted=False,
                                     model=model, max_tokens=ww_mt)
            elif method == "step_by_step":
                ww_utils.step_by_step(client, cases_dir, is_handcrafted=False,
                                      model=model, max_tokens=ww_mt)
            elif method == "binary_search":
                ww_utils.binary_search(client, cases_dir, is_handcrafted=False,
                                       model=model, max_tokens=ww_mt)
            elif method == "a2p":
                a2p_utils.all_at_once(client, cases_dir, is_handcrafted=False,
                                      model=model, max_tokens=a2p_mt, a2p=True)
            else:
                raise ValueError("unknown method %s" % method)
    except BaseException:
        # Interrupted/failed run: keep the .part for inspection, warn loudly,
        # and never publish it under the final name (skip-if-exists + scorer
        # only ever look at the final name).
        print("[warn] method %s did NOT complete — partial output kept at %s; "
              "the final file %s was NOT written (re-run to retry)."
              % (method, part_file, out_file), file=sys.stderr)
        raise
    os.replace(part_file, out_file)   # atomic publish: only complete runs get the final name
    n_calls = client.calls - calls_before
    d_tot = client.total_tokens - tok_before
    d_pro = client.prompt_tokens - ptok_before
    d_com = client.completion_tokens - ctok_before
    print("[done] %s -> %s (API calls: %d, tokens: %d total = %d prompt + %d completion, seed=%d)"
          % (method, out_file, n_calls, d_tot, d_pro, d_com, RANDOM_SEED))


def main():
    ap = argparse.ArgumentParser(description="Who&When + A2P harness on agentfault (SPEC §1.2)")
    ap.add_argument("--method", choices=list(METHODS) + ["all"], default="all")
    ap.add_argument("--dataset-dir", default=None,
                    help="agentfault tree; sets --cases-dir <dir>/whowhen/cases and "
                         "--out-dir <dir>/whowhen/outputs unless those flags are given. "
                         "Default = (archived) agentfault (v1). v2: (archived) agentfault_v2")
    ap.add_argument("--cases-dir", default=None,
                    help="explicit cases dir (overrides --dataset-dir); default = v1")
    ap.add_argument("--out-dir", default=None,
                    help="explicit outputs dir (overrides --dataset-dir); default = v1")
    ap.add_argument("--max-cases", type=int, default=None,
                    help="smoke: copy first N cases to a temp dir and run on those")
    ap.add_argument("--force", action="store_true", help="re-run even if output exists")
    ap.add_argument("--dry-run", action="store_true",
                    help="offline self-check: load vendored modules, build client, "
                         "enumerate cases, exit without any API request")
    ap.add_argument("--api-base", default=None,
                    help="OpenAI-compatible base URL (default: env DEEPSEEK_API_BASE "
                         "or https://api.deepseek.com/v1)")
    ap.add_argument("--api-key-env", default="DEEPSEEK_API_KEY",
                    help="NAME of the environment variable holding the API key "
                         "(resolved from repo-root .env / os.environ; never pass "
                         "the key itself)")
    ap.add_argument("--model", default=None,
                    help="judge model id (default: env DEEPSEEK_MODEL or deepseek-chat)")
    ap.add_argument("--tag", default="deepseek",
                    help="judge tag embedded in output filenames <method>_<tag>.txt "
                         "(default 'deepseek' keeps the historical names)")
    ap.add_argument("--max-tokens", type=int, default=None,
                    help="override max_tokens for ALL methods (default: 1024 whowhen / "
                         "4096 a2p). Needed for thinking-type judges (glm-5.2 burns the "
                         "budget on reasoning tokens -> empty content at 1024)")
    args = ap.parse_args()

    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.tag):
        ap.error("--tag must match [A-Za-z0-9._-]+ (it is embedded in output filenames)")

    # path resolution: every default falls back to the v1 constant, so running with
    # no path flags reproduces the original harness exactly (v2 must not touch v1).
    if args.dataset_dir:
        base = args.dataset_dir if os.path.isabs(args.dataset_dir) \
            else os.path.join(REPO, args.dataset_dir)
        if args.cases_dir is None:
            args.cases_dir = os.path.join(base, "whowhen", "cases")
        if args.out_dir is None:
            args.out_dir = os.path.join(base, "whowhen", "outputs")
    if args.cases_dir is None:
        args.cases_dir = DEFAULT_CASES_DIR
    if args.out_dir is None:
        args.out_dir = DEFAULT_OUT_DIR

    ww_utils = load_vendored(WW_UTILS_PATH, "ww_utils")
    a2p_utils = load_vendored(A2P_UTILS_PATH, "a2p_utils")
    client, model, api_base = build_client(api_base=args.api_base,
                                           api_key_env=args.api_key_env,
                                           model=args.model,
                                           allow_missing_key=args.dry_run)

    cases_dir = args.cases_dir
    case_files = list_cases(cases_dir)
    methods = list(METHODS) if args.method == "all" else [args.method]

    if args.dry_run:
        print("[dry-run] ww_utils loaded from %s (has: %s)"
              % (WW_UTILS_PATH,
                 [n for n in ("all_at_once", "step_by_step", "binary_search")
                  if hasattr(ww_utils, n)]))
        print("[dry-run] a2p_utils loaded from %s (has all_at_once=%s, construct_a2p_prompt=%s)"
              % (A2P_UTILS_PATH, hasattr(a2p_utils, "all_at_once"),
                 hasattr(a2p_utils, "construct_a2p_prompt")))
        print("[dry-run] client constructed OK (model=%s, base=%s, tag=%s, calls so far=%d)"
              % (model, api_base, args.tag, client.calls))
        n = min(len(case_files), args.max_cases) if args.max_cases else len(case_files)
        print("[dry-run] cases dir %s: %d case files; would run methods %s on %d cases"
              % (cases_dir, len(case_files), methods, n))
        if args.max_cases:
            print("[dry-run] smoke mode: cases would be copied to a temp dir "
                  "(cleaned after the run) and outputs would land in %s — "
                  "never in the full-run filenames %s/<method>_%s.txt"
                  % (os.path.join(args.out_dir, "_smoke_first%d" % args.max_cases),
                     args.out_dir, args.tag))
        print("[dry-run] no API request sent; exiting.")
        return 0

    if not case_files:
        print("ERROR: no case files in %s — run make_whowhen_cases.py first." % cases_dir)
        return 1

    out_dir = args.out_dir
    smoke_cases_dir = None
    if args.max_cases:
        cases_dir = smoke_cases_dir = smoke_subset(cases_dir, args.max_cases)
        # Smoke outputs go to a distinct subdir so a smoke artifact can never
        # (a) satisfy the skip-if-exists check of a later full run, nor
        # (b) be picked up by score_whowhen.py, which reads exactly
        # outputs/<method>_<tag>.txt — scoring a 2-case smoke file against
        # the full GT would silently deflate the method's numbers.
        out_dir = os.path.join(args.out_dir, "_smoke_first%d" % args.max_cases)
        print("[smoke] using first %d cases in %s" % (args.max_cases, cases_dir))
        print("[smoke] outputs -> %s (kept separate from full-run outputs)" % out_dir)

    try:
        for m in methods:
            run_method(m, client, model, cases_dir, out_dir, args.force,
                       ww_utils, a2p_utils, tag=args.tag, max_tokens=args.max_tokens)
    finally:
        if smoke_cases_dir and os.path.isdir(smoke_cases_dir):
            # smoke temp cases dir is a throwaway copy — always clean it up
            shutil.rmtree(smoke_cases_dir, ignore_errors=True)
            print("[smoke] cleaned temp cases dir %s" % smoke_cases_dir)
    print("[total] API calls this session: %d | tokens: %d total = %d prompt + %d completion"
          % (client.calls, client.total_tokens, client.prompt_tokens, client.completion_tokens))
    return 0


if __name__ == "__main__":
    sys.exit(main())
