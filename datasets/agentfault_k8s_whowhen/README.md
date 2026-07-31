# agentfault_k8s — Who&When-format delivery view

**What this is.** A Who&When-schema-compatible *view* of
`datasets/agentfault_k8s/` (the trajectory-attribution slice of our agent-fault
dataset). Each file mirrors the official
[Who&When](https://github.com/ag2ai/Agents_Failure_Attribution) case schema so
the official Who&When / A2P failure-attribution methods run on this folder
**as-is** (point their loader at `Injection-Generated/`, `is_handcrafted=False`).

- **96 cases**, one per faulted multi-agent recommendation run.
- Kind breakdown: context_drift=36, format_violation=12, hallucinate=36, wrong_item_pick=12.
- Filenames are integers (`1.json` .. `96.json`) like the official set. The
  subfolder is named **`Injection-Generated`** (honest: our cases are produced by
  fault *injection* into a live 4-agent pipeline — not algorithm-generated and
  not hand-crafted).
- `cases_index.json` maps each integer file back to its original `case_id`,
  `kind`, and `mistake_agent` for traceability.

## Provenance guarantee (same data the baselines scored)

The five method-consumed fields are copied **byte-identical** from the already
scored cases at `datasets/agentfault_k8s/whowhen/cases/case_*.json` — the exact
inputs the Who&When / A2P baselines ran on:

> `question`, `ground_truth`, `history`, `mistake_agent`, `mistake_step`

The build script re-verifies byte-identity (canonical JSON dump equality) for all
96 cases on every run. This view only **adds** auxiliary metadata; it never
regenerates or edits the scored data. The originals are treated as read-only.

## Field mapping vs official Who&When (Algorithm-Generated schema, 10 fields)

| Official field  | In this view | How |
|-----------------|--------------|-----|
| `question`      | identical    | copied verbatim from scored case (byte-identical core field) |
| `ground_truth`  | identical    | copied verbatim (byte-identical core field) |
| `history`       | identical    | copied verbatim — list of `{name, content}` (byte-identical core field) |
| `mistake_agent` | identical    | copied verbatim (byte-identical core field) |
| `mistake_step`  | identical    | copied verbatim — **kept as `int`** (see note below) (byte-identical core field) |
| `question_ID`   | **added**    | opaque, deterministic `sha256(case_id)[:16]`. Non-revealing; the real `case_id` lives only in `_provenance` (not judge-visible) |
| `mistake_reason`| **added (auto-derived)** | derived from the injection ground truth and prefixed `"[auto-derived from injection ground truth — not human-annotated]"`. Content per kind: hallucinate → "semantic hallucination injected at &lt;agent&gt;"; context_drift → "upstream message from &lt;dropped_agent&gt; deleted from &lt;agent&gt;'s input"; wrong_item_pick → "recommended ASIN forced to sentinel B00EKWZK5E"; format_violation → "output contract violated (&lt;subtype&gt;)" |
| `system_prompt` | **added**    | dict `{agent_name: verbatim prompt}` for all 4 pipeline agents, extracted verbatim from `services/recommendation_agent/agents/prompts.py` |
| `level`         | **omitted**  | we have no difficulty grading; omitted rather than fabricated |
| `is_correct`    | **omitted**  | task-level correctness is undefined for our cases — see the semantic-mismatch note below |
| —               | `_provenance`| **extra** (ours): `{case_id, kind, format_subtype, trace_id}` for traceability |

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

Setting `is_correct=false` for all 96 would assert "all 96 tasks failed," which
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
> being caused by X.* (See `datasets/agentfault_k8s/EVAL_NOTES.md` §1.)

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
`datasets/agentfault_k8s/`, **NOT here**. Consequently this view **alone cannot
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
