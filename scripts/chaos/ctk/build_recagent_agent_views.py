# -*- coding: utf-8 -*-
"""build_recagent_agent_views.py - add the two agent-layer top-level views to a
packaged single_recagent delivery.

WHY THIS EXISTS
  The 15 single_recagent cases are TRADITIONAL infrastructure faults (CPU
  saturation / 450ms network delay / pod kill) injected into the service that
  hosts the 4-agent LangGraph pipeline. On top of the normal per-case delivery
  folders, each native case also carries raw/agent_spans/spans.jsonl: an
  observe-only in-process trace of that pipeline (AGENTFAULT_OBSERVE=1, ZERO
  agent-level injection). package_for_delivery.py does not know about that
  directory - it predates it - so this script ships it as two SIBLING top-level
  views instead of burying it inside each case's raw/.

WHAT IT WRITES (under <delivery-root>/, all additive; per-case folders untouched)
  agent_traces/<sample_id>/spans.jsonl   verbatim native agent spans
  agent_traces/README.md + index.json    what the spans are, per-case counts
  whowhen/Infra-Negative/<n>.json        Who&When-shaped conversations
  whowhen/cases_index.json + README.md

THE POINT OF whowhen/
  These are NEGATIVE CONTROLS, not agent faults. Ground truth is "no agent made
  a mistake; the root cause is infrastructure". A content-aware attribution
  judge (Who&When / A2P) that names an agent here has produced a FALSE POSITIVE.
  That measurement is the reason to ship them: it is the error mode the
  agentfault_v2 positive set cannot measure.

SELECTION RULE (stated so nobody has to reverse-engineer it)
  One conversation per pipeline run whose FIRST span starts inside the F1 fault
  window and which reached all 4 agents. No sampling, no cap - every qualifying
  run ships. pod_failure cases contribute ZERO runs by construction (the pod is
  gone; the agent layer emits nothing). That absence is itself reported in
  cases_index.json rather than hidden.

TWO GROUPS, BECAUSE THE RUNS ARE NOT ALL ALIKE
  Infra-Negative/           all 4 agents produced real analysis -> clean FP test:
                            the conversation reads normal, only latency moved.
  Infra-Negative-Degraded/  1-4 of the agents failed under the fault and their
                            turn degraded to a placeholder string. Ground truth
                            is STILL "no agent at fault" - the agent did not
                            reason badly, its host was starved - but a judge sees
                            visibly broken turns, so this is a much harder
                            control and MUST be scored separately.
                            `by_n_degraded_agents` in cases_index.json says how
                            many agents degraded per case; do not assume it is
                            always the last one.
  Mixing the two would let a good score on the easy group hide the hard one.

USAGE
  python build_recagent_agent_views.py \
      --delivery-dir (delivery) single_recagent_20260722 \
      --pilot-dir    (native trees) single_recagent
"""
import argparse
import collections
import datetime
import json
import os
import shutil
import sys

AGENT_NAMES = ["Sequence_Recommender", "User_Behavior_Analyzer",
               "Product_Analyzer", "Recommendation_Synthesizer"]

QUESTION = (
    "A multi-agent e-commerce recommendation pipeline with 4 agents collaborating "
    "sequentially (Sequence_Recommender -> User_Behavior_Analyzer -> Product_Analyzer "
    "-> Recommendation_Synthesizer) must produce a ranked product recommendation "
    "with explanations for a user, given that user's historical interaction sequence."
)

FAULT_BLURB = {
    "service_cpu_saturation": "CPU saturation on the pod hosting the agent pipeline",
    "network_delay": "450ms network delay (90ms jitter) on the pod hosting the agent pipeline",
    "service_unavailable": "the pod hosting the agent pipeline was killed",
}


def jload(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def jdump(obj, p):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def read_spans(p):
    # BOM is present on the pod-side writer's first line; utf-8-sig handles both.
    with open(p, encoding="utf-8-sig") as f:
        return [json.loads(l) for l in f if l.strip()]


def iso2ns(s):
    if not s:
        return None
    return int(datetime.datetime.fromisoformat(
        s.replace("Z", "+00:00")).timestamp() * 1e9)


def agent_by_ancestry(trace_spans):
    """span_id -> owning agent name, by walking up to the nearest agent.<Name>.

    ONLY valid for spans the app emits itself (the injector's
    agentfault.resolved_input). The LangChain/openinference spans hang off a
    separate root - the four agent.<Name> spans are siblings of, not ancestors
    of, the LangGraph node spans - so those must be attributed via
    metadata.langgraph_node instead. Checked, not assumed.
    """
    byid = {s["span_id"]: s for s in trace_spans}
    owner = {}
    for s in trace_spans:
        cur, seen = s, set()
        while cur is not None and cur["span_id"] not in seen:
            seen.add(cur["span_id"])
            if cur["name"].startswith("agent."):
                owner[s["span_id"]] = cur["attributes"].get("recweb.agent.name")
                break
            cur = byid.get(cur.get("parent_span_id"))
    return owner


def langgraph_node_of(span):
    try:
        return json.loads(span["attributes"].get("metadata") or "{}").get("langgraph_node")
    except ValueError:
        return None


# services/recommendation_agent/workflow.py:144-158 - the three analyzer agents
# SWALLOW their exception, set ERROR on the agent.<Name> span, and return this
# placeholder so the chain can continue. Only the Synthesizer lets the exception
# bubble. Consequences that cost us a wrong headline number once already:
#   - the LangGraph node span stays OK for analyzers -> reading status off the
#     node span sees only the Synthesizer's 12 failures and misses 37 others
#   - the placeholder is non-empty -> an "is the text blank?" check misses it too
DEGRADED_MARK = u"暂时不可用"


def node_content(trace_spans, agent):
    """That agent's contribution + whether it degraded.

    Returns (content, degraded). The LangGraph node span named exactly <agent>
    carries output.value = {"messages": [<the one message this node appended>]}.
    When it has no output, fall back to the last ChatOpenAI completion inside the
    agent's subtree (the Synthesizer's answer leaves via a tool call, so
    tool-call arguments are appended).

    `degraded` is read off the agent.<Name> span - the one the application
    actually marks - not off the LangGraph node span. See DEGRADED_MARK above.
    """
    errored = any(s["name"] == "agent." + agent and s.get("status_code") == "ERROR"
                  for s in trace_spans)
    for s in trace_spans:
        if s["name"] != agent:
            continue
        try:
            msgs = json.loads(s["attributes"].get("output.value") or "{}").get("messages") or []
            if msgs:
                txt = (msgs[-1].get("data") or {}).get("content") or ""
                if txt.strip():
                    return txt, errored
        except (ValueError, TypeError, AttributeError):
            pass
    best = None
    for s in trace_spans:
        if s["name"] != "ChatOpenAI" or langgraph_node_of(s) != agent:
            continue
        if best is None or s["start_unix_nano"] > best["start_unix_nano"]:
            best = s
    if best is not None:
        a = best["attributes"]
        txt = a.get("llm.output_messages.0.message.content") or ""
        tc = a.get("llm.output_messages.0.message.tool_calls.0.tool_call.function.arguments")
        if tc:
            txt = (txt + "\n[tool_call arguments]\n" + tc).strip()
        return txt, errored
    return "", True


def system_prompts(trace_spans, owner):
    """Per-agent system prompt, recovered from the observe-only spans."""
    out = {}
    for s in trace_spans:
        if s["name"] != "agentfault.resolved_input":
            continue
        a = s["attributes"]
        if a.get("agentfault.resolved_input_messages.0.message.role") != "system":
            continue
        ag = owner.get(s["span_id"])
        if ag and ag not in out:
            out[ag] = a.get("agentfault.resolved_input_messages.0.message.content") or ""
    return out


def agent_of(span):
    return span["attributes"].get("recweb.agent.name")


def build(delivery_dir, pilot_dir):
    cases = []
    for md_p in sorted(os.path.join(delivery_dir, d, "metadata.json")
                       for d in os.listdir(delivery_dir)
                       if os.path.isdir(os.path.join(delivery_dir, d))
                       and os.path.exists(os.path.join(delivery_dir, d, "metadata.json"))):
        md = jload(md_p)
        native = None
        for root, dirs, _ in os.walk(pilot_dir):
            if os.path.basename(root) == md["source_case_id"]:
                native = root
                break
        if native is None:
            sys.exit("[ERR] no native case for %s" % md["source_case_id"])
        cases.append((md, os.path.dirname(md_p), native))
    if not cases:
        sys.exit("[ERR] no delivery cases under %s" % delivery_dir)
    print("[views] %d delivery cases" % len(cases))

    tr_root = os.path.join(delivery_dir, "agent_traces")
    ww_base = os.path.join(delivery_dir, "whowhen")
    GROUPS = {"clean": "Infra-Negative", "degraded": "Infra-Negative-Degraded"}
    # Rebuild the generated subtrees only. Hand-written README.md files sitting
    # at agent_traces/ and whowhen/ are never touched (learned the hard way: a
    # generator once clobbered a hand-written Chinese README for shijie).
    for p in [tr_root] + [os.path.join(ww_base, g) for g in GROUPS.values()]:
        if os.path.exists(p):
            for name in os.listdir(p):
                q = os.path.join(p, name)
                if name.lower() == "readme.md":
                    continue
                shutil.rmtree(q) if os.path.isdir(q) else os.remove(q)
        else:
            os.makedirs(p)

    tr_index = []
    ww_cases = {"clean": [], "degraded": []}
    sysprompt_ref = {}

    for md, dev_dir, native in cases:
        sid = md["sample_id"]
        gt = jload(os.path.join(native, "groundtruth.json"))
        ftype = gt["fault_types"][0]
        f1 = gt["component_fault_windows"]["F1"]
        fs, fe = iso2ns(f1["start"]), iso2ns(f1["end"])
        src = os.path.join(native, "raw", "agent_spans", "spans.jsonl")

        os.makedirs(os.path.join(tr_root, sid))
        shutil.copyfile(src, os.path.join(tr_root, sid, "spans.jsonl"))
        spans = read_spans(src)

        by_trace = collections.defaultdict(list)
        for s in spans:
            by_trace[s["trace_id"]].append(s)

        runs = {"pre_fault": 0, "during_fault": 0, "post_recovery": 0}
        emitted = 0
        for tid, rs in sorted(by_trace.items(),
                              key=lambda kv: min(s["start_unix_nano"] for s in kv[1])):
            if not any(s["name"] == "LangGraph" for s in rs):
                continue                       # not a pipeline run
            t0 = min(s["start_unix_nano"] for s in rs)
            stage = ("during_fault" if fs <= t0 <= fe
                     else "pre_fault" if t0 < fs else "post_recovery")
            runs[stage] += 1
            if stage != "during_fault":
                continue
            reached = {agent_of(s) for s in rs if s["name"].startswith("agent.")}
            reached.discard(None)
            if len(reached) != 4:
                continue                       # partial run: not a fair judge input

            owner = agent_by_ancestry(rs)
            sp = system_prompts(rs, owner)
            if len(sp) > len(sysprompt_ref):
                sysprompt_ref = sp
            history, broken = [], []
            for ag in AGENT_NAMES:
                txt, err = node_content(rs, ag)
                history.append({"name": ag, "content": txt})
                if err or not txt.strip() or DEGRADED_MARK in txt:
                    broken.append(ag)
            group = "degraded" if broken else "clean"
            ww_cases[group].append({
                "question": QUESTION,
                "question_ID": tid[:16],
                "ground_truth": "N/A (open-ended recommendation task; no single reference answer)",
                "history": history,
                "mistake_agent": None,
                "mistake_step": None,
                "mistake_reason": (
                    "NO AGENT IS AT FAULT. This conversation was produced while an "
                    "infrastructure fault (%s: %s) was active on service rec-agent. "
                    "Zero agent-level injection was applied. A judge that names an "
                    "agent here has produced a false positive.%s"
                    % (ftype, FAULT_BLURB.get(ftype, ftype),
                       ("" if not broken else
                        " NOTE: %d of the 4 agents (%s) contributed no analysis - "
                        "their calls failed under the infrastructure fault and the "
                        "pipeline degraded to a placeholder turn. That is a starved "
                        "host, not bad reasoning."
                        % (len(broken), ", ".join(broken))))),
                "system_prompt": sp or sysprompt_ref,
                "_provenance": {"sample_id": sid, "source_case_id": md["source_case_id"],
                                "fault_type": ftype, "root_cause_services": gt["root_cause_services"],
                                "stage": "during_fault", "trace_id": tid,
                                "kind": "infra_negative_control", "group": group,
                                "degraded_agents": broken,
                                "n_degraded_agents": len(broken)},
            })
            emitted += 1

        tr_index.append({"sample_id": sid, "source_case_id": md["source_case_id"],
                         "fault_type": ftype, "spans": len(spans),
                         "pipeline_runs": runs, "whowhen_cases_emitted": emitted})
        print("  %-62s %-22s spans=%-6d runs=%s -> ww=%d"
              % (sid, ftype, len(spans), runs, emitted))

    # ---- whowhen/ : official integer filenames, per group ----
    ww_index, groups_meta = [], {}
    for gkey, gdir in GROUPS.items():
        for i, c in enumerate(ww_cases[gkey], 1):
            jdump(c, os.path.join(ww_base, gdir, "%d.json" % i))
            ww_index.append({"file": "%s/%d.json" % (gdir, i),
                             "mistake_agent": None, **c["_provenance"]})
        groups_meta[gdir] = {
            "n_cases": len(ww_cases[gkey]),
            "by_fault_type": dict(sorted(collections.Counter(
                c["_provenance"]["fault_type"] for c in ww_cases[gkey]).items())),
            "by_n_degraded_agents": dict(sorted(collections.Counter(
                c["_provenance"]["n_degraded_agents"] for c in ww_cases[gkey]).items())),
            "description": ("all 4 agents produced real analysis; only latency moved"
                            if gkey == "clean" else
                            "1-4 agents failed under the fault and degraded to a "
                            "placeholder turn (host starved, not bad reasoning); "
                            "score separately - see by_n_degraded_agents"),
        }

    jdump({"schema_version": "recagent-agent-traces.v1",
           "note": "observe-only agent spans; ZERO agent-level injection",
           "cases": tr_index,
           "totals": {"cases": len(tr_index),
                      "spans": sum(e["spans"] for e in tr_index),
                      "whowhen_cases": len(ww_index)}},
          os.path.join(tr_root, "index.json"))
    jdump({"schema_version": "whowhen-infra-negative.v2",
           "n_cases": len(ww_index),
           "ground_truth_is_uniform": True,
           "uniform_ground_truth": "no agent at fault (root cause is infrastructure)",
           "scoring": "report FP rate per group; never pool the two groups",
           "groups": groups_meta,
           "cases": ww_index},
          os.path.join(ww_base, "cases_index.json"))

    print("[views] agent_traces: %d cases, %d spans"
          % (len(tr_index), sum(e["spans"] for e in tr_index)))
    for gdir, m in groups_meta.items():
        print("[views] whowhen %-26s %d conversations %s"
              % (gdir, m["n_cases"], m["by_fault_type"]))
    return tr_index, ww_index


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delivery-dir", required=True)
    ap.add_argument("--pilot-dir", required=True)
    a = ap.parse_args()
    build(a.delivery_dir, a.pilot_dir)


if __name__ == "__main__":
    main()
