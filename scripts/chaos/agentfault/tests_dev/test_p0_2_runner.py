# -*- coding: utf-8 -*-
"""P0-2 runner 改造离线自检(不调 API/不起服务)。

覆盖 a-e:
  a) build_combos 矩阵 + 每 agent 当 GT 的 combo 计数(先验:Synth 3/8=37.5%)。
  b) build_env 各 combo(context_drift env / format per-rep subtype 轮换 / hallu/wrongpick/normal 回归)。
  c) 载体轮换:小 pool(3)rep_1/2/3 → carrier[0/1/2];probe(port, seq=X) 给/不给的回退。
  d) compute_context_drift_outcome:假 case recovered/silent_wrong/unknown 判定 + 计数。
  e) py_compile(由 runner 脚本外部执行)。

跑法:PYTHONIOENCODING=utf-8 python tests_dev/test_p0_2_runner.py
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))                 # .../agentfault/tests_dev
AGENTFAULT_DIR = os.path.dirname(HERE)                            # .../agentfault
COLLECT_DIR = os.path.join(AGENTFAULT_DIR, "collect")
INJECTOR_DIR = os.path.join(AGENTFAULT_DIR, "injector")
sys.path.insert(0, COLLECT_DIR)
sys.path.insert(0, AGENTFAULT_DIR)

import agentfault_runner as R          # noqa: E402
import injector_smoke as ISM           # noqa: E402
import compute_context_drift_outcome as CDO  # noqa: E402

FAILS = []


def check(cond, msg):
    status = "OK " if cond else "FAIL"
    print(f"   [{status}] {msg}")
    if not cond:
        FAILS.append(msg)


AGENT_SHORT = {
    "Sequence_Recommender": "Seq",
    "User_Behavior_Analyzer": "UB",
    "Product_Analyzer": "Product",
    "Recommendation_Synthesizer": "Synth",
}


def test_a():
    print("\n[a] build_combos 矩阵 + GT 先验分布")
    combos = R.build_combos()
    print(f"   combos ({len(combos)}):")
    gt_counts = {a: 0 for a in R.AGENT_NAMES}
    for c in combos:
        gt = c["agent"] if c["faulted"] else None
        extra = ""
        if c.get("drop"):
            extra = f" drop={c['drop']}"
        elif c.get("subtypes"):
            extra = f" subtypes={[s[0] for s in c['subtypes']]}"
        print(f"     {c['id']:42s} kind={c['kind']:16s} GT={gt}{extra}")
        if gt:
            gt_counts[gt] += 1
    faulted = [c for c in combos if c["faulted"]]
    check(len(combos) == 9, f"9 combos total (got {len(combos)})")
    check(len(faulted) == 8, f"8 faulted combos (got {len(faulted)})")
    print(f"   GT counts (per agent, over {len(faulted)} faulted): "
          + ", ".join(f"{AGENT_SHORT[a]}={gt_counts[a]}" for a in R.AGENT_NAMES))
    synth = gt_counts["Recommendation_Synthesizer"]
    pct = synth / len(faulted) * 100
    print(f"   Synth prior = {synth}/{len(faulted)} = {pct:.1f}%")
    check(synth == 3, f"Synth GT count == 3 (got {synth})")
    check(abs(pct - 37.5) < 0.01, f"Synth prior == 37.5% (got {pct:.1f}%)")
    check(gt_counts["Sequence_Recommender"] == 1, "Seq GT == 1")
    check(gt_counts["User_Behavior_Analyzer"] == 2, "UB GT == 2")
    check(gt_counts["Product_Analyzer"] == 2, "Product GT == 2")


def _get_env(combo, subtype=None, field=None):
    return R.build_env(combo, 5199, "/tmp/span.jsonl", "/tmp/led.jsonl",
                       subtype=subtype, field=field)


def test_b():
    print("\n[b] build_env 分支")
    by_id = {c["id"]: c for c in R.build_combos()}

    # 起手 pop 隔离:预置 stale 旋钮,验证被清
    os.environ["AGENTFAULT_DROP_AGENT"] = "STALE_AGENT"
    os.environ["AGENTFAULT_KIND_STALE"] = "STALE"

    # --- context_drift ---
    for cid, target, drop in [
        ("ctxdrift_ub_from_seq", "User_Behavior_Analyzer", "Sequence_Recommender"),
        ("ctxdrift_prod_from_ub", "Product_Analyzer", "User_Behavior_Analyzer"),
        ("ctxdrift_synth_from_prod", "Recommendation_Synthesizer", "Product_Analyzer"),
    ]:
        env = _get_env(by_id[cid])
        check(env.get("AGENTFAULT_KIND_" + target) == "context_drift",
              f"{cid}: AGENTFAULT_KIND_{target}=context_drift")
        check(env.get("AGENTFAULT_DROP_AGENT") == drop,
              f"{cid}: AGENTFAULT_DROP_AGENT={drop} (got {env.get('AGENTFAULT_DROP_AGENT')})")
        check(env.get("AGENTFAULT_INJECT") == "1", f"{cid}: AGENTFAULT_INJECT=1")
        check("AGENTFAULT_KIND_STALE" not in env, f"{cid}: stale KIND popped")
        check("AGENTFAULT_WRONG_ASIN" not in env, f"{cid}: no WRONG_ASIN")

    # --- format per-rep subtype 轮换 ---
    fmt = by_id["format_Recommendation_Synthesizer"]
    expected = ["missing_field", "type_violation", "empty_required", "malformed_json",
                "missing_field", "type_violation"]
    got_rot = []
    for i in range(1, 7):
        st, fld = R.rep_subtype(fmt, i)
        got_rot.append(st)
    print(f"   format rep_subtype rotation (rep 1..6): {got_rot}")
    check(got_rot == expected, f"format subtype rotates [(i-1)%4] (got {got_rot})")
    # build_env 用 per-rep 传入的 subtype
    for i in (1, 2, 3, 4):
        st, fld = R.rep_subtype(fmt, i)
        env = _get_env(fmt, subtype=st, field=fld)
        check(env.get("AGENTFAULT_FORMAT_SUBTYPE") == st,
              f"format rep{i}: AGENTFAULT_FORMAT_SUBTYPE={st}")
        check(env.get("AGENTFAULT_KIND_Recommendation_Synthesizer") == "format_violation",
              f"format rep{i}: KIND=format_violation")
        check("AGENTFAULT_DROP_AGENT" not in env, f"format rep{i}: no DROP_AGENT")

    # --- hallucinate 回归 ---
    for a in ("Sequence_Recommender", "User_Behavior_Analyzer", "Product_Analyzer"):
        env = _get_env(by_id[f"hallu_{a}"])
        check(env.get("AGENTFAULT_KIND_" + a) == "hallucinate", f"hallu_{a}: KIND=hallucinate")
        check(env.get("AGENTFAULT_INJECT") == "1", f"hallu_{a}: INJECT=1")
        check("AGENTFAULT_WRONG_ASIN" not in env, f"hallu_{a}: no WRONG_ASIN")
        check("AGENTFAULT_FORMAT_SUBTYPE" not in env, f"hallu_{a}: no FORMAT_SUBTYPE")
        check("AGENTFAULT_DROP_AGENT" not in env, f"hallu_{a}: stale DROP_AGENT popped")

    # --- wrong_item_pick 回归 ---
    env = _get_env(by_id["wrongpick_Recommendation_Synthesizer"])
    check(env.get("AGENTFAULT_KIND_Recommendation_Synthesizer") == "wrong_item_pick",
          "wrongpick: KIND=wrong_item_pick")
    check(env.get("AGENTFAULT_WRONG_ASIN") == R.WRONG_ASIN, "wrongpick: WRONG_ASIN set")
    check("AGENTFAULT_DROP_AGENT" not in env, "wrongpick: no DROP_AGENT")
    check("AGENTFAULT_FORMAT_SUBTYPE" not in env, "wrongpick: no FORMAT_SUBTYPE")

    # --- normal 回归 ---
    env = _get_env(by_id["normal"])
    check("AGENTFAULT_INJECT" not in env, "normal: no AGENTFAULT_INJECT")
    check(not any(k.startswith("AGENTFAULT_KIND_") for k in env),
          "normal: no AGENTFAULT_KIND_*")
    check(env.get("AGENTFAULT_INSTRUMENT") == "1", "normal: INSTRUMENT=1 (content capture on)")
    check(env.get("AGENTFAULT_LEDGER") == "/tmp/led.jsonl", "normal: LEDGER set")
    check("AGENTFAULT_DROP_AGENT" not in env, "normal: no DROP_AGENT")

    os.environ.pop("AGENTFAULT_DROP_AGENT", None)
    os.environ.pop("AGENTFAULT_KIND_STALE", None)


def test_c():
    print("\n[c] 载体轮换 + probe 后向兼容")
    # probe 参数传递(monkeypatch injector_smoke._req 捕获 body,不实际发网络)
    captured = {}

    class _FakeResp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"trace_id":"tid_fake","success":true,' \
                   b'"recommendation":{"recommended_product":"B0","product_title":"T"},' \
                   b'"conversation":{}}'

    def fake_req(url, method="GET", body=None, timeout=90):
        captured["body"] = body
        return _FakeResp()

    orig_req = ISM._req
    ISM._req = fake_req
    try:
        ISM.probe(5199)
        b0 = json.loads(captured["body"])
        check(b0["item_sequence"] == ISM.PROBE_SEQ,
              f"probe(port) falls back to PROBE_SEQ (got {b0['item_sequence']})")
        check(b0["top_k"] == ISM.PROBE_TOPK, "probe(port) falls back to PROBE_TOPK")

        ISM.probe(5199, seq=["X1", "X2"], top_k=7)
        b1 = json.loads(captured["body"])
        check(b1["item_sequence"] == ["X1", "X2"], "probe(port, seq=X) uses X")
        check(b1["top_k"] == 7, "probe(port, top_k=7) uses 7")
    finally:
        ISM._req = orig_req

    # rep_i -> carrier[i-1] 映射 + run_one_rep 用 carrier.history 作 seq、回传 carrier_seq_id
    carriers = [
        {"seq_id": 100, "history": ["A1", "A2"]},
        {"seq_id": 200, "history": ["B1", "B2"]},
        {"seq_id": 300, "history": ["C1", "C2"]},
    ]
    seen = {}

    def fake_probe(port, seq=None, top_k=None):
        seen["seq"] = seq
        return 200, 10.0, {"trace_id": "", "success": True,
                           "recommendation": {"recommended_product": "B0"},
                           "conversation": {}}

    orig_probe = ISM.probe
    ISM.probe = fake_probe
    normal = R.COMBO_BY_ID["normal"]
    try:
        for i in (1, 2, 3):
            carrier = carriers[i - 1]   # 与 _emit_rep 内 carriers[i-1] 同索引
            res = R.run_one_rep(normal, 5199, None, "/no/span.jsonl", "/no/led.jsonl",
                                f"normal__r{i}", 10, carrier=carrier, subtype=None)
            cseq = res[9]
            check(seen["seq"] == carrier["history"],
                  f"rep{i}: probe seq == carrier[{i-1}].history {carrier['history']} (got {seen['seq']})")
            check(cseq == carrier["seq_id"],
                  f"rep{i}: returned carrier_seq_id == carrier[{i-1}].seq_id {carrier['seq_id']}")
    finally:
        ISM.probe = orig_probe

    # load_carrier_pool need>len 报错
    try:
        R.load_carrier_pool(need=10 ** 9)
        check(False, "load_carrier_pool(need=huge) should raise")
    except RuntimeError:
        check(True, "load_carrier_pool raises when pool < need")


def _write_case(dataset_dir, case_id, kind, carrier_seq_id, asin, title):
    jr = os.path.join(dataset_dir, "journal")
    rw = os.path.join(dataset_dir, "raw")
    os.makedirs(jr, exist_ok=True)
    os.makedirs(rw, exist_ok=True)
    with open(os.path.join(jr, f"{case_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"case_id": case_id, "kind": kind,
                   "probe": {"carrier_seq_id": carrier_seq_id}}, f, ensure_ascii=False)
    with open(os.path.join(rw, f"{case_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"resp": {"recommendation": {"recommended_product": asin,
                                               "product_title": title}}},
                  f, ensure_ascii=False)


def test_d():
    print("\n[d] compute_context_drift_outcome 判定")
    d = tempfile.mkdtemp(prefix="ctxdrift_test_")
    # carrier 10: normal 与 ctxdrift 同推荐 -> recovered
    _write_case(d, "normal__r1", "normal", 10, "ASIN_A", "TitleA")
    _write_case(d, "ctxdrift_ub_from_seq__r1", "context_drift", 10, "ASIN_A", "TitleA")
    # carrier 20: 推荐不同 -> silent_wrong
    _write_case(d, "normal__r2", "normal", 20, "ASIN_B", "TitleB")
    _write_case(d, "ctxdrift_ub_from_seq__r2", "context_drift", 20, "ASIN_C", "TitleC")
    # carrier 99: 无同载体 normal -> unknown
    _write_case(d, "ctxdrift_prod_from_ub__r1", "context_drift", 99, "ASIN_D", "TitleD")

    outcomes, counts, warnings = CDO.compute(d)
    print(f"   outcomes = {json.dumps({k: v['outcome'] for k, v in outcomes.items()}, ensure_ascii=False)}")
    print(f"   counts = {counts}")
    print(f"   warnings = {warnings}")
    check(outcomes["ctxdrift_ub_from_seq__r1"]["outcome"] == "recovered",
          "same carrier + same rec -> recovered")
    check(outcomes["ctxdrift_ub_from_seq__r2"]["outcome"] == "silent_wrong",
          "same carrier + diff rec -> silent_wrong")
    check(outcomes["ctxdrift_prod_from_ub__r1"]["outcome"] == "unknown",
          "no matching normal -> unknown")
    check(counts == {"recovered": 1, "silent_wrong": 1, "unknown": 1},
          f"counts recovered=1/silent_wrong=1/unknown=1 (got {counts})")
    check(len(warnings) == 1, "one unknown warning")
    # 幂等:写文件 + 再算一致
    out_path = os.path.join(d, "context_drift_outcomes.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(outcomes, f, ensure_ascii=False, indent=2)
    o2, c2, _ = CDO.compute(d)
    check(o2 == outcomes and c2 == counts, "idempotent recompute")


def main():
    test_a()
    test_b()
    test_c()
    test_d()
    print("\n" + "=" * 60)
    if FAILS:
        print(f"SELF-CHECK FAILED: {len(FAILS)} check(s):")
        for m in FAILS:
            print(f"   - {m}")
        return 1
    print("SELF-CHECK PASSED (all a-d checks green)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
