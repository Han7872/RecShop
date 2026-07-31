#!/usr/bin/env python
"""G2ext Phase A offline 自测(无集群/无网络): 用合成 stages 单元级验证
multi_leg_retarget_gate / _build_fault_profile / build_root_metric_contract / membership_for
的骨架与参数化同构逻辑。不跑 kubectl/prom(monkeypatch cfs_throttle_posthoc), 纯 offline。

跑: python3 scripts/chaos/ctk/g2ext_offline_selftest.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chaos_k8s_runner as R

Z = timezone.utc
T0 = datetime(2026, 7, 23, 12, 0, 0, tzinfo=Z)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def snap(carrier, off_s, ok, dur):
    return {"carrier_name": carrier, "ts": iso(T0 + timedelta(seconds=off_s)),
            "request_success": ok, "request_duration_ms": dur}


def make_stages(during_snaps):
    """3 stage; baseline snaps at off 0..8, during at 60.. per given, post at 200.."""
    pre = {"stage": "pre_fault", "snapshots": [], "window_start_dt": T0, "window_end_dt": T0 + timedelta(seconds=30)}
    during = {"stage": "during_fault", "snapshots": during_snaps,
              "window_start_dt": T0 + timedelta(seconds=40), "window_end_dt": T0 + timedelta(seconds=140)}
    post = {"stage": "post_recovery", "snapshots": [], "window_start_dt": T0 + timedelta(seconds=200),
            "window_end_dt": T0 + timedelta(seconds=230)}
    return [pre, during, post]


# ---- monkeypatch cfs_throttle_posthoc: 按 regex 里的 svc 名返回合成 throttle ----
_THROTTLE_SVCS = set()   # 测试设定"哪些 svc 有 throttle"


def _fake_throttle(ws, we, regex, step=15):
    for svc in _THROTTLE_SVCS:
        # regex 形如 "cart-.*" 或 "catalog-(?:[^g]|g[^w]).*"
        if regex.startswith(svc + "-") or regex.startswith(svc + "-("):
            return {f"{svc}-abc-xyz": 0.5}
    return None


R.cfs_throttle_posthoc = _fake_throttle

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}")


# ==========================================================================
# TEST 1: 双-18 cart_cpu_x_order_cpu (simultaneous 2 cpu) — 期望 PASS
# ==========================================================================
print("TEST 1: cart_cpu_x_order_cpu (dual simultaneous 2×cpu)")
fault1 = "cart_cpu_x_order_cpu"
combo1 = R.G2EXT_COMBOS[fault1]
# F1=cart(cpu whole during), F2=order(cpu whole during); windows = whole during [40,140]
f1win = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f2win = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f3win = [None, None]
# baseline snaps (before window) + in-window slow snaps for cart/order; disjoint user flat
ds = []
for off in (5, 10, 15):   # baseline (< 40)
    ds += [snap("cart", off, True, 20), snap("order", off, True, 20), snap("user", off, True, 15)]
for off in (60, 80, 100):  # in-window: cart/order slow (ratio ~5x), user flat
    ds += [snap("cart", off, True, 100), snap("order", off, True, 110), snap("user", off, True, 16)]
stages1 = make_stages(ds)
_THROTTLE_SVCS = {"cart", "order"}
nsb = {"cart-abc-xyz": 0, "order-abc-xyz": 0, "user-abc-xyz": 0, "pricing-abc-xyz": 0}
nsa = dict(nsb)   # no restarts (cpu legs)
passed1, ev1 = R.multi_leg_retarget_gate(stages1, f1win, f2win, f3win, R._parse_carriers(combo1["carriers"], item="i", user_token="u", cart_user="c"),
                                          fault1, combo1["legs"], combo1["disjoint"],
                                          ns_restarts_before=nsb, ns_restarts_after=nsa,
                                          checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("gate passed", passed1 is True)
check("2 legs, both arms pass", ev1["n_legs"] == 2 and ev1["per_leg"]["F1"]["arm_passed"] and ev1["per_leg"]["F2"]["arm_passed"])
check("cart throttle present", ev1["per_leg"]["F1"]["throttle_present"] is True)
check("per_root_F1/F2 aliases", ev1["per_root_F1"] and ev1["per_root_F2"])
check("victim_set = cart,order", set(ev1["victim_set"]) == {"cart", "order"})
prof1 = R._build_fault_profile(fault1, "gw", "cat", {"F1": iso(f1win[0]), "F2": iso(f2win[0])},
                               {"F1": iso(f1win[1]), "F2": iso(f2win[1])},
                               leg_pods={"cart": "cart-abc-xyz", "order": "order-abc-xyz"})
check("profile root_cause_services=[cart,order]", prof1["root_cause_services"] == ["cart", "order"])
check("profile cgt len 2 (root_count auto)", len(prof1["component_ground_truth"]) == 2)
check("profile both svccpu → service_cpu_saturation", all(c["fault_type"] == "service_cpu_saturation" for c in prof1["component_ground_truth"]))
contract1 = R.build_root_metric_contract(ev1, fault1)
check("contract valid True + F1&F2", contract1["valid"] and contract1["F1"] and contract1["F2"] and "F3" not in contract1)


# ==========================================================================
# TEST 2: 三-05 checkout_podfail_x_cart_cpu_x_pricing_cpu — 期望 PASS
# ==========================================================================
print("TEST 2: checkout_podfail_x_cart_cpu_x_pricing_cpu (triple partial_overlap)")
fault2 = "checkout_podfail_x_cart_cpu_x_pricing_cpu"
combo2 = R.G2EXT_COMBOS[fault2]
# F1=checkout podfail (INNER subwindow [70,110]), F2=cart cpu (whole [40,140]), F3=pricing cpu (whole [40,140])
f1win = [T0 + timedelta(seconds=70), T0 + timedelta(seconds=110)]   # podfail subwindow
f2win = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f3win = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
ds = []
# baseline (<40): all flat/ok
for off in (5, 10, 15):
    ds += [snap("checkout", off, True, 30), snap("cart", off, True, 20),
           snap("pricing", off, True, 25), snap("user", off, True, 15)]
# checkout podfail window [70,110]: error burst; cart/pricing slow whole-during; user flat
for off in (75, 90, 105):
    ds += [snap("checkout", off, False, 3000), snap("cart", off, True, 120),
           snap("pricing", off, True, 130), snap("user", off, True, 16)]
# cart/pricing extra slow samples outside podfail win but in during (50,60,120,130) → F2/F3 F1_only bucket
for off in (50, 60, 120, 130):
    ds += [snap("cart", off, True, 115), snap("pricing", off, True, 125), snap("user", off, True, 16)]
stages2 = make_stages(ds)
_THROTTLE_SVCS = {"cart", "pricing"}   # pricing carrier_hard=False → throttle hard, carrier soft
# checkout podfail → restart_delta 1
nsb = {"checkout-p-x": 0, "cart-p-x": 0, "pricing-p-x": 0, "user-p-x": 0}
nsa = {"checkout-p-x": 1, "cart-p-x": 0, "pricing-p-x": 0, "user-p-x": 0}
passed2, ev2 = R.multi_leg_retarget_gate(stages2, f1win, f2win, f3win, R._parse_carriers(combo2["carriers"], item="i", user_token="u", cart_user="c"),
                                          fault2, combo2["legs"], combo2["disjoint"],
                                          ns_restarts_before=nsb, ns_restarts_after=nsa,
                                          checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("gate passed", passed2 is True)
check("3 legs all arms pass", ev2["n_legs"] == 3 and all(ev2["per_leg"][f]["arm_passed"] for f in ("F1", "F2", "F3")))
check("checkout podfail restart_delta=1 ok", ev2["per_leg"]["F1"]["restart_ok"] is True)
check("pricing carrier_hard=False (soft carrier)", ev2["per_leg"]["F3"]["carrier_hard"] is False)
check("per_root_F3 present", ev2.get("per_root_F3") is True)
check("control_plane_healthy (podfail target exempt)", ev2["control_plane_healthy"] is True)
prof2 = R._build_fault_profile(fault2, "gw", "cat",
                               {"F1": iso(f1win[0]), "F2": iso(f2win[0]), "F3": iso(f3win[0])},
                               {"F1": iso(f1win[1]), "F2": iso(f2win[1]), "F3": iso(f3win[1])},
                               leg_pods={"checkout": "checkout-p-x", "cart": "cart-p-x", "pricing": "pricing-p-x"})
check("profile 3 distinct root services", prof2["root_cause_services"] == ["checkout", "cart", "pricing"])
check("profile checkout=service_unavailable (podfail canon)", prof2["component_ground_truth"][0]["fault_type"] == "service_unavailable")
check("profile cart/pricing=service_cpu_saturation", prof2["component_ground_truth"][1]["fault_type"] == "service_cpu_saturation" and prof2["component_ground_truth"][2]["fault_type"] == "service_cpu_saturation")
check("triple in TRIPLE_ROOT_FAULTS", fault2 in R.TRIPLE_ROOT_FAULTS)
contract2 = R.build_root_metric_contract(ev2, fault2)
check("contract valid + F1&F2&F3", contract2["valid"] and contract2["F1"] and contract2["F2"] and contract2["F3"])


# ==========================================================================
# TEST 3: fail-closed — 双-21 user podfail leaf: restart missing → arm fails
# ==========================================================================
print("TEST 3: user_podfail_x_backend_cpu leaf fail-closed (no restart data)")
fault3 = "user_podfail_x_backend_cpu"
combo3 = R.G2EXT_COMBOS[fault3]
f1win = [T0 + timedelta(seconds=70), T0 + timedelta(seconds=110)]
f2win = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
ds = []
for off in (5, 10, 15):
    ds += [snap("user", off, True, 15), snap("backend", off, True, 20), snap("announcement", off, True, 10)]
for off in (75, 90, 105):
    ds += [snap("user", off, False, 3000), snap("backend", off, True, 120), snap("announcement", off, True, 11)]
for off in (50, 60, 120, 130):
    ds += [snap("backend", off, True, 118), snap("announcement", off, True, 11)]
stages3 = make_stages(ds)
_THROTTLE_SVCS = {"backend"}
# NO restart data → user podfail arm (restart-only, leaf carrier_hard=False) must fail-closed
passed3, ev3 = R.multi_leg_retarget_gate(stages3, f1win, f2win, [None, None], R._parse_carriers(combo3["carriers"], item="i", user_token="u", cart_user="c"),
                                          fault3, combo3["legs"], combo3["disjoint"],
                                          ns_restarts_before=None, ns_restarts_after=None,
                                          checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("gate FAILS (no restart data, leaf arm fail-closed)", passed3 is False)
check("user leg arm_passed False (restart None)", ev3["per_leg"]["F1"]["arm_passed"] is False)
check("disjoint=announcement", ev3["disjoint_name"] == "announcement")

# with restart data → passes
nsb = {"user-p": 0, "backend-p": 0}
nsa = {"user-p": 1, "backend-p": 0}
passed3b, ev3b = R.multi_leg_retarget_gate(stages3, f1win, f2win, [None, None], R._parse_carriers(combo3["carriers"], item="i", user_token="u", cart_user="c"),
                                           fault3, combo3["legs"], combo3["disjoint"],
                                           ns_restarts_before=nsb, ns_restarts_after=nsa,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("gate PASSES with restart_delta=1 (leaf carrier soft)", passed3b is True)


# ==========================================================================
# TEST 4: fail-closed — checksum drift → fail
# ==========================================================================
print("TEST 4: checksum drift → gate fails")
passed4, ev4 = R.multi_leg_retarget_gate(stages1, f1win, f2win, [None, None], R._parse_carriers(combo1["carriers"], item="i", user_token="u", cart_user="c"),
                                          fault1, combo1["legs"], combo1["disjoint"],
                                          ns_restarts_before=nsb if False else {"cart-x": 0, "order-x": 0},
                                          ns_restarts_after={"cart-x": 0, "order-x": 0},
                                          checksum_pre=R.CHECKSUM_BASELINE, checksum_post={"items": 1, "inventory": 1})
check("gate FAILS on checksum drift", passed4 is False and ev4["checksum_zero_drift"] is False)


# ==========================================================================
# TEST 5: registry / churn sanity
# ==========================================================================
print("TEST 5: registry + churn")
check("all 9 combos in G2EXT_COMBOS (Phase A 5 + Phase B 2 + Phase C 2)", len(R.G2EXT_COMBOS) == 9)
check("2 triples in TRIPLE_ROOT_FAULTS", "order_podfail_x_reviewquery_cpu_x_catalog_cpu" in R.TRIPLE_ROOT_FAULTS and "checkout_podfail_x_cart_cpu_x_pricing_cpu" in R.TRIPLE_ROOT_FAULTS)
check("Phase C 三-07 in TRIPLE_ROOT_FAULTS", "recagent_netdelay_x_sasrec_cpu_x_catalog_podfail" in R.TRIPLE_ROOT_FAULTS)
check("Phase C 双-17 NOT in TRIPLE_ROOT_FAULTS", "checkout_podfail_x_inv_latency" not in R.TRIPLE_ROOT_FAULTS)
check("duals NOT in TRIPLE_ROOT_FAULTS", "cart_cpu_x_order_cpu" not in R.TRIPLE_ROOT_FAULTS)
check("三-08 design_note flags deviation", "partial_overlap" in R.G2EXT_COMBOS["order_podfail_x_reviewquery_cpu_x_catalog_cpu"]["design_note"])
# churn: a non-podfail-target pod restart → control_plane unhealthy
nsb = {"cart-x": 0, "order-x": 0, "sasrec-x": 0}
nsa = {"cart-x": 0, "order-x": 0, "sasrec-x": 1}   # sasrec churned (not a leg)
_THROTTLE_SVCS = {"cart", "order"}
passed5, ev5 = R.multi_leg_retarget_gate(stages1, f1win, f2win, [None, None], R._parse_carriers(combo1["carriers"], item="i", user_token="u", cart_user="c"),
                                         fault1, combo1["legs"], combo1["disjoint"],
                                         ns_restarts_before=nsb, ns_restarts_after=nsa,
                                         checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("gate FAILS on non-leg churn (sasrec restart)", passed5 is False and ev5["control_plane_healthy"] is False)


# ==========================================================================
# TEST 6 (S4-1a): 双-19 search_podfail_x_reviewquery_cpu — 期望 PASS
# ==========================================================================
print("TEST 6: search_podfail_x_reviewquery_cpu (dual partial_overlap podfail+cpu)")
import re as _re

# regex-aware throttle fake(镜像真 cfs_throttle_posthoc 语义: prom =~ 全锚定 → fullmatch 过滤 pod 名)
_THROTTLE_PODS = {}


def _fake_throttle_pods(ws, we, regex, step=15):
    out = {p: v for p, v in _THROTTLE_PODS.items() if _re.fullmatch(regex, p)}
    return out or None


R.cfs_throttle_posthoc = _fake_throttle_pods

fault6 = "search_podfail_x_reviewquery_cpu"
combo6 = R.G2EXT_COMBOS[fault6]
f1win6 = [T0 + timedelta(seconds=70), T0 + timedelta(seconds=110)]   # search podfail INNER
f2win6 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]   # review-query cpu whole during
ds6 = []
for off in (5, 10, 15):
    ds6 += [snap("search", off, True, 30), snap("review_query", off, True, 20), snap("user", off, True, 15)]
for off in (75, 90, 105):   # podfail subwindow: search error burst; rq slow; user flat
    ds6 += [snap("search", off, False, 500), snap("review_query", off, True, 120), snap("user", off, True, 16)]
for off in (50, 60, 120, 130):   # cpu window outside podfail: rq slow; user flat
    ds6 += [snap("review_query", off, True, 115), snap("user", off, True, 16)]
stages6 = make_stages(ds6)
_THROTTLE_PODS = {"review-query-6b7f-abc": 0.4}
nsb6 = {"search-5c8-x": 0, "review-query-6b7f-abc": 0, "user-p": 0}
nsa6 = {"search-5c8-x": 1, "review-query-6b7f-abc": 0, "user-p": 0}
passed6, ev6 = R.multi_leg_retarget_gate(stages6, f1win6, f2win6, [None, None],
                                         R._parse_carriers(combo6["carriers"], item="i", user_token="u", cart_user="c"),
                                         fault6, combo6["legs"], combo6["disjoint"],
                                         ns_restarts_before=nsb6, ns_restarts_after=nsa6,
                                         checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-19 gate passed", passed6 is True)
check("双-19 F1 search podfail arm (restart+carrier err)", ev6["per_leg"]["F1"]["restart_ok"] is True and ev6["per_leg"]["F1"]["carrier_err_ok"] is True)
check("双-19 F2 rq throttle present (regex rq pods)", ev6["per_leg"]["F2"]["throttle_present"] is True and ev6["per_leg"]["F2"]["throttle_max"] == 0.4)
check("双-19 disjoint_data_ok True (user samples present)", ev6.get("disjoint_data_ok") is True)
prof6 = R._build_fault_profile(fault6, "gw", "cat",
                               {"F1": iso(f1win6[0]), "F2": iso(f2win6[0])},
                               {"F1": iso(f1win6[1]), "F2": iso(f2win6[1])},
                               leg_pods={"search": "search-5c8-x", "review-query": "review-query-6b7f-abc"})
check("双-19 profile roots=[search,review-query]", prof6["root_cause_services"] == ["search", "review-query"])
check("双-19 profile F1=service_unavailable(podfail canon)", prof6["component_ground_truth"][0]["fault_type"] == "service_unavailable")

# ==========================================================================
# TEST 7 (S4-1b): 三-08 order_podfail_x_reviewquery_cpu_x_catalog_cpu — 期望 PASS
#   ★重点: catalog throttle 正则 _G2EXT_THROTTLE_RE 排除 catalog-gw(正反例)
# ==========================================================================
print("TEST 7: order_podfail_x_reviewquery_cpu_x_catalog_cpu (triple; catalog-gw regex exclusion)")
# (a) 正则直测(prom =~ 全锚定语义 = fullmatch)
_cat_re = R._G2EXT_THROTTLE_RE["catalog"]
check("regex: catalog-5f64d9-xyz MATCH", _re.fullmatch(_cat_re, "catalog-5f64d9-xyz") is not None)
check("regex: catalog-abc MATCH", _re.fullmatch(_cat_re, "catalog-abc") is not None)
check("regex: catalog-gw-7c9-abc NO match", _re.fullmatch(_cat_re, "catalog-gw-7c9-abc") is None)
check("regex: catalog-gw-x NO match", _re.fullmatch(_cat_re, "catalog-gw-x") is None)
# (b) 门级: catalog-gw pod throttle 0.9 不得混入 catalog 腿(应取 catalog-5f64d9 的 0.5)
fault7 = "order_podfail_x_reviewquery_cpu_x_catalog_cpu"
combo7 = R.G2EXT_COMBOS[fault7]
f1win7 = [T0 + timedelta(seconds=70), T0 + timedelta(seconds=110)]   # order podfail INNER
f2win7 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]   # rq cpu whole
f3win7 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]   # catalog cpu whole(载体见证=pricing)
ds7 = []
for off in (5, 10, 15):
    ds7 += [snap("order", off, True, 30), snap("review_query", off, True, 20),
            snap("pricing", off, True, 25), snap("user", off, True, 15)]
for off in (75, 90, 105):
    ds7 += [snap("order", off, False, 500), snap("review_query", off, True, 120),
            snap("pricing", off, True, 130), snap("user", off, True, 16)]
for off in (50, 60, 120, 130):
    ds7 += [snap("review_query", off, True, 115), snap("pricing", off, True, 125), snap("user", off, True, 16)]
stages7 = make_stages(ds7)
_THROTTLE_PODS = {"review-query-6b7f-abc": 0.4, "catalog-5f64d9-xyz": 0.5, "catalog-gw-7c9-abc": 0.9}
nsb7 = {"order-9d-x": 0, "review-query-6b7f-abc": 0, "catalog-5f64d9-xyz": 0, "catalog-gw-7c9-abc": 0, "user-p": 0}
nsa7 = {"order-9d-x": 1, "review-query-6b7f-abc": 0, "catalog-5f64d9-xyz": 0, "catalog-gw-7c9-abc": 0, "user-p": 0}
passed7, ev7 = R.multi_leg_retarget_gate(stages7, f1win7, f2win7, f3win7,
                                         R._parse_carriers(combo7["carriers"], item="i", user_token="u", cart_user="c"),
                                         fault7, combo7["legs"], combo7["disjoint"],
                                         ns_restarts_before=nsb7, ns_restarts_after=nsa7,
                                         checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-08 gate passed", passed7 is True)
check("三-08 F1 order podfail arm passed", ev7["per_leg"]["F1"]["arm_passed"] is True)
check("三-08 F3 catalog throttle=0.5 (catalog-gw 0.9 EXCLUDED)", ev7["per_leg"]["F3"]["throttle_max"] == 0.5)
check("三-08 per_root_F3 present", ev7.get("per_root_F3") is True)
prof7 = R._build_fault_profile(fault7, "gw", "cat",
                               {"F1": iso(f1win7[0]), "F2": iso(f2win7[0]), "F3": iso(f3win7[0])},
                               {"F1": iso(f1win7[1]), "F2": iso(f2win7[1]), "F3": iso(f3win7[1])},
                               leg_pods={"order": "order-9d-x", "review-query": "review-query-6b7f-abc",
                                         "catalog": "catalog-5f64d9-xyz"})
check("三-08 profile 3 roots", prof7["root_cause_services"] == ["order", "review-query", "catalog"])
contract7 = R.build_root_metric_contract(ev7, fault7)
check("三-08 contract valid F1&F2&F3", contract7["valid"] and contract7["F1"] and contract7["F2"] and contract7["F3"])

# ==========================================================================
# TEST 8 (S4-2): B2 修后 disjoint 零样本 fail-closed 回归
# ==========================================================================
print("TEST 8: disjoint zero-sample fail-closed (B2 regression)")
# (a) dj_n=0: 双-18 stages 但完全去掉 user(disjoint) snapshots → 旧代码 fail-open 恒过, 修后必 FAIL
f1win8 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f2win8 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
ds8 = []
for off in (5, 10, 15):
    ds8 += [snap("cart", off, True, 20), snap("order", off, True, 20)]
for off in (60, 80, 100):
    ds8 += [snap("cart", off, True, 100), snap("order", off, True, 110)]
stages8 = make_stages(ds8)
R.cfs_throttle_posthoc = _fake_throttle_pods
_THROTTLE_PODS = {"cart-abc-xyz": 0.5, "order-abc-xyz": 0.5}
nsb8 = {"cart-abc-xyz": 0, "order-abc-xyz": 0}
nsa8 = dict(nsb8)
passed8, ev8 = R.multi_leg_retarget_gate(stages8, f1win8, f2win8, [None, None],
                                         R._parse_carriers(combo1["carriers"], item="i", user_token="u", cart_user="c"),
                                         fault1, combo1["legs"], combo1["disjoint"],
                                         ns_restarts_before=nsb8, ns_restarts_after=nsa8,
                                         checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("dj_n=0 → gate FAILS (fail-closed)", passed8 is False)
check("dj_n=0 → disjoint_flat False + disjoint_data_ok False + n=0",
      ev8["disjoint_flat"] is False and ev8["disjoint_data_ok"] is False and ev8["disjoint_n"] == 0)
check("dj_n=0 → legs arms still pass (fail 只因 disjoint)", ev8["all_arms_pass"] is True)
# (b) dj_n=1(单样本地板): 仍 FAIL
ds8b = list(ds8) + [snap("user", 80, True, 16)]
stages8b = make_stages(ds8b)
passed8b, ev8b = R.multi_leg_retarget_gate(stages8b, f1win8, f2win8, [None, None],
                                           R._parse_carriers(combo1["carriers"], item="i", user_token="u", cart_user="c"),
                                           fault1, combo1["legs"], combo1["disjoint"],
                                           ns_restarts_before=nsb8, ns_restarts_after=nsa8,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("dj_n=1 (<MIN_SEP_SAMPLES=2) → still FAILS", passed8b is False and ev8b["disjoint_data_ok"] is False)
# (c) dj_n>=2 正例已由 TEST1/6/7 覆盖(disjoint_data_ok=True + passed)

# ==========================================================================
# TEST 9 (S4-3): B1 修后 summary 冒烟 — write_case offline(双-19 + 三-08)
#   双组 summary 不得含 M1 CFG+CFG 撒谎字样; 须含各腿服务名。
# ==========================================================================
print("TEST 9: write_case summary smoke (B1: honest g2ext summary)")
import tempfile
import types

R._resolve_pods = lambda *a, **k: []   # offline: 禁 kubectl 回退查询(返回空 → 回退 pod 名=svc, 不 crash)


def _mk_full_stage(name, start_off, end_off, snaps, leg_svcs):
    ws, we = iso(T0 + timedelta(seconds=start_off)), iso(T0 + timedelta(seconds=end_off))
    mrecs = []
    if name == "during_fault":
        for svc in leg_svcs:   # leg_pods 解析用: during 遥测唯一 pod
            mrecs.append({"ts": we, "source": "cadvisor", "entity_type": "pod",
                          "entity": f"{svc}-pod-1", "service": svc,
                          "metric": "container_cpu_usage_cores", "value": 0.1, "unit": "cores",
                          "metric_type": "gauge", "labels": {"pod": f"{svc}-pod-1"}})
    spans = ([{"start_time": iso(T0 + timedelta(seconds=(start_off + end_off) // 2)),
               "service": "search_service", "tags": []}] if name == "during_fault" else [])
    return {"stage": name, "window_start": ws, "window_end": we,
            "window_start_dt": T0 + timedelta(seconds=start_off), "window_end_dt": T0 + timedelta(seconds=end_off),
            "seconds": end_off - start_off, "poll_interval": 2,
            "expected_snapshots": 10, "observed_snapshots": 10,
            "snapshots": snaps, "probe_records": [], "metric_records": mrecs,
            "spans": spans, "logs": {"stub": "log line\n"},
            "traffic_stats": {"ok_ratio": 1.0}}


def _smoke_write_case(fault, ev, gate_passed_flag, f1w, f2w, f3w, leg_svcs, snaps):
    stg = [_mk_full_stage("pre_fault", 0, 30, [], leg_svcs),
           _mk_full_stage("during_fault", 40, 140, snaps, leg_svcs),
           _mk_full_stage("post_recovery", 200, 230, [], leg_svcs)]
    inj = {"F1": iso(f1w[0]), "F2": iso(f2w[0])}
    rec = {"F1": iso(f1w[1]), "F2": iso(f2w[1])}
    if f3w and all(f3w):
        inj["F3"], rec["F3"] = iso(f3w[0]), iso(f3w[1])
    args9 = types.SimpleNamespace(
        fault=fault, stage_seconds=100, poll=2, item="i", carriers=None,
        f2_offset_seconds=30.0, f2_duration_seconds=40.0, cat_delay_ms=None, inv_delay_ms=None,
        deep=True, user_token="u", cart_user_token="c", target_service="catalog")
    cdir = tempfile.mkdtemp(prefix=f"g2ext_smoke_{fault[:12]}_")
    R.write_case(cdir, f"smoke_{fault}", "run_smoke", args9, stg, gate_passed_flag, ev,
                 inj, rec, f1w, f2w, R.CHECKSUM_BASELINE, R.CHECKSUM_BASELINE, [],
                 fault=fault, f3win=(f3w or [None, None]))
    with open(os.path.join(cdir, "summary.md"), encoding="utf-8") as f:
        return f.read()


# 双-19 summary
sum6 = _smoke_write_case(fault6, ev6, True, f1win6, f2win6, None,
                         ["search", "review-query"], ds6)
check("双-19 summary 不含 client_timeout_too_short", "client_timeout_too_short" not in sum6)
check("双-19 summary 不含 retry_disabled", "retry_disabled" not in sum6)
check("双-19 summary 不含 trigger_amplifier 硬编码头", "nested trigger_amplifier" not in sum6)
check("双-19 summary 含两腿服务名", "search" in sum6 and "review-query" in sum6)
check("双-19 summary 标 dual_root + partial_overlap", "dual_root" in sum6 and "partial_overlap" in sum6.lower())
check("双-19 summary root-evidence header dual-root", "## root evidence (dual-root," in sum6)
check("双-19 summary 含 disjoint_data_ok 证据", "disjoint_data_ok" in sum6 or "data_ok" in sum6)
# 三-08 summary(N4 顺带: root points 从 per_leg 实填非空)
sum7 = _smoke_write_case(fault7, ev7, True, f1win7, f2win7, f3win7,
                         ["order", "review-query", "catalog"], ds7)
check("三-08 summary 不含 client_timeout_too_short", "client_timeout_too_short" not in sum7)
check("三-08 summary 不含 '(gate_evidence 无匹配 root 点)'(N4)", "无匹配 root 点" not in sum7)
check("三-08 summary 含三腿服务名", "order" in sum7 and "review-query" in sum7 and "catalog" in sum7)
check("三-08 summary 标 triple_root", "triple_root" in sum7)
check("三-08 summary root-evidence header triple-root", "## root evidence (triple-root," in sum7)
check("三-08 summary 含逐腿 restart/throttle 证据", "restart_delta" in sum7 and "throttle_max" in sum7)
# gate-fail warn 文案(不含 M1 catalog-bad 排障指引)
sum6f = _smoke_write_case(fault6, ev6, False, f1win6, f2win6, None,
                          ["search", "review-query"], ds6)
check("双-19 fail summary warn 是 g2ext 文案(含 multi_leg_retarget_gate)", "multi_leg_retarget_gate did NOT pass" in sum6f)
check("双-19 fail summary 不教人查 catalog-bad(M1 残留)", "catalog-bad" not in sum6f)

# ==========================================================================
# TEST 10 (Phase B): 双-20 recagent_cpu_x_backend_cpu — 期望 PASS + 五件套等价点
# ==========================================================================
print("TEST 10: recagent_cpu_x_backend_cpu (dual simultaneous 2×cpu; G1 五件套 g2ext 等价)")
fault10 = "recagent_cpu_x_backend_cpu"
combo10 = R.G2EXT_COMBOS[fault10]
f1win10 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f2win10 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
ds10 = []
for off in (5, 10, 15):
    ds10 += [snap(R.RECAGENT_GATE_CARRIER, off, True, 30), snap("backend", off, True, 20), snap("user", off, True, 15)]
for off in (60, 80, 100):
    # recagent_health/backend 慢(ratio ~5x); user 平; ★毒化 recagent_recommend(错误+50s)必须不进任何臂(gate 按 carrier_name 过滤等价)
    ds10 += [snap(R.RECAGENT_GATE_CARRIER, off, True, 150), snap("backend", off, True, 110),
             snap("user", off, True, 16), snap(R.RECAGENT_RECOMMEND_CARRIER, off, False, 50000)]
stages10 = make_stages(ds10)
_THROTTLE_PODS = {"rec-agent-6b7f-abc": 0.5, "backend-555d-x": 0.4}
nsb10 = {"rec-agent-6b7f-abc": 0, "backend-555d-x": 0, "user-p": 0}
nsa10 = dict(nsb10)
carr10 = R._parse_carriers(combo10["carriers"], item="i", user_token="u", cart_user="c")
passed10, ev10 = R.multi_leg_retarget_gate(stages10, f1win10, f2win10, [None, None], carr10,
                                           fault10, combo10["legs"], combo10["disjoint"],
                                           ns_restarts_before=nsb10, ns_restarts_after=nsa10,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-20 gate passed", passed10 is True)
check("双-20 F1 rec-agent 臂: throttle + recagent_health ratio 硬判",
      ev10["per_leg"]["F1"]["throttle_present"] is True and ev10["per_leg"]["F1"]["carrier_ratio_ok"] is True
      and ev10["per_leg"]["F1"]["carrier"] == R.RECAGENT_GATE_CARRIER)
check("双-20 recommend 毒化不进臂(all_arms_pass 仍 True)", ev10["all_arms_pass"] is True)
check("双-20 recommend 不是任何腿的 carrier(gate 过滤等价)",
      all(lg.get("carrier") != R.RECAGENT_RECOMMEND_CARRIER for lg in combo10["legs"]))
check("双-20 victim_set = rec-agent,backend", set(ev10["victim_set"]) == {"rec-agent", "backend"})
# 负例: rec-agent throttle 缺 → F1 臂 fail
_THROTTLE_PODS = {"backend-555d-x": 0.4}
passed10b, ev10b = R.multi_leg_retarget_gate(stages10, f1win10, f2win10, [None, None], carr10,
                                             fault10, combo10["legs"], combo10["disjoint"],
                                             ns_restarts_before=nsb10, ns_restarts_after=nsa10,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-20 负例: rec-agent throttle 缺 → gate FAIL + F1 臂 False",
      passed10b is False and ev10b["per_leg"]["F1"]["arm_passed"] is False)
_THROTTLE_PODS = {"rec-agent-6b7f-abc": 0.5, "backend-555d-x": 0.4}
prof10 = R._build_fault_profile(fault10, "gw", "cat",
                                {"F1": iso(f1win10[0]), "F2": iso(f2win10[0])},
                                {"F1": iso(f1win10[1]), "F2": iso(f2win10[1])},
                                leg_pods={"rec-agent": "rec-agent-6b7f-abc", "backend": "backend-555d-x"})
check("双-20 GT roots=[rec-agent,backend] G=2", prof10["root_cause_services"] == ["rec-agent", "backend"]
      and len(set(prof10["root_cause_services"])) == 2)
check("双-20 canon 双腿 service_cpu_saturation", all(c["fault_type"] == "service_cpu_saturation" for c in prof10["component_ground_truth"]))
check("双-20 affected=各腿 self(rec-agent 不拓扑推断, 五件套#4 等价)", prof10["affected_services"] == ["rec-agent", "backend"])
check("双-20 不在 TRIPLE_ROOT_FAULTS", fault10 not in R.TRIPLE_ROOT_FAULTS)
contract10 = R.build_root_metric_contract(ev10, fault10)
check("双-20 contract valid F1&F2 无 F3", contract10["valid"] and contract10["F1"] and contract10["F2"] and "F3" not in contract10)

# ==========================================================================
# TEST 11 (Phase B): 三-06 backend_cpu_x_sasrec_cpu_x_gw_netdelay — netdelay 新腿臂正反例
# ==========================================================================
print("TEST 11: backend_cpu_x_sasrec_cpu_x_gw_netdelay (triple simultaneous; netdelay arm)")
fault11 = "backend_cpu_x_sasrec_cpu_x_gw_netdelay"
combo11 = R.G2EXT_COMBOS[fault11]
f1win11 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f2win11 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f3win11 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]


def _mk_ds11(gw_in_ms=2100, ctl_in_ms=16, ctl_in_present=True):
    ds = []
    for off in (5, 10, 15):
        ds += [snap("pricing_direct", off, True, 25), snap("catalog_direct", off, True, 15),
               snap("backend", off, True, 20), snap("user", off, True, 15),
               snap("recommend_probe", off, True, 900)]
    for off in (60, 80, 100):
        ds += [snap("pricing_direct", off, True, gw_in_ms), snap("backend", off, True, 120),
               snap("user", off, True, 16), snap("recommend_probe", off, True, 5000)]
        if ctl_in_present:
            ds += [snap("catalog_direct", off, True, ctl_in_ms)]
    return ds


stages11 = make_stages(_mk_ds11())
_THROTTLE_PODS = {"backend-555d-x": 0.4, "sasrec-7c9-x": 0.6}
nsb11 = {"backend-555d-x": 0, "sasrec-7c9-x": 0, "catalog-gw-7c9-abc": 0, "user-p": 0}
nsa11 = dict(nsb11)
carr11 = R._parse_carriers(combo11["carriers"], item="i", user_token="u", cart_user="c", catalog_base="http://cb:5005")
passed11, ev11 = R.multi_leg_retarget_gate(stages11, f1win11, f2win11, f3win11, carr11,
                                           fault11, combo11["legs"], combo11["disjoint"],
                                           ns_restarts_before=nsb11, ns_restarts_after=nsa11,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-06 gate passed", passed11 is True)
check("三-06 F1 backend 臂硬判过", ev11["per_leg"]["F1"]["arm_passed"] is True and ev11["per_leg"]["F1"]["carrier_ratio_ok"] is True)
check("三-06 F2 sasrec 臂: throttle 硬判 + carrier SOFT(recommend 不二分)",
      ev11["per_leg"]["F2"]["arm_passed"] is True and ev11["per_leg"]["F2"]["carrier_hard"] is False
      and ev11["per_leg"]["F2"]["throttle_max"] == 0.6)
check("三-06 F3 netdelay 臂: gw 绝对位移>=800 + control 平",
      ev11["per_leg"]["F3"]["arm_passed"] is True and ev11["per_leg"]["F3"]["gw_shift_ok"] is True
      and ev11["per_leg"]["F3"]["control_flat"] is True and ev11["per_leg"]["F3"]["gw_p95_shift_ms"] >= 800)
check("三-06 per_root_F3 present", ev11.get("per_root_F3") is True)
check("三-06 rule 带 netdelay 臂说明(条件拼接)", "netdelay 腿臂" in ev11["rule"])
check("三-06 victim_set 含 catalog-gw", "catalog-gw" in ev11["victim_set"])
# 负例 a: gw 位移不足(<800) → F3 fail
passed11a, ev11a = R.multi_leg_retarget_gate(make_stages(_mk_ds11(gw_in_ms=300)), f1win11, f2win11, f3win11, carr11,
                                             fault11, combo11["legs"], combo11["disjoint"],
                                             ns_restarts_before=nsb11, ns_restarts_after=nsa11,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-06 负例a: gw 位移<800 → gate FAIL + F3 臂 False",
      passed11a is False and ev11a["per_leg"]["F3"]["arm_passed"] is False and ev11a["per_leg"]["F3"]["gw_shift_ok"] is False)
# 负例 b: control 也位移>=800(catalog 自身劣化嫌疑) → control_flat False → F3 fail
passed11b, ev11b = R.multi_leg_retarget_gate(make_stages(_mk_ds11(ctl_in_ms=2000)), f1win11, f2win11, f3win11, carr11,
                                             fault11, combo11["legs"], combo11["disjoint"],
                                             ns_restarts_before=nsb11, ns_restarts_after=nsa11,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-06 负例b: control 位移>=800 → gate FAIL + control_flat False",
      passed11b is False and ev11b["per_leg"]["F3"]["control_flat"] is False)
# 负例 c: control 腿窗零样本 → fail-closed
passed11c, ev11c = R.multi_leg_retarget_gate(make_stages(_mk_ds11(ctl_in_present=False)), f1win11, f2win11, f3win11, carr11,
                                             fault11, combo11["legs"], combo11["disjoint"],
                                             ns_restarts_before=nsb11, ns_restarts_after=nsa11,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-06 负例c: control 零样本 → fail-closed(control_flat False)",
      passed11c is False and ev11c["per_leg"]["F3"]["control_flat"] is False and ev11c["per_leg"]["F3"]["control_n"] == 0)
# 负例 d: sasrec throttle 缺 → F2 臂 fail(recommend soft 不能救)
_THROTTLE_PODS = {"backend-555d-x": 0.4}
passed11d, ev11d = R.multi_leg_retarget_gate(stages11, f1win11, f2win11, f3win11, carr11,
                                             fault11, combo11["legs"], combo11["disjoint"],
                                             ns_restarts_before=nsb11, ns_restarts_after=nsa11,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-06 负例d: sasrec throttle 缺 → gate FAIL + F2 臂 False",
      passed11d is False and ev11d["per_leg"]["F2"]["arm_passed"] is False)
_THROTTLE_PODS = {"backend-555d-x": 0.4, "sasrec-7c9-x": 0.6}
prof11 = R._build_fault_profile(fault11, "gw-pod-1", "cat",
                                {"F1": iso(f1win11[0]), "F2": iso(f2win11[0]), "F3": iso(f3win11[0])},
                                {"F1": iso(f1win11[1]), "F2": iso(f2win11[1]), "F3": iso(f3win11[1])},
                                leg_pods={"backend": "backend-555d-x", "sasrec": "sasrec-7c9-x",
                                          "catalog-gw": "catalog-gw-7c9-abc"})
check("三-06 GT roots=[backend,sasrec,catalog-gw] G=3", prof11["root_cause_services"] == ["backend", "sasrec", "catalog-gw"]
      and len(set(prof11["root_cause_services"])) == 3)
check("三-06 canon: F3 network_delay + NetworkChaos",
      prof11["component_ground_truth"][2]["fault_type"] == "network_delay"
      and prof11["component_ground_truth"][2]["crd"] == "NetworkChaos"
      and prof11["component_ground_truth"][2]["injection_fault"] == "network_delay_injected")
check("三-06 canon: F1/F2 service_cpu_saturation",
      prof11["component_ground_truth"][0]["fault_type"] == "service_cpu_saturation"
      and prof11["component_ground_truth"][1]["fault_type"] == "service_cpu_saturation")
check("三-06 sasrec 腿 intensity workers=8(如实, 非全局 2)",
      prof11["component_ground_truth"][1]["intensity"]["workers"] == 8)
check("三-06 in TRIPLE_ROOT_FAULTS", fault11 in R.TRIPLE_ROOT_FAULTS)
contract11 = R.build_root_metric_contract(ev11, fault11)
check("三-06 contract valid F1&F2&F3", contract11["valid"] and contract11["F1"] and contract11["F2"] and contract11["F3"])
# CRD 渲染: sasrec 腿 workers=8 + duration 抬升(cpu_workers 参数)
import tempfile as _tf
_rdir = _tf.mkdtemp(prefix="g2ext_render_")
_ry, _rc, _rk = R._render_retarget_crd("service_cpu_single", "sasrec", _rdir, duration_s=660, cpu_workers=8)
with open(_ry, encoding="utf-8") as _f:
    _rbody = _f.read()
check("sasrec 渲染 CRD: workers: 8", "workers: 8" in _rbody)
check("sasrec 渲染 CRD: duration 660s", 'duration: "660s"' in _rbody)
check("sasrec 渲染 CRD: app: sasrec", "app: sasrec" in _rbody)
check("sasrec 渲染 CRD: 无 limit=500m 假话", "limit=500m" not in _rbody)
# 默认路径零回归: cpu_workers=None → workers: 2 + 原 500m 注释
_ry2, _, _ = R._render_retarget_crd("service_cpu_single", "order", _rdir, duration_s=660)
with open(_ry2, encoding="utf-8") as _f:
    _rbody2 = _f.read()
check("默认渲染零回归: workers: 2 + 500m 注释保留", "workers: 2" in _rbody2 and "cpu limit=500m" in _rbody2)

# ==========================================================================
# TEST 12 (Phase B): summary/metadata 冒烟 — 双-20 + 三-06
# ==========================================================================
print("TEST 12: write_case summary/metadata smoke (Phase B)")


def _smoke_case_dir(fault, ev, gate_passed_flag, f1w, f2w, f3w, leg_svcs, snaps):
    stg = [_mk_full_stage("pre_fault", 0, 30, [], leg_svcs),
           _mk_full_stage("during_fault", 40, 140, snaps, leg_svcs),
           _mk_full_stage("post_recovery", 200, 230, [], leg_svcs)]
    inj = {"F1": iso(f1w[0]), "F2": iso(f2w[0])}
    rec = {"F1": iso(f1w[1]), "F2": iso(f2w[1])}
    if f3w and all(f3w):
        inj["F3"], rec["F3"] = iso(f3w[0]), iso(f3w[1])
    args12 = types.SimpleNamespace(
        fault=fault, stage_seconds=100, poll=2, item="i", carriers=None,
        f2_offset_seconds=30.0, f2_duration_seconds=40.0, cat_delay_ms=None, inv_delay_ms=None,
        deep=True, user_token="u", cart_user_token="c", target_service="catalog")
    cdir = tempfile.mkdtemp(prefix=f"g2ext_smokeB_{fault[:12]}_")
    R.write_case(cdir, f"smoke_{fault}", "run_smoke", args12, stg, gate_passed_flag, ev,
                 inj, rec, f1w, f2w, R.CHECKSUM_BASELINE, R.CHECKSUM_BASELINE, [],
                 fault=fault, f3win=(f3w or [None, None]))
    return cdir


import json as _json
# 双-20
cdir10 = _smoke_case_dir(fault10, ev10, True, f1win10, f2win10, None, ["rec-agent", "backend"], ds10)
with open(os.path.join(cdir10, "summary.md"), encoding="utf-8") as f:
    sum10 = f.read()
check("双-20 summary 标 dual_root + 含两腿服务名", "dual_root" in sum10 and "rec-agent" in sum10 and "backend" in sum10)
check("双-20 summary 不含 M1 虚构(client_timeout/retry_disabled)", "client_timeout_too_short" not in sum10 and "retry_disabled" not in sum10)
check("双-20 summary 含 G1 定式声明(gate 只吃 recagent_health)", "recagent_health" in sum10)
check("双-20 summary 含 DeepSeek 不废 case 声明", "不废 case" in sum10)
with open(os.path.join(cdir10, "metadata.json"), encoding="utf-8") as f:
    meta10 = _json.load(f)
check("双-20 metadata 溯源: recagent_gate_carrier(五件套#5 等价)",
      meta10["config"].get("recagent_gate_carrier") == R.RECAGENT_GATE_CARRIER
      and meta10["config"].get("recagent_recommend_carrier") == R.RECAGENT_RECOMMEND_CARRIER)
check("双-20 metadata root_count=2", meta10.get("root_count") == 2)
# 三-06
cdir11 = _smoke_case_dir(fault11, ev11, True, f1win11, f2win11, f3win11,
                         ["backend", "sasrec", "catalog-gw"], _mk_ds11())
with open(os.path.join(cdir11, "summary.md"), encoding="utf-8") as f:
    sum11 = f.read()
check("三-06 summary 标 triple_root + 三腿服务名", "triple_root" in sum11 and "backend" in sum11
      and "sasrec" in sum11 and "catalog-gw" in sum11)
check("三-06 summary 含 netdelay 臂证据(gw_p95_shift_ms)", "gw_p95_shift_ms" in sum11)
check("三-06 summary 含 network_delay 腿标注", "network_delay" in sum11)
check("三-06 summary 含 recommend 不二分声明(审计 B5)", "不二分" in sum11 or "无法二分" in sum11)
check("三-06 summary 不含 '(gate_evidence 无匹配 root 点)'", "无匹配 root 点" not in sum11)
with open(os.path.join(cdir11, "metadata.json"), encoding="utf-8") as f:
    meta11 = _json.load(f)
check("三-06 metadata root_count=3", meta11.get("root_count") == 3)
check("三-06 metadata g2ext_legs sasrec 带 workers=8",
      any(lg.get("svc") == "sasrec" and lg.get("workers") == 8 for lg in meta11["config"].get("g2ext_legs", [])))
check("三-06 metadata netdelay 溯源键(net_iso_margin_ms)", meta11["config"].get("net_iso_margin_ms") == R.NET_ISO_MARGIN_MS)
_gt11 = _json.load(open(os.path.join(cdir11, "groundtruth.json"), encoding="utf-8")) if os.path.exists(os.path.join(cdir11, "groundtruth.json")) else None
if _gt11 is not None:
    check("三-06 groundtruth roots G=3 异服务", len(set(_gt11.get("root_cause_services", []))) == 3)

# ==========================================================================
# TEST 13 (Phase C): 双-17 checkout_podfail_x_inv_latency — invlat 新腿臂 + staggered + churn 豁免
# ==========================================================================
print("TEST 13: checkout_podfail_x_inv_latency (dual staggered; podfail + invlat)")
fault13 = "checkout_podfail_x_inv_latency"
combo13 = R.G2EXT_COMBOS[fault13]
check("双-17 arity 2, timing staggered", combo13["arity"] == 2 and combo13["timing"] == "staggered")
check("双-17 legs = checkout podfail(F1) + inventory invlat(F2)",
      combo13["legs"][0]["kind"] == "podfail" and combo13["legs"][0]["svc"] == "checkout"
      and combo13["legs"][1]["kind"] == "invlat" and combo13["legs"][1]["svc"] == "inventory")
# ★时序契约(Phase C 编排 fix): staggered 由 set-env 注入点后移实现 —— invlat set-env 挪到【podfail 子窗 recover 之后】
#   (NOT during 起点 fire-forget)。故 injected_at[F2](=inventory-slow 窗起)必落在 checkout podfail 子窗 recover 之后:
#   base 窗(pre_fault + checkout podfail 子窗期)inventory 必须干净(set-env 尚未注)→ baseline_p95_ms 低 → shift~2000ms >800 门。
#   旧编排 bug: set-env 于 during 起点 fire-forget + 单节点 rollout ~4s → inventory 从 during 头就慢 → base 桶被污染
#   (base_p95~2026ms) → shift≈0 <800 → invlat 臂 fail-closed。本 case 数据即按 fixed 时序构造(base inventory 全快)。
# staggered 窗: F1 checkout 早子窗 [40,80]; F2 inventory 晚窗 [120,260]（错峰, 不重叠, F2 窗起在 podfail recover 后）
f1win13 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=80)]
f2win13 = [T0 + timedelta(seconds=120), T0 + timedelta(seconds=260)]
during13 = {"stage": "during_fault", "snapshots": [],
            "window_start_dt": T0 + timedelta(seconds=35), "window_end_dt": T0 + timedelta(seconds=270)}
_ds13 = []
# baseline(<40): 全快/全 ok; checkout F1 子窗(40..80): checkout 503 error burst; inventory F2 晚窗(120..260): inventory_direct 慢 2000ms
for off in (10, 20, 30):
    _ds13 += [snap("checkout", off, True, 40), snap("inventory_direct", off, True, 15), snap("user", off, True, 15)]
for off in (45, 55, 65, 75):   # F1_only(checkout podfail 子窗): checkout down, inventory 仍快(set-env 尚未注→干净 base; 时序 fix 保证)
    _ds13 += [snap("checkout", off, False, 3000), snap("inventory_direct", off, True, 16), snap("user", off, True, 15)]
for off in (130, 160, 190, 220, 250):   # F2 晚窗: inventory 慢, checkout 已回
    _ds13 += [snap("checkout", off, True, 42), snap("inventory_direct", off, True, 2100), snap("user", off, True, 16)]
during13["snapshots"] = _ds13
pre13 = {"stage": "pre_fault", "snapshots": [], "window_start_dt": T0, "window_end_dt": T0 + timedelta(seconds=30)}
post13 = {"stage": "post_recovery", "snapshots": [], "window_start_dt": T0 + timedelta(seconds=280),
          "window_end_dt": T0 + timedelta(seconds=310)}
stages13 = [pre13, during13, post13]
_THROTTLE_PODS = {}   # 双-17 无 cpu 腿 → throttle 不相关
# checkout podfail restart_delta=2(合法签名); inventory rollout 新 pod(before=absent, after present 0 restart → 不 churn; 且豁免)
nsb13 = {"checkout-abc-1": 0, "inventory-old-1": 0, "cart-p": 0}
nsa13 = {"checkout-abc-1": 2, "inventory-new-2": 0, "cart-p": 0}   # checkout +2; inventory 换 pod(rollout)
carr13 = R._parse_carriers(combo13["carriers"], item="i", user_token="u", cart_user="c")
passed13, ev13 = R.multi_leg_retarget_gate(stages13, f1win13, f2win13, [None, None], carr13,
                                           fault13, combo13["legs"], combo13["disjoint"],
                                           ns_restarts_before=nsb13, ns_restarts_after=nsa13,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-17 gate passed", passed13 is True)
check("双-17 F1 checkout podfail 臂: restart_delta=2 + error burst 硬判",
      ev13["per_leg"]["F1"]["arm_passed"] is True and ev13["per_leg"]["F1"]["restart_delta"] == 2
      and ev13["per_leg"]["F1"]["carrier_err_ok"] is True)
check("双-17 F2 inventory invlat 臂: 绝对位移>=800 + inv_shift_ok",
      ev13["per_leg"]["F2"]["arm_passed"] is True and ev13["per_leg"]["F2"]["inv_shift_ok"] is True
      and ev13["per_leg"]["F2"]["inv_p95_shift_ms"] >= 800)
# ★时序契约断言(Phase C 编排 fix 回归护栏): F2 invlat 腿 base 窗必须干净 —— set-env 挪到 podfail recover 之后注,
#   故 base 桶(pre_fault + podfail 子窗期 inventory 快样本)baseline_p95_ms 必须低(~16ms, 无 2000ms 污染)。
#   若旧 fire-forget 编排回归(set-env 于 during 起点)则 base 桶掺 inventory-slow 样本 → baseline_p95_ms 飙至 ~2000ms
#   → shift≈0 <800 → 此断言 + 上面 inv_shift_ok 同时 FAIL(双护栏钉死时序契约)。
check("双-17 时序契约: invlat F2 base 窗干净(baseline_p95_ms<100 = set-env 在 podfail recover 后注, base 未污染)",
      ev13["per_leg"]["F2"]["baseline_p95_ms"] is not None and ev13["per_leg"]["F2"]["baseline_p95_ms"] < 100)
check("双-17 inventory rollout pod 不算 churn(control_plane_healthy)", ev13["control_plane_healthy"] is True)
# ★review S5: 豁免正例必须真走到豁免分支——上面 mock inventory delta=0 是空转; 这里 delta=1(rollout 中容器真重启)
#   仍须 healthy(豁免生效), 而同 delta 的 cart 会 unhealthy(负例 c 对照)= 豁免既生效又不过宽。
nsb13e = {"checkout-abc-1": 0, "inventory-old-1": 0, "cart-p": 0}
nsa13e = {"checkout-abc-1": 2, "inventory-old-1": 1, "cart-p": 0}   # inventory 同 pod delta=1 → 走豁免分支
_p13e, ev13e = R.multi_leg_retarget_gate(stages13, f1win13, f2win13, [None, None], carr13,
                                         fault13, combo13["legs"], combo13["disjoint"],
                                         ns_restarts_before=nsb13e, ns_restarts_after=nsa13e,
                                         checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-17 inventory delta=1 真走豁免分支仍 healthy(S5)", ev13e["control_plane_healthy"] is True and _p13e is True)
check("双-17 rule 带 invlat 臂说明(条件拼接)", "invlat 腿臂" in ev13["rule"])
check("双-17 victim_set = checkout,inventory", set(ev13["victim_set"]) == {"checkout", "inventory"})
# 负例 a: inventory 位移不足(<800) → F2 fail
_ds13a = [s for s in _ds13 if s["carrier_name"] != "inventory_direct"]
for off in (130, 160, 190, 220, 250):
    _ds13a.append(snap("inventory_direct", off, True, 300))   # 慢不到位
for off in (10, 20, 30):
    _ds13a.append(snap("inventory_direct", off, True, 15))
during13a = dict(during13, snapshots=_ds13a)
passed13a, ev13a = R.multi_leg_retarget_gate([pre13, during13a, post13], f1win13, f2win13, [None, None], carr13,
                                             fault13, combo13["legs"], combo13["disjoint"],
                                             ns_restarts_before=nsb13, ns_restarts_after=nsa13,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-17 负例a: inventory 位移<800 → gate FAIL + F2 臂 False",
      passed13a is False and ev13a["per_leg"]["F2"]["arm_passed"] is False)
# 负例 b: inventory F2 窗零样本 → fail-closed
_ds13b = [s for s in _ds13 if s["carrier_name"] != "inventory_direct"]
for off in (10, 20, 30):
    _ds13b.append(snap("inventory_direct", off, True, 15))   # 只 baseline, F2 窗空
during13b = dict(during13, snapshots=_ds13b)
passed13b, ev13b = R.multi_leg_retarget_gate([pre13, during13b, post13], f1win13, f2win13, [None, None], carr13,
                                             fault13, combo13["legs"], combo13["disjoint"],
                                             ns_restarts_before=nsb13, ns_restarts_after=nsa13,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-17 负例b: inventory F2 窗零样本 → fail-closed(inv_shift_ok False)",
      passed13b is False and ev13b["per_leg"]["F2"]["inv_shift_ok"] is False and ev13b["per_leg"]["F2"]["inwin_n"] == 0)
# 负例 c: 非-腿非-inventory pod churn(cart 重启)→ control_plane unhealthy
nsa13c = dict(nsa13); nsa13c["cart-p"] = 1
passed13c, ev13c = R.multi_leg_retarget_gate(stages13, f1win13, f2win13, [None, None], carr13,
                                             fault13, combo13["legs"], combo13["disjoint"],
                                             ns_restarts_before=nsb13, ns_restarts_after=nsa13c,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("双-17 负例c: cart 非-腿 churn → gate FAIL + control_plane unhealthy",
      passed13c is False and ev13c["control_plane_healthy"] is False)
# profile + contract
prof13 = R._build_fault_profile(fault13, "gw", "cat",
                                {"F1": iso(f1win13[0]), "F2": iso(f2win13[0])},
                                {"F1": iso(f1win13[1]), "F2": iso(f2win13[1])},
                                leg_pods={"checkout": "checkout-abc-1", "inventory": "inventory-new-2"})
check("双-17 GT roots=[checkout,inventory] G=2", prof13["root_cause_services"] == ["checkout", "inventory"]
      and len(set(prof13["root_cause_services"])) == 2)
check("双-17 canon: F1 service_unavailable(podfail) + F2 dependency_latency(invlat)",
      prof13["component_ground_truth"][0]["fault_type"] == "service_unavailable"
      and prof13["component_ground_truth"][1]["fault_type"] == "dependency_latency"
      and prof13["component_ground_truth"][1]["injection_fault"] == "dependency_latency_injected")
check("双-17 invlat 腿 chaos_engine=app_env_hook(非 chaosmesh)",
      prof13["component_ground_truth"][1]["chaos_engine"].startswith("app_env_hook"))
check("双-17 affected=self [checkout,inventory]", prof13["affected_services"] == ["checkout", "inventory"])
contract13 = R.build_root_metric_contract(ev13, fault13)
check("双-17 contract valid F1&F2 无 F3", contract13["valid"] and contract13["F1"] and contract13["F2"] and "F3" not in contract13)

# ==========================================================================
# TEST 14 (Phase C): 三-07 recagent_netdelay_x_sasrec_cpu_x_catalog_podfail — netdelay 无 control + 五件套
# ==========================================================================
print("TEST 14: recagent_netdelay_x_sasrec_cpu_x_catalog_podfail (triple partial_overlap; rec-agent netdelay 无 control)")
fault14 = "recagent_netdelay_x_sasrec_cpu_x_catalog_podfail"
combo14 = R.G2EXT_COMBOS[fault14]
check("三-07 arity 3, timing partial_overlap", combo14["arity"] == 3 and combo14["timing"] == "partial_overlap")
check("三-07 F1 rec-agent netdelay 渲染腿(有 net_delay_ms=450, 无 control)",
      combo14["legs"][0]["kind"] == "netdelay" and combo14["legs"][0]["net_delay_ms"] == 450
      and "control" not in combo14["legs"][0])
check("三-07 post_probe=recommend_probe(sasrec 腿证据)", combo14["post_probe"] == "recommend_probe")
# F1 rec-agent netdelay + F2 sasrec cpu 整 during co-active; F3 catalog podfail 子窗
f1win14 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f2win14 = [T0 + timedelta(seconds=40), T0 + timedelta(seconds=140)]
f3win14 = [T0 + timedelta(seconds=70), T0 + timedelta(seconds=110)]   # catalog podfail INNER 子窗


def _mk_ds14(recagent_in_ms=1050, cat_err=False):
    ds = []
    for off in (5, 10, 15):   # baseline
        ds += [snap(R.RECAGENT_GATE_CARRIER, off, True, 20), snap("catalog_direct", off, True, 15),
               snap("user", off, True, 15), snap("recommend_probe", off, True, 900)]
    for off in (50, 60, 120, 130):   # F1F2 (netdelay+cpu 整窗; catalog 子窗外 → catalog_direct 快-200)
        ds += [snap(R.RECAGENT_GATE_CARRIER, off, True, recagent_in_ms), snap("catalog_direct", off, True, 16),
               snap("user", off, True, 16), snap("recommend_probe", off, True, 5000)]
    for off in (75, 85, 95, 105):   # F3 子窗: catalog podfail → catalog_direct 000/error
        ds += [snap(R.RECAGENT_GATE_CARRIER, off, True, recagent_in_ms),
               snap("catalog_direct", off, False, 0), snap("user", off, True, 16),
               snap("recommend_probe", off, True, 5200)]
    return ds


# 用自定 during 窗(catalog podfail 子窗需在 during 内)
pre14 = {"stage": "pre_fault", "snapshots": [], "window_start_dt": T0, "window_end_dt": T0 + timedelta(seconds=30)}
during14 = {"stage": "during_fault", "snapshots": _mk_ds14(),
            "window_start_dt": T0 + timedelta(seconds=35), "window_end_dt": T0 + timedelta(seconds=140)}
post14 = {"stage": "post_recovery", "snapshots": [], "window_start_dt": T0 + timedelta(seconds=200),
          "window_end_dt": T0 + timedelta(seconds=230)}
stages14 = [pre14, during14, post14]
_THROTTLE_PODS = {"sasrec-7c9-x": 0.6}   # sasrec cpu 腿 throttle(rec-agent netdelay / catalog podfail 无 throttle)
nsb14 = {"rec-agent-6b7f-abc": 0, "sasrec-7c9-x": 0, "catalog-5f64d9-xyz": 0, "user-p": 0}
nsa14 = {"rec-agent-6b7f-abc": 0, "sasrec-7c9-x": 0, "catalog-5f64d9-xyz": 2, "user-p": 0}   # catalog podfail +2
carr14 = R._parse_carriers(combo14["carriers"], item="i", user_token="u", cart_user="c", catalog_base="http://cb:5005")
passed14, ev14 = R.multi_leg_retarget_gate(stages14, f1win14, f2win14, f3win14, carr14,
                                           fault14, combo14["legs"], combo14["disjoint"],
                                           ns_restarts_before=nsb14, ns_restarts_after=nsa14,
                                           checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-07 gate passed", passed14 is True)
check("三-07 F1 rec-agent netdelay 臂: gw 绝对位移>=800 + 无 control(control_carrier None)",
      ev14["per_leg"]["F1"]["arm_passed"] is True and ev14["per_leg"]["F1"]["gw_shift_ok"] is True
      and ev14["per_leg"]["F1"]["control_carrier"] is None and ev14["per_leg"]["F1"]["gw_p95_shift_ms"] >= 800)
check("三-07 F2 sasrec 臂: throttle 硬判 + carrier(recommend_probe)SOFT",
      ev14["per_leg"]["F2"]["arm_passed"] is True and ev14["per_leg"]["F2"]["carrier_hard"] is False
      and ev14["per_leg"]["F2"]["throttle_max"] == 0.6)
check("三-07 F3 catalog podfail 臂: restart_delta=2 + catalog_direct 000 error burst",
      ev14["per_leg"]["F3"]["arm_passed"] is True and ev14["per_leg"]["F3"]["restart_delta"] == 2
      and ev14["per_leg"]["F3"]["carrier_err_ok"] is True)
check("三-07 per_root_F3 present", ev14.get("per_root_F3") is True)
check("三-07 catalog podfail target 豁免后 control_plane_healthy", ev14["control_plane_healthy"] is True)
# 负例 a: rec-agent netdelay 位移不足 → F1 fail
passed14a, ev14a = R.multi_leg_retarget_gate([pre14, dict(during14, snapshots=_mk_ds14(recagent_in_ms=200)), post14],
                                             f1win14, f2win14, f3win14, carr14, fault14, combo14["legs"], combo14["disjoint"],
                                             ns_restarts_before=nsb14, ns_restarts_after=nsa14,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-07 负例a: rec-agent 位移<800 → gate FAIL + F1 臂 False",
      passed14a is False and ev14a["per_leg"]["F1"]["arm_passed"] is False)
# 负例 b: sasrec throttle 缺 → F2 fail(recommend soft 不能救)
_THROTTLE_PODS = {}
passed14b, ev14b = R.multi_leg_retarget_gate(stages14, f1win14, f2win14, f3win14, carr14,
                                             fault14, combo14["legs"], combo14["disjoint"],
                                             ns_restarts_before=nsb14, ns_restarts_after=nsa14,
                                             checksum_pre=R.CHECKSUM_BASELINE, checksum_post=R.CHECKSUM_BASELINE)
check("三-07 负例b: sasrec throttle 缺 → gate FAIL + F2 臂 False",
      passed14b is False and ev14b["per_leg"]["F2"]["arm_passed"] is False)
_THROTTLE_PODS = {"sasrec-7c9-x": 0.6}
prof14 = R._build_fault_profile(fault14, "gw", "cat",
                                {"F1": iso(f1win14[0]), "F2": iso(f2win14[0]), "F3": iso(f3win14[0])},
                                {"F1": iso(f1win14[1]), "F2": iso(f2win14[1]), "F3": iso(f3win14[1])},
                                leg_pods={"rec-agent": "rec-agent-6b7f-abc", "sasrec": "sasrec-7c9-x",
                                          "catalog": "catalog-5f64d9-xyz"})
check("三-07 GT roots=[rec-agent,sasrec,catalog] G=3", prof14["root_cause_services"] == ["rec-agent", "sasrec", "catalog"]
      and len(set(prof14["root_cause_services"])) == 3)
check("三-07 canon: F1 network_delay + F2 service_cpu_saturation + F3 service_unavailable",
      prof14["component_ground_truth"][0]["fault_type"] == "network_delay"
      and prof14["component_ground_truth"][1]["fault_type"] == "service_cpu_saturation"
      and prof14["component_ground_truth"][2]["fault_type"] == "service_unavailable")
check("三-07 rec-agent 渲染腿 intensity delay_ms=450(如实, 非静态 500)",
      prof14["component_ground_truth"][0]["intensity"]["delay_ms"] == 450)
check("三-07 sasrec 腿 intensity workers=8", prof14["component_ground_truth"][1]["intensity"]["workers"] == 8)
check("三-07 affected=self [rec-agent,sasrec,catalog](五件套#4 等价)",
      prof14["affected_services"] == ["rec-agent", "sasrec", "catalog"])
check("三-07 in TRIPLE_ROOT_FAULTS", fault14 in R.TRIPLE_ROOT_FAULTS)
contract14 = R.build_root_metric_contract(ev14, fault14)
check("三-07 contract valid F1&F2&F3", contract14["valid"] and contract14["F1"] and contract14["F2"] and contract14["F3"])
# rec-agent netdelay 渲染 CRD: net-delay-rec-agent + app: recommendation_agent + delay 450ms
_ry14, _rc14, _rk14 = R._render_retarget_crd("net_delay_single", "rec-agent", _rdir, net_delay_ms=450, net_jitter_ms=90)
with open(_ry14, encoding="utf-8") as _f:
    _rbody14 = _f.read()
check("rec-agent netdelay 渲染: latency 450ms", 'latency: "450ms"' in _rbody14)
check("rec-agent netdelay 渲染: jitter 90ms", 'jitter: "90ms"' in _rbody14)
check("rec-agent netdelay 渲染: app: recommendation_agent(RETARGET_APP_LABEL)", "app: recommendation_agent" in _rbody14)
check("rec-agent netdelay 渲染: metadata.name net-delay-rec-agent", _rc14 == "net-delay-rec-agent")

# ==========================================================================
# TEST 15 (Phase C): summary/metadata 冒烟 — 双-17 + 三-07
# ==========================================================================
print("TEST 15: write_case summary/metadata smoke (Phase C)")
# 双-17
cdir13 = _smoke_case_dir(fault13, ev13, True, f1win13, f2win13, None, ["checkout", "inventory"], _ds13)
with open(os.path.join(cdir13, "summary.md"), encoding="utf-8") as f:
    sum13 = f.read()
check("双-17 summary 标 dual_root + staggered", "dual_root" in sum13 and "staggered" in sum13.lower())
check("双-17 summary 含两腿服务名", "checkout" in sum13 and "inventory" in sum13)
check("双-17 summary 含 invlat 臂证据(inv_p95_shift_ms)", "inv_p95_shift_ms" in sum13)
check("双-17 summary 含 dependency_latency 腿标注", "dependency_latency" in sum13)
check("双-17 summary 不含 M1 虚构", "client_timeout_too_short" not in sum13 and "retry_disabled" not in sum13)
with open(os.path.join(cdir13, "metadata.json"), encoding="utf-8") as f:
    meta13 = _json.load(f)
check("双-17 metadata root_count=2 + invlat 溯源键(inv_latency_margin_ms)",
      meta13.get("root_count") == 2 and meta13["config"].get("inv_latency_margin_ms") == R.INV_LATENCY_MARGIN_MS)
# 三-07
cdir14 = _smoke_case_dir(fault14, ev14, True, f1win14, f2win14, f3win14,
                         ["rec-agent", "sasrec", "catalog"], _mk_ds14())
with open(os.path.join(cdir14, "summary.md"), encoding="utf-8") as f:
    sum14 = f.read()
check("三-07 summary 标 triple_root + 三腿服务名", "triple_root" in sum14 and "rec-agent" in sum14
      and "sasrec" in sum14 and "catalog" in sum14)
check("三-07 summary 含 rec-agent netdelay 无 control 声明", "无 control 臂" in sum14)
check("三-07 summary 含 rec-agent 五件套声明(recagent_health)", "recagent_health" in sum14)
check("三-07 summary 不含 '(gate_evidence 无匹配 root 点)'", "无匹配 root 点" not in sum14)
with open(os.path.join(cdir14, "metadata.json"), encoding="utf-8") as f:
    meta14 = _json.load(f)
check("三-07 metadata root_count=3 + netdelay 渲染溯源(net_delay_ms=450)",
      meta14.get("root_count") == 3 and meta14["config"].get("net_delay_ms") == 450)
check("三-07 metadata recagent 溯源(recagent_gate_carrier)",
      meta14["config"].get("recagent_gate_carrier") == R.RECAGENT_GATE_CARRIER)

print(f"\n==== RESULT: {PASS} passed, {FAIL} failed ====")
sys.exit(1 if FAIL else 0)
