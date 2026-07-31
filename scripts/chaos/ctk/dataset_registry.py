# -*- coding: utf-8 -*-
"""dataset_registry.py — datasets/ 的唯一入口。

★ 存在的理由(2026-07-13,吃过亏才写的):
  在此之前,每个脚本都自己 glob '(native trees) *_dense/*_reps_v19/*',
  于是:
    - 加一族数据(single_spread)要改 N 个脚本,漏一个就静默少算 55 个 case;
      实测 m9_eadro_adapter.py:475 写死 "{}_dense".format(arity) —— 它【看不见 spread】。
    - 打包往 native case 里写 mr2/,逼得 4 个脚本各自写防御性 skip。
    - 交付树用后缀区分版本(_140_20260713 vs _..._gtfix),两棵都在,一棵 GT 是错的
      —— 差一点把错 GT 的包发给指导老师。

  ⇒ 现在:datasets/REGISTRY.json 是唯一真相源。加数据 = 加一条 json。

★★ schema v2(2026-07-13):family 从 tree 下沉到 fault_type。
  family 是 fault_type 的属性,【不是 tree 的属性】—— 一个 dual combo 的两条腿完全可以
  一条 root_local 一条 propagation(实测 70/195 case 就是这样)。v1 把 family 挂在 trees[]
  上是物理上不可能对的。现在:
    fault_classes: fault_type -> signal_class ∈ {root_local, propagation, off_graph}
    case 的 family = 它【所有腿】的 signal_class 集合;单一类取该类,混合取 "mixed"。
  ⇒ family 一律【从 groundtruth.json 现算】,不从目录名/树名猜。

★ family 是评测【必须分栏】的依据:
    root_local   本地型  —— 根因自己的 cAdvisor 直接可见。★送分题,合并报会稀释先验。
    propagation  传播型  —— 信号只在根因的【上游调用方】身上。数据集真正的难核。
    off_graph    图外型  —— 根因不在服务调用图上(host / mysql 表锁)。
    mixed        混合型  —— 腿的 signal_class 不唯一。多根因树的主体。

★ fail-loud(2026-07-13):未知 family/arity/status 一律 raise ValueError。
  以前是【静默返回空列表】—— 评测会 0 个 case 跑完、不报错、打出一张空表。
  这是最坏的失败模式(错得无声无息),比崩掉危险得多。

★★★ 事故记录(2026-07-13,护栏建成【当天】就被打穿):
  assert_not_native() 建好了,但【没接全】—— 于是一次 `eval_k8s_supervised.py --help`
  就把 (native trees) BASELINE_RESULTS_supervised.txt 写成了一段 argparse usage 文本。
  两个成因叠加,缺一不可:
    (1) 默认输出根【就是 native】 —— PILOT = DR.NATIVE_ROOT,输出文件直接落在采集树里;
    (2) 写盘在 `finally:` 里【无条件执行】 —— --help / argparse 报错 / 任何提前异常,
        都会把当时捕获到的 stdout 当作"结果"写进去。usage 文本于是变成了"监督式基线结果"。
  ⇒ 教训:
    · 护栏只有【接到每一个写入口】才算数;建了不接 = 没建。
    · 派生物默认输出根一律 runtime_dir(),【永远不要】默认写 native。
    · 清理型 finally 里不许落盘;只有真跑出结果才写(produced 标志位)。
    · 读入口同理收口到 feature_csv():找不到就 raise,【绝不静默降级】
      (package_for_delivery --eval-only 找不到 csv 曾静默打全集 —— 比崩掉危险得多)。
  ★ 合法的 native 写入者只有 chaos_k8s_runner.py(采集器,native 的生产者)
    与 fix_net_gt.py(带备份的一次性 GT 原地修复,FORBIDDEN 护栏另有一套)。其余脚本一律只读。
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DATASETS = REPO / "datasets"
REGISTRY_PATH = DATASETS / "REGISTRY.json"

# 目录契约。脚本一律用这些常量,不许再拼字符串。
# ⚠ _runtime/ 与 _delivery/ 目前【磁盘上还不存在】,是目标契约不是既成事实。
#   交付树实际位置一律查 registry 的 deliveries[].path(见 delivery_dir),不许拼 DELIVERY_ROOT/tag。
NATIVE_ROOT = DATASETS / "k8s_pilot"        # 原始采集 —— 只读
RUNTIME_ROOT = DATASETS / "_runtime"        # 派生物 —— 删了能重生
DELIVERY_ROOT = DATASETS / "_delivery"      # 交付快照(目标位置) —— 发出去就冻结
ARCHIVE_ROOT = DATASETS / "_archive"        # 作废 —— 不参与任何评测

MIXED = "mixed"

_CACHE = None
_CASE_CACHE: dict = {}


def registry() -> dict:
    global _CACHE
    if _CACHE is None:
        with open(REGISTRY_PATH, encoding="utf-8") as f:
            _CACHE = json.load(f)
    return _CACHE


# ---------------- family / signal_class ----------------

def fault_classes() -> dict:
    """fault_type -> signal_class。跳过 _ 开头的注记键。"""
    return {k: v["signal_class"]
            for k, v in registry()["fault_classes"].items()
            if not k.startswith("_")}


def family_names() -> list[str]:
    """合法 family 名。跳过 _doc / counts 等非 family 键。"""
    return [k for k, v in registry()["families"].items()
            if not k.startswith("_") and isinstance(v, dict) and "label" in v]


def signal_class(fault_type: str) -> str:
    """fault_type -> signal_class。未知 fault_type 【报错】,绝不猜。

    新增一种故障类型而忘了登记 -> 这里炸,而不是被静默归进某一族污染分栏。
    """
    fc = fault_classes()
    if fault_type not in fc:
        raise ValueError(
            "未知 fault_type: %r\n合法值(见 datasets/REGISTRY.json 的 fault_classes): %s\n"
            "★ 新增故障类型必须先在 REGISTRY.json 登记 signal_class,否则评测分栏会静默错。"
            % (fault_type, sorted(fc))
        )
    return fc[fault_type]


def case_family(case_dir) -> str:
    """从 groundtruth.json 现算 case 的 family。

    规则:取【所有腿】的 signal_class 集合;单一类取该类,多于一类取 "mixed"。
    """
    case_dir = Path(case_dir)
    key = str(case_dir)
    if key in _CASE_CACHE:
        return _CASE_CACHE[key]
    with open(case_dir / "groundtruth.json", encoding="utf-8") as f:
        gt = json.load(f)
    legs = gt.get("component_ground_truth")
    if not legs:
        raise ValueError("case 缺 component_ground_truth,无法定 family: %s" % case_dir)
    classes = {signal_class(leg["fault_type"]) for leg in legs}
    fam = classes.pop() if len(classes) == 1 else MIXED
    _CASE_CACHE[key] = fam
    return fam


# ---------------- 校验(fail-loud) ----------------

def _check(value, legal, what):
    if value is not None and value not in legal:
        raise ValueError(
            "未知 %s: %r\n合法值: %s\n"
            "(以前这里【静默返回空列表】—— 评测会 0 个 case 跑完还不报错。)"
            % (what, value, sorted(legal))
        )


def _legal_arities() -> set:
    return {t["arity"] for t in registry()["trees"]}


def _legal_statuses() -> set:
    return {t.get("status") for t in registry()["trees"] if t.get("status")} | {"active"}


def _legal_trees() -> set:
    return {t["id"] for t in registry()["trees"]}


# 历史语料("dense 三棵")。2026-07-13 前所有脚本硬写的 "{arity}_dense" 就是它 = 140 case。
# ★ 它【不是】一个 family —— family 是 fault_type 的属性(见 fault_classes),
#   propagation 只有 25 个 case。要复现历史的 140,得按【树】筛,不能按 family 筛。
DENSE_TREES = ["single_dense", "dual_dense", "triple_dense"]


# ---------------- 数据树 / case ----------------

def trees(family: str | None = None, arity: str | None = None,
          status: str = "active", tree_ids: list[str] | None = None) -> list[dict]:
    """在役数据树。

    ★ family 过滤 = 【该树是否含有该 family 的 case】(查 family_by_type),
      不是『该树属于该 family』—— tree 没有 family,那是 v1 的错。
      一棵树可以同时出现在 root_local / propagation / off_graph 三个 family 的结果里。

    tree_ids: 显式点名数据树(如 DENSE_TREES)。未知 id 【报错】,不静默丢。
    """
    _check(family, family_names(), "family")
    _check(arity, _legal_arities(), "arity")
    _check(status, _legal_statuses(), "status")
    for tid in (tree_ids or []):
        _check(tid, _legal_trees(), "tree_id")
    out = []
    for t in registry()["trees"]:
        if status and t.get("status") != status:
            continue
        if tree_ids and t["id"] not in tree_ids:
            continue
        if family and family not in (t.get("family_by_type") or {}):
            continue
        if arity and t.get("arity") != arity:
            continue
        out.append(t)
    return out


def tree_dir(tree_id: str) -> Path:
    for t in registry()["trees"]:
        if t["id"] == tree_id:
            return DATASETS / t["path"]
    raise KeyError("unknown tree_id: %s (见 datasets/REGISTRY.json)" % tree_id)


_GROUP_MARK = "_reps_v"          # 分组层目录名的标记: _<key>_reps_v19


def _group_type(dir_name: str) -> str:
    """分组层目录名 -> type。 `_dual01_reps_v19` -> `dual01`"""
    n = dir_name
    return n[1:].rsplit(_GROUP_MARK, 1)[0] if n.startswith("_") else n.rsplit(_GROUP_MARK, 1)[0]


def _flat_type(case_id: str) -> str:
    """扁平布局的 type = case_id 去掉 `_r<N>` 重复后缀。

    `cart_order_cpu_r1` -> `cart_order_cpu`,同一组 5 个 rep 共享一个 type,
    与嵌套布局的 type(= 分组目录名 = "一组配置")语义一致。
    ★ 安全性(2026-07-26 核过):`type` 字段【当前全仓无消费者】,group-aware CV 用的是
      group_id = fault_type(eval_k8s_supervised.py),不靠 type 分组,故此口径不影响防泄漏切分。
    """
    head, sep, tail = case_id.rpartition("_r")
    return head if (sep and head and tail.isdigit()) else case_id


def _iter_cases(root: Path):
    """遍历一棵树下的 case,yield (type, case_dir)。支持【两种布局】:

      嵌套(dense/spread/recagent):  <tree>/_<key>_reps_vNN/<case_id>/groundtruth.json
      扁平(G2ext dual_ext/triple_ext): <tree>/<case_id>/groundtruth.json

    ★ 2026-07-26 修的 bug:原来只 glob("*_reps_v*"),扁平树匹配 0 个 ->
      dual_ext 25 + triple_ext 20 【静默】不出现在 cases() 里,all_cases() 返 210 而非 255
      (trees() 是对的,漏只在 case 枚举这层 —— 又一次"静默少算"事故,和 v1 的 spread 同款)。

    判据:目录里有 groundtruth.json = case;否则若是 `*_reps_v*` 分组层就【只】往下一层找。
    两种布局在磁盘上互斥(实测嵌套树根 0 个 gt、扁平树根 0 个分组层),不会重复计数;
    且 case_dir 是绝对路径,即便将来混布也不可能同一 case 出现两次。
    """
    for d in sorted(root.iterdir()):
        if not d.is_dir() or d.name == "mr2":     # mr2/ 是打包派生物,不是 case
            continue
        if (d / "groundtruth.json").exists():     # —— 扁平布局
            yield _flat_type(d.name), d
            continue
        if _GROUP_MARK not in d.name:
            # —— 名字不含 `_reps_v` 的分组层(2026-07-27 加)。
            # 判据从【看名字】改成【看内容】:下探一层, 只要有子目录带 groundtruth.json 就当分组层。
            # 起因:D 档 `dprobe_crosslayer/<surface>/<case>/` 按名字判会被整树跳过;
            # 而旧 WARN 文案自己写着"若这是新布局的 case…请改 _iter_cases 而不是让它默默漏掉"。
            # ★改前实测过全库影响面, 确认只动两棵【非 active】树:
            #   dprobe_crosslayer +48(本意) · i1_netloss_search +2 ——
            #   后者 REGISTRY 早就声明 n_cases=2 却一个都枚举不到(既存隐性不一致),
            #   本改动让实际与声明对上, 不是回归。**active 树 0 个受影响, cases() 仍 255(逐字段零差核过)。**
            if not any(c.is_dir() and (c / "groundtruth.json").exists()
                       for c in d.iterdir()):
                sys.stderr.write(
                    "WARN dataset_registry: 跳过无法识别的目录 %s\n"
                    "     (既无 groundtruth.json、名字不含 '%s'、下一层也没有带 groundtruth.json "
                    "的子目录 —— 若这是又一种新布局,枚举会【静默少算】,请改 _iter_cases。)\n"
                    % (d, _GROUP_MARK)
                )
                continue
        key = _group_type(d.name)                 # —— 嵌套布局,只下降一层
        for cd in sorted(d.iterdir()):
            if not cd.is_dir() or cd.name == "mr2":
                continue
            if not (cd / "groundtruth.json").exists():
                continue
            yield key, cd


def cases(family: str | None = None, arity: str | None = None,
          status: str = "active", tree_ids: list[str] | None = None) -> list[dict]:
    """遍历 case。返回 [{tree, family, arity, type, case_id, case_dir, fault_types, n_legs}, ...]。

    ★ family 是【每个 case 现算的】(case_family),不是继承树的 —— 所以
      cases(family='propagation') 会把藏在 *_dense 里的传播型 case 也捞出来,
      而不是只给你某一棵树。

    ★ case_id 升序是【钉死】的:打分产物的行序若随 imap_unordered 的完成顺序变,
      bootstrap CI 会在等价的两次运行间漂移 —— 发表的 CI 就复现不出来。
      (case_id 全局唯一 —— 255 个 0 重复,实测 —— 故这是全序,不依赖遍历顺序。)

    ★ 两种磁盘布局都吃(2026-07-26):嵌套 `_<key>_reps_vNN/<case>/` 与扁平 `<case>/`。
      type 语义 = "一组配置":嵌套取分组目录名,扁平取 case_id 去 `_r<N>`。见 _iter_cases。
    """
    _check(family, family_names(), "family")
    _check(arity, _legal_arities(), "arity")
    _check(status, _legal_statuses(), "status")

    out = []
    # ★ 树级不按 family 筛(family 逐 case 现算);tree_ids 是显式点名,合法。
    for t in trees(arity=arity, status=status, tree_ids=tree_ids):
        root = DATASETS / t["path"]
        if not root.is_dir():
            continue
        # ★ 布局无关的枚举(嵌套 + 扁平),见 _iter_cases
        for key, cd in _iter_cases(root):
            fam = case_family(cd)
            if family and fam != family:
                continue
            with open(cd / "groundtruth.json", encoding="utf-8") as f:
                legs = json.load(f)["component_ground_truth"]
            out.append({
                "tree": t["id"],
                "family": fam,
                "arity": t["arity"],
                "type": key,
                "case_id": cd.name,
                "case_dir": str(cd).replace("\\", "/"),
                "fault_types": [l["fault_type"] for l in legs],
                "n_legs": len(legs),
            })
    out.sort(key=lambda r: r["case_id"])

    if not out:
        sys.stderr.write(
            "WARN dataset_registry.cases(family=%r, arity=%r, status=%r, tree_ids=%r) 返回【0 个 case】。\n"
            "     参数合法但没匹配上任何数据 —— 评测将跑空。请确认这是你要的。\n"
            % (family, arity, status, tree_ids)
        )
    return out


# ---------------- 派生物落盘位置(统一入口,别再各写各的) ----------------

def runtime_dir(kind: str, tag: str = "default", make: bool = True) -> Path:
    """派生物目录。kind ∈ {package, scores, features, eadro, smoke}。

    例: runtime_dir('package', '20260713') -> (runtime) package/20260713/
    """
    allowed = {"package", "scores", "features", "eadro", "smoke"}
    if kind not in allowed:
        raise ValueError("kind 必须是 %s,拿到 %r" % (sorted(allowed), kind))
    p = RUNTIME_ROOT / kind / tag
    if make:
        p.mkdir(parents=True, exist_ok=True)
    return p


FEATURES_DIR = RUNTIME_ROOT / "features"


def feature_csv(name: str = "features_k8s.csv", required: bool = True,
                search_dir=None) -> Path | None:
    """特征视图 CSV 的【唯一解析入口】。默认位置 (runtime) features/<name>。

    ★ 绝不返回一个不存在的路径。找不到时:
        required=True  -> raise FileNotFoundError,并直说怎么生成它;
        required=False -> 返回 None(调用方必须显式处理 None,不许拿它当路径用)。
      以前各脚本自己拼 pilot_dir/'features_k8s.csv',拼出来的幽灵路径要么
      pd.read_csv 崩在别处、要么(package_for_delivery)被当成"没有过滤器"静默放行。

    search_dir: 显式覆盖(--out-dir / --pilot-dir 指过来的目录)。None = 默认 runtime 目录。
    """
    d = Path(search_dir) if search_dir else FEATURES_DIR
    p = d / name
    if p.is_file():
        return p
    if not required:
        return None
    raise FileNotFoundError(
        "找不到特征视图: %s\n"
        "生成它:\n"
        "  python scripts/chaos/ctk/make_k8s_feature_view.py\n"
        "  (默认输出 -> %s;native 采集树 (native trees)  只读,不再往里写派生物)\n"
        "或用 --pilot-dir/--out-dir 显式指定另一个目录。" % (p, FEATURES_DIR)
    )


def deliveries() -> list[dict]:
    return registry()["deliveries"]


def delivery_dir(tag: str) -> Path:
    """交付快照的【真实磁盘路径】—— 查 registry 的 deliveries[].path。

    ★ 2026-07-13 修:以前是 DELIVERY_ROOT/tag 拼出来的,而 (delivery)  【根本不存在】
      —— 于是每次调用都返回一个幽灵路径。现在如实查表;路径不存在就报错,不装作没事。
    ★ 发出去后【不可变】—— 要改就开新 tag。
    """
    for d in deliveries():
        if d["tag"] == tag:
            p = DATASETS / d["path"]
            if not p.is_dir():
                raise FileNotFoundError(
                    "delivery %r 登记的路径不存在: %s (见 REGISTRY.json deliveries[].path)" % (tag, p)
                )
            return p
    raise KeyError(
        "unknown delivery tag: %r\n合法值: %s (见 datasets/REGISTRY.json)"
        % (tag, [d["tag"] for d in deliveries()])
    )


def assert_not_native(path) -> None:
    """写产物前调它。防止任何脚本再往 native 采集树里拉屎(mr2/ 就是这么来的)。"""
    p = Path(path).resolve()
    try:
        p.relative_to(NATIVE_ROOT.resolve())
    except ValueError:
        return
    raise RuntimeError(
        "拒绝往 native 采集树写入: %s\n"
        "(native trees)  是唯一原始数据,脚本只读。派生物走 dataset_registry.runtime_dir()。" % p
    )


if __name__ == "__main__":
    import collections
    r = registry()
    print("REGISTRY  schema v%s  updated %s\n" % (r["schema_version"], r["updated"]))

    all_cases = cases()
    print("== fault_type -> signal_class ==")
    fc = fault_classes()
    legs = collections.Counter(ft for c in all_cases for ft in c["fault_types"])
    for ft in sorted(fc, key=lambda k: (fc[k], k)):
        print("  %-30s %-12s %3d 腿" % (ft, fc[ft], legs[ft]))

    print("\n== case 级 family(从 groundtruth 现算)==")
    for fam in family_names():
        meta = r["families"][fam]
        cs = cases(family=fam)
        by_tree = collections.Counter(c["tree"] for c in cs)
        print("\n[%s] %s — %d case" % (fam, meta["label"], len(cs)))
        print("    分布: %s" % dict(by_tree))
        print("    ⚠ %s" % meta["eval_note"][:160])

    print("\n合计在役: %d case" % len(all_cases))
    declared = r["families"]["counts"]
    got = collections.Counter(c["family"] for c in all_cases)
    bad = [f for f in family_names() if declared.get(f) != got[f]]
    print("REGISTRY 声明 vs 现算: %s" % ("一致 ✓" if not bad else "★不一致 %s" % bad))
