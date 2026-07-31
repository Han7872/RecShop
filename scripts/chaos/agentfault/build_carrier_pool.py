# -*- coding: utf-8 -*-
"""P0-0 载体池构建(agentfault v2 设计稿 §(C) 历史侧过滤)。

取 SASRec /dataset/test_sequences 的真实用户 Leave-One-Out 历史序列(天然在词表内),
用 electronics.item 的真标题集做**历史侧过滤**:history 里每个商品都必须有真实标题
(非 Product_<id> 占位符、非空、在文件内)。候选侧已由 tools.py 服务过滤根治(过采×3+滤),
故本脚本不预跑 SASRec 筛候选,不碰 9.2GB pickle —— 纯 API + 本地文本。

产出 assets/carrier_pool.json:{meta, sequences:[{seq_id,user_id,history,label,hist_len}]}。
采集时每 rep 取一条载体(跨 combo 同 rep-index 用同序列 → faulted/normal 可比;
rep 间不同序列 → 场景多样性)。

用法(sasrec 在跑,本地 8200 或改 --sasrec):
  PYTHONIOENCODING=utf-8 python build_carrier_pool.py --max-users 500 --want 32 --min-hist 3
只读、可重跑、确定性(不 random,按端点返回序 + 稳定筛)。
"""
import argparse
import json
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
DEFAULT_ITEM_FILE = os.path.join(REPO, "shared", "data", "electronics.item")
DEFAULT_OUT = os.path.join(HERE, "assets", "carrier_pool.json")


def load_real_title_ids(item_file):
    """扫 electronics.item,返回有真标题的 item_id 集合(剔占位符 Product_<id>/空)。"""
    real = set()
    total = 0
    with open(item_file, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        id_idx = header.index("item_id")
        title_idx = header.index("title")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) <= max(id_idx, title_idx):
                continue
            total += 1
            iid = parts[id_idx]
            title = parts[title_idx]
            if title and title != "Product_%s" % iid:
                real.add(iid)
    return real, total


def fetch_sequences(sasrec, split, max_users):
    url = "%s/dataset/test_sequences?split=%s&max_users=%d" % (sasrec, split, max_users)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=120) as r:
        d = json.loads(r.read().decode("utf-8"))
    return d.get("sequences", [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sasrec", default="http://127.0.0.1:8200")
    ap.add_argument("--split", default="test")
    ap.add_argument("--max-users", type=int, default=500)
    ap.add_argument("--want", type=int, default=32, help="载体池目标条数(取够即停,给足备用)")
    ap.add_argument("--min-hist", type=int, default=3, help="历史最小长度(太短推荐质量差)")
    ap.add_argument("--item-file", default=DEFAULT_ITEM_FILE)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    # 绕 Clash
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"

    print("[1/3] loading real-title id set from %s ..." % args.item_file)
    real_ids, total = load_real_title_ids(args.item_file)
    print("      real-title ids: %d / %d rows (%.1f%% real)"
          % (len(real_ids), total, 100.0 * len(real_ids) / max(1, total)))

    print("[2/3] fetching %d test sequences from %s ..." % (args.max_users, args.sasrec))
    seqs = fetch_sequences(args.sasrec, args.split, args.max_users)
    print("      got %d sequences" % len(seqs))

    print("[3/3] filtering: history all real-title & len>=%d ..." % args.min_hist)
    clean, n_short, n_dirty = [], 0, 0
    for s in seqs:
        hist = s.get("history", []) or []
        if len(hist) < args.min_hist:
            n_short += 1
            continue
        if all(h in real_ids for h in hist):
            clean.append(s)
        else:
            n_dirty += 1

    yield_rate = 100.0 * len(clean) / max(1, len(seqs))
    print("      clean=%d  (too_short=%d, has_placeholder_hist=%d)  yield=%.1f%%"
          % (len(clean), n_short, n_dirty, yield_rate))

    picked = clean[:args.want]
    pool = {
        "meta": {
            "source": "sasrec /dataset/test_sequences",
            "split": args.split,
            "max_users_queried": args.max_users,
            "min_hist": args.min_hist,
            "fetched": len(seqs),
            "clean_available": len(clean),
            "yield_rate_pct": round(yield_rate, 2),
            "picked": len(picked),
            "filter": "history all in real-title set (non Product_<id>, non-empty)",
            "note": "候选侧由 tools.py 服务过滤根治(过采x3+滤占位符),本池只管历史侧",
        },
        "sequences": [
            {"seq_id": i, "user_id": s.get("user_id"), "history": s.get("history"),
             "label": s.get("label"), "hist_len": len(s.get("history", []))}
            for i, s in enumerate(picked)
        ],
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)
    print("\n[written] %s (%d sequences)" % (args.out, len(picked)))
    if len(picked) < args.want:
        print("[WARN] picked %d < want %d — raise --max-users to get more."
              % (len(picked), args.want), file=sys.stderr)


if __name__ == "__main__":
    main()
