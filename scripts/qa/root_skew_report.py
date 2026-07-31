import json, glob, collections
roots={}
for tag,pat in (("gtfix140","(delivery) 20260713_gtfix/**/groundtruth.json"),
                ("spread55","(delivery) single_spread_20260716/**/groundtruth.json")):
    for g in glob.glob(pat, recursive=True):
        d=json.load(open(g,encoding="utf-8"))
        roots[d.get("sample_id") or g]=(tag, set(d.get("root_cause_services") or []))
print("total cases:", len(roots))
cnt=collections.Counter()
for _,(t,rs) in roots.items():
    for r in rs: cnt[r]+=1
print("\n根因服务出现的 case 数:")
for k,v in cnt.most_common(): print("  %-16s %3d  (%.1f%%)" % (k,v,100*v/len(roots)))
# 常量基线: 永远预测某服务的 Hit@1 (top1 命中 GT 集合即算中)
print("\n常量基线 Hit@1(永远猜该服务):")
for k,v in cnt.most_common(4): print("  猜 %-14s %.3f" % (k, v/len(roots)))
# 单根 vs 多根拆开
single={k:v for k,v in roots.items() if len(v[1])==1}
multi={k:v for k,v in roots.items() if len(v[1])>1}
for nm,dd in (("single-root",single),("multi-root",multi)):
    c=collections.Counter()
    for _,(t,rs) in dd.items():
        for r in rs: c[r]+=1
    top=c.most_common(1)[0] if c else ("-",0)
    print("\n%s: n=%d, 最高频根因=%s %d (%.1f%%)" % (nm,len(dd),top[0],top[1],100*top[1]/max(1,len(dd))))
    print("   分布:", dict(c.most_common()))
