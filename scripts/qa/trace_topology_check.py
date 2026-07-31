import json,glob,os,sys,collections
def load(d):
    sp=[]
    for f in glob.glob(os.path.join(d,"*traces*.jsonl")):
        for l in open(f,encoding="utf-8",errors="replace"):
            l=l.strip()
            if l:
                try: sp.append(json.loads(l))
                except: pass
    return sp
def stat(name,d):
    sp=load(d)
    if not sp: print(name,"NO DATA"); return
    by={s.get("span_id"):s for s in sp}
    wp=[s for s in sp if s.get("parent_span_id")]
    found=[s for s in wp if s.get("parent_span_id") in by]
    cross=collections.Counter()
    for s in found:
        a=by[s["parent_span_id"]].get("service"); b=s.get("service")
        if a and b and a!=b: cross[(a,b)]+=1
    tid=collections.Counter(s.get("trace_id") for s in sp)
    multi=sum(1 for t,c in tid.items() if c>1)
    print("%s: spans=%d 有父=%d 父可解析=%d 跨服务边=%d | distinct_trace=%d 多span的trace=%d 最大trace=%d"
          % (name,len(sp),len(wp),len(found),sum(cross.values()),len(tid),multi,tid.most_common(1)[0][1]))
    for k,v in cross.most_common(4): print("     ",k[0],"->",k[1],v)
stat("NATIVE  dual01", "(native trees) dual_dense/_dual01_reps_v19/dual01_uni_r1/raw/traces")
import glob as g
dv=g.glob("(delivery) 20260713_gtfix/dual/*/raw/traces")
if dv: stat("DELIVER dual[0]", dv[0])
