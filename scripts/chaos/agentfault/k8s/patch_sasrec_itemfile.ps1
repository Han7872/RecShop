# =============================================================================
# patch_sasrec_itemfile.ps1 —— 给 K8S 的 sasrec pod 挂上 electronics.item
#
# 为什么需要(2026-07-27 实测定位,B 档第二轮)
# -----------------------------------------------------------------------------
# `(upstream batch)`(本机批次)的候选面**有真实商品标题**;
# B 档在 K8S 采的两轮,候选面**全是"未知商品"** —— 46 个 distinct 候选,真标题 0 个。
#
# 根因链(逐环实测):
#   1. rec-agent `agents/tools.py:126` 渲染候选时用的是 **sasrec 响应里的 rec['title']**
#      (`title = rec.get("title") or "未知商品"`),不是自己那份 title cache;
#   2. sasrec `api_server.py:189-203` 从 `electronics.item` 建 `item_info` 才有 title;
#   3. K8S 的 sasrec pod 里 `/app/shared/data/` 是**空目录**、本地也没有 electronics.item
#      => item_info 空 => title=None => rec-agent 兜底成"未知商品"。
#   4. 本机之所以没暴露:宿主 `shared/data/electronics.item` 存在,sasrec 读到了。
#
# ★口径指纹(证明 v2 的候选标题确实出自 sasrec,所以必须从 sasrec 侧修):
#     v2 的 46 个候选里 32 个被截,且**同时满足 len==80 且以 '...' 结尾** = `[:77]+'...'`
#     —— 正是 api_server.py:201-202 的截法。
#     rec-agent 本地 cache 的截法是 `tools.py:43` 的 `[:80]`(**不加省略号**)。
#   => 若改 tools.py 用本地 cache 兜底,标题会变成 80 字符硬截断,与 v2 逐字不一致:
#      验收 `title_matches` 的 '...' 前缀分支失效 => B7 会真的判"与权威表不符";
#      E 组跨批次可比性受损。**所以正解是给 sasrec 补元数据,不是改渲染。**
#
# ★注意这不是"筛选坏了"
#   2026-07-19 拍板的服务侧过滤(过采 top_k×3 -> _load_title_cache() 剔占位符 -> 截回 top_k)
#   **一直在跑且有效**:46/46 候选在权威表里都有真标题、零占位符;而源文件占位符率 26%,
#   46 个全真的概率约 1.4e-6。过滤器查了 cache,只是没把标题传给输出而已。
#
# 做什么
# -----------------------------------------------------------------------------
#   · 把已有 PVC `recagent-data` 里的 electronics.item 以 subPath + readOnly 挂进 sasrec
#     的 `/app/shared/data/electronics.item`(= api_server.py 的默认查找路径);
#   · 显式钉 env `SASREC_ITEM_FILE` 防默认值漂移(照 SASREC_CACHE_PATH/MODEL_PATH 的先例);
#   · 等 rollout + 断言 pod 内文件在、且 /recommend 真的返回 title。
#
#   零代码改动、零镜像重建。RWO PVC 限的是【节点】不是【pod】,本集群单节点
#   => sasrec 与 rec-agent 可同时只读共挂(见 (project docs)/REF-k8s-chaosmesh-troubleshooting.md)。
#
# 代价 / 风险(如实列)
# -----------------------------------------------------------------------------
#   · sasrec 启动多约 20-30s(逐行读 267MB 建 1.95M 条 dict)。startupProbe 预算 =
#     failureThreshold 30 × periodSeconds 10 = **300s**,余量够,但会吃掉一部分。
#   · sasrec 目前**平均每天自己 exit 0 重启约 7 次**(实测 restartCount=56/8 天,
#     lastState.reason=Completed/exitCode=0,原因未查) —— 每次恢复都多这 20-30s。
#   · 内存:sasrec 只限 CPU(limits.cpu=4)、**不限内存**(requests.memory=12Gi),
#     多约 400-600MB,不会 OOM。
#   · ★动的是 25 服务共享栈里的 sasrec = 传统 255 的采集环境。已采的 255 在盘上不受影响,
#     但为保后续传统采集的可比性,**采完请跑 restore_sasrec_stock.ps1 还原**。
#
# 用法
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_sasrec_itemfile.ps1
# =============================================================================
param(
    [string]$ExpectSize = '266818680',   # 权威表字节数(与宿主 shared/data/electronics.item 一致)
    [switch]$SkipProbe                   # 跳过 /recommend 实探(只想改 spec 时用)
)
$ErrorActionPreference = 'Stop'
$ns     = 'recweb-chaos'
$deploy = 'sasrec'
$itemPath = '/app/shared/data/electronics.item'

function Assert-LastOk($msg) { if ($LASTEXITCODE -ne 0) { throw $msg } }

Write-Host "== [preflight] deploy/$deploy 存在?" -ForegroundColor Cyan
kubectl get deploy $deploy -n $ns | Out-Host
Assert-LastOk "找不到 deploy/$deploy"

Write-Host "== [preflight] PVC recagent-data 已 Bound?(electronics.item 就在里面)" -ForegroundColor Cyan
kubectl get pvc recagent-data -n $ns | Out-Host
Assert-LastOk "PVC recagent-data 不存在 -- 先 kubectl apply -f k8s/pilot/01b-recagent-data.yaml 并跑 load_recagent_data.ps1"

Write-Host "== [before] sasrec 现在能不能返回 title(应为不能)" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- sh -c 'ls -la /app/shared/data/ 2>/dev/null || echo NO-DIR' | Out-Host

# strategic merge patch:volumes / volumeMounts / env 都按 name 合并(加不删)
$patch = @"
spec:
  template:
    spec:
      containers:
        - name: sasrec
          env:
            # 显式钉死,防 api_server.py:190 的默认值漂移(照 SASREC_CACHE_PATH 的先例)
            - name: SASREC_ITEM_FILE
              value: $itemPath
          volumeMounts:
            - name: recagent-data
              mountPath: $itemPath
              subPath: electronics.item
              readOnly: true
      volumes:
        - name: recagent-data
          persistentVolumeClaim:
            claimName: recagent-data
            readOnly: true
"@
$pf = Join-Path $env:TEMP 'sasrec_itemfile_patch.yaml'
Set-Content -Path $pf -Value $patch -Encoding utf8

Write-Host "== [patch] 应用 strategic merge patch($pf)" -ForegroundColor Cyan
kubectl patch deploy $deploy -n $ns --patch-file $pf
Assert-LastOk "kubectl patch 失败"

Write-Host "== [rollout] 等待就绪(Recreate + 多加载 267MB, 给 420s)" -ForegroundColor Cyan
kubectl rollout status deploy/$deploy -n $ns --timeout=420s
if ($LASTEXITCODE -ne 0) {
    throw ("rollout 未就绪 -- 若是 startupProbe 超时(预算 300s), 说明加载 267MB 把启动顶过了预算, " +
           "需调大 startupProbe.failureThreshold 后重试; kubectl describe pod -l app=sasrec -n $ns 查因。")
}

Write-Host "== [verify] pod 内 electronics.item 在且字节数对" -ForegroundColor Cyan
$sz = (kubectl exec -n $ns deploy/$deploy -- sh -c "stat -c %s $itemPath 2>/dev/null || echo 0" | Select-Object -Last 1)
Write-Host "   size = $sz (期望 $ExpectSize)"
if ("$sz".Trim() -ne $ExpectSize) { throw "pod 内 electronics.item 字节数不符(实得 $sz) -- 挂载没生效或 PVC 内容不对" }

Write-Host "== [verify] env SASREC_ITEM_FILE" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- printenv SASREC_ITEM_FILE | Out-Host

if (-not $SkipProbe) {
    Write-Host "== [verify] ★/recommend 是否真的返回 title(这条才是目的)" -ForegroundColor Cyan
    # 用词表内 ASIN(injector_smoke.PROBE_SEQ)作历史序列;只要 recommendations[0].title
    # 非 null 即达标。
    # ★★这里必须走 base64:PowerShell 5.1 会**吞掉**传给 native exe 的双引号,
    #   直接传 JSON 会让 pod 里的 curl 收到没引号的 body,sasrec 返
    #   `json_invalid / Expecting property name enclosed in double quotes`(实测踩到)。
    #   这是本仓今天第 4 次撞同一个坑(前 3 处见 restore_recagent_stock.ps1 L69 的长注),
    #   而且是在刚修完那 3 处之后又在本脚本里犯了一次 —— 所以定个硬规矩:
    #   **ps1 里凡是要把带引号的 payload 传进 kubectl exec,一律 base64 编码后在 pod 内解**,
    #   命令串里不出现任何双引号。
    $body = '{"item_sequence":["015600206X","6300215695","0446673145"],"top_k":5}'
    $b64  = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($body))
    $url  = 'http://127.0.0.1:8200/recommend'
    $cmd  = "echo $b64 | base64 -d > /tmp/sasrec_probe.json && curl -s --max-time 60 -X POST $url -H Content-Type:application/json -d @/tmp/sasrec_probe.json"
    $out = kubectl exec -n $ns deploy/$deploy -- sh -c $cmd
    $out | Out-Host
    if ("$out" -match '"title"\s*:\s*null') {
        throw "★/recommend 仍返回 title=null -- item_info 没建起来。查 sasrec 日志里的 '找不到 .item 文件' 警告。"
    }
    if ("$out" -notmatch '"title"') {
        throw "★/recommend 响应里没有 title 字段 -- 响应结构与 api_server.py:455 不符,先看上面原文。"
    }
}

Write-Host ""
Write-Host "DONE. sasrec 已挂上 electronics.item, 候选侧标题口径与本机 v2 对齐([:77]+'...')。" -ForegroundColor Green
Write-Host "★下一步: 重新 patch rec-agent 到采集态, 再重采(CSV 是 append-only, 必须换/清空目标树)。" -ForegroundColor Green
Write-Host "★采完请还原: restore_sasrec_stock.ps1(保后续传统 255 采集的环境可比性)。" -ForegroundColor Yellow
