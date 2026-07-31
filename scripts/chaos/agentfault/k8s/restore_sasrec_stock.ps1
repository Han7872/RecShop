# =============================================================================
# restore_sasrec_stock.ps1 —— 把 sasrec 从"B 档采集态"还回 stock
#
# 与 patch_sasrec_itemfile.ps1 成对。为什么要还原:
#   sasrec 在 25 服务共享栈里,也是**传统 255 数据集的采集环境**。给它挂 electronics.item
#   会改变启动耗时(+20-30s)与内存基线(+400-600MB) —— 已采的 255 在盘上不受影响,
#   但后续若再采传统数据,环境就与那 255 不可比了。故 B 档采完即还原。
#
# 做法:kubectl replace -f k8s/pilot/20-sasrec.yaml(整 spec 替换回 stock)。
#   ★不能用 `kubectl patch` 反向删 —— strategic merge patch 只能加不能删。
#   ★env `SASREC_ITEM_FILE` 与 volume/volumeMount 都随整 spec 替换一起消失。
#
# 用法
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_sasrec_stock.ps1
# =============================================================================
$ErrorActionPreference = 'Stop'
$ns       = 'recweb-chaos'
$deploy   = 'sasrec'
$manifest = 'k8s/pilot/20-sasrec.yaml'

if (-not (Test-Path $manifest)) {
    throw "找不到 $manifest -- 请在仓库根(${REPO_DIR})执行本脚本"
}

Write-Host "== [before] 当前 spec 里的 item 相关设置" -ForegroundColor Cyan
kubectl get deploy $deploy -n $ns -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' |
    Select-String 'SASREC_ITEM_FILE' | Out-Host
kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.volumes[*].name}' | Out-Host

Write-Host "== [restore] kubectl replace -f $manifest" -ForegroundColor Cyan
kubectl replace -f $manifest
if ($LASTEXITCODE -ne 0) { throw "kubectl replace 失败 -- 若对象状态异常可 delete -f 后 apply -f" }

Write-Host "== [rollout] 等待 stock 就绪" -ForegroundColor Cyan
kubectl rollout status deploy/$deploy -n $ns --timeout=420s
if ($LASTEXITCODE -ne 0) { throw "rollout 未就绪 -- kubectl describe pod -l app=sasrec -n $ns 查因" }

# ---- 先收集再断言(照 restore_recagent_stock.ps1 的 F5 教训:断言 throw 会挡住诊断输出)----
Write-Host "== [state] pod 现状" -ForegroundColor Cyan
kubectl get pods -n $ns -l app=sasrec | Out-Host

$vols = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.volumes[*].name}'
$envItem = kubectl exec -n $ns deploy/$deploy -- sh -c 'printenv SASREC_ITEM_FILE || echo UNSET'
$hasItem = kubectl exec -n $ns deploy/$deploy -- sh -c 'test -f /app/shared/data/electronics.item && echo PRESENT || echo ABSENT'
Write-Host "== [verify] volumes='$vols'  SASREC_ITEM_FILE=$envItem  item 文件=$hasItem" -ForegroundColor Cyan

$bad = @()
if ("$vols" -match 'recagent-data') { $bad += "还挂着 recagent-data 卷(实得 '$vols') -- replace 没吃掉?" }
if ("$envItem" -notmatch 'UNSET')   { $bad += "env SASREC_ITEM_FILE 未摘(实得 $envItem)" }
if ("$hasItem" -match 'PRESENT')    { $bad += "pod 内仍能看到 electronics.item -- 卷未摘净" }
if ($bad.Count -gt 0) {
    throw ("还原不完整, sasrec 未回到传统 255 的环境口径:`r`n  - " + ($bad -join "`r`n  - "))
}

Write-Host ""
Write-Host "DONE. sasrec 已还原 stock(PVC recagent-data 本身保留在 ns, 只是不再挂给 sasrec)。" -ForegroundColor Green
Write-Host "★注意: 还原后候选侧标题会重新退化成'未知商品' —— 这是 stock 环境的既有性质," -ForegroundColor Yellow
Write-Host "  不是新 bug(traditional 255 与 G1 的 single_recagent 15 都是在这个口径下采的)。" -ForegroundColor Yellow
