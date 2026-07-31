# restore_recagent_stock.ps1 -- 采集收尾: rec-agent 还原 stock 镜像 + 摘除埋点/注入旋钮
# ============================================================================
# 与 patch_recagent_observe.ps1(G1/D 档 observe-only)和 patch_recagent_collect.ps1
#   (B 档 --backend k8s 采集形态)**均**配对 —— 两条线都用它收尾, 别怀疑拿错了脚本。
# 还原策略 = kubectl replace -f 01-rec-agent.yaml:
#   patch 加的 env 4 项 / envFrom(deepseek-env) / volume+volumeMount(agentfault-data) / 变体镜像
#   全部随 spec 整体替换回 stock(replace 用 manifest 的完整 spec, 不做三方合并 —— kubectl apply
#   对"imperative patch 加的字段"可能因 last-applied 不含它们而残留, 故不用 apply)。
#   ★这只 replace rec-agent 一个 Deployment+Service, 不碰其余 24 服务(README: 勿 apply 25
#     manifest"警示针对整树重刷, 单服务定点还原不在其列)。
#   ★deepseek-env secret 本身【保留在 ns】(spike 遗产, 备后续跨层采集;只摘 Deployment 对它的引用)。
# ★还原前先确认所有 case 的 /agentfault-data/spans.jsonl 已回收 —— replace 触发 pod 重建,
#   emptyDir 卷即清空, 没回收的轨迹永久丢(轨迹事后补不回)。
#   (B 档 `--backend k8s` 采集时 runner 每个 rep 都会 tail 回本地 spans/ledgers/,
#    收尾前核一下 <out-dir>/spans/ 与 ledgers/ 里 9 个 combo 的文件都非空即可。)
# ★PVC recagent-data(electronics.item, 254MiB)**保留不删** —— 它是独立生命周期对象,
#   删了下次重采还要重灌。本脚本只 replace Deployment+Service, 碰不到 PVC。
# ★本脚本尾部会**断言**环境确实回到了 traditional 255 的口径(image=:latest /
#   limits.memory=256Mi / strategy=RollingUpdate / 无 AGENTFAULT env)——
#   B 档采集期间抬过内存上限与 strategy, 不断言就可能带着"1.5Gi + Recreate"去采
#   traditional, 跨批次的 container_spec_memory_limit_bytes 与恢复时延都会失真。
# ★必须 PowerShell(同 patch 脚本头注: Git Bash MSYS 会改写含 =/路径 的 kubectl 参数;本脚本虽无
#   此类参数, 但配对脚本钉死 PowerShell, 统一入口防误用)。
# 用法(必须带 -ConfirmedCollected, 否则脚本只展示卷内容后中止 -- 防误跑一把清掉未回收台账):
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1 -ConfirmedCollected
# ============================================================================
param(
    [switch]$ConfirmedCollected   # reviewer SHOULD-FIX#1: 回收顺序必须由脚本强制, 不能只靠 Yellow 警告
)
$ErrorActionPreference = 'Stop'
$ns       = 'recweb-chaos'
$deploy   = 'rec-agent'
$manifest = 'k8s/pilot/01-rec-agent.yaml'   # 相对仓库根;stock 定义(image=recweb-rec-agent:latest, 无 AGENTFAULT env)

if (-not (Test-Path $manifest)) {
    throw "找不到 $manifest -- 请在仓库根(${REPO_DIR})执行本脚本"
}

Write-Host "== [guard] 还原会重建 pod 并清空 emptyDir -- agent_spans 必须已全部回收" -ForegroundColor Yellow
Write-Host "   当前卷内容:" -ForegroundColor Yellow
# ★2026-07-27 修:原来 fallback 写 echo "(卷不存在/已是 stock)" —— PowerShell 5.1 把传给
#   native exe 的内层双引号吞掉,sh 收到裸的 `echo (卷不存在/已是 stock)`,`(` 是 sh 的
#   子 shell 语法 ⇒ syntax error,fallback 分支实际打不出东西。改成无需引号的裸 token。
kubectl exec -n $ns deploy/$deploy -- sh -c 'ls -la /agentfault-data 2>/dev/null || echo VOLUME-ABSENT-or-ALREADY-STOCK' | Out-Host

if (-not $ConfirmedCollected) {
    throw "中止: 未带 -ConfirmedCollected。确认上面卷里的 spans/台账已全部拷回宿主(collect 脚本的回收命令)后, 重跑本脚本并加 -ConfirmedCollected。emptyDir 随 pod 重建即清空, 未回收的轨迹永久丢。"
}

Write-Host "== [restore] kubectl replace -f $manifest(整 spec 替换回 stock)" -ForegroundColor Cyan
kubectl replace -f $manifest
if ($LASTEXITCODE -ne 0) { throw "kubectl replace 失败 -- 若对象状态异常可 delete -f 后 apply -f(会丢 pod, 台账须已回收)" }

Write-Host "== [rollout] 等待 stock 滚动就绪" -ForegroundColor Cyan
kubectl rollout status deploy/$deploy -n $ns --timeout=180s
if ($LASTEXITCODE -ne 0) { throw "rollout 未就绪 -- kubectl describe pod -l app=recommendation_agent -n $ns 查因" }

# ---- 采集数据 + 诊断输出先打完, 再做断言 ----
# ★顺序很重要(回归审查 F5): 断言一旦 throw, 后面的 kubectl get pods 就不会执行了,
#   操作者拿不到"pod 现在到底是什么状态"这条最需要的信息。所以**先收集再断言**。
Write-Host "== [state] pod 现状" -ForegroundColor Cyan
kubectl get pods -n $ns -l app=recommendation_agent | Out-Host

$img   = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.containers[0].image}'
$mem   = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}'
$cpu   = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}'
$strat = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.strategy.type}'
$vols  = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.volumes[*].name}'
Write-Host "== [verify] image=$img  mem.limit=$mem  cpu.limit=$cpu  strategy=$strat  volumes='$vols'" -ForegroundColor Cyan

Write-Host "== [verify] 容器内不应再有 AGENTFAULT/SPAN_FILE/ITEM_FILE_PATH env(CLEAN=干净)" -ForegroundColor Cyan
# ★★2026-07-27 修:这一条曾是【永远翻不动的空闸】。原写 grep -E "A|B|C" ——
#   PowerShell 5.1 吞掉传给 native exe 的内层双引号,sh 实际收到:
#       env | grep -E AGENTFAULT | SPAN_FILE | ITEM_FILE_PATH || echo CLEAN
#   ⇒ grep 的输出被"命令不存在"的 SPAN_FILE 吃掉;管道退出码 = 最后一个命令的 127(非零)
#     ⇒ `|| echo CLEAN` 必然触发 ⇒ $leftover **恒等于 CLEAN**,
#     于是下面 L82 的 `-notmatch 'CLEAN'` 断言【无论 env 是否真残留都不会 fire】。
#   (正常路径上 kubectl replace 确实会把 env 换干净,所以一直没暴露 —— 但这条断言的
#    职责恰恰是"replace 万一没吃掉时报警",它坏了等于这个保护根本不存在。)
#   改成多个 -e:全是裸字母、不需要任何引号 ⇒ 不受 PS 引号处理影响。
$leftover = kubectl exec -n $ns deploy/$deploy -- sh -c 'env | grep -e AGENTFAULT -e SPAN_FILE -e ITEM_FILE_PATH || echo CLEAN'
$leftover | Out-Host

# ---- 断言: 镜像回 stock + 资源上限回 stock + AGENTFAULT env 全摘 + 卷已摘 ----
# ★这几条是**断言**不是打印: 采集期改过的每一项都必须证明已回到 traditional 255 的环境口径
#   (期望值与 k8s/pilot/01-rec-agent.yaml 的 stock spec 对齐: :latest / 500m / 256Mi /
#    无 strategy 字段 = 默认 RollingUpdate / 无 volumes)。逐条收集后一次报全, 不是撞一条停一条。
$bad = @()
if ($img -ne 'recweb-rec-agent:latest') { $bad += "image 未回 stock(实得 $img)" }
if ($mem -ne '256Mi') { $bad += "memory limit 未回 stock 256Mi(实得 $mem) -- 跨批次内存指标会失真" }
if ($cpu -ne '500m')  { $bad += "cpu limit 未回 stock 500m(实得 $cpu)" }
if ($strat -ne 'RollingUpdate') { $bad += "strategy 未回默认 RollingUpdate(实得 $strat)" }
if ($vols) { $bad += "还挂着卷 '$vols' -- 采集用的 agentfault-data/recagent-data 应随 replace 全摘" }
if ($leftover -notmatch 'CLEAN') { $bad += "容器内仍有采集期 env 残留 -- replace 没吃掉?手工 kubectl set env ... KEY- 清" }
if ($bad.Count -gt 0) {
    throw ("还原不完整, 环境未回到 traditional 255 的口径:`r`n  - " + ($bad -join "`r`n  - "))
}

Write-Host ""
Write-Host "DONE. rec-agent 已还原 stock(deepseek-env secret + PVC recagent-data 保留在 ns)。" -ForegroundColor Green
Write-Host "环境已回到 traditional 255 的口径(:latest / 256Mi / RollingUpdate / 无数据卷)。" -ForegroundColor Green
Write-Host "★PVC recagent-data 有意保留: 它是独立生命周期对象, 重采免重灌 electronics.item。" -ForegroundColor Green
