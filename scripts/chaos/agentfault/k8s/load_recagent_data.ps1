# load_recagent_data.ps1 -- 把 electronics.item(254 MiB)灌进 PVC recagent-data
# ============================================================================
# 为什么要灌:rec-agent 的 title cache(agents/tools.py `_load_title_cache`)靠
#   shared/data/electronics.item。当前 K8S 镜像里 /app/shared/data **是空的** ->
#   `get_item_title` 恒返"未知商品"、`_filter_real_title` 会把候选**全部滤光**,
#   与 (archived) agentfault_v2 的本机采集完全不同口径, 跨集比较无从谈起。
#
# 为什么走 PVC 而不是 hostPath / 烘进镜像:
#   · hostPath **已证伪**:Docker Desktop 的节点 desktop-control-plane 是 kind 容器,
#     里面没有 /run/desktop/mnt/host(实测)。同坑记在 k8s/pilot/20-sasrec.yaml L115-117。
#   · 烘进镜像违反本仓铁律(根 .dockerignore 里 `**/*.item` 与 pkl/pth/inter 同列),
#     +254MiB 层、改一行代码就重传, 而且该文件 gitignored, 烘进去也不解决第三方可复现,
#     反倒制造"镜像自足"的假象。PVC 只灌一次, 跨 rollout / pod 重建持久。
#
# ★★必须 PowerShell 跑:Git Bash(MSYS)会把 `desktop-control-plane:/var/...` 里的
#   `/var/...` 改写成 Windows 路径, docker cp 必失败。(非要用 bash 则整条命令前缀
#   MSYS2_ARG_CONV_EXCL='*'。)
#
# 顺序为什么是 apply -> patch -> cp -> restart:
#   PVC 的 storageClass 是 WaitForFirstConsumer -> **必须先有 pod 挂它**才会 Bound,
#   Bound 之后 PV 目录才存在, 才能 docker cp 进去。灌完还要 rollout restart, 因为
#   title cache 是进程内单例(_title_cache), 已经加载过的进程不会重读。
#   ★因此本脚本在 PVC 未 Bound 时会**自举调用一次** patch_recagent_collect.ps1 -SkipDataCheck
#     把卷挂上。所以按 README 的 "load -> patch" 顺序走时, patch 总共会跑两次 —— 这是
#     预期行为, 不是重复操作/出错。
#
# 用法(仓库根执行):
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/load_recagent_data.ps1
# ============================================================================
param(
    [string]$ItemFile = 'shared/data/electronics.item',
    [string]$Node     = 'desktop-control-plane',
    [switch]$Force                       # 已存在且大小正确时仍重灌
)
$ErrorActionPreference = 'Stop'
$ns   = 'recweb-chaos'
$pvc  = 'recagent-data'
$deploy = 'rec-agent'
$EXPECT_SIZE = 266818680               # 实测字节数(1,946,169 行 / 占位符 506,946 / 真标题 1,439,223)
# ★权威哈希取自仓库根 README.md L70 的"自备大文件"表。原实现只**打印**宿主与 pod 的 sha256,
#   既不互比也不与这个常量比, 大小不符也只 Yellow 警告继续跑 -> 拿一个截断/换版的 .item
#   全程绿灯, 直到候选口径悄悄与 v2 不同(而消除这类偏差正是 B 档的目的)。现改为**断言**。
$EXPECT_SHA256 = '6dc34e4bccf0a98fe8693ced9d244474bbd8d17b6c11606e1bfc6f3e8d7a0717'

function Assert-LastOk($msg) { if ($LASTEXITCODE -ne 0) { throw $msg } }

if (-not (Test-Path $ItemFile)) {
    throw @"
找不到 $ItemFile -- 请在仓库根(${REPO_DIR})执行, 且该文件需自备。
它是 gitignored 的大文件(同模型权重):1,946,169 行 TSV, item_id/title 两列。
它是 agent 内容层口径的决定项: 缺它 -> title cache 全是"未知商品" -> tools.py 的
_filter_real_title 会把候选【全部滤光】, 与 (archived) agentfault_v2 完全不同口径。
"@
}
$src = (Resolve-Path $ItemFile).Path
$srcSize = (Get-Item $src).Length
Write-Host "== [src] $src ($srcSize B)" -ForegroundColor Cyan
if ($srcSize -ne $EXPECT_SIZE -and -not $Force) {
    throw "源文件大小 $srcSize != 基准 $EXPECT_SIZE -- 数据版本不对(截断?换版?)。明知故犯加 -Force。"
}
Write-Host "== [src] sha256 校验(与仓库根 README.md 的权威哈希比)" -ForegroundColor Cyan
$srcSha = (Get-FileHash $src -Algorithm SHA256).Hash.ToLower()
Write-Host "   $srcSha"
if ($srcSha -ne $EXPECT_SHA256) {
    if ($Force) {
        Write-Host "   ! sha256 与权威值不符, -Force 放行 -- 务必记进 provenance" -ForegroundColor Yellow
    } else {
        throw "源文件 sha256 不符 -- 期待 $EXPECT_SHA256, 实得 $srcSha。换版/截断的 .item 会让候选口径与 v2 不同。明知故犯加 -Force。"
    }
}

Write-Host "== [1/5] apply PVC(k8s/pilot/01b-recagent-data.yaml)" -ForegroundColor Cyan
kubectl apply -f k8s/pilot/01b-recagent-data.yaml
Assert-LastOk "apply PVC 失败"

Write-Host "== [2/5] 让 rec-agent 挂上这个卷(WaitForFirstConsumer: 不挂就不会 Bound)" -ForegroundColor Cyan
# ★Select-Object -Last 1: kubectl 输出按行返回, 多行时是数组, 直接比较/转型会出意外
$phase = (kubectl get pvc $pvc -n $ns -o jsonpath='{.status.phase}' | Select-Object -Last 1)
if ($phase -ne 'Bound') {
    Write-Host "   PVC 当前 phase=$phase -> 先跑 patch_recagent_collect.ps1 -SkipDataCheck 把卷挂上" -ForegroundColor Yellow
    & powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1 -SkipDataCheck
    if ($LASTEXITCODE -ne 0) { throw "patch(挂卷)失败" }
    kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/$pvc -n $ns --timeout=180s
    Assert-LastOk "PVC 迟迟不 Bound -- kubectl describe pvc $pvc -n $ns 查因"
}
Write-Host "   PVC Bound ✓"

Write-Host "== [3/5] 解析 PV 在节点上的目录" -ForegroundColor Cyan
$pv = (kubectl get pvc $pvc -n $ns -o jsonpath='{.spec.volumeName}' | Select-Object -Last 1)
if (-not $pv) { throw "读不到 PVC 的 volumeName" }
# rancher local-path 的目录命名约定:<pv>_<namespace>_<pvcName>
$dir = "/var/local-path-provisioner/${pv}_${ns}_${pvc}"
Write-Host "   pv=$pv"
Write-Host "   dir=$dir"
docker exec $Node sh -c "ls -ld $dir" | Out-Host
Assert-LastOk "节点上找不到 $dir -- local-path provisioner 的目录约定变了?用 kubectl describe pv $pv 核实"

Write-Host "== [4/5] docker cp 直写节点容器(254MiB, 秒级)" -ForegroundColor Cyan
$cur = ((docker exec $Node sh -c "stat -c %s $dir/electronics.item 2>/dev/null || echo 0" | Select-Object -Last 1) -as [string]).Trim()
if (-not $Force -and [int64]$cur -eq $srcSize) {
    Write-Host "   已存在且大小一致($cur B), 跳过(要重灌加 -Force)" -ForegroundColor Green
} else {
    docker cp $src "${Node}:$dir/electronics.item"
    Assert-LastOk "docker cp 失败(★用 PowerShell 跑, Git Bash 会改写 $dir 路径)"
}

Write-Host "== [5/5] 校验 + 让进程重读(title cache 是进程内单例, 不 restart 不会重读)" -ForegroundColor Cyan
kubectl rollout restart deploy/$deploy -n $ns
Assert-LastOk "rollout restart 失败"
kubectl rollout status deploy/$deploy -n $ns --timeout=300s
Assert-LastOk "rollout 未就绪"

$sz = (kubectl exec -n $ns deploy/$deploy -- sh -c 'stat -c %s /app/shared/data/electronics.item' | Select-Object -Last 1)
Write-Host "   pod 内 size = $sz (期待 $srcSize)"
if ([int64]$sz -ne $srcSize) { throw "pod 内文件大小对不上 -- 灌入不完整" }

# ★sha256 必须**互比**(原实现只打印两边, 谁也不比谁 -- 灌坏了照样绿灯)。
Write-Host "   sha256(宿主) = $srcSha" -ForegroundColor Cyan
$podShaLine = (kubectl exec -n $ns deploy/$deploy -- sha256sum /app/shared/data/electronics.item | Select-Object -Last 1)
$podSha = ($podShaLine -split '\s+')[0].ToLower()
Write-Host "   sha256(pod ) = $podSha" -ForegroundColor Cyan
if ($podSha -ne $srcSha) { throw "pod 内 sha256 与宿主不一致 -- docker cp 灌入损坏(宿主 $srcSha / pod $podSha)" }
Write-Host "   sha256 一致 ✓" -ForegroundColor Green

Write-Host "== [semantic] title cache 语义校验(期待 1946169 / 506946 / 1439223)" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- python -c "import sys;sys.path.insert(0,'/app/services/recommendation_agent');from agents import tools;c=tools._load_title_cache();ph=sum(1 for k,v in c.items() if v=='Product_'+k);print('entries=%d placeholder=%d real=%d'%(len(c),ph,len(c)-ph))" | Out-Host

Write-Host ""
Write-Host "DONE. PVC recagent-data 已就绪(采完 restore 时**保留不删**, 重采免重灌)。" -ForegroundColor Green
Write-Host "下一步: powershell -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1" -ForegroundColor Green
