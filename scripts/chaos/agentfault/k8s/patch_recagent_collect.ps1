# patch_recagent_collect.ps1 -- B 档: rec-agent 切"全栈 agent 故障采集"形态
# ============================================================================
# 与 patch_recagent_observe.ps1 的分工(★observe 脚本一个字都没改,别混用):
#   patch_recagent_observe.ps1 = G1/D 档专用(observe-only, 只观测不注入)。它是那两批
#     已产出数据(single_recagent 15 / dprobe 48)的**环境身份**, 冻结不动。
#   本脚本(collect)          = B 档专用: 把 rec-agent 摆成"可被 agentfault_runner
#     --backend k8s 驱动"的形态。差别在四处:
#       (1) 变体镜像换 :agentfault-v2(叠了仓库当前 tools.py + electronics.item 口径);
#       (2) 挂 PVC recagent-data -> /app/shared/data(title cache 数据源);
#       (3) 抬内存/CPU 上限(title cache 实测净增 436.8 MB, 256Mi 必 OOM);
#       (4) strategy 改 Recreate;
#     并且**不设** AGENTFAULT_INJECT/OBSERVE —— 那两个旋钮由 runner 每个 combo
#     `kubectl set env` 全量下发(本脚本反而要先把它们清干净, 否则 normal 臂的 observer
#     会被残留的 INJECT 顶掉, loader 会打 'AGENTFAULT_OBSERVE ignored', 口径就废了)。
#
# ★★为什么必须是 PowerShell(照 patch_recagent_observe.ps1 的坑 #2, 原样继承):
#   Git Bash(MSYS)会把命令行里 `VAR=/tmp/x` 这类"看起来像 POSIX 路径"的参数自动改写成
#   `C:/Users/.../Temp/x` 再传给 kubectl -> Linux 容器里 env 值变成 Windows 路径, 写盘必败。
#   (agentfault_runner 走 subprocess 直调 kubectl、不过 shell, 天然没这个问题;
#    只有人手敲的命令有。)
#
# ★内存上限只写在这里, **绝不写进 k8s/pilot/01-rec-agent.yaml** ——
#   traditional 255(含 15 个 single_recagent)是在 limits 256Mi 下采的。改 manifest 会让
#   `restore_recagent_stock.ps1` 的 `kubectl replace -f 01-rec-agent.yaml` 还原不回原环境。
#   采完 restore 一跑, 整 spec(镜像/内存/卷/env)回 stock, 与产出 255 的环境逐字节一致。
#
# 前置:
#   1) 变体镜像已 build(见 Dockerfile.agentfault 头注, 含 --build-context repo=<仓库根>);
#   2) PVC 已建且已灌数据:load_recagent_data.ps1(它内部会 apply 01b-recagent-data.yaml);
#   3) secret deepseek-env 在 ns 里。
#
# 用法(仓库根执行):
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_collect.ps1 -Mode observe
# 还原: restore_recagent_stock.ps1 -ConfirmedCollected
# ============================================================================
param(
    [string]$Image      = 'recweb-rec-agent:agentfault-v2',
    [string]$MemLimit   = '1536Mi',   # 实测稳态 ≈ 548MB(110.9 现基线 + 436.8 cache), ≈2.8x 余量
    [string]$MemRequest = '768Mi',
    [string]$CpuLimit   = '2',        # 267MB/1.95M 行逐行 split 在 500m 上会被 cfs 节流到几十秒,
    [string]$CpuRequest = '250m',     # 还会在 container_cpu_cfs_throttled_* 上留下假资源伪影
    [ValidateSet('collect', 'observe')]
    [string]$Mode       = 'collect',
    [switch]$SkipDataCheck            # 明知没灌数据也要 patch(灌数据流程里第一次 patch 就是这样)
)
$ErrorActionPreference = 'Stop'
$ns     = 'recweb-chaos'
$deploy = 'rec-agent'

function Assert-LastOk($msg) { if ($LASTEXITCODE -ne 0) { throw $msg } }

Write-Host "== [preflight] deploy/$deploy 存在?" -ForegroundColor Cyan
kubectl get deploy $deploy -n $ns | Out-Host
Assert-LastOk "deploy/$deploy 不在 ns=$ns -- 集群未起或 manifest 未部署(k8s/pilot/01-rec-agent.yaml)"

Write-Host "== [preflight] secret deepseek-env 存在?(缺 = LLM 路径无 key)" -ForegroundColor Cyan
kubectl get secret deepseek-env -n $ns | Out-Host
Assert-LastOk "secret/deepseek-env 不在 ns=$ns -- 从 .env 重建后再跑本脚本"

Write-Host "== [preflight] 变体镜像 $Image 在本地镜像库?" -ForegroundColor Cyan
docker image inspect $Image --format '{{.Id}}' | Out-Host
if ($LASTEXITCODE -ne 0) {
    # ★路径必须转成正斜杠: PowerShell 下 $PWD 渲染成 ${REPO_DIR}(反斜杠),
    #   与 README / Dockerfile 头注给的正斜杠写法不一致, 复制粘贴会出两种命令(复现审查⑤)。
    #   规范命令只有一份: 见 scripts/chaos/agentfault/README.md §"K8S 分支:从零到开采" 步骤 2。
    $repoFwd = ((Get-Location).Path -replace '\\', '/')
    throw ("镜像 $Image 不存在 -- 先 build(仓库根执行, 注意 --build-context 必须指仓库根): " +
           "docker build -f scripts/chaos/agentfault/k8s/Dockerfile.agentfault " +
           "--build-context repo=$repoFwd --build-arg SRC_GIT_SHA=$(git rev-parse --short HEAD) " +
           "-t $Image scripts/chaos/agentfault  " +
           "★build 完若 rollout 报 ErrImageNeverPull, 说明镜像没进节点 containerd, " +
           "见 README 同一节的 'docker save | ctr import' 救援命令(★必须 cmd.exe 跑;PowerShell 会因管道缓存 3GB 而 OOM)")
}

Write-Host "== [preflight] PVC recagent-data 存在?" -ForegroundColor Cyan
kubectl get pvc recagent-data -n $ns | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "PVC recagent-data 不存在 -- 先跑 load_recagent_data.ps1(它会 apply k8s/pilot/01b-recagent-data.yaml 并灌数据)"
}

# ---- strategic merge patch ----
# env 列表按 name 合并(追加不覆盖既有 env);volumes/volumeMounts/envFrom 同为按 name 合并。
# ★strategy 换 Recreate 时必须把 rollingUpdate 显式置 null, 否则 API server 会因
#   "RollingUpdate 字段在 Recreate 下不合法" 拒绝。
# ★为什么必须 Recreate: deploy 实测 rollingUpdate={maxSurge 25%, maxUnavailable 25%},
#   replicas=1 -> maxSurge 向上取整=1 / maxUnavailable 向下取整=0 => 新 pod Ready 前旧 pod
#   一直留在 Endpoints 里。经 Service proxy 的探针可能打到**旧配置 pod**(format combo 每 rep
#   换 subtype 时尤其危险), 而且 span 会写进旧 pod 的 emptyDir 被丢。照 sasrec 先例改 Recreate。
$patchFile = Join-Path $env:TEMP 'recagent_collect_patch.yaml'
@"
spec:
  strategy:
    type: Recreate
    rollingUpdate: null
  template:
    spec:
      containers:
        - name: rec-agent
          image: $Image
          env:
            - name: AGENTFAULT_INSTRUMENT
              value: "1"
            - name: SPAN_FILE
              value: /agentfault-data/spans.jsonl
            - name: AGENTFAULT_LEDGER
              value: /agentfault-data/ledger.jsonl
            - name: ITEM_FILE_PATH
              value: /app/shared/data/electronics.item
          envFrom:
            - secretRef:
                name: deepseek-env
          volumeMounts:
            - name: agentfault-data
              mountPath: /agentfault-data
            - name: recagent-data
              mountPath: /app/shared/data
              readOnly: true
          resources:
            requests:
              cpu: "$CpuRequest"
              memory: "$MemRequest"
            limits:
              cpu: "$CpuLimit"
              memory: "$MemLimit"
      volumes:
        - name: agentfault-data
          emptyDir: {}
        - name: recagent-data
          persistentVolumeClaim:
            claimName: recagent-data
            readOnly: true
"@ | Set-Content -Path $patchFile -Encoding ascii

Write-Host "== [patch] 应用 strategic merge patch($patchFile)" -ForegroundColor Cyan
kubectl patch deploy $deploy -n $ns --patch-file $patchFile
Assert-LastOk "kubectl patch 失败"

# ---- 注入旋钮:全量重置 ----
# strategic merge patch **只能加不能删** env, 所以残留的 AGENTFAULT_INJECT/KIND_* 必须
# 靠 `kubectl set env KEY-` 显式清掉。collect 模式下这些旋钮由 runner 每 combo 全量下发。
$knobs = @(
    'AGENTFAULT_INJECT-', 'AGENTFAULT_OBSERVE-',
    'AGENTFAULT_KIND_Sequence_Recommender-', 'AGENTFAULT_KIND_User_Behavior_Analyzer-',
    'AGENTFAULT_KIND_Product_Analyzer-', 'AGENTFAULT_KIND_Recommendation_Synthesizer-',
    'AGENTFAULT_WRONG_ASIN-', 'AGENTFAULT_DROP_AGENT-',
    'AGENTFAULT_FORMAT_SUBTYPE-', 'AGENTFAULT_FORMAT_FIELD-',
    'AGENTFAULT_SUBLLM_MODEL-', 'AGENTFAULT_HALLU_MODE-', 'AGENTFAULT_DEBUG-'
)
if ($Mode -eq 'observe') {
    # observe 形态(不采集, 只挂内容层观察器) —— 与 patch_recagent_observe.ps1 等价的臂
    $knobs = $knobs | Where-Object { $_ -ne 'AGENTFAULT_OBSERVE-' }
    $knobs += 'AGENTFAULT_OBSERVE=1'
}
Write-Host "== [knobs] 重置注入旋钮(Mode=$Mode)" -ForegroundColor Cyan
# ★用 $knobs 不是 @knobs:@ splatting 是 cmdlet 语法, 对 native exe 用数组变量本身即可
#   (PowerShell 会把每个元素当作独立参数)。
kubectl set env deploy/$deploy -n $ns -c rec-agent $knobs
Assert-LastOk "kubectl set env 重置旋钮失败"

Write-Host "== [rollout] 等待滚动就绪(Recreate: 会有短暂全停)" -ForegroundColor Cyan
kubectl rollout status deploy/$deploy -n $ns --timeout=300s
if ($LASTEXITCODE -ne 0) {
    # ★注意: 双引号字符串里绝不要写反引号(PowerShell 的转义符), 这里用中文引号引命令
    throw ("rollout 未就绪 -- kubectl describe pod -l app=recommendation_agent -n $ns 查因。" +
           "★若是 ErrImageNeverPull/ImagePullBackOff: 新 build 的镜像没进节点 containerd, " +
           "见 Dockerfile.agentfault 头注的「docker save | ctr -n k8s.io images import」灌入命令。")
}

# ============================ verify ============================
Write-Host "== [verify] image / strategy / resources" -ForegroundColor Cyan
$img = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.containers[0].image}'
$strat = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.strategy.type}'
$mem = kubectl get deploy $deploy -n $ns -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}'
Write-Host "   image=$img strategy=$strat mem.limit=$mem"
if ($img -ne $Image)      { throw "image 不是 $Image(实得 $img)" }
if ($strat -ne 'Recreate'){ throw "strategy 不是 Recreate(实得 $strat) -- 双 pod 会污染采集窗" }
if ($mem -ne $MemLimit)   { throw "memory limit 不是 $MemLimit(实得 $mem)" }

Write-Host "== [verify] 镜像源码溯源 RECWEB_SRC_GIT_SHA(抓'以为换了其实没换')" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- printenv RECWEB_SRC_GIT_SHA | Out-Host

Write-Host "== [verify] 代码口径: tools.py 里 _filter_real_title 必须 >= 1 处(当前源码 = 2)" -ForegroundColor Cyan
# ★Select-Object -Last 1: kubectl 可能返回多行(数组), 直接 [int] 转型会炸
$n = (kubectl exec -n $ns deploy/$deploy -- sh -c 'grep -c _filter_real_title /app/services/recommendation_agent/agents/tools.py || echo 0' | Select-Object -Last 1)
Write-Host "   grep -c _filter_real_title = $n"
if ([int]$n -lt 1) {
    throw ("镜像里还是 2026-07-19 的旧 tools.py(无 _filter_real_title) -- 与本机 v2 不同口径。" +
           "重 build 变体镜像并确认 --build-context repo=<仓库根> 生效。")
}

if (-not $SkipDataCheck) {
    Write-Host "== [verify] 数据口径: electronics.item(期待 266818680 B)" -ForegroundColor Cyan
    $sz = (kubectl exec -n $ns deploy/$deploy -- sh -c 'stat -c %s /app/shared/data/electronics.item 2>/dev/null || echo 0' | Select-Object -Last 1)
    Write-Host "   size = $sz"
    if ([int64]$sz -lt 200MB) {
        throw ("PVC 里没有 electronics.item(size=$sz) -- 先跑 load_recagent_data.ps1。" +
               "★只补代码不挂数据比现状更糟: _filter_real_title 会把候选【全部滤光】。")
    }
}

Write-Host "== [verify] /agentfault-data 可写(emptyDir 挂载;注入器 open('a') 前不 mkdir)" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- sh -c 'touch /agentfault-data/.probe && ls -la /agentfault-data' | Out-Host
Assert-LastOk "/agentfault-data 不可写 -- emptyDir 挂载失败(spike 坑 #1 的守门检查)"

Write-Host "== [verify] 容器内 AGENTFAULT_*/SPAN_FILE/ITEM_FILE_PATH env" -ForegroundColor Cyan
# ★2026-07-27 修:原来写 grep -E "A|B|C" —— PowerShell 5.1 把传给 native exe 的**内层双引号
#   吞掉**,于是 sh 收到的是没引号的 `grep -E A|B|C`,`|` 被当管道拆成多条命令,
#   实测打出三行 `sh: 1: SPAN_FILE: not found` 而真正的 env 一行没显示(这段核对形同没跑)。
#   改成多个 -e:全是裸字母、无需任何引号 ⇒ 不受 PS 引号处理影响。
#   (同款判别实验:内层带引号那版 sh 会额外报一条 `<第二个词>: command not found`,
#    多 -e 版不会 —— 就是靠这条差异定位的。)
kubectl exec -n $ns deploy/$deploy -- sh -c 'env | grep -e AGENTFAULT -e SPAN_FILE -e ITEM_FILE_PATH -e SASREC | sort' | Out-Host

Write-Host "== [verify] pod 内探下游 sasrec(★宿主 loopback 探不算数)" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- curl -s -o /dev/null -w "sasrec http=%{http_code}`n" --max-time 8 http://sasrec:8200/health | Out-Host

Write-Host ""
Write-Host "DONE. rec-agent 已切 B 档采集形态(Mode=$Mode)。" -ForegroundColor Green
Write-Host "★下一步开采(runner 自己还会跑一遍更深的 preflight):" -ForegroundColor Green
Write-Host "   bash scripts/chaos/agentfault/run_collect_agentfault.sh --yes --backend k8s" -ForegroundColor Green
Write-Host "★注意: title cache 是【懒加载】(第一次调 tool 时才读 254MiB), 每次 pod 重建后首发" -ForegroundColor Yellow
Write-Host "  /recommend 要多花 3-8s。runner 每个 combo 的 warmup 探针会吸收掉这一次, 不必手动预热。" -ForegroundColor Yellow
Write-Host "★采完还原: restore_recagent_stock.ps1 -ConfirmedCollected(会重建 pod 并清空 emptyDir!)" -ForegroundColor Yellow
