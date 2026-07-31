# patch_recagent_observe.ps1 -- G1: rec-agent 切 agentfault 变体镜像 + observe-only 内容层埋点
# ============================================================================
# ★★【2026-07-27 分工声明】本脚本**冻结不改** —— 它是 single_recagent 15(G1)与
#   dprobe_crosslayer 48(D 档)两批已产出数据的**环境身份**(镜像 :agentfault / limits 256Mi /
#   无数据卷 / observe-only), 改它就等于事后篡改那两批的 provenance。
#   B 档"K8S 全栈 agent 故障采集"用**另一个**脚本: patch_recagent_collect.ps1
#   (变体镜像 :agentfault-v2 = 叠了仓库当前 tools.py;挂 PVC recagent-data 给 title cache;
#    内存上限抬到 1536Mi;strategy 改 Recreate;注入旋钮全量重置交给 runner 下发)。
#   两者互斥, 采一批前先跑对应那个;还原都用 restore_recagent_stock.ps1 -ConfirmedCollected。
# ★注意本脚本头注下面第 6 行"Docker Desktop 内置 K8S 与 docker build 共享镜像库"是
#   spike 遗留的错话, G1 已推翻((project docs)/archive/TASK-K8S-G1-recagent-traditional.md 坑 3): 新版 Docker Desktop
#   的节点 containerd(k8s.io ns)与 docker build 存储**不共享**, 重 build 的同 tag 镜像
#   在 IfNotPresent 下会继续跑旧 digest, 必须 `docker save | ctr -n k8s.io images import` 灌入。
# ============================================================================
# 任务书: (project docs)/archive/TASK-K8S-G1-recagent-traditional.md 改动 3。
# 做什么(对 deploy/rec-agent, ns=recweb-chaos):
#   1. image -> recweb-rec-agent:agentfault(spike 2026-07-19 已 build, 5/5 PASS;
#      Docker Desktop 内置 K8S 与 docker build 共享镜像库, imagePullPolicy=IfNotPresent 即用)
#   2. env: AGENTFAULT_INSTRUMENT=1(openinference 内容层 span) + AGENTFAULT_OBSERVE=1(只观测不注入)
#      ★不注任何故障 env(AGENTFAULT_INJECT / AGENTFAULT_TARGET / AGENTFAULT_FORMAT_SUBTYPE ...)——
#        observe-only 是 G1 定案: infra 故障下结构通道应全程"上游集完整" = 结构检测器的阴性对照。
#      ★为什么 INSTRUMENT 也开: G1 采的就是"infra 故障下的 agent 内容层轨迹", 只开 OBSERVE 拿不到
#        LLM I/O 内容 span(openinference 靠 INSTRUMENT arm);v2 数据集 normal 基线同款 = (1)+(3) 组合
#        (scripts/chaos/agentfault/COLLECTION_DESIGN.md)。observe wrapper 自身强制非流式(install_observer
#        docstring), 与 v2 采集口径一致 -> provenance 记"批内统一非流式"。
#   3. SPAN_FILE=/agentfault-data/spans.jsonl + AGENTFAULT_LEDGER=/agentfault-data/ledger.jsonl,
#      落 emptyDir 卷 agentfault-data(挂 /agentfault-data)。
#      ★为什么 emptyDir 而不是平铺 /tmp(spike 坑 #1 的正解): 注入器/exporter 对落盘路径 open('a') 前
#        【不 mkdir】—— 目录不存在时 FileNotFoundError 会被 SAFETY 层吞掉, 注入/观测记录【静默丢】
#        (spike 2026-07-19 实证: AGENTFAULT_LEDGER=/tmp/af/ledger.jsonl 因 /tmp/af 不存在整窗台账蒸发)。
#        emptyDir 由 kubelet 在容器起前自动建目录并挂载 -> 路径必然存在, 且 pod in-place 重启(pause-swap
#        的 pod_failure 形态)不清卷; 平铺 /tmp 也可用但 /tmp 随容器文件系统, 容器重启即丢 -> 弃。
#   4. envFrom secretRef deepseek-env(spike 遗产, secret 已留在 ns —— K8S 140-case 线从未注过
#      DeepSeek key, 跨 LLM 路径采集必须有;若 secret 不在会 preflight FAIL 提示)。
#
# ★★为什么本脚本必须是 PowerShell(spike 坑 #2, 写死):
#   Git Bash(MSYS)会把命令行里 `VAR=/tmp/x` 这类"看起来像 POSIX 路径"的参数自动改写成
#   `C:/Users/.../Temp/x` 再传给 kubectl -> Linux 容器里 env 值变成 Windows 路径, 写盘必败。
#   spike 2026-07-19 实证: 同一条 kubectl set env 在 Git Bash 下坏、在 PowerShell 下干净。
#   (MSYS2_ARG_CONV_EXCL 可绕但易忘 -> 直接钉死 PowerShell。)
#
# 用法(还原见同目录 restore_recagent_stock.ps1):
#   powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/patch_recagent_observe.ps1
# ============================================================================
$ErrorActionPreference = 'Stop'
$ns     = 'recweb-chaos'
$deploy = 'rec-agent'
$image  = 'recweb-rec-agent:agentfault'

Write-Host "== [preflight] deploy/$deploy 存在?" -ForegroundColor Cyan
kubectl get deploy $deploy -n $ns | Out-Host
if ($LASTEXITCODE -ne 0) { throw "deploy/$deploy 不在 ns=$ns -- 集群未起或 manifest 未部署(k8s/pilot/01-rec-agent.yaml)" }

Write-Host "== [preflight] secret deepseek-env 存在?(spike 遗产;缺 = LLM 路径无 key, 必须先建)" -ForegroundColor Cyan
kubectl get secret deepseek-env -n $ns | Out-Host
if ($LASTEXITCODE -ne 0) { throw "secret/deepseek-env 不在 ns=$ns -- 按 spike 记录从 .env 重建后再跑本脚本" }

Write-Host "== [preflight] 变体镜像 $image 在本地镜像库?" -ForegroundColor Cyan
docker image inspect $image --format '{{.Id}}' | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw ("镜像 $image 不存在 -- 先 build(仓库根执行, context 必须是 scripts/chaos/agentfault 绕开根 .dockerignore): " +
           "docker build -f scripts/chaos/agentfault/k8s/Dockerfile.agentfault -t $image scripts/chaos/agentfault")
}

# ---- strategic merge patch(env 列表按 name 合并 = 追加不覆盖既有 env;volume/mount/envFrom 同为追加)----
$patchFile = Join-Path $env:TEMP 'recagent_observe_patch.yaml'
@'
spec:
  template:
    spec:
      containers:
        - name: rec-agent
          image: recweb-rec-agent:agentfault
          env:
            - name: AGENTFAULT_INSTRUMENT
              value: "1"
            - name: AGENTFAULT_OBSERVE
              value: "1"
            - name: SPAN_FILE
              value: /agentfault-data/spans.jsonl
            - name: AGENTFAULT_LEDGER
              value: /agentfault-data/ledger.jsonl
          envFrom:
            - secretRef:
                name: deepseek-env
          volumeMounts:
            - name: agentfault-data
              mountPath: /agentfault-data
      volumes:
        - name: agentfault-data
          emptyDir: {}
'@ | Set-Content -Path $patchFile -Encoding ascii

Write-Host "== [patch] 应用 strategic merge patch($patchFile)" -ForegroundColor Cyan
kubectl patch deploy $deploy -n $ns --patch-file $patchFile
if ($LASTEXITCODE -ne 0) { throw "kubectl patch 失败" }

Write-Host "== [rollout] 等待滚动就绪" -ForegroundColor Cyan
kubectl rollout status deploy/$deploy -n $ns --timeout=180s
if ($LASTEXITCODE -ne 0) { throw "rollout 未就绪 -- kubectl describe pod -l app=recommendation_agent -n $ns 查因" }

# ---- 验证(照 spike live 半清单 3): env 注入 + loader 双臂装载 + 落盘目录可写 ----
Write-Host "== [verify] 容器内 AGENTFAULT_*/SPAN_FILE env" -ForegroundColor Cyan
# ★2026-07-27 修:同款 PS 引号坑(见 restore_recagent_stock.ps1 L69 的长注) ——
#   PowerShell 5.1 吞掉内层双引号,`|` 被 sh 当管道拆成多条命令,这段核对形同没跑。
kubectl exec -n $ns deploy/$deploy -- sh -c 'env | grep -e AGENTFAULT -e SPAN_FILE' | Out-Host

Write-Host "== [verify] 落盘目录 /agentfault-data 存在且可写(emptyDir 挂载)" -ForegroundColor Cyan
kubectl exec -n $ns deploy/$deploy -- sh -c 'touch /agentfault-data/.probe && ls -la /agentfault-data' | Out-Host
if ($LASTEXITCODE -ne 0) { throw "/agentfault-data 不可写 -- emptyDir 挂载失败(spike 坑 #1 的守门检查)" }

Write-Host "== [verify] loader 装载日志(期待 'armed openinference' + observer;OBSERVE 臂无 'AGENTFAULT_OBSERVE ignored')" -ForegroundColor Cyan
kubectl logs -n $ns deploy/$deploy --tail=50 | Select-String -Pattern 'agentfault-loader' | Out-Host

Write-Host ""
Write-Host "DONE. rec-agent 已切 observe-only 变体。采集期间勿再动 env;" -ForegroundColor Green
Write-Host "每 case 后回收: kubectl exec -n $ns deploy/$deploy -- cat /agentfault-data/spans.jsonl > <case>/raw/agent_spans/spans.jsonl" -ForegroundColor Green
Write-Host "采完还原: powershell -ExecutionPolicy Bypass -File scripts/chaos/agentfault/k8s/restore_recagent_stock.ps1" -ForegroundColor Green
