# k8s/pilot — RecShop 的 K8S manifest 目录

> ⚠️ **本文标题以下的绝大部分是 M0（三服务小试）时期写的**，只讲 `00/10/20/30` 四个
> manifest。目录里现在有 **28 个 yaml**（全栈已铺开），照那份旧清单 apply 完是**跑不起全栈**的。
> 需要"把 25 服务全栈拉起来"请直接看下面新增的 **§全栈 bring-up**；M0 那部分保留作历史
> 依据（GO/NO-GO 裁决记录 + 各条实测坑），不要照它当部署手册用。

---

## 全栈 bring-up（`agentfault --backend k8s` 与 traditional 采集的共同前置）

### 1. build 镜像

manifest 里引用的镜像 = `recweb-<服务短名>:latest`（外加两个外部镜像
`nginx:alpine` 给 catalog-gw、`registry.k8s.io/kube-state-metrics:v2.13.0`）。
先建共享 base，再逐服务薄镜像（**build context 一律仓库根**，`-f` 指各服务的 Dockerfile）：

```bash
docker build -f docker/recweb-base.Dockerfile -t recweb-base:latest .
docker build -f services/catalog_service/Dockerfile -t recweb-catalog:latest .
# … 其余同理，镜像名照 manifest 里的 image: 字段
```

当前 manifest 需要的完整镜像名清单（用它核对哪个还没 build）：

```bash
grep -h "image:" k8s/pilot/[0-9]*.yaml | sed 's/^ *//' | sort -u
```

> Docker Desktop 内置 K8S 与 `docker build` **通常**共享镜像库，无需 `kind load`。
> 但新版的 containerd `k8s.io` namespace 有可能不共享 —— 若 pod 报 `ErrImageNeverPull`
> 或跑的还是旧代码，用 `docker save … | ctr -n k8s.io images import -` 灌入
> （见 `scripts/chaos/agentfault/k8s/Dockerfile.agentfault` 头注，★★必须 **cmd.exe** —— Git Bash 与 **PowerShell 都不行**，PS 会报 Insufficient memory（2026-07-27 实测））。

### 2. 两个 secret（apply 之前建）

```powershell
# ① db-cred — 各服务的 MySQL 口令（键名固定 password；DB_HOST=host.docker.internal，
#    库仍在宿主 MySQL shopify2，不迁库）
kubectl create secret generic db-cred -n recweb-chaos --from-literal=password='<.env 的 DB_PASSWORD>'

# ② deepseek-env — rec-agent 的 LLM 凭据（envFrom 整包注入，键名必须与 .env 一致）
kubectl create secret generic deepseek-env -n recweb-chaos `
  --from-literal=DEEPSEEK_API_KEY='<你的 key>' `
  --from-literal=DEEPSEEK_API_BASE='https://api.deepseek.com/v1' `
  --from-literal=DEEPSEEK_MODEL='deepseek-chat'
```

> `deepseek-env` 只在 agentfault 的 patch 脚本里被引用（`envFrom.secretRef`），
> stock 的 `01-rec-agent.yaml` 不引用它 —— 但**它必须先存在**，否则 patch 会 fail。

### 3. apply（顺序 = 文件名排序；ns 必须最先）

```powershell
kubectl apply -f k8s/pilot/00-namespace.yaml
Get-ChildItem k8s/pilot/[0-9]*.yaml | Sort-Object Name |
  Where-Object { $_.Name -ne '11b-catalog-bad.yaml' } |
  ForEach-Object { kubectl apply -f $_.FullName }
```

按需取舍：

| 文件 | 说明 |
|---|---|
| `01b-recagent-data.yaml` | PVC `recagent-data`（agentfault B 档的 `electronics.item` 载体）。非 B 档可跳；`load_recagent_data.ps1` 自己也会 apply 它 |
| `11-catalog-gw.yaml` | nginx 网关（traditional 线的 catalog 卡口）。B 档不需要，但留着无害 |
| `11b-catalog-bad.yaml` | **故意坏**的 catalog 变体（实验用）→ 默认**不要** apply |
| `12-kube-state-metrics.yaml` | **host 水位必需** —— 不装则 Prometheus 的 kube-state 指标缺失 |
| `chaos-*.yaml` | Chaos Mesh CRD 实例，**装了 Chaos Mesh 才 apply**。★agentfault B 档反而要求 ns 里**没有**这些（有则 preflight 拦下） |
| `20-sasrec.yaml` | 最慢（hostPath 挂 9.2GB pkl，startupProbe 容忍 ~300s） |

### 4. 验收

```bash
kubectl get pods -n recweb-chaos          # 期望各 Deployment 都 1/1 Running（sasrec 最后就绪）
kubectl -n recweb-chaos exec deploy/rec-agent -- curl -s -o /dev/null -w '%{http_code}' http://sasrec:8200/health
```

### 5. 全停

```bash
kubectl delete namespace recweb-chaos     # 连带删 ns 内所有资源（含 PVC）
```

---

## ↓↓↓ 以下为 M0（2026-06-25 三服务小试）历史记录 ↓↓↓

> 分支 `feat/k8s-chaosmesh`。本目录 = TASK-K8S 迭代 1（M0 de-risk）的可复现 manifest/脚本。
> **只写文件, 不运行任何命令** —— 集群起好后由主循环 build/apply/smoke, 据本文 §GO/NO-GO 六条裁决。
> 权威依据: `(project docs)/archive/TASK-K8S-migration.md`「迭代 1」+「M0 pilot 1-10 条」+「[主循环] 2026-06-25 裸进程基线」; `(project docs)/archive/TASK-K8S-acceptance-spec.md` §7(⛔该文档已降级历史存档;其 §7 容器/manifest 内容大体仍可用,但 **Kustomize 条款已死**——全仓无 kustomization 文件)。

---

## 文件清单

| 文件 | 作用 |
|---|---|
| `00-namespace.yaml` | namespace `recweb-chaos`（最先 apply） |
| `10-catalog.yaml` | catalog_service Deployment + ClusterIP Service（:5005, label app=catalog） |
| `20-sasrec.yaml` | sasrec_api Deployment + Service（:8200, 16Gi limit + hostPath 挂 pkl/pth + startupProbe ~300s） |
| `30-backend.yaml` | backend_api Deployment + Service（:5000, walking skeleton, SASREC_API_URL=http://sasrec:8200） |
| `chaos-smoke-podchaos.yaml` | PodChaos pod-failure 打 catalog smoke（**装完 Chaos Mesh 再 apply**） |
| `../../docker/recweb-base.Dockerfile` | 共享 base 镜像（公共依赖） |
| `../../services/{catalog_service,backend_api,sasrec_api}/Dockerfile` | 三服务镜像 |
| `../../.dockerignore` | 排大文件/无关目录 |
| `(内部 QA 脚本)` | 正常窗 旧栈 vs 新栈 等价 diff（GO 第⑥条） |

---

## apply 顺序（主循环执行；集群已启用 Docker Desktop K8S + .wslconfig 已生效）

### 1. build 镜像（仓库根执行，build context = 仓库根）

```bash
# base 先建（catalog/backend 依赖它）
docker build -f docker/recweb-base.Dockerfile -t recweb-base:latest .

# 三服务镜像（-f 指 Dockerfile, context 为仓库根以便 COPY shared/）
docker build -f services/catalog_service/Dockerfile -t recweb-catalog:latest .
docker build -f services/backend_api/Dockerfile     -t recweb-backend:latest .
docker build -f services/sasrec_api/Dockerfile       -t recweb-sasrec:latest .
```

> Docker Desktop 内置 K8S 与 docker build 共享同一 daemon/镜像库 → build 完即可用，**无需 `kind load`**（kind 才要）。`imagePullPolicy: IfNotPresent` 用本地镜像、不去 registry（绕 Clash G7）。

### 2. DB 密码 Secret（apply 前；manifest 里 DB_PASSWORD 是占位 `REPLACE_ME`）

> ⛔ **已过时**：secret 的实际名字是 **`db-cred`** 不是 `recweb-db`（manifest 现在都写死
> `secretKeyRef.name: db-cred`），且 manifest 早已改成 `valueFrom` 不再是占位。
> 以上面 §全栈 bring-up 步骤 2 的命令为准。

```bash
# 🔶 主循环实测确认: .env 里 DB_PASSWORD=<your-mysql-password>。建 Secret 后把 10-catalog/30-backend 的
#    env DB_PASSWORD value 改成 valueFrom.secretKeyRef（或临时直接填值, M0 内网可接受）。
kubectl -n recweb-chaos create secret generic recweb-db \
  --from-literal=password='<.env 里的 DB_PASSWORD>'
```

### 3. apply manifests（按序）

```bash
kubectl apply -f k8s/pilot/00-namespace.yaml
kubectl apply -f k8s/pilot/10-catalog.yaml
kubectl apply -f k8s/pilot/20-sasrec.yaml      # sasrec 最慢, 起后 startupProbe 容忍 ~300s
kubectl apply -f k8s/pilot/30-backend.yaml
kubectl get pods -n recweb-chaos -w            # 等三个都 Running + READY 1/1
```

### 4. port-forward（runner / 等价 diff / 浏览器访问）

```bash
kubectl -n recweb-chaos port-forward svc/catalog 15005:5005 &
kubectl -n recweb-chaos port-forward svc/sasrec  18200:8200 &
kubectl -n recweb-chaos port-forward svc/backend  15000:5000 &
```

### 5. smoke + 等价 diff

```bash
# 健康
curl -s --noproxy '*' http://127.0.0.1:15005/health   # catalog
curl -s --noproxy '*' http://127.0.0.1:18200/health   # sasrec(model_loaded:true)
curl -s --noproxy '*' http://127.0.0.1:15000/health   # backend(sasrec_api:healthy = 跨服务可达)

# 等价 diff（旧裸栈 catalog 在 5005, 新 K8S 经 15005）
# python tests/qa/k8s_equiv_diff.py (excluded from staging) \
  --old http://127.0.0.1:5005 \
  --new http://127.0.0.1:15005 \
  --path "/api/items?per_page=5"

# Chaos Mesh smoke（装完 namespace-scoped Chaos Mesh 后）
kubectl apply -f k8s/pilot/chaos-smoke-podchaos.yaml
kubectl get podchaos -n recweb-chaos          # AllInjected
kubectl get pods -n recweb-chaos -w           # catalog 30s 内不可用→恢复
kubectl delete -f k8s/pilot/chaos-smoke-podchaos.yaml
```

### 全停

```bash
kubectl delete namespace recweb-chaos          # 连带删 ns 内所有资源
```

---

## ⚠ 安全护栏（apply/smoke 时不可越）

- **items/inventory 只读零写**：基线 `items=3849590678 inventory=3935678504`。catalog 三端点全 GET 只读。**勿用 backend_api 的 `/api/recommend`**（全链路 INSERT recommendations/interactions, 不在 CHECKSUM 覆盖内）；M0 walking skeleton 验跨服务 trace 用 backend 只读端点或直打 sasrec 纯推理。
- **DB 外接宿主**：`DB_HOST=host.docker.internal`，CHECKSUM 旧基线零改、不碰 9GB 迁移。
- **大文件绝不进镜像**：`.dockerignore` 已排 `*.pkl/*.pth/*.inter`；sasrec 用 hostPath 只读挂卷。

---

## GO / NO-GO 量化裁决（六条全达才 GO；任一未达 = NO-GO → 转 M1 docker compose）

> 阈值锚点 = 裸进程基线（黑板 [主循环] 2026-06-25）：pickle.load 39.6s / 总加载 40.5s / 峰值工作集 13.34GB。
> 客观化触发：任一红立即转；**投入 ≥1.5 工作日仍未全绿自动转**；删除"压力大"主观触发。

- [ ] **① sasrec 加载 + 内存**：sasrec pod `/health` `model_loaded:true` 的耗时 ≤ 裸进程基线 ×3（40.5s ×3 ≈ **120s**；9p 慢盘最多到 startupProbe 上限 300s 仍算红）；且 pod 未 OOMKill（实测常驻峰值 < limit 16Gi，即 13.34GB ×1.2 余量未破）。
- [ ] **② WSL2 内存峰值**：三服务全 Running 时 WSL2/Vmmem 内存峰值 < `.wslconfig` 配额 ×0.7（配额 24GB → ceiling **16.8GB**）。超则 25 服务全量必爆 → 缩 OTel 容器或转 M1。
- [ ] **③ per-service metric 非空率 100%**：catalog 进集群后跑 per_service_metrics，期望 BASE 列**非空率 100%**（非"有就行"）。先 `curl 'http://127.0.0.1:9090/api/v1/label/exported_job/values'` 确认 Prometheus `exported_job` 标签未因换 collector 断链（G1 最致命，OTLP 仍指宿主 compose collector → exported_job 应与裸栈一致）。
- [ ] **④ Chaos Mesh 信号可分**：PodChaos pod-failure apply → catalog pod 状态变化（30s 不可用→恢复）→ 注入窗 vs 正常窗 OTel 信号**最小可分**（catalog up/QPS/error 在窗内变暗）。
- [ ] **⑤ 跨服务 trace 关联**：backend `/health` 显示 `sasrec_api:healthy`（经 svc DNS `http://sasrec:8200` 可达）；Jaeger 出现 backend_api→sasrec_api 跨 span trace；pod 内**无残留 127.0.0.1:50xx 撞自己**（backend 的 SASREC_API_URL 已指 svc 名）。
- [ ] **⑥ 正常窗等价 diff 通过**：`k8s_equiv_diff.py` 退出码 0（catalog 只读 GET 正常窗结构等价 + 延迟差在阈内）。

**全绿 → GO（走 K8S，论文目标栈）。任一红 / ≥1.5 工作日未全绿 → NO-GO → 转 M1 docker compose（上游认可，容器内 netem/Pumba 补真丢包/corrupt/pod-kill）。**

---

## 🔶 主循环 apply 前需实测确认的清单

1. **python base tag**：`python:3.11-slim` 是否匹配 conda env recweb2 的 Python 版本（本任务未侦察 recweb2 Python 版本）。
2. **torch 安装**：sasrec Dockerfile 的 torch 装法（CPU wheel vs cu118）+ recbole vendor 运行时还缺哪些三方包（实测 import 验证）。
3. **`.pth` 真实文件名**：`20-sasrec.yaml` 的 `SASREC_MODEL_PATH` / hostPath / mountPath 三处都写 `SASRec-Feb-24-2026_17-54-22.pth`（取自黑板基线条目），核对宿主实际文件名一致。
4. **hostPath 实际路径**：`/run/desktop/mnt/host/d/<repo-root>/services/sasrec_api/...`（盘符小写 d）能否挂到；若 9p 读 9GB 超 300s，cp 进 WSL2 ext4 改指向（G8）。
5. **`host.docker.internal` 解析**：catalog/backend pod 内能否解析（连宿主 MySQL :3306 + 宿主 OTel collector :4317）；部分 Docker Desktop 版本需 `hostAliases` 兜底。
6. **collector OTLP 端口**：已从 `ops/docker-compose.otel.yml` 实测 = **4317**（gRPC，宿主映射 `4317:4317`）；8889 是 Prometheus scrape 端点。manifest OTLP endpoint 已用 4317，核对 compose 栈仍 UP。
7. **DB_PASSWORD Secret**：manifest 占位 `REPLACE_ME`，apply 前建 Secret（.env 里是 <your-mysql-password>）。
8. **Chaos Mesh CRD apiVersion**：`chaos-mesh.org/v1alpha1` 随实装 Chaos Mesh 版本核对。
