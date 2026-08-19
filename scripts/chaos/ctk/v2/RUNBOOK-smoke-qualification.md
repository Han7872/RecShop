# v2 Smoke & Qualification Runbook

> **文档用途**：traditional v2 的工程 smoke、资格块 B1、正式块 B2–B5 的执行 runbook。
>
> **权威来源**：HANDOFF-2026-08-10 §9（正式采集设计）、§10（在线停止规则）、§11（验收标准）、§14（第一批动作）。
> 本 runbook 是这些章节的可执行展开，不替代它们。

## 前置条件（任何 smoke/B1 前必须满足）

1. **P0 全绿**：P0-1..P0-9 全部实现，自动化拒绝测试（`tests/unit/v2/`）全绿。
   - 当前状态（2026-08-11）：P0-7（attempt ledger）、P0-8（packager gate）、schema 契约层完成；
     P0-1/2/3/4/5/6/9 待实现（需 K8S live 验证）。
2. **K8S 集群运行**：`kubectl get nodes` 返回 Ready 节点；Chaos Mesh controller running。
3. **环境卡可采集**：`EnvironmentCard`（v2/schema.py）的所有 required 字段能动态读取（machine_id、版本、镜像 digest）。
4. **原子 lease 可用**：`AttemptLedger`（v2/ledger.py）能 open_attempt/transition；单集群同时刻仅一个 run。
5. **v1 冻结确认**：`datasets/k8s_pilot/`（7 棵在役树 255 case）只读；v2 路径独立（`datasets/k8s_pilot_v2/` 或类似）。

## Stage 1：工程 smoke（约 10 个，**永不进入数据集**）

**目的**：让协议和代码稳定，不是采数据。任何 smoke 数据都不得晋升为正式 case（HANDOFF §9.1、§15）。

smoke 必须覆盖以下场景（每个至少 1 次）：

| # | smoke 场景 | 验证什么 P0 |
|---|---|---|
| S1 | StressChaos service CPU（如 catalog）apply→active→recover | P0-2 stable-pod、P0-4 逐腿 gate |
| S2 | NetworkChaos delay apply→active→recover | P0-4 actual target 回填 |
| S3 | PodChaos pod-failure apply→active→recover（含 hard reset） | P0-4 recovery timeout、P0-9 cleanup |
| S4 | runtime hook（FAULT_RAISE/FAULT_DELAY_MS）**新机制**（非 kubectl set env）+ sham toggle | P0-2 消除 rollout 指纹 |
| S5 | multi-root dual（如 dual01 配置+网络） | P0-3 状态机 transition、P0-4 多腿 |
| S6 | dual06 新机制（重设计后的配置腿）或 calibration 判定 config-state-only | P0-4 manifestation、§9.4 dual06 决策点 |
| S7 | telemetry 查询失败注入（prometheus down / jaeger cap 400+） | P0-6 metrics/traces coverage |
| S8 | log 抓取：kubectl --previous（pod 重启场景）+ truncation 检测 | P0-6 logs |
| S9 | recovery timeout → hard reset → 孤儿 CRD 检测 | P0-9 原子清理 |
| S10 | packager 拒绝门：smoke 数据跑 `package_for_delivery.py --strict-gate` 确认被拒 | P0-8 |

**smoke 通过标准**（全部满足才进 B1）：
- 每个 smoke 的 `ReleaseContract.releaseable_strict` 或 `releaseable_auxiliary` 正确反映 gate 结果；
- sham toggle（S4）证明 pod UID/restart/start time 不变；
- telemetry coverage（S7/S8）正确区分 no_series/query_error/timeout/zero；
- recovery timeout（S9）触发 hard reset + 孤儿 CRD 检测；
- 所有 smoke 数据在 `AttemptLedger` 里 state=`excluded`（不计入正式分母）。

## Stage 2：冻结

smoke 全过后，冻结以下内容（任何变更结束当前 protocol epoch，HANDOFF §10 rule 7）：

- `protocol_version`（v2/schema.py PROTOCOL_VERSION）
- runner commit（git sha）
- scenario/config/workload hash
- 所有镜像 digest
- Chaos Mesh / K8S / OTel / Prometheus / Jaeger / Loki 版本
- 随机 seed（B1..B5 每块独立预注册 seed）

冻结后写入 `manifests/protocol.json` + `manifests/environment.json`。

## Stage 3：B1 资格块（51 fault attempts + ≥6 controls）

**计划分母**：51 个场景 × 1 次 + ≥6 个 no-fault/sham controls = 57+ attempts。

**随机化**（HANDOFF §9.2）：
- 51 个场景 + 6 controls 随机排序，用预注册 seed；
- controls 分布在块首/块中/块尾（不是全堆一头）；
- scenario/replicate/顺序/seed/control 位置**在看到结果前**提交到 manifest。

**B1 晋升 replicate 1 的条件**（全部满足，HANDOFF §9.3）：
1. 所有共享协议（collector/gate/workload/schema/fault primitive）冻结，smoke 后零修改；
2. 无环境/时钟/镜像/config drift；
3. strict cases 全部满足逐腿、telemetry、recovery、checksum 门；
4. controls 全部通过（no-fault 期间无异常症状）；
5. 独立 Agent 从 raw 盲审，不只信 runner 自报；
6. B1 结束后无需任何影响数据语义的代码或阈值修改。

**关键约束**：如果 B1 期间发现必须改共享协议 → **B1 全部降级为 engineering qualification**，修完后从头重跑新 B1；不能把改动前后 case 混为同一 replicate（HANDOFF §9.3）。

## Stage 4：B2–B5 正式块

每块：51 场景 × 1 次 + ≥6 controls，重新用预注册 seed 随机顺序。

- 同一集群注入串行执行；
- 可以并行做只读校验、导出、离线审计；
- 不能并发注入。

## Stage 5：重建交付

从 native raw 全重建（HANDOFF §13）：
- `strict/`、`auxiliary/`、`controls/` 三个一级目录；
- `views/benchmark_blind/`（删 operations/fault flag/root-local 标记，主分数用）；
- `views/raw_audit/`（保留完整 operations/controller conditions/actual target/失败 attempt）；
- `views/compatibility/`（BARO/RCD/Eadro/Who&When 旧方法兼容，不改写 raw 语义）；
- `manifests/`（protocol.json/environment.json/split.json/attempts.jsonl/attrition.json/schema.json/MANIFEST.json/SHA256SUMS.txt）；
- `README.md`/`DATASHEET.md`/`EVAL_NOTES.md`。

---

## strict / auxiliary 判定表

每个 case 根据其 `ReleaseContract`（v2/schema.py）分类：

| track | 判定条件 | 含义 |
|---|---|---|
| **strict** | `releaseable_strict`==True（全部 11 个合取项 pass + track="strict"） | 所有硬门通过，进入 strict main benchmark |
| **auxiliary** | `releaseable_auxiliary`==True（apply/active/recover/checksum pass + track="auxiliary"） | gray/masked/config-state-only，单独评估，不稀释 strict |
| control | no-fault/sham，通过健康检查 | 基线对照，不计入 fault attempts 分母 |
| excluded | gate 失败（任何 required 字段 false/missing） | 记入 `excluded_ledger.json`，不进任何交付目录 |

### Known limitation — P0-8 单字段 gate 的盲区（P0-4 互补）

P0-8 的 `check_release_gate` 读 v1 metadata 的 `ready_for_release`/`sample_status`/`checksum_guard` 字段。
它能挡住**字段级**失败（如 `podfail_cart_r4` ready=false），但**无法**捕获 v1 采集时聚合门掩盖的单腿未恢复（如 `dual08_uni_r5`：某根因腿 recover_poll=false，但聚合 post ok_ratio>=0.8 仍让 ready=True）。

这层盲区由 **P0-4 逐腿 fail-closed gate** 补上：runner 实现后，每条 leg 的 `LegGateResult.releaseable`（v1 case 经 `leg_gate_from_metadata_fault` 桥接后必为 False，因为 v1 从未记录 actual_targets/controller_recovered/root_probe_ok）会驱动 `ReleaseContract.every_leg_recovery_pass`，从而堵住 `dual08_uni_r5` 这类聚合门掩盖 case。两层互补：P0-8 守字段、P0-4 守语义。

### dual06 决策点（HANDOFF §9.4）

| smoke 后观测 | 决策 | track |
|---|---|---|
| 重设计/校准后，两条根因腿各有预注册 activation + manifestation 证据 | 进入 strict dual main | strict |
| 仍只能看到配置状态变化，普通业务遥测无独立足迹 | 诚实标 `config-state-only` | auxiliary |
| 任意一条腿 apply/active/recover/checksum 失败 | excluded | excluded |

**禁止**：用 whitelist 把 `validity_pass=false` 变成 strict ready（v1 的 `V1_DEGRADED_WHITELIST` 4 个 id 在 v2 strict 中全部移除）。

**若要 strict 主集维持 51×5=255**：必须在 B1 冻结前用预注册+过 smoke 的新 dual 场景替换 dual06；dual06 原样进 auxiliary。不得在看完正式结果后临时找替代 case。

---

## 在线停止规则速查（HANDOFF §10）

| 触发条件 | 动作 |
|---|---|
| 同一 precheck 连续失败 2 次 / block 累计 3 次 | 暂停，诊断环境 |
| 任一 fault leg 未 active 或 actual target ≠ GT | attempt 立即 invalid |
| 同一 scenario manifestation 连续失败 2 次 | 暂停该场景，回 calibration；不得无限重试直到有症状 |
| 任一 recovery timeout | attempt invalid + hard reset；连续 2 次 / block >2% 暂停整块 |
| no-fault/sham control 失败 | 隔离自上一个健康 control 以来所有 case，复核后决定重采 |
| 必需 telemetry/clock/hash/schema gate 失败 | 不能用填值补成 retained |
| 代码/配置/镜像/阈值变更 | 当前 protocol epoch 结束；新版本不混入旧 block |
| 失败 attempt | 全进 ledger；正式集只按预注册规则保留，不做"最好 retry"选择 |
