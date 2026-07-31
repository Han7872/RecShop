# scripts/chaos/ctk —— **传统微服务故障**（K8S + Chaos Mesh）采集与评测

> 这一支只管**基础设施层故障**（CPU 饱和 / pod 失败 / 网络延迟丢包 / 依赖超时 / DB 锁 …），
> 实体空间 = **服务**。
> **agent 语义故障**（幻觉 / 上下文丢失 / 选错商品 / 格式违背，实体空间 = agent）在
> `scripts/chaos/agentfault/`，两支不要混。
> 数据集目录契约的唯一真相源 = `datasets/REGISTRY.json`（经 `dataset_registry.py` 读）。

## 1. 采集

| 文件 | 作用 |
|---|---|
| `chaos_k8s_runner.py` | **核心 runner**（~14.6k 行）。三阶段采集 pre_fault → during_fault → post_recovery，含注入原语 / 21 道 gate / GT 出标。**勿整体加载**（顶破上下文），改前先读 `(项目文档)` 再 grep 切片 |
| `collect-*.sh` | **可复现命令日志**，一个数据集树一份。每行一条 `nohup` 采集命令 + 结果注释，末尾 `PROVENANCE` 记口径。见下表 |
| `g1_bulk_collect.sh` | G1 放量**编排器**（重试 / agent_spans 回收 / 台账）。`collect-*.sh` 是"照着能复现"，本脚本是"无人值守能跑完" |
| `db_contention_injector.py` | DB 争用注入载体（只读，被 runner import） |

### collect-*.sh 与数据集树的对应

| 脚本 | 树 | 规模 |
|---|---|---|
| `collect-single-dense.sh` | `k8s_pilot/single_dense` | 40（8 类型 × 5） |
| `collect-dual-dense.sh` | `k8s_pilot/dual_dense` | 80（16 组合 × 5） |
| `collect-triple-dense.sh` | `k8s_pilot/triple_dense` | 20（4 组合 × 5） |
| `collect-single-spread.sh` | `k8s_pilot/single_spread` | 55（根因摊开到 6 个从未当过根因的服务） |
| `collect-net-spread.sh` | 同上（net 类） | — |
| `collect-single-recagent.sh` | `k8s_pilot/single_recagent` | 15（G1：补 rec-agent 这个漏掉的注入目标） |
| `collect-single.sh` / `collect-dual.sh` / `collect-triple.sh` | 前身（v18 树，已归档） | — |

## 2. 环境守护（采集期间必须在跑）

| 文件 | 作用 |
|---|---|
| `pfwd_start.sh` | **入口**。清掉所有 port-forward 再干净重起（★排除 5013，勿直接用 supervisor） |
| `pfwd_watchdog.sh` | 基于健康检查的守护（每 45s 探一次，非 churn 式） |
| `pfwd_{catalog,inventory,pricing,user}_restarter.sh` | 各载体服务的专属 restarter，**按 combo 取用**，切 combo 前 kill 上一个 |

> 起法与 kill 法（PID-file 模式，本机 Git Bash **无 pkill**）见
> `(project docs)/archive/TASK-K8S-M8-overnight-recollect.md` §1.5-A。

## 3. 校验（每个 case 采完就跑）

| 文件 | 作用 |
|---|---|
| `verify_dual.py <case_dir>` | 三段完整性 + **根实例 ∈ during_fault 遥测**。单根/多根通用 |
| `instance_check.py <case_dir> <root_svc>` | GT 钉的 pod 与故障窗实际 pod 一致 |
| `per_service_canon.py` | 服务名归一 + 故障类型 canon。★`pod_failure` 的 `fault_type` 出 `service_unavailable`，**勿按目录名当类型** |
| `fix_net_gt.py` | net 类 GT 的一次性修正器（历史） |

## 4. 打包交付

| 文件 | 作用 |
|---|---|
| `build_full_delivery.py` | **★统一交付树总组装器**（一条命令出 `_delivery/RecShop_<tag>/`）：两个一级目录 traditional/（single/dual/triple）+ agent/，README+MANIFEST 脚本生成。默认 `--mode copy` 从冻结包逐字节拷（非重推导）；`--mode rebuild` 走 native 全重建（~1-2h）。复现说明在文件头注释 |
| `package_for_delivery.py` | **单批入口**：native 树 → 交付格式。`--flat-traces` 拍平 / `--with-calltree` 附原生调用树 / `--with-eval` 出 data.csv / `--with-gt-distinct` 补 n_distinct_root_services / `--eval-only` 剔 dev / `--bare` 平铺 |
| `build_recagent_agent_views.py` | single_recagent 专用：把 native 的 `raw/agent_spans/` 做成 `agent_traces/` + `whowhen/` 两个一级目录（packager 不认识这个目录，它比 packager 新） |
| `mr2_load_adapter.py` | 载入前置 adapter（被 packager 调） |
| `make_k8s_feature_view.py` | 产 `features_k8s.csv`（供 `--eval-only`） |
| `dataset_registry.py` | 读 `datasets/REGISTRY.json` 的唯一入口，**所有脚本经它拿路径**，勿硬编码 |

## 5. 评测

| 文件 | 作用 |
|---|---|
| `m9_score.py` | **单 case 的 BARO + RCD 打分器**。MRCBench 四族指标（Hit@K / Recall@R / FullHit@K / NDCG@K）也在这 |
| `n5_run.py` + `n5_lib.py` | 140 case 全量重打分（× gap_aware × {full,resource} × seed 0-4） |
| `m9_report.py` | **`(project docs)/` 全部产物的唯一存活生成器**（单入口、进仓、可复现） |
| `m9_adapter.py` / `m9_eadro_adapter.py` | per-service 宽表 adapter / Eadro 输入 adapter |
| `eval_k8s_ranking.py` | 无监督 per-service 异常排名基线 |
| `eval_k8s_supervised.py` | 有监督 group-aware 分类（**天花板参考，不是同侪基线**） |
| `eval_k8s_trace.py` | 基于 trace 的传播感知 RCA 基线 |

> ★评测铁律：**禁止按 case 切分**（会泄漏，产生过公开 ERRATUM）；每个分数必须**并排常量基线**
> （140 树的常量先验 0.643 是 **GT-aware oracle 不是基线**；随机地板 0.115 才是诚实下界）。
> 详见 `(delivery) 20260713_gtfix/EVAL_NOTES.md`。

## 6. m9 驱动器（通宵采集用）

`m9_drive.sh`（顺序批驱动）· `m9_night.sh`（两段式：先逐类型验 → 人看过 → 再全量补 reps）·
`m9_guard.sh`（"守护者的守护者"，自愈常驻）· `m9r_drive.sh`（根因摊开采集）

## 7. 历史实验目录（不在现役流程里）

`_i0_experiment` / `_i0b_experiment` / `_i1_pilot` / `_i1b_synth_http` / `_audit_20260715`
—— anti-trivial 与 benchmark 可行性的一次性实验，结论见
`(project docs)/archive/TASK-K8S-I1-anti-trivial-pilot.md` 与 memory `recweb2-m13-benchmark-not-viable`。
**留档不删**（结论随时可能要翻），但改代码时不用管它们。

## 8. 踩过的坑（照抄前先看）

- **Git Bash 的 MSYS 会改写 kubectl 参数里的 Linux 路径**（`/agentfault-data/...` →
  `C:/Program Files/Git/agentfault-data/...`）→ 必须 `MSYS2_ARG_CONV_EXCL='*'` 或改用 PowerShell。
  2026-07-22 因此静默丢过 10 个 case 的轨迹（拷贝失败但配对的 truncate 成功）。
- **副产物回收要做非空硬校验**：runner 的 `VERIFY=PASS` 只管 metrics/GT，**不管**你另外收的东西。
- **`--wide-metrics` / `--deep` 是替换语义**：会把采集拓扑整体换成固定 12 服务集，新目标服务不在集里就会被静默丢掉。
