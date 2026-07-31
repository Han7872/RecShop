# legacy_agentchaos —— agent 故障线的**第一代**脚本(superseded, 保留可复现)

这三个脚本原本躺在 `scripts/chaos/ctk/`(传统故障目录)里, 属于**归类错误** ——
agentchaos 是 agent 故障线的第一代数据集, 不是传统微服务故障。2026-07-22 归位至此。

| 脚本 | 作用 |
|---|---|
| `agentchaos_runner.py` | 采集器。黑盒故障钩子(`AGENT_FAULT_<NAME>=delay\|error\|garbage`)驱动临时实例(端口 5101) |
| `make_agentchaos_features.py` | 由 `dataset_agentchaos.csv` 生成防泄漏特征视图 |
| `eval_agentchaos.py` | 评测(确定性, SEEDS=[0,1,2,3,4]) → `BASELINE_RESULTS.md` |

## 它和现行 agent 故障线的关系(勿混淆三代)

1. **agentchaos**(本目录, 240 窗) = **内容层埋点之前的黑盒基线**。只有 per-agent span 的
   时长/状态, 看不见 prompt/completion。实测黑盒 Recall 0.241 **低于** Dummy 0.357;
   换成 per-agent span 才 0.673。**它的价值就是这个对照** —— 证明没有内容层就定位不了。
2. **agentfault**(v1, 72 case) = 内容层埋点之后的第一版, `superseded_by_v2` 但作对照保留。
3. **agentfault_v2**(96 faulted + 12 normal) = **现行**。

数据集在 `(archived) agentchaos/`(REGISTRY 标 `superseded`;`registry.cases()` 不覆盖它, 结构不同)。
运行方式见 `(archived) agentchaos/runbook.md`(路径已同步更新到本目录)。

**勿把本目录当现行 agent 故障采集入口** —— 现行入口是
`scripts/chaos/agentfault/run_collect_agentfault.sh` / `run_eval_agentfault.sh`。
