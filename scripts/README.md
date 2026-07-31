# scripts — 脚本总目录（导航）

## 在役

| 位置 | 是什么 |
|---|---|
| [`chaos/ctk/`](chaos/ctk/) | **K8S 采集 harness（当前主线,23 件）**：主 runner `chaos_k8s_runner.py`（~14.6k 行,勿整读,复用速查见 `(项目文档)`）· 一键采集 provenance 日志 `collect-{triple,dual,single}.sh` · 验证 `verify_dual.py`+`instance_check.py` · 打包 `package_for_delivery.py`+`mr2_load_adapter.py` · db_lock 注入载体 `db_contention_injector.py` · 守护 `pfwd_*.sh`×6 · 特征/评测 `make_k8s_feature_view.py`+`eval_k8s_*.py`+`per_service_canon.py` · Agent 线参考 `agentchaos_runner.py`+`eval_agentchaos.py`+`make_agentchaos_features.py` |
| `build_database.sql` | **一键建库入口**（仓库根目录执行;内部 `SOURCE scripts/database_schema.sql`） |
| `database_schema.sql` | **唯一权威 DDL 源**（43KB 全表+视图+过程;旧 migrate_* 已并入） |
| `import_data.py` · `seed_demo_data.sql` · `seed_example_data.py` | 从零重建：RecBole→MySQL 导入 + 交易链/内容演示种子（两个 seed 表集互斥,都要） |
| `eval_sasrec_ndcg.py` | SASRec 离线评估（NDCG@10/Hit@10 sampled-99） |
| [`dev/snapshot_topology.py`](dev/snapshot_topology.py) | Jaeger 拓扑快照器——**Eadro 静态依赖图资产**（产物=`(项目文档)`(deps_tree.json+svcs_tree.json);`(reports)/.deps_fig.json` 为同型历史快照,本脚本不刷新） |
| [`release/`](release/) | OSS 验收契约校验器与候选外探针运输器；完整 gate producer 尚未实现，不能据此声称技术就绪或发布许可 |

## 归档

[`_archive/`](_archive/README.md)：`toxiproxy/`（35 件旧栈 runner/eval/probe/mock）· `k8s_oneoff/`（8 件一次性）· `sql_migrations/`（4 件已并入 schema 的旧 SQL）· `dev/`（旧 dev QA）· `oss_release_20260730/backup/`（2 件一次性预清理防护工具）——收录清单、sys.path 兼容说明、断链记录见该 README。

> 执行铁律（conda env / NO_PROXY / CHECKSUM / 只读表等）见 README.md;编排经验见 `(项目文档)`。
