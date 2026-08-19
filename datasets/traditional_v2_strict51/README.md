# traditional_v2_strict51 — 传统基础设施故障数据集 v2（strict51 协议化重采）

> **完整数据集（255 case，全量三模态遥测 + GT + doc 层）托管在 Google Drive：**
> 🔗 https://drive.google.com/drive/folders/1-YcWVTDeD1T-F_E_70-K4mvEcfRZhem2 （`Dataset/` 子目录）
> 文件 = `strict51_20260819.zip`（346.1 MB），SHA256 =
> `79624aea3a00315c4b65a6dfef471e9835fc91a752c8e5be4198962a426dc62f`。
> 下载后解压到本目录（`datasets/traditional_v2_strict51/`）即可使用。

## 这是什么

与 v1（`datasets/traditional_k8s/`）**同一拓扑、同一故障机制词汇表、同一 GT 口径**的一次
**采集协议升级重采**：51 场景 × 5 区组的随机化完全区组设计（RCBD）、预冻结
schedule/seed/身份链（SHA256）、逐 case 资格门、修正案留痕（242 主槽 + 13 补采，
`amended` 可过滤）。单战役 3 天同环境采完（v1 为跨约两周 5 个异质批次）。

## 规模（255 case，按去重根因服务数 G 分档）

| 档 | case 数 | G |
|---|---|---|
| single | 130 | 1 |
| dual | 100 | 2 |
| triple | 25 | 3 |
| **合计** | **255** | |

常量基线与 v1 逐层相同（0.192 / 0.600 / 0.600 / 合并 0.373），两版方法分数可直接对比。

## 关键结果（macro Hit@1，resource 通道）

| 方法 | v1 | **v2** |
|---|---|---|
| BARO | 0.608 | **0.698** |
| BARO / full | 0.286 | **0.447**（首超常量基线 0.373 的点估计；McNemar p=0.18 未达显著） |
| RCD（5 seed） | 0.216 | 0.258 |
| 朴素 delta_z | 0.816 | 0.769 |

方法学、逐档拆解、局限（triple 档双饱和、raw 侧捷径、注入伪影族 55 可过滤、补采加权）
见完整数据集内的 `DATASHEET.md`；51 场景逐条设计见 `FAULT_DESIGN.md`。

## 本目录内即开即用

- **`per_case_scores_255.csv`** —— 255 case × 16 方法/通道组合的全指标
  （BARO / RCD 5 seed / delta_z / delta_ratio × full/resource），无需下载遥测即可做方法分析。

## 重新采集（可选）

strict51 采集线 = `scripts/chaos/ctk/run-traditional-v2-lite.ps1`（入口）+
`scripts/chaos/ctk/traditional_v2_lite/`（协议实现）+ 协议工件
`docs/acceptance/contracts/traditional-v2-lite-strict51-20260816/`
（contract = 51 场景注册表、freeze report = 冻结身份链）。
