# SASRec 最小推理包使用说明

本目录包含运行已训练的 SASRec 模型所需的最小文件，无需安装 RecBole（已打包源码）。

## 目录结构
```
sasrec_api/
  api_server.py                 # FastAPI 推理服务入口
  test_client.py                # 调用示例
  SASRec-Feb-24-2026_17-54-22.pth   # 训练好的模型权重
  standard_cache.pkl            # 数据集/配置缓存（token2id/id2token 等）
  electronics.item              # 商品元信息（标题显示，可选但建议）
  vendor/recbole/...            # 打包的 RecBole 源码
```

## 环境依赖
- Python 3.10+
- 需要安装的第三方包：`torch`, `fastapi`, `uvicorn`, `pandas`, `pydantic`, `requests`
- 如果有 GPU 会自动用 CUDA；没有则自动切换 CPU。如需固定 CPU，可在 `api_server.py` 中将 `device = 'cuda' if ... else 'cpu'` 改为 `device = 'cpu'`。

## 启动服务
```bash
cd services/sasrec_api
python -m uvicorn api_server:app --host 0.0.0.0 --port 8000
```
启动后关键接口：
- `GET /health`：健康检查，返回是否加载模型、数据集信息
- `POST /recommend`：核心推荐接口

## 输入输出格式
### 请求（POST /recommend）
```json
{
  "item_sequence": ["015600206X", "6300215695", "0446673145"],  // 用户历史交互商品ID序列（ASIN），长度 1-200，内部只取最近 50 个
  "top_k": 10,                   // 可选，默认 10，范围 1-100
  "exclude_history": true        // 可选，默认 true，是否排除历史商品
}
```

### 响应示例
```json
{
  "success": true,
  "recommendations": [
    {
      "item_id": "B0001AVSLO",
      "score": 5.93,
      "title": "LaCie 500 GB Big Disk Triple External Hard Drive ...",
      "rank": 1
    },
    {
      "item_id": "B00005ATZN",
      "score": 5.74,
      "title": "Canon PowerShot S300 2MP Digital ELPH Camera Kit ...",
      "rank": 2
    }
  ],
  "inference_time": 0.36,
  "message": "成功生成 10 个推荐"
}
```
字段说明：
- `item_id`：推荐的商品 ASIN
- `score`：模型打分（越大越相关）
- `title`：若 `electronics.item` 存在则返回标题，否则为空
- `rank`：推荐排序（1 开始）

### 健康检查（GET /health）
返回示例：
```json
{"status":"healthy","model_loaded":true,"dataset_info":{"user_num":1627158,"item_num":433028,"interaction_num":13750662}}
```

## 快速测试
1. 启动服务（见“启动服务”）。
2. 新开终端运行：
```bash
cd services/sasrec_api
python test_client.py
```
`test_client.py` 会先调 `/health`，再用示例序列请求 `/recommend`。

## 文件作用简述
- `api_server.py`：加载缓存/权重，启动 FastAPI，完成 token2id 映射和推荐计算。
- `standard_cache.pkl`：包含 RecBole 的 config 与 dataset（token 映射）。缺失会导致无法还原 id 映射。
- `SASRec-Feb-24-2026_17-54-22.pth`：模型权重。
- `electronics.item`：用于补充推荐结果标题，缺失时只返回 item_id。
- `vendor/recbole`：打包的 RecBole 源码，避免外部安装依赖。

## 注意
- 如迁移到纯 CPU 环境，请修改 `api_server.py` 里的设备选择为 `cpu`。
- `item_sequence` 中如果包含未出现在训练数据的商品，将被忽略；至少需要 1 个有效商品 ID。
- 内部实际只使用最近 50 个商品进行推理。

## Agent 层重排与指标计算（NDCG@10 / Recall@10）

如果你的前端/Agent 需要做到：
1) 调用 SASRec 拿到 Top20 推荐结果
2) 在 Agent 层从 Top20 里筛选/重排出 Top10
3) 用真实标签（ground-truth）计算 NDCG@10 和 Recall@10

那么最少需要准备如下输入与标签。

### 需要的输入

对每个评估样本（一个用户的一次预测），你需要：
- **history**：用户历史交互商品序列（ASIN 列表），作为 `/recommend` 的 `item_sequence`
- **top20**：SASRec 返回的 Top20（调用 `/recommend` 时 `top_k=20`）
- **top10**：Agent 从 Top20 里筛选/重排得到的 Top10（用于最终指标计算）
- **label**：真实的下一步交互商品（ground-truth item_id）

推荐的样本结构（示例）：
```json
{
  "user_id": "U123",
  "history": ["i1", "i2", "i3"],
  "label": "i4",
  "sasrec_top20": ["p1", "p2", "..."],
  "agent_top10": ["p2", "p8", "..."],
  "split": "test"
}
```

### 真实标签（label）如何构造

序列推荐的常见做法（RecBole 默认的时间序列切分，按时间排序）：

对每个用户的交互序列 `seq = [i1, i2, ..., in]`（按 `timestamp` 升序）：
- **valid label**：`seq[-2]`
- **test label**：`seq[-1]`
- **valid history**：`seq[:-2]`
- **test history**：`seq[:-1]`

因此：
- 评估 **valid**：输入 `valid history`，标签为 `valid label`
- 评估 **test**：输入 `test history`，标签为 `test label`

数据来源通常为 `electronics.inter`（包含 `user_id`, `item_id`, `timestamp` 三列）。

### 指标如何计算（单正样本 label）

在上面的切分方式下，每次预测通常只有 1 个正例 label。

设 Agent 最终用于评估的推荐列表为 `pred10`（长度 10 的 item_id 列表）：

#### Recall@10
- 若 `label` 出现在 `pred10` 中：`Recall@10 = 1`
- 否则：`Recall@10 = 0`

在“单正样本”场景里，`Recall@10` 等价于 `Hit@10`（也常被叫做 HR@10）。

#### NDCG@10
- 若 `label` 在 `pred10` 中，排名为 `rank`（从 1 开始）：
  - `NDCG@10 = 1 / log2(rank + 1)`
- 若不在 Top10：
  - `NDCG@10 = 0`

例如：
- 命中第 1 名：`NDCG@10 = 1`
- 命中第 2 名：`NDCG@10 ≈ 0.6309`
- 命中第 10 名：`NDCG@10 ≈ 0.2891`

### 与 SASRec Top20 -> Agent Top10 的关系

你可以在评估时分别统计：
- **SASRec 原始表现**：直接用 `sasrec_top20` 的前 10 个作为 `pred10`
- **Agent 重排后表现**：用 `agent_top10` 作为 `pred10`

两者都用同一条样本的 `history` 和 `label` 计算 NDCG@10 / Recall@10，即可比较 Agent 策略是否带来提升。
