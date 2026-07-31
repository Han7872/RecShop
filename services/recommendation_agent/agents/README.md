# agents/ - 推荐系统 Agent 工具与提示词

本包包含多Agent推荐工作流所需的提示词定义和工具函数。

## 文件说明

- **`prompts.py`** - 各 Agent 的系统提示词（Supervisor、Sequence_Recommender、User_Behavior_Analyzer、Product_Analyzer、Recommendation_Synthesizer）
- **`tools.py`** - LangChain 工具函数（调用 SASRec API、商品标题查询、用户历史分析等）

## 工具函数

| 函数 | 说明 |
|------|------|
| `get_sequence_recommendations` | 调用 SASRec API 获取序列推荐 |
| `analyze_user_history` | 分析用户历史交互行为模式 |
| `get_product_details` | 获取商品详细信息 |
| `check_recommendation_service` | 检查 SASRec 服务健康状态 |
| `get_item_title` | 从本地缓存获取商品标题 |
