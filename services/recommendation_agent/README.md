# 商品推荐多Agent系统

基于 LangGraph 的多Agent商品推荐系统，集成 SASRec 序列推荐模型，通过多个专业Agent协作完成个性化商品推荐。

## 目录结构

```
recommendation_agent/
├── agents/
│   ├── __init__.py
│   ├── prompts.py           # Agent提示词定义
│   └── tools.py             # 工具函数（调用SASRec模型、商品信息查询）
├── static/
│   ├── index.html           # 前端页面
│   ├── style.css            # 样式文件
│   └── app.js               # 前端交互逻辑
├── __init__.py
├── app.py                   # Flask应用入口
├── workflow.py              # 主工作流文件
├── test_recommendation.py   # 测试脚本
└── README.md
```

## 系统架构图

```
                    ┌─────────────────┐
                    │   用户输入      │
                    │ (商品ID序列)    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Supervisor    │
                    │   (监督协调)    │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  Sequence     │   │ User_Behavior │   │   Product     │
│  Recommender  │   │   Analyzer    │   │   Analyzer    │
│ (序列推荐专家)│   │ (用户行为分析)│   │ (商品分析专家)│
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                             ▼
                ┌─────────────────────┐
                │ Recommendation      │
                │ Synthesizer         │
                │ (综合推荐分析师)    │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   最终推荐结果      │
                │ (商品+原因+置信度)  │
                └─────────────────────┘
```

## Agent 详细说明

### 1. Supervisor（监督者）

**角色**：工作流调度与协调

**职责**：
- 接收用户请求，决定下一步调用哪个Agent
- 按照预定顺序协调各Agent的执行
- 确保所有必要的分析完成后才进行综合推荐

**调用顺序**：
1. Sequence_Recommender（必须首先调用）
2. User_Behavior_Analyzer
3. Product_Analyzer
4. Synthesize（最终综合）

---

### 2. Sequence_Recommender（序列推荐专家）

**角色**：基于深度学习的序列推荐

**职责**：
- 调用 SASRec 模型获取商品推荐
- 分析推荐结果，提供商品ID、得分、标题
- 初步解释为什么推荐这些商品

**使用工具**：`get_sequence_recommendations`

**输出示例**：
```
排名1: B0001AVSLO (得分: 5.9328) - LaCie 500 GB External Hard Drive
排名2: B00005ATZN (得分: 5.7393) - Canon PowerShot S300 Digital Camera
...
```

---

### 3. User_Behavior_Analyzer（用户行为分析师）

**角色**：用户行为模式分析

**职责**：
- 分析用户历史交互序列
- 识别用户偏好类别、购买频率、价格范围偏好
- 评估品牌忠诚度倾向
- 提供洞察帮助解释推荐原因

**使用工具**：`analyze_user_history`

**分析维度**：
- 用户偏好类别
- 购买频率模式
- 价格范围偏好
- 品牌忠诚度倾向

---

### 4. Product_Analyzer（商品分析专家）

**角色**：商品特征与市场分析

**职责**：
- 分析推荐商品的类别和特征
- 评估价格定位
- 识别潜在使用场景
- 解释商品对用户的吸引力

**使用工具**：`get_product_details`

**分析维度**：
- 产品类别与特征
- 价格定位
- 潜在使用案例
- 用户吸引力分析

---

### 5. Recommendation_Synthesizer（综合推荐分析师）

**角色**：最终推荐决策与解释

**职责**：
- 综合所有Agent的分析结果
- 选择最适合用户的TOP推荐商品
- 提供详细的推荐原因
- 给出推荐置信度评分

**输出格式**：
```json
{
  "recommended_product": "B0001AVSLO",
  "product_title": "LaCie 500 GB External Hard Drive",
  "recommendation_reason": "详细推荐原因...",
  "confidence": 0.85
}
```

## Agent 协作流程

```
1. 用户提交历史交互商品ID序列
        ↓
2. Supervisor 接收请求，首先调用 Sequence_Recommender
        ↓
3. Sequence_Recommender 调用 SASRec API 获取 Top-K 推荐结果
        ↓
4. Supervisor 调用 User_Behavior_Analyzer 分析用户行为
        ↓
5. User_Behavior_Analyzer 分析用户偏好、购买模式等
        ↓
6. Supervisor 调用 Product_Analyzer 分析推荐商品
        ↓
7. Product_Analyzer 分析商品特征、价格定位、使用场景
        ↓
8. Supervisor 调用 Synthesize 进行最终综合
        ↓
9. Recommendation_Synthesizer 综合所有分析，输出最终推荐
        ↓
10. 返回推荐结果和各Agent对话记录
```

## 技术实现

### LangGraph 状态管理

```python
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]  # 累加所有消息
    next: str  # 下一个要执行的节点
    item_sequence: List[str]  # 用户历史商品序列
    top_k: int  # 推荐数量
```

### 消息传递机制

- 每个Agent执行后，输出作为 `HumanMessage` 添加到 `messages` 状态
- 后续Agent可以看到之前所有Agent的输出
- Recommendation_Synthesizer 能够综合所有分析结果做出最终决策

## API接口

- `POST /recommend` - 推荐接口，接收 `item_sequence` 和 `top_k`
- `GET /recommend/health` - 健康检查
- `GET /recommend/chat-messages` - 获取对话消息

## 启动方式

```bash
# 1. 先启动SASRec推荐服务（端口8000）
cd services/sasrec_api
python api_server.py

# 2. 启动推荐系统Flask服务（端口5001）
cd services/recommendation_agent
python app.py

# 3. 测试推荐系统
python test_recommendation.py
```

## 请求示例

```json
POST /recommend
{
  "item_sequence": ["015600206X", "6300215695", "0446673145"],
  "top_k": 5
}
```

## 响应示例

```json
{
  "success": true,
  "recommendation": {
    "recommended_product": "B001234567",
    "product_title": "推荐商品名称",
    "recommendation_reason": "基于用户历史行为...",
    "confidence": 0.85
  },
  "conversation": {
    "SequenceRecommender": "...",
    "UserBehaviorAnalyzer": "...",
    "ProductAnalyzer": "...",
    "RecommendationSynthesizer": "..."
  }
}
```

## 依赖

- Flask
- flask-cors
- langchain
- langchain-openai
- langgraph
- requests
