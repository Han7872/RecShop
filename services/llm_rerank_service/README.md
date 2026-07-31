# LLM Rerank Service

基于 LLM 的推荐重排序微服务。接收 SASRec 模型的候选商品列表，利用大语言模型（DeepSeek）对候选结果进行智能重排序，选出最可能被用户点击/购买的商品。

> 灵感来源：[LLM4Rerank (WWW'25)](https://doi.org/10.1145/3589335.3651921) — Gao et al.

---

## 目录

- [架构概览](#架构概览)
- [文件结构](#文件结构)
- [核心流程](#核心流程)
- [API 接口](#api-接口)
- [Prompt 策略](#prompt-策略)
- [离线评测](#离线评测)
- [环境变量](#环境变量)
- [启动方式](#启动方式)
- [与 ShopWeb 的集成](#与-shopweb-的集成)

---

## 架构概览

```
用户点击 ShopWeb "✦ AI Picks"
         │
         ▼
  ShopWeb /ai-picks (routes.py)
         │  POST /api/recommend
         ▼
  Backend API (port 5000)
         │  POST /recommend
         ▼
  SASRec API (port 8000)          ← 返回 top-10 候选 + scores
         │
         ▼
  ShopWeb 拿到候选后
         │  POST /rerank
         ▼
┌─────────────────────────────────────┐
│  LLM Rerank Service (port 5002)    │  ← 本服务
│                                     │
│  app.py  →  reranker.py            │
│    ↓           ↓                    │
│  Flask     build_prompt()           │
│  /rerank   request_llm() → DeepSeek│
│            validate_rerank_response │
│    ↓                                │
│  返回 {selected_item_id, reason}    │
└─────────────────────────────────────┘
         │
         ▼
  ShopWeb 渲染 AI 精选页面
  展示: AI Top Pick + 推荐理由 + 其他候选
```

---

## 文件结构

```
llm_rerank_service/
├── app.py                    # Flask 服务入口 (port 5002)
├── reranker.py               # 核心 rerank 逻辑
│                               - build_prompt(): 组装 prompt
│                               - request_llm(): 调用 DeepSeek API
│                               - rerank(): 完整流程 (调用→校验→重试→fallback)
├── prompts/
│   └── rerank_prompt.txt     # Prompt 模板 (v2, 已优化)
├── utils/
│   ├── __init__.py
│   └── validator.py          # LLM 输出校验 (JSON解析 + 字段检查 + item_id匹配)
├── build_eval_data.py        # 离线评测样本生成脚本
├── evaluate_rerank.py        # 离线评测脚本 (支持 --direct 模式)
├── sample_eval_data.json     # 50 条评测样本 (基于 electronics 数据集)
├── rerank_eval_results.csv   # V1 评测结果 (旧 prompt, Hit@1=40%)
├── rerank_eval_results_v2.csv# V2 评测结果 (优化 prompt, Hit@1=68%)
└── README.md                 # 本文档
```

### 各文件职责

| 文件 | 类型 | 说明 |
|------|------|------|
| `app.py` | **运行时** | Flask 服务，提供 `/rerank` 和 `/health` API 端点 |
| `reranker.py` | **运行时** | 核心模块：prompt 组装 → LLM 调用 → 输出校验 → fallback |
| `prompts/rerank_prompt.txt` | **运行时** | Prompt 模板，启动时加载一次 |
| `utils/validator.py` | **运行时** | 校验 LLM 输出：JSON 解析、必需字段、item_id 合法性、title 匹配 |
| `build_eval_data.py` | **工具** | 从 electronics.inter/item 文件生成 leave-one-out 评测样本 |
| `evaluate_rerank.py` | **工具** | 离线评测脚本，对比 SASRec baseline 和 LLM rerank |
| `sample_eval_data.json` | **数据** | 50 条评测样本 |
| `rerank_eval_results*.csv` | **数据** | 评测结果记录 |

---

## 核心流程

`reranker.py` 中的 `rerank()` 函数：

```
1. build_prompt(user_history, candidates)
   → 用户历史 + 带排名的候选列表 填入 prompt 模板

2. for attempt in [1, 2]:         # 最多 2 次尝试
       raw = request_llm(prompt)  # 调用 DeepSeek API
       result = validate(raw)     # 校验 JSON、字段、item_id
       if valid → return result (source="llm")

3. fallback → return SASRec top-1 (source="fallback")
```

**关键设计**：
- LLM 失败时自动 fallback 到 SASRec top-1，保证服务永远有返回
- 校验模块支持多种 LLM 输出格式（裸 JSON、```json 代码块、嵌入文本中的 JSON）
- title 必须与 item_id 精确匹配，防止 LLM 幻觉

---

## API 接口

### POST /rerank

**请求体**：
```json
{
  "user_history": [
    {"item_id": "B001", "title": "Sony WH-1000XM5 Headphones"},
    {"item_id": "B002", "title": "Bose QuietComfort 45"}
  ],
  "candidates": [
    {"item_id": "B003", "title": "Apple AirPods Pro", "score": 6.12},
    {"item_id": "B004", "title": "Samsung Galaxy Buds2", "score": 5.87}
  ]
}
```

**成功响应** (200)：
```json
{
  "success": true,
  "result": {
    "selected_item_id": "B003",
    "selected_title": "Apple AirPods Pro",
    "reason": "User shows interest in premium audio...",
    "source": "llm"
  }
}
```

**`source` 取值**：
- `"llm"` — LLM 成功重排
- `"fallback"` — LLM 失败，返回 SASRec top-1

### GET /health

返回服务健康状态。

---

## Prompt 策略

当前使用 **V2 优化版 prompt**（`prompts/rerank_prompt.txt`），核心原则：

1. **Model score 为主信号** — #1 候选约 60-70% 准确率，默认保持
2. **严格 override 条件** — 仅在用户历史有明确模式 + 分差小 + 更优匹配时才更改
3. **不透明标题处理** — 遇到 `Product_Bxxxxxxxx` 直接信任 model score
4. **禁止选 rank 5+** — 除非有压倒性证据
5. **候选带排名编号** — `#1`, `#2`, ... 强化 LLM 对位置的感知
6. **Step-by-step 引导** — (1) #1 合理吗? (2) 有强理由换吗? (3) 不确定就选 #1

---

## 离线评测

### 生成评测样本

```bash
# 需要 SASRec API 运行在 localhost:8000
python build_eval_data.py --n 50 --source file
```

从 `shared/data/electronics.inter` + `electronics.item` 中采样用户，按 leave-one-out 切分，调用 SASRec `/score/sampled` 获取候选列表。

### 运行评测

```bash
# 推荐：直接调用 reranker（无需启动 Flask 服务）
python evaluate_rerank.py --direct

# 或通过 HTTP 调用已运行的 Flask 服务
python evaluate_rerank.py --url http://localhost:5002/rerank
```

### 评测结果对比

| 版本 | Prompt 策略 | SASRec Hit@1 | LLM Hit@1 | LLM==SASRec#1 | rank4+ 选择 |
|------|-------------|--------------|-----------|---------------|-------------|
| V1 | "score is useful but not the only factor" | 64% | **40%** | 50% | 15 次 |
| V2 | Score 为主信号 + 严格 override 条件 | 64% | **68%** | 96% | **0 次** |

**V2 关键改进**：消除了 LLM 对 SASRec 正确答案的无效推翻（V1 有 14 次），同时保留了 LLM 纠正 SASRec 错误的能力（2 次有效 override）。

---

## 环境变量

在项目根目录 `.env` 中配置：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEEPSEEK_API_KEY` | (必填) | DeepSeek API 密钥 |
| `DEEPSEEK_API_BASE` | `https://api.deepseek.com/v1` | API 基础 URL |
| `DEEPSEEK_MODEL` | `deepseek-chat` | 使用的模型名称 |
| `RERANK_PORT` | `5002` | 服务端口 |
| `RERANK_HOST` | `0.0.0.0` | 监听地址 |
| `RERANK_DEBUG` | `false` | Flask debug 模式(默认关 reloader / 单进程;设 `true` 可开调试) |

---

## 启动方式

```bash
cd services/llm_rerank_service
python app.py
# → LLM Rerank Service 启动: http://0.0.0.0:5002
```

测试：
```bash
curl -X POST http://localhost:5002/rerank \
  -H "Content-Type: application/json" \
  -d '{"user_history": [{"item_id": "B001", "title": "Headphones"}], "candidates": [{"item_id": "B003", "title": "AirPods Pro", "score": 6.12}]}'
```

---

## 与 ShopWeb 的集成

ShopWeb 的 `/ai-picks` 页面（`services/shop_web/app/routes.py`）通过 HTTP 调用本服务：

```
ShopWeb (port 5001)
  → POST {BACKEND_API_URL}/api/recommend   获取 SASRec top-10
  → 构建 user_history (从 Interaction 表查询)
  → POST {RERANK_SERVICE_URL}/rerank       调用 LLM 重排序
  → 渲染 ai_picks.html                    展示 AI 精选 + 推荐理由
```

**Fallback 机制**：如果 LLM Rerank 服务不可用或调用失败，ShopWeb 会自动降级为 SASRec top-1。

---

## 学术引用

本项目灵感来源于 LLM4Rerank：

```bibtex
@inproceedings{gao2025llm4rerank,
  title={LLM4Rerank: LLM-based Auto-Reranking Framework for Recommendations},
  author={Gao, Jingtong and Chen, Bo and Zhao, Xiangyu and Liu, Weiwen and Li, Xiangyang and Wang, Yichao and Wang, Wanyu and Guo, Huifeng and Tang, Ruiming},
  booktitle={Proceedings of the ACM on Web Conference 2025},
  pages={228--239},
  year={2025}
}
```
