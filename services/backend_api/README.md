# Backend API

ShopWeb 项目的后端 API 服务，负责处理用户数据、交互历史、推荐请求，并作为 ShopWeb 前端和 SASRec 推理服务之间的中间层。

---

## 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| Python | 3.9+ | 核心编程语言 |
| Flask | 3.0+ | Web 框架 |
| Flask-CORS | - | 跨域请求支持 |
| MySQL | 8.0+ | 关系型数据库 |
| mysql-connector-python | - | MySQL 数据库连接器 |
| Requests | - | HTTP 客户端（调用 SASRec API） |

---

## 目录结构

```
backend_api/
└── app.py          # Flask 应用入口，包含所有 API 路由和业务逻辑
```

---

## 核心功能

### 1. 用户管理
- **获取用户信息**：根据 `user_token` 查询用户详情
- **用户历史查询**：获取用户所有交互记录（点击、浏览等）

### 2. 交互记录
- **记录用户行为**：保存用户对商品的点击、浏览、购买等交互
- **支持多种交互类型**：`view`、`click`、`purchase`、`rating`、`add_to_cart`

### 3. 推荐服务
- **调用 SASRec API**：根据用户历史交互序列获取个性化推荐
- **推荐结果存储**：将推荐结果保存到数据库，包括分数、排名、推理时间等
- **商品详情补全**：从数据库查询推荐商品的详细信息（标题、类别、价格等）

---

## 文件说明

### `app.py`

**职责**：Flask 应用的唯一文件，包含所有业务逻辑

**主要组成部分**：

1. **配置管理**
   - 从环境变量加载数据库配置（`DB_HOST`、`DB_USER`、`DB_PASSWORD`、`DB_NAME`）
   - 配置 SASRec API 地址（`SASREC_API_URL`）

2. **数据库连接池**
   - 使用 `mysql.connector.pooling.MySQLConnectionPool` 管理连接
   - 默认连接池大小：3

3. **工具函数**
   - `get_db_connection()`：获取数据库连接
   - `handle_db_error`：数据库错误处理装饰器

4. **API 路由**
   - `GET /`：健康检查
   - `GET /api/users/<user_token>`：获取用户信息
   - `GET /api/users/<user_token>/history`：获取用户交互历史
   - `POST /api/interaction`：记录用户交互
   - `POST /api/recommend`：获取推荐结果

---

## API 接口文档

### 1. 健康检查

```http
GET /
```

**响应示例**：
```json
{
  "status": "healthy",
  "message": "SASRec Demo Backend API is running",
  "database": "connected",
  "sasrec_api": "http://localhost:8000"
}
```

---

### 2. 获取用户信息

```http
GET /api/users/<user_token>
```

**路径参数**：
- `user_token` (string)：用户唯一标识符

**响应示例**：
```json
{
  "user_token": "user_001",
  "username": "张三",
  "email": "zhangsan@example.com",
  "avatar_url": "https://example.com/avatar.jpg"
}
```

---

### 3. 获取用户交互历史

```http
GET /api/users/<user_token>/history
```

**查询参数**：
- `limit` (int, 可选)：返回记录数量限制，默认 100

**响应示例**：
```json
{
  "user_token": "user_001",
  "history": [
    {
      "item_id": "B001234567",
      "interaction_type": "click",
      "timestamp": 1678901234000,
      "session_id": "session_abc"
    },
    {
      "item_id": "B009876543",
      "interaction_type": "view",
      "timestamp": 1678901230000,
      "session_id": "session_abc"
    }
  ],
  "count": 2
}
```

---

### 4. 记录用户交互

```http
POST /api/interaction
```

**请求体**：
```json
{
  "user_token": "user_001",
  "item_id": "B001234567",
  "interaction_type": "click",
  "session_id": "session_abc",
  "source": "homepage",
  "timestamp": 1678901234000
}
```

**字段说明**：
- `user_token` (必需)：用户标识
- `item_id` (必需)：商品 ID
- `interaction_type` (必需)：交互类型（`view`、`click`、`purchase`、`rating`、`add_to_cart`）
- `session_id` (可选)：会话 ID
- `source` (可选)：来源（`homepage`、`search`、`recommendation` 等）
- `timestamp` (可选)：Unix 时间戳（毫秒），默认使用当前时间

**响应示例**：
```json
{
  "success": true,
  "message": "交互记录成功",
  "interaction_id": 12345
}
```

---

### 5. 获取推荐结果

```http
POST /api/recommend
```

**请求体**：
```json
{
  "user_token": "user_001",
  "top_k": 10,
  "exclude_history": true
}
```

**字段说明**：
- `user_token` (必需)：用户标识
- `top_k` (可选)：推荐数量，默认 10
- `exclude_history` (可选)：是否排除用户已交互商品，默认 `true`

**处理流程**：
1. 查询用户交互历史，提取商品 ID 序列（按时间升序）
2. 调用 SASRec API (`POST {SASREC_API_URL}/recommend`)
3. 从数据库查询推荐商品的详细信息
4. 保存推荐结果到 `recommendations` 表
5. 返回带商品详情的推荐列表

**响应示例**：
```json
{
  "success": true,
  "recommendations": [
    {
      "item_id": "B0001AVSLO",
      "score": 5.93,
      "rank": 1,
      "title": "LaCie 500 GB External Hard Drive",
      "category": "Electronics",
      "brand": "LaCie",
      "price": 199.99,
      "image_url": "https://example.com/product.jpg"
    }
  ],
  "input_sequence": ["B001234567", "B009876543"],
  "inference_time": 0.36
}
```

---

## 数据库依赖

Backend API 依赖以下 MySQL 数据表：

### `users` 表
- 存储用户信息（`user_token`、`username`、`email`、`password_hash`、`avatar_url`）

### `items` 表
- 存储商品信息（`item_id`、`title`、`category`、`brand`、`price`、`image_url`）

### `interactions` 表
- 存储用户交互记录（`user_token`、`item_id`、`interaction_type`、`timestamp`、`session_id`）

### `recommendations` 表
- 存储推荐历史（`user_token`、`item_id`、`score`、`rank`、`input_sequence`、`inference_time`）

数据库 Schema 详见：`scripts/database_schema.sql`

---

## 环境变量配置

在项目根目录的 `.env` 文件中配置以下变量：

```bash
# 数据库配置（必需）
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=shopify

# Backend API 配置
BACKEND_HOST=0.0.0.0
BACKEND_PORT=5000

# SASRec API 地址
SASREC_API_URL=http://127.0.0.1:8000
```

---

## 启动方式

### 前置条件
1. MySQL 数据库已安装并运行
2. 已执行 `scripts/database_schema.sql` 创建数据库和表
3. SASRec API 服务已启动（端口 8000）

### 启动命令

```bash
# 进入服务目录
cd services/backend_api

# 运行服务
python app.py
```

服务将在 `http://0.0.0.0:5000` 启动。

---

## 典型调用场景

### 场景 1：用户浏览商品页面

```python
# 1. ShopWeb 前端记录点击行为
requests.post('http://localhost:5000/api/interaction', json={
    'user_token': 'user_001',
    'item_id': 'B001234567',
    'interaction_type': 'click',
    'session_id': 'session_abc',
    'source': 'homepage'
})

# 2. 用户返回首页，请求推荐
response = requests.post('http://localhost:5000/api/recommend', json={
    'user_token': 'user_001',
    'top_k': 3
})

# 3. 显示推荐商品
recommendations = response.json()['recommendations']
```

### 场景 2：查看用户行为历史

```python
# 获取用户最近 50 条交互记录
response = requests.get('http://localhost:5000/api/users/user_001/history?limit=50')
history = response.json()['history']
```

---

## 错误处理

API 使用标准 HTTP 状态码：

| 状态码 | 说明 |
|-------|------|
| 200 | 请求成功 |
| 400 | 请求参数错误（缺少必需字段等） |
| 404 | 资源不存在（用户不存在等） |
| 500 | 服务器内部错误（数据库错误、SASRec API 调用失败等） |
| 503 | 服务不可用（数据库连接失败等） |

**错误响应示例**：
```json
{
  "error": "数据库错误: Connection refused",
  "success": false
}
```

---

## 与其他服务的集成

```
┌─────────────────┐
│   ShopWeb       │  用户界面
│  (Port 3000)    │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────┐
│  Backend API    │  本服务
│  (Port 5000)    │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────┐
│  SASRec API     │  推理服务
│  (Port 8000)    │
└─────────────────┘
```

---

## 性能优化建议

1. **连接池调优**：根据并发量调整 `pool_size`
2. **查询优化**：为 `user_token`、`item_id`、`timestamp` 字段建立索引
3. **缓存推荐结果**：使用 Redis 缓存热门用户的推荐
4. **异步调用**：对 SASRec API 的调用可改为异步（`aiohttp`）

---

## 依赖说明

安装依赖：
```bash
pip install flask flask-cors mysql-connector-python requests python-dotenv
```

或参考项目根目录的 `requirements.txt`。

---

*最后更新：2026 年 3 月*
