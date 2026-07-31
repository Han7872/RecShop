# shop_web Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# shop_web(:3000) BFF / 三端 Blueprint(买家/商家/管理)前台。本族最复杂一极:
#   - SQLAlchemy ORM(Flask-SQLAlchemy)经 PyMySQL 连 MySQL shopify2(读权威 + 部分写)
#   - Flask-Login 会话鉴权 + 归属校验
#   - Jinja2 前端模板(各 Blueprint templates/)+ static/ 必须 COPY 进镜像
#   - HTTP 代理几乎所有下游域服务(经 app/service_discovery.py, NACOS=false 走 env fallback)
#   - AI 助手经 openai SDK(httpx)调 DeepSeek(外部 API)
# 依赖全在 recweb-base —— 根 requirements.txt 已含 Flask-SQLAlchemy / Flask-Login /
#   PyMySQL / cryptography / openai / opentelemetry-instrumentation-sqlalchemy,
#   故此处【无需】额外 RUN pip install, 只叠代码层。
#   (config.py 用 mysql+pymysql:// 驱动 → 依赖 PyMySQL, 根 requirements.txt 已含。)
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/shop_web/Dockerfile -t recweb-shop-web:latest .
#   (build context = 仓库根, 因要 COPY shared/; -f 指到本文件)
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# shop_web 入口是 run.py(import app.create_app), 用 Path(__file__).resolve().parents[2]
# 定位项目根读 .env; config.py / config_ai.py / app/ 包(含 templates/、static/、
# 各 Blueprint、models.py、service_discovery.py)均为本服务目录内模块。
# COPY 整个服务目录即把 Jinja2 模板 + 静态资源 + 全部 Python 代码带进镜像。
# 复刻仓库布局: /app/services/shop_web/{run.py,config.py,app/...} + /app/shared/。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv override=False 不覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/shop_web/ /app/services/shop_web/

WORKDIR /app/services/shop_web

# shop_web 监听 0.0.0.0:3000 (SHOPWEB_PORT 默认 3000, 见 run.py __main__)
EXPOSE 3000

# 入口是 run.py(非 app.py); debug 由 SHOPWEB_DEBUG 控制(默认 False → 无 reloader)
CMD ["python", "run.py"]
