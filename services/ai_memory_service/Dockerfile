# ai_memory_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# ai_memory_service(:5008) AI 记忆读列表/删除(C-content 族, 只读删不调 LLM)。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*),
# app.py 仅 import 这些 + dotenv, 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/ai_memory_service/Dockerfile -t recweb-ai-memory:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# ai_memory_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/ai_memory_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/ai_memory_service/ /app/services/ai_memory_service/

WORKDIR /app/services/ai_memory_service

# ai_memory_service 监听 0.0.0.0:5008 (AI_MEMORY_SERVICE_PORT 默认 5008, 见 app.py __main__)
EXPOSE 5008

CMD ["python", "app.py"]
