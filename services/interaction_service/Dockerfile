# interaction_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# interaction_service(:5020) 交互埋点写中枢(C-content 族, 多端 fan-in)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/interaction_service/Dockerfile -t recweb-interaction:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# interaction_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/interaction_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/interaction_service/ /app/services/interaction_service/

WORKDIR /app/services/interaction_service

# interaction_service 监听 0.0.0.0:5020 (INTERACTION_SERVICE_PORT 默认 5020, 见 app.py __main__)
EXPOSE 5020

CMD ["python", "app.py"]
