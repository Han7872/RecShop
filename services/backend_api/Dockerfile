# backend_api Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# backend_api(:5000) 推荐编排, M0 pilot 三服务之一(walking skeleton):
#   经 K8S Service DNS 调 sasrec 纯推理, 验跨服务 trace 关联(must-fix#3)。
# 依赖全在 recweb-base, 只叠代码 + service_discovery.py。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/backend_api/Dockerfile -t recweb-backend:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# backend_api app.py 同样用 parents[2] 定位项目根 + import shared.nacos_client,
# 且 from service_discovery import get_sasrec_api_url(本服务目录内模块)。
# 复刻仓库布局: /app/services/backend_api/{app.py,service_discovery.py} + /app/shared/。
COPY shared/ /app/shared/
COPY services/backend_api/ /app/services/backend_api/

WORKDIR /app/services/backend_api

# backend_api 监听 0.0.0.0:5000 (BACKEND_PORT 默认 5000, 见 app.py __main__)
EXPOSE 5000

CMD ["python", "app.py"]
