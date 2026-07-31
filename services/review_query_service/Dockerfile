# review_query_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# review_query_service(:5018) 商品评论列表读(C-content 族, JOIN users, enrich→catalog)。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*),
# app.py 仅 import 这些 + dotenv, 无额外 pip 依赖, 这里只叠代码层。
# 下游 catalog 经 shared.nacos_client.get_service_url(关 Nacos 时回退 CATALOG_SERVICE_URL)。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/review_query_service/Dockerfile -t recweb-review-query:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# review_query_service app.py 用 parents[2] 定位项目根读 .env + sys.path 插项目根 import shared.nacos_client。
# 复刻仓库布局: /app/services/review_query_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/review_query_service/ /app/services/review_query_service/

WORKDIR /app/services/review_query_service

# review_query_service 监听 0.0.0.0:5018 (REVIEW_QUERY_SERVICE_PORT 默认 5018, 见 app.py __main__)
EXPOSE 5018

CMD ["python", "app.py"]
