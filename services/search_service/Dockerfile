# search_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# search_service(:5017) 标题/品牌/分类 LIKE 检索(C-content 族, enrich→catalog)。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*),
# app.py 仅 import 这些 + dotenv, 无额外 pip 依赖, 这里只叠代码层。
# 下游 catalog 经 shared.nacos_client.get_service_url(关 Nacos 时回退 CATALOG_SERVICE_URL),
# 不需要本服务目录内的额外模块文件。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/search_service/Dockerfile -t recweb-search:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# search_service app.py 用 parents[2] 定位项目根读 .env + sys.path 插项目根 import shared.nacos_client。
# 复刻仓库布局: /app/services/search_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/search_service/ /app/services/search_service/

WORKDIR /app/services/search_service

# search_service 监听 0.0.0.0:5017 (SEARCH_SERVICE_PORT 默认 5017, 见 app.py __main__)
EXPOSE 5017

CMD ["python", "app.py"]
