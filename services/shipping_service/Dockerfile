# shipping_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# shipping_service(:5016) 发货: 建/查发货单, 建单后经 HTTP 通知 notification_service(深链尾)。
# ⚠ 启动幂等建表: __main__ 里建 shipments 表 CREATE TABLE IF NOT EXISTS(自有新表)。
#   容器首启会对 shopify2 建 shipments 表。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/shipping_service/Dockerfile -t recweb-shipping:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# shipping_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(下游 notification 解析)。
# 复刻仓库布局: /app/services/shipping_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/shipping_service/ /app/services/shipping_service/

WORKDIR /app/services/shipping_service

# shipping_service 监听 0.0.0.0:5016 (SHIPPING_SERVICE_PORT 默认 5016, 见 app.py __main__)
EXPOSE 5016

CMD ["python", "app.py"]
