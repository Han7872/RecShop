# order_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# order_service(:5010) 订单只读(列表/详情+订单行); 可选 enrich 经 HTTP 调 catalog 补当前商品。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/order_service/Dockerfile -t recweb-order:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# order_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(下游 catalog 解析)。
# 复刻仓库布局: /app/services/order_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/order_service/ /app/services/order_service/

WORKDIR /app/services/order_service

# order_service 监听 0.0.0.0:5010 (ORDER_SERVICE_PORT 默认 5010, 见 app.py __main__)
EXPOSE 5010

CMD ["python", "app.py"]
