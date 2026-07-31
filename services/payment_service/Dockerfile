# payment_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# payment_service(:5012) 支付: 查支付记录 / mock 收款。独立无下游服务。
# ⚠ 启动幂等建表: __main__ 里 _ensure_schema() CREATE TABLE IF NOT EXISTS payments
#   (自有新表, 不动 items/inventory)。容器首启会对 shopify2 建 payments 表。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/payment_service/Dockerfile -t recweb-payment:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# payment_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client。
# 复刻仓库布局: /app/services/payment_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/payment_service/ /app/services/payment_service/

WORKDIR /app/services/payment_service

# payment_service 监听 0.0.0.0:5012 (PAYMENT_SERVICE_PORT 默认 5012, 见 app.py __main__)
EXPOSE 5012

CMD ["python", "app.py"]
