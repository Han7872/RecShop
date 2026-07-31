# pricing_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# pricing_service(:5014) 定价: 调 catalog 取基价 × markup × tax = 含税终价。
# ⚠ 启动幂等建表: __main__ 里建 price_rules 表 CREATE TABLE IF NOT EXISTS(自有新表)。
#   容器首启会对 shopify2 建 price_rules 表。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/pricing_service/Dockerfile -t recweb-pricing:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# pricing_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(下游 catalog 解析)。
# 复刻仓库布局: /app/services/pricing_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/pricing_service/ /app/services/pricing_service/

WORKDIR /app/services/pricing_service

# pricing_service 监听 0.0.0.0:5014 (PRICING_SERVICE_PORT 默认 5014, 见 app.py __main__)
EXPOSE 5014

CMD ["python", "app.py"]
