# promotion_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# promotion_service(:5015) 促销/优惠券: 列有效优惠 / 校验券码。独立无下游服务。
# ⚠ 启动幂等建表: __main__ 里建 promotions 表 CREATE TABLE IF NOT EXISTS(自有新表)。
#   容器首启会对 shopify2 建 promotions 表。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/promotion_service/Dockerfile -t recweb-promotion:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# promotion_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client。
# 复刻仓库布局: /app/services/promotion_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/promotion_service/ /app/services/promotion_service/

WORKDIR /app/services/promotion_service

# promotion_service 监听 0.0.0.0:5015 (PROMOTION_SERVICE_PORT 默认 5015, 见 app.py __main__)
EXPOSE 5015

CMD ["python", "app.py"]
