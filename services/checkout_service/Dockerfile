# checkout_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# checkout_service(:5011) 结账编排(扇出): preview 三路 fan-out → cart/pricing/inventory,
# 并只读 cart_items 表。★ 症状漂移源头。自身不建表、不下单、不写。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/checkout_service/Dockerfile -t recweb-checkout:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# checkout_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(三下游解析)。
# 复刻仓库布局: /app/services/checkout_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/checkout_service/ /app/services/checkout_service/

WORKDIR /app/services/checkout_service

# checkout_service 监听 0.0.0.0:5011 (CHECKOUT_SERVICE_PORT 默认 5011, 见 app.py __main__)
EXPOSE 5011

CMD ["python", "app.py"]
