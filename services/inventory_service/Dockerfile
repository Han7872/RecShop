# inventory_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# inventory_service(:5013) 库存: 查可用(stock-reserved)/预留(FOR UPDATE 防超卖); 调 catalog 校验。
# ⚠ 启动幂等建表: __main__ 里建 inventory 表 CREATE TABLE IF NOT EXISTS(自有新表,
#   非业务只读 items/inventory 表名巧合—— 本服务自有 `inventory` 表)。容器首启会对 shopify2 建表。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/inventory_service/Dockerfile -t recweb-inventory:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# inventory_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(下游 catalog 解析)。
# 复刻仓库布局: /app/services/inventory_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/inventory_service/ /app/services/inventory_service/

WORKDIR /app/services/inventory_service

# inventory_service 监听 0.0.0.0:5013 (INVENTORY_SERVICE_PORT 默认 5013, 见 app.py __main__)
EXPOSE 5013

CMD ["python", "app.py"]
