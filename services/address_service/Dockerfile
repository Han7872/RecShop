# address_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# address_service(:5007) 收货地址 CRUD(C-content 族)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/address_service/Dockerfile -t recweb-address:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# address_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/address_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/address_service/ /app/services/address_service/

WORKDIR /app/services/address_service

# address_service 监听 0.0.0.0:5007 (ADDRESS_SERVICE_PORT 默认 5007, 见 app.py __main__)
EXPOSE 5007

CMD ["python", "app.py"]
