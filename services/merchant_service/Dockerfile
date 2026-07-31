# merchant_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# merchant_service(:5019) 商家资料+店铺读取/部分更新(C-content 族)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/merchant_service/Dockerfile -t recweb-merchant:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# merchant_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/merchant_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/merchant_service/ /app/services/merchant_service/

WORKDIR /app/services/merchant_service

# merchant_service 监听 0.0.0.0:5019 (MERCHANT_SERVICE_PORT 默认 5019, 见 app.py __main__)
EXPOSE 5019

CMD ["python", "app.py"]
