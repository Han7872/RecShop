# notification_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# notification_service(:5021) 通知叶子(C-content 族, 被 order/payment/shipping fan-in)。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*),
# app.py 仅 import 这些 + dotenv, 无额外 pip 依赖, 这里只叠代码层。
# ⚠ 启动时 _ensure_schema() 幂等 CREATE TABLE notifications(自有表, 非 items/inventory),
#   首次起容器需 DB 可达 + recweb_k8s 用户对 shopify2 有 CREATE/INSERT 权限(只读用户会建表失败,
#   但源码 try/except 容错只打日志不崩溃) —— 见 manifest notes。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/notification_service/Dockerfile -t recweb-notification:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# notification_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/notification_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/notification_service/ /app/services/notification_service/

WORKDIR /app/services/notification_service

# notification_service 监听 0.0.0.0:5021 (NOTIFICATION_SERVICE_PORT 默认 5021, 见 app.py __main__)
EXPOSE 5021

CMD ["python", "app.py"]
