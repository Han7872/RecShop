# announcement_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# announcement_service(:5009) 公告读写(C-content 族)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/announcement_service/Dockerfile -t recweb-announcement:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# announcement_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/announcement_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/announcement_service/ /app/services/announcement_service/

WORKDIR /app/services/announcement_service

# announcement_service 监听 0.0.0.0:5009 (ANNOUNCEMENT_SERVICE_PORT 默认 5009, 见 app.py __main__)
EXPOSE 5009

CMD ["python", "app.py"]
