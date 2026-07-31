# admin_audit_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# admin_audit_service(:5022) 管理操作审计写/读(C-content 族, 叶子)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/admin_audit_service/Dockerfile -t recweb-admin-audit:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# admin_audit_service app.py 用 parents[2] 定位项目根读 .env + (__main__) import shared.nacos_client。
# 复刻仓库布局: /app/services/admin_audit_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/admin_audit_service/ /app/services/admin_audit_service/

WORKDIR /app/services/admin_audit_service

# admin_audit_service 监听 0.0.0.0:5022 (ADMIN_AUDIT_SERVICE_PORT 默认 5022, 见 app.py __main__)
EXPOSE 5022

CMD ["python", "app.py"]
