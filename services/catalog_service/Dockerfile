# catalog_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# catalog_service(:5005) 高 fan-in 纯只读目录服务, M0 pilot 三服务之一(最轻一极)。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*), 这里只叠代码。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/catalog_service/Dockerfile -t recweb-catalog:latest .
#   (build context = 仓库根, 因要 COPY shared/; -f 指到本文件)
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# catalog_service app.py 顶部用 Path(__file__).resolve().parents[2] 定位项目根读 .env,
# 并 sys.path 插项目根 import shared.nacos_client。为保持该相对路径成立,
# 容器内目录布局必须复刻仓库: /app/services/catalog_service/app.py + /app/shared/。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv override=False 不会覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/catalog_service/ /app/services/catalog_service/

WORKDIR /app/services/catalog_service

# catalog_service 监听 0.0.0.0:5005 (CATALOG_SERVICE_PORT 默认 5005, 见 app.py __main__)
EXPOSE 5005

# debug=False(源码固定), 直接 python app.py 起 Werkzeug dev server(与裸进程栈一致, 不生产化)
CMD ["python", "app.py"]
