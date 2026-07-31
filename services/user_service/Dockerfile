# user_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# user_service(:5004) 用户资料读写(C-content 族)。依赖全在 recweb-base
# (flask/flask-cors/mysql-connector/opentelemetry-*), app.py 仅 import 这些 + dotenv,
# 无额外 pip 依赖, 这里只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/user_service/Dockerfile -t recweb-user:latest .
#   (build context = 仓库根, 因要 COPY shared/; -f 指到本文件)
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# user_service app.py 用 Path(__file__).resolve().parents[2] 定位项目根读 .env,
# 并(__main__ 内)sys.path 插项目根 import shared.nacos_client。为保持该相对路径成立,
# 容器内目录布局必须复刻仓库: /app/services/user_service/app.py + /app/shared/。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv override=False 不会覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/user_service/ /app/services/user_service/

WORKDIR /app/services/user_service

# user_service 监听 0.0.0.0:5004 (USER_SERVICE_PORT 默认 5004, 见 app.py __main__)
EXPOSE 5004

# debug=False(源码固定 _DEBUG=False), 直接 python app.py 起 Werkzeug dev server(与裸进程栈一致)
CMD ["python", "app.py"]
