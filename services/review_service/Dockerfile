# review_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# review_service(:5003) 评论写/审核/回复 + 单事务重算 items.rating。B-trade 族。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/opentelemetry-*), 这里只叠代码。
# import 仅标准库 + flask + mysql.connector + dotenv(均在 base), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/review_service/Dockerfile -t recweb-review:latest .
#   (build context = 仓库根, 因要 COPY shared/; -f 指到本文件)
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# review_service app.py 顶部用 Path(__file__).resolve().parents[2] 定位项目根读 .env,
# 并 sys.path 插项目根 import shared.nacos_client。为保持该相对路径成立,
# 容器内目录布局必须复刻仓库: /app/services/review_service/app.py + /app/shared/。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv override=False 不会覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/review_service/ /app/services/review_service/

WORKDIR /app/services/review_service

# review_service 监听 0.0.0.0:5003 (REVIEW_SERVICE_PORT 默认 5003, 见 app.py __main__)
EXPOSE 5003

# debug=False(源码固定 _DEBUG=False), 直接 python app.py 起 Werkzeug dev server
CMD ["python", "app.py"]
