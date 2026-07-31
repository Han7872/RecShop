# recommendation_agent Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# recommendation_agent(:5001) LangGraph 多 Agent 推荐编排服务。
#   无 DB(纯内存编排), 下游: sasrec_api(序列推荐) + DeepSeek(外部 LLM API)。
# 依赖全在 recweb-base —— 根 requirements.txt 已含 langchain / langchain-core /
#   langchain-openai / langgraph / openai / opentelemetry-instrumentation-httpx,
#   故此处【无需】额外 RUN pip install, 只叠代码层。
#
# ⚠ 大文件 electronics.inter(~2.4GB)与运行无关(仅离线评测脚本用), 已被根
#   .dockerignore 的 **/*.inter 规则排除, 绝不进镜像。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/recommendation_agent/Dockerfile -t recweb-rec-agent:latest .
#   (build context = 仓库根, 因要 COPY shared/; -f 指到本文件)
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# recommendation_agent app.py 用 Path(__file__).resolve().parents[2] 定位项目根读 .env,
# 且 workflow.py / agents 为本服务目录内模块(from workflow import ...,
# from agents.prompts/agents.tools import ...)。WORKDIR 切到服务目录后这些相对 import 才成立。
# 复刻仓库布局: /app/services/recommendation_agent/{app.py,workflow.py,agents/,static/}
#               + /app/shared/(shared.nacos_client, NACOS_ENABLED=false 时只走 fallback)。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv override=False 不覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/recommendation_agent/ /app/services/recommendation_agent/

WORKDIR /app/services/recommendation_agent

# recommendation_agent 监听 0.0.0.0:5001 (RECOMMENDATION_PORT 默认 5001, 见 app.py __main__)
EXPOSE 5001

# debug=False(源码硬编码), 直接 python app.py 起 Werkzeug dev server(与裸进程栈一致)
CMD ["python", "app.py"]
