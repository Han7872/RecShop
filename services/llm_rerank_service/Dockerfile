# llm_rerank_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# llm_rerank_service(:5002) LLM 重排序服务(AI Picks): 历史 + SASRec 候选 →
#   DeepSeek 选最优单品(失败 fallback 取最高分)。无 DB, 唯一下游 DeepSeek(外部 API)。
# 依赖全在 recweb-base —— 根 requirements.txt 已含 openai / opentelemetry-instrumentation-httpx,
#   reranker.py 内 `from openai import OpenAI` 即用该 SDK 调 DeepSeek(OpenAI 兼容 /v1),
#   故此处【无需】额外 RUN pip install, 只叠代码层。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/llm_rerank_service/Dockerfile -t recweb-llm-rerank:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# llm_rerank_service app.py 用 Path(__file__).resolve().parents[2] 定位项目根读 .env
# 并 sys.path 插项目根 import shared.nacos_client; reranker.py / utils.validator
# 为本服务目录内模块(from reranker import rerank, from utils.validator import ...)。
# 复刻仓库布局: /app/services/llm_rerank_service/{app.py,reranker.py,utils/,prompts/}
#               + /app/shared/。
# (.env 不进镜像; 配置全经 K8S env 注入, load_dotenv 不覆盖已注入 env。)
COPY shared/ /app/shared/
COPY services/llm_rerank_service/ /app/services/llm_rerank_service/

WORKDIR /app/services/llm_rerank_service

# llm_rerank_service 监听 0.0.0.0:5002 (RERANK_PORT 默认 5002, 见 app.py __main__)
EXPOSE 5002

# debug 由 RERANK_DEBUG 控制(默认 false → 无 reloader); 直接 python app.py 起 dev server
CMD ["python", "app.py"]
