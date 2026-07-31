# sasrec_api Dockerfile — 单独重镜像(含 torch + recbole vendor), 不 FROM recweb-base
# ----------------------------------------------------------------------------
# sasrec_api(:8200) FastAPI 推理核心, M0 pilot 三服务之一(最重一极)。
# 与薄服务隔离成独立镜像: torch 巨大, 不该污染 recweb-base 公共层。
#
# 铁律(绝不违反):
#   - standard_cache.pkl(8.57GB) / SASRec-*.pth(260MB) / electronics.inter / *.item
#     【绝不进镜像】—— .dockerignore 已排, 运行时用 hostPath 只读挂卷(见 k8s/pilot Deployment)。
#   - 镜像里只放代码 + vendor/recbole 源码 + 依赖。
#
# build(仓库根执行):
#   docker build -f services/sasrec_api/Dockerfile -t recweb-sasrec:latest .
# ----------------------------------------------------------------------------

# 主循环实测确认(2026-06-25): recweb2 = Python 3.10.20 → 3.10-slim 对齐。
#    M0 仍 CPU 推理, 装 CPU 版 torch(镜像小、不依赖宿主 CUDA)。
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# recbole/pandas 编译可能需要的最小系统依赖
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- 依赖先装, 吃 layer cache ----
# 根 requirements.txt 提供公共依赖(fastapi/uvicorn/pandas/opentelemetry-* 等), 但【不含 torch】。
# torch 是裸进程栈用 conda/pip 单独装的, 仓库无 sasrec 专属 requirements 文件。
# 🔶 主循环实测确认: 下面 torch / recbole 运行时依赖清单需主循环按裸进程 env 实测补全:
#    - torch 版本(基线 2.5.1; M0 CPU 用 CPU wheel: --index-url https://download.pytorch.org/whl/cpu)
#    - recbole 是 vendor 源码(services/sasrec_api/vendor/recbole), 但其运行时还依赖哪些三方包
#      (如 colorama/tensorboard 已在根 requirements; 是否还缺 kmeans-pytorch/thop 等)需实测 import 验证。
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /app/requirements.txt

# 主循环确认: 装 CPU 版 torch 2.5.1(M0 CPU 推理; 与基线同版本号, cu118→cpu wheel)
# 🔶 build 时若 recbole vendor 还缺三方包(scipy/thop 等), 据 import 报错增补
RUN pip install torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu

# ---- 代码 + vendor ----
# api_server.py 用 Path(__file__).resolve().parents[2] 读项目根 .env, 并 sys.path 插
# 项目根 import shared.nacos_client + HERE/vendor 加 recbole。复刻仓库布局:
#   /app/services/sasrec_api/{api_server.py,vendor/...} + /app/shared/
# .dockerignore 保证此 COPY 不会把 *.pkl/*.pth/*.inter/*.item 带进来。
COPY shared/ /app/shared/
COPY services/sasrec_api/ /app/services/sasrec_api/

WORKDIR /app/services/sasrec_api

# sasrec_api 监听 0.0.0.0:8200 (SASREC_PORT 默认 8200, 见 api_server.py __main__)
EXPOSE 8200

# 直接跑 api_server.py 的 __main__(uvicorn.run), startup event 加载模型(读挂卷的 pkl/pth)
CMD ["python", "api_server.py"]
