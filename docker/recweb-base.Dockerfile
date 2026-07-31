# recweb-base.Dockerfile — 共享 Python base 镜像（M0 pilot / 迭代 2 复用）
# ----------------------------------------------------------------------------
# 用途: catalog_service / backend_api 等"薄" Flask 服务 FROM 它, 公共依赖只装一次,
#       各服务镜像只叠自己的代码层 → 薄镜像共享 base 层, 不会出 10GB+ 怪物。
# 注意:
#   - 不含 torch / recbole(那是 sasrec 单独重镜像 docker/sasrec_api.Dockerfile 的事)。
#   - 大文件(*.pkl/*.pth/*.inter)由 .dockerignore 排除, 绝不进任何镜像, 用 hostPath 挂卷。
#   - 先 COPY requirements 再 pip install, 吃满 layer cache(改代码不触发重装依赖)。
#
# build(主循环在仓库根执行, M0 集群起后):
#   docker build -f docker/recweb-base.Dockerfile -t recweb-base:latest .
# ----------------------------------------------------------------------------

# 主循环实测确认(2026-06-25): recweb2 conda env = Python 3.10.20 → 用 3.10-slim 对齐
#    (避免 unpickling dataset 对象 / recbole C-extension 跨小版本不兼容)
FROM python:3.10-slim

# 不写 .pyc、stdout/stderr 不缓冲(日志即时进 OTel/容器日志)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# mysql-connector-python / cryptography 等可能需要的最小系统依赖。
# 🔶 主循环实测确认: 若 pip install 报缺 gcc/构建依赖, 加 build-essential(装完可清)。
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先只 COPY requirements → 吃 layer cache。
# 🔶 主循环实测确认: 根 requirements.txt 含 torch?  —— 实测【不含】, torch 由 sasrec 镜像单独装,
#    base 镜像不应拉 torch(否则 base 也变重)。本任务确认根 requirements.txt 无 torch 行。
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /app/requirements.txt

# 各服务镜像在此基础上 COPY 自己的代码 + shared/。base 不放业务代码。
CMD ["python", "--version"]
