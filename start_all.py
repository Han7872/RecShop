"""
RecWeb 一键启动脚本
用法:
    python start_all.py              启动全部服务（含 Docker OTel 可观测性栈）
    python start_all.py --no-docker  只启 Python 服务，跳过 Docker OTel 栈
    python start_all.py --stop       停止全部服务（含 Docker OTel 栈）

启动顺序:
    1) Docker OTel 可观测性栈 (otel-collector / jaeger / prometheus / loki / grafana)
       —— 先于 Python 服务起来，避免业务服务早期遥测丢失
    2) 25 个 Python 微服务:sasrec_api(8200) 最先(等模型加载完成),
       backend_api(5000) / recommendation_agent(5001) / llm_rerank_service(5002) /
       review_service(5003) + 各域服务(5004–5022),shop_web(3000) 最后(依赖其余服务)。
       完整清单与启动顺序以下方 SERVICES 为准;服务总览见 README.md

shop_web 包含三个 Blueprint: 买家端(/) / 商家端(/merchant) / 管理端(/admin)
按 Ctrl+C 可优雅地停止 Python 服务（默认保留 Docker OTel 栈继续看数据）。
OTel 栈说明: Grafana 映射在宿主 :3001（避开 shop_web 的 3000）。
--no-docker 适用于本机没装 / 不想用 Docker 的场景。
"""

import subprocess
import sys
import os
import time
import signal
import argparse
import socket
import shutil
import urllib.request
from pathlib import Path

# Windows GBK 控制台下,emoji (✓ ✅ ❌) print 时会抛 UnicodeEncodeError。
# Python 3.7+ 用 sys.stdout.reconfigure 强制 stdout/stderr 为 UTF-8。
# errors='replace' 兜底:不可表示的字符替换为 ? 而不是崩溃。
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        # Python < 3.7 fallback (本项目用 3.10,实际不会走到这里)
        pass

# 上面的 reconfigure 只修了本脚本自己的 stdout。下面这行是为各子服务进程准备的:
# PYTHONIOENCODING 由 Python 解释器在“启动时”读取,在本进程里改 os.environ 救不了自己,
# 但 subprocess.Popen 启动的子进程是全新解释器、会继承本进程的 os.environ,
# 于是子服务的 stdout/stderr 也变 UTF-8,它们的 emoji 不会在 GBK 控制台崩。
# 写在这里 = 内置进脚本,不必再去 PyCharm Run Config 配环境变量。
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ==================== 服务定义 ====================

ROOT = Path(__file__).resolve().parent

# Docker OTel 可观测性栈 compose 文件（绝对路径，避免受 cwd 影响）
OTEL_COMPOSE = ROOT / "ops" / "docker-compose.otel.yml"
# Docker Desktop 可执行文件常见安装路径（Windows，Hyper-V backend）
# 注意: 这是 GUI 进程，用于 ensure_docker_running() 里 Popen 拉起界面，
#       不能用它跑 info/compose 子命令——那是下面 DOCKER_CLI 的 docker.exe 的活。
DOCKER_DESKTOP_EXE = Path(r"docker-desktop")


def _resolve_docker_cli() -> str:
    """解析 docker CLI 的可执行路径，三段优先级 fallback。

    为什么不直接依赖 PATH: Docker Desktop 默认不把 docker.exe 写进系统 PATH，
    而本脚本常被 PyCharm / 双击 / 计划任务等“干净环境”拉起，裸 "docker" 会
    FileNotFoundError。故先探固定安装路径，再退 shutil.which，最后才裸名兜底。
    """
    # 第一优先: Docker Desktop CLI 固定安装路径（已实测存在）
    for p in (Path(r"docker"),):
        if p.exists():
            return str(p)
    # 第二优先: 用户把 docker 加进了 PATH / 非默认安装位置
    found = shutil.which("docker")
    if found:
        return found
    # 第三优先(兜底): 保持与 Linux/Mac 及任意 PATH 命中环境的兼容，行为同现状
    return "docker"


# docker CLI 可执行路径（专指 docker.exe，跑 info/compose 子命令用；与 DOCKER_DESKTOP_EXE 各司其职）
DOCKER_CLI = _resolve_docker_cli()
# collector OTLP HTTP 端口（用于探测栈是否就绪）
OTEL_COLLECTOR_PORT = 4318
# Nacos 服务注册中心（本机 standalone 安装；默认在 RecWeb2 同级目录的 nacos/，可用 NACOS_HOME 覆盖）
NACOS_HOME = Path(os.environ.get("NACOS_HOME", str(ROOT.parent / "nacos")))
NACOS_PORT = 8848

SERVICES = [
    {
        "name": "SASRec API",
        "cwd": ROOT / "services" / "sasrec_api",
        "cmd": [sys.executable, "api_server.py"],
        "port": 8200,
        "health": "http://127.0.0.1:8200/health",
        "wait": 15,        # 模型加载较慢，最多等 15 秒
    },
    {
        "name": "Backend API",
        "cwd": ROOT / "services" / "backend_api",
        "cmd": [sys.executable, "app.py"],
        "port": 5000,
        "health": "http://127.0.0.1:5000/health",
        "wait": 3,
    },
    {
        "name": "Recommendation Agent",
        "cwd": ROOT / "services" / "recommendation_agent",
        "cmd": [sys.executable, "app.py"],
        "port": 5001,
        "health": "http://127.0.0.1:5001/recommend/health",
        "wait": 5,
    },
    {
        "name": "LLM Rerank Service",
        "cwd": ROOT / "services" / "llm_rerank_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5002,
        "health": "http://127.0.0.1:5002/health",
        "wait": 3,
    },
    {
        "name": "Review Service",
        "cwd": ROOT / "services" / "review_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5003,
        "health": "http://127.0.0.1:5003/health",
        "wait": 3,
    },
    {
        "name": "Catalog Service",
        "cwd": ROOT / "services" / "catalog_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5005,
        "health": "http://127.0.0.1:5005/health",
        "wait": 3,
    },
    {
        "name": "Cart Service",
        "cwd": ROOT / "services" / "cart_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5006,
        "health": "http://127.0.0.1:5006/health",
        "wait": 3,
    },
    {
        "name": "User Service",
        "cwd": ROOT / "services" / "user_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5004,
        "health": "http://127.0.0.1:5004/health",
        "wait": 3,
    },
    {
        "name": "Address Service",
        "cwd": ROOT / "services" / "address_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5007,
        "health": "http://127.0.0.1:5007/health",
        "wait": 3,
    },
    {
        "name": "AI Memory Service",
        "cwd": ROOT / "services" / "ai_memory_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5008,
        "health": "http://127.0.0.1:5008/health",
        "wait": 3,
    },
    {
        "name": "Announcement Service",
        "cwd": ROOT / "services" / "announcement_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5009,
        "health": "http://127.0.0.1:5009/health",
        "wait": 3,
    },
    {
        "name": "Inventory Service",
        "cwd": ROOT / "services" / "inventory_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5013,
        "health": "http://127.0.0.1:5013/health",
        "wait": 3,
    },
    {
        "name": "Pricing Service",
        "cwd": ROOT / "services" / "pricing_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5014,
        "health": "http://127.0.0.1:5014/health",
        "wait": 3,
    },
    {
        "name": "Promotion Service",
        "cwd": ROOT / "services" / "promotion_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5015,
        "health": "http://127.0.0.1:5015/health",
        "wait": 3,
    },
    {
        "name": "Payment Service",
        "cwd": ROOT / "services" / "payment_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5012,
        "health": "http://127.0.0.1:5012/health",
        "wait": 3,
    },
    {
        "name": "Order Service",
        "cwd": ROOT / "services" / "order_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5010,
        "health": "http://127.0.0.1:5010/health",
        "wait": 3,
    },
    {
        "name": "Checkout Service",
        "cwd": ROOT / "services" / "checkout_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5011,
        "health": "http://127.0.0.1:5011/health",
        "wait": 3,
    },
    {
        "name": "Interaction Service",
        "cwd": ROOT / "services" / "interaction_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5020,
        "health": "http://127.0.0.1:5020/health",
        "wait": 3,
    },
    {
        "name": "Merchant Service",
        "cwd": ROOT / "services" / "merchant_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5019,
        "health": "http://127.0.0.1:5019/health",
        "wait": 3,
    },
    {
        "name": "Admin Audit Service",
        "cwd": ROOT / "services" / "admin_audit_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5022,
        "health": "http://127.0.0.1:5022/health",
        "wait": 3,
    },
    {
        "name": "Notification Service",
        "cwd": ROOT / "services" / "notification_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5021,
        "health": "http://127.0.0.1:5021/health",
        "wait": 3,
    },
    {
        "name": "Review Query Service",
        "cwd": ROOT / "services" / "review_query_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5018,
        "health": "http://127.0.0.1:5018/health",
        "wait": 3,
    },
    {
        "name": "Search Service",
        "cwd": ROOT / "services" / "search_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5017,
        "health": "http://127.0.0.1:5017/health",
        "wait": 3,
    },
    {
        "name": "Shipping Service",
        "cwd": ROOT / "services" / "shipping_service",
        "cmd": [sys.executable, "app.py"],
        "port": 5016,
        "health": "http://127.0.0.1:5016/health",
        "wait": 3,
    },
    {
        "name": "ShopWeb",
        "cwd": ROOT / "services" / "shop_web",
        "cmd": [sys.executable, "run.py"],
        "port": 3000,
        "health": "http://127.0.0.1:3000/health",
        "wait": 3,
    },
]

# ==================== 工具函数 ====================

processes: list[subprocess.Popen] = []

# 本次运行是否启用了 Docker OTel 栈（用于 Ctrl+C 退出时给出保留提示）
_otel_stack_active = False

def _color(text: str, code: int) -> str:
    """ANSI 颜色包装，Windows 10+ 支持"""
    return f"\033[{code}m{text}\033[0m"

def info(msg: str):
    print(_color(f"[INFO]  {msg}", 36))

def ok(msg: str):
    print(_color(f"[  OK]  {msg}", 32))

def warn(msg: str):
    print(_color(f"[WARN]  {msg}", 33))

def err(msg: str):
    print(_color(f"[FAIL]  {msg}", 31))

def check_health(url: str, timeout: int = 2) -> bool:
    """尝试访问健康检查端点"""
    try:
        req = urllib.request.urlopen(url, timeout=timeout)
        return req.status == 200
    except Exception:
        return False

def wait_for_service(svc: dict) -> bool:
    """等待服务就绪，返回是否成功"""
    url = svc["health"]
    max_wait = svc["wait"]
    if url is None:
        # 没有健康检查端点，简单等待
        time.sleep(min(max_wait, 2))
        return True
    for i in range(max_wait):
        if check_health(url):
            return True
        time.sleep(1)
    return False

# ---------- Docker OTel 可观测性栈编排 ----------
# Windows 上 docker/compose 输出可能是 GBK/OEM 代码页，统一用
# encoding="utf-8", errors="ignore" 解码（与下方 stop_all 的 netstat 同款套路）。

def docker_available() -> bool:
    """docker daemon 是否就绪。跑 `docker info`，短超时，返回 True/False。"""
    try:
        result = subprocess.run(
            [str(DOCKER_CLI), "info"],
            capture_output=True, text=True,
            encoding="utf-8", errors="ignore",
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        # docker 未安装(FileNotFoundError) / daemon 卡死(TimeoutExpired) 等
        return False

def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    """探测某个 TCP 端口是否可连接。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def ensure_docker_running() -> bool:
    """确保 docker daemon 就绪；不通则尝试拉起 Docker Desktop 并轮询等待。

    返回 daemon 最终是否可用。起不来只 warn 不抛错（可观测性栈是增强项）。
    """
    if docker_available():
        ok("Docker 引擎已就绪")
        return True

    info("Docker 引擎未就绪，尝试启动 Docker Desktop ...")
    if not DOCKER_DESKTOP_EXE.exists():
        warn(f"未找到 Docker Desktop ({DOCKER_DESKTOP_EXE})，跳过 OTel 栈")
        return False

    try:
        # 直接拉起 Docker Desktop GUI 进程；它会在后台把 daemon 带起来
        subprocess.Popen([str(DOCKER_DESKTOP_EXE)])
    except Exception as e:
        warn(f"启动 Docker Desktop 失败: {e}，跳过 OTel 栈")
        return False

    # 轮询等待 daemon 就绪，最多 ~90 秒
    info("正在等待 Docker 引擎就绪（最多 90 秒，首次启动较慢）...")
    deadline = time.time() + 90
    while time.time() < deadline:
        if docker_available():
            ok("Docker 引擎已就绪")
            return True
        remaining = int(deadline - time.time())
        info(f"  ... 仍在等待 Docker 引擎（剩余约 {remaining}s）")
        time.sleep(5)

    warn("等待 Docker 引擎超时，跳过 OTel 栈（业务服务将照常启动）")
    return False

def start_otel_stack() -> bool:
    """`docker compose up -d` 拉起 OTel 栈，并等待 collector 端口可连。

    成功返回 True；失败只 warn 不退出——可观测性栈挂了也允许业务服务照常启动。
    """
    if not OTEL_COMPOSE.exists():
        warn(f"未找到 compose 文件 ({OTEL_COMPOSE})，跳过 OTel 栈")
        return False

    info("正在拉起 OTel 可观测性栈 (docker compose up -d) ...")
    try:
        result = subprocess.run(
            [str(DOCKER_CLI), "compose", "-f", str(OTEL_COMPOSE), "up", "-d"],
            capture_output=True, text=True,
            encoding="utf-8", errors="ignore",
            timeout=180,
        )
    except Exception as e:
        warn(f"docker compose up 失败: {e}（OTel 栈跳过，业务服务继续）")
        return False

    if result.returncode != 0:
        warn("docker compose up 返回非零，OTel 栈可能未完全起来（业务服务继续）")
        if result.stderr:
            warn(f"  详情: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else ''}")
        return False

    # 等待 collector 的 4318 端口可连，让后续 Python 服务的早期遥测不至于全丢
    info(f"等待 OTel collector 就绪 (port {OTEL_COLLECTOR_PORT}) ...")
    deadline = time.time() + 30
    while time.time() < deadline:
        if _port_open(OTEL_COLLECTOR_PORT):
            ok("OTel 栈已就绪  ✓  (Grafana: http://localhost:3001  Jaeger: http://localhost:16686)")
            return True
        time.sleep(1)

    warn("OTel collector 端口未在预期时间内就绪（容器可能仍在启动，业务服务继续）")
    return False

def stop_otel_stack():
    """`docker compose down` 停掉 OTel 栈。

    注意: 仅 down，不带 -v，命名 volume 会保留——grafana/prometheus 等数据不丢。
    （如需连数据一起清除，请手动 `docker compose -f ops/docker-compose.otel.yml down -v`）
    """
    if not OTEL_COMPOSE.exists():
        return
    if not docker_available():
        warn("Docker 引擎不可用，跳过停止 OTel 栈")
        return
    info("正在停止 OTel 可观测性栈 (docker compose down) ...")
    try:
        subprocess.run(
            [str(DOCKER_CLI), "compose", "-f", str(OTEL_COMPOSE), "down"],
            capture_output=True, text=True,
            encoding="utf-8", errors="ignore",
            timeout=120,
        )
        ok("OTel 栈已停止（数据卷已保留）")
    except Exception as e:
        warn(f"停止 OTel 栈时出错: {e}")

# ---------- Nacos 服务注册中心编排 ----------
# 必须在业务服务之前起来:否则各服务启动时注册不上(只走本地 fallback),且 Nacos 宕时
# 每次服务发现要等 ~2s 重试超时——这正是"不启 Nacos 时操作延迟高/偶发 network error"的根因。

def _nacos_enabled() -> bool:
    return os.environ.get("NACOS_ENABLED", "true").strip().lower() == "true"

def start_nacos() -> bool:
    """启动本机 Nacos(standalone)。已在跑 / 未启用 / 未安装 都跳过;失败不阻塞业务服务。"""
    if not _nacos_enabled():
        info("NACOS_ENABLED != true，跳过 Nacos(服务发现走本地 URL)")
        return False
    if _port_open(NACOS_PORT):
        ok(f"Nacos 已在运行  ✓  (port {NACOS_PORT})")
        return True
    startup = NACOS_HOME / "bin" / ("startup.cmd" if sys.platform == "win32" else "startup.sh")
    if not startup.exists():
        warn(f"未找到 Nacos 启动脚本 ({startup})，跳过(服务发现将走本地 fallback)")
        return False
    info(f"正在启动 Nacos (standalone) ... ({NACOS_HOME})")
    try:
        if sys.platform == "win32":
            subprocess.Popen(
                ["cmd", "/c", str(startup), "-m", "standalone"],
                cwd=str(NACOS_HOME / "bin"),
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
        else:
            subprocess.Popen(["bash", str(startup), "-m", "standalone"], cwd=str(NACOS_HOME / "bin"))
    except Exception as e:
        warn(f"启动 Nacos 失败: {e}（服务发现走本地 fallback，业务继续）")
        return False
    info(f"等待 Nacos 就绪 (port {NACOS_PORT}) ...")
    deadline = time.time() + 60
    while time.time() < deadline:
        if _port_open(NACOS_PORT):
            ok(f"Nacos 已就绪  ✓  (控制台: http://localhost:{NACOS_PORT}/nacos)")
            return True
        time.sleep(2)
    warn("Nacos 端口未在 60s 内就绪（可能仍在启动，业务服务继续，发现暂走 fallback）")
    return False

def stop_nacos():
    """停掉本机 Nacos(若在跑)。"""
    if not _port_open(NACOS_PORT):
        return
    shutdown = NACOS_HOME / "bin" / ("shutdown.cmd" if sys.platform == "win32" else "shutdown.sh")
    if not shutdown.exists():
        warn(f"Nacos 在跑但未找到 shutdown 脚本 ({shutdown})，请手动停止")
        return
    info("正在停止 Nacos ...")
    try:
        subprocess.run(
            ["cmd", "/c", str(shutdown)] if sys.platform == "win32" else ["bash", str(shutdown)],
            cwd=str(NACOS_HOME / "bin"),
            capture_output=True, text=True, encoding="utf-8", errors="ignore", timeout=30,
        )
        ok("Nacos 已停止")
    except Exception as e:
        warn(f"停止 Nacos 时出错: {e}")

# ---------- Python 子进程关闭 ----------

def shutdown_all():
    """优雅关闭所有子进程"""
    print()
    info("正在停止所有服务 ...")
    for proc in reversed(processes):
        if proc.poll() is None:
            proc.terminate()
    # 给进程 5 秒优雅退出
    deadline = time.time() + 5
    for proc in processes:
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()
    ok("所有 Python 服务已停止")
    # 默认不停 Docker（用户可能想保留栈继续看数据），仅给出提示
    if _otel_stack_active:
        info("OTel 容器仍在运行，如需停止：python start_all.py --stop "
             "或 docker compose -f ops/docker-compose.otel.yml down")

def signal_handler(sig, frame):
    shutdown_all()
    sys.exit(0)

# ==================== 主流程 ====================

def start_all(no_docker: bool = False):
    global _otel_stack_active  # 本函数会写它(启栈成功时)并在汇总打印时读它

    # Windows 启用 ANSI 颜色
    if sys.platform == "win32":
        os.system("")  # 启用 VT100 转义序列

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print()
    print("=" * 60)
    print(_color("  RecWeb 一键启动", 1))
    print("=" * 60)
    print()

    # ---- 先拉起 Docker OTel 可观测性栈（业务服务之前）----
    if no_docker:
        info("已指定 --no-docker，跳过 Docker OTel 栈，仅启动 Python 服务")
        print()
    else:
        if ensure_docker_running():
            start_otel_stack()
            _otel_stack_active = True
        else:
            warn("Docker 不可用，跳过 OTel 栈（业务服务将照常启动）")
        print()

    # ---- 启动 Nacos 服务注册中心（务必在业务服务之前：确保各服务能注册、且发现不卡 ~2s）----
    start_nacos()
    print()

    for idx, svc in enumerate(SERVICES, 1):
        info(f"[{idx}/{len(SERVICES)}] 启动 {svc['name']} (port {svc['port']}) ...")

        # 注:此处不传 env=,子服务直接继承 os.environ。故障注入演练时,只需在启动
        # start_all 前设置 REVIEW_SERVICE_URL=http://127.0.0.1:18503(Toxiproxy 代理口),
        # shop_web 便会经代理调 review_service,无需改码(详见 # (archived script),已归档)。
        proc = subprocess.Popen(
            svc["cmd"],
            cwd=str(svc["cwd"]),
            # 子进程共享父进程的 stdin/stdout，日志实时可见
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        processes.append(proc)

        # 检查是否立即崩溃
        time.sleep(0.5)
        if proc.poll() is not None:
            err(f"{svc['name']} 启动失败 (exit code {proc.returncode})")
            shutdown_all()
            sys.exit(1)

        # 等待服务就绪
        if wait_for_service(svc):
            ok(f"{svc['name']} 已就绪  ✓")
        else:
            warn(f"{svc['name']} 未通过健康检查，但进程仍在运行（可能仍在加载中）")

        print()

    print("=" * 60)
    ok("全部服务已启动！")
    print("=" * 60)
    # OTel 三件套前端: 栈未运行时这些打不开,据 _otel_stack_active 给一行提示
    otel_status = (
        "OTel 栈已就绪 ✓"
        if _otel_stack_active
        else "OTel 栈未由本脚本启动 (如未运行: docker compose -f ops/docker-compose.otel.yml up -d)"
    )
    print(f"""
  ── 业务服务 ──────────────────────────────────────
  SASRec API        : http://localhost:8200
  SASRec API Docs   : http://localhost:8200/docs
  Backend API       : http://localhost:5000
  Recommendation    : http://localhost:5001
  LLM Rerank        : http://localhost:5002
  Review Service    : http://localhost:5003
  ShopWeb (买家端)  : http://localhost:3000
  ShopWeb (商家端)  : http://localhost:3000/merchant
  ShopWeb (管理端)  : http://localhost:3000/admin

  ── 可观测性 · OTel 三件套 ────────────────────────
  Grafana (三件套统一面板) : http://localhost:3001
  Jaeger  (Trace 链路追踪) : http://localhost:16686
  Prometheus (Metric 指标) : http://localhost:9090
  Loki    (Log 日志)       : 经 Grafana 查看 (数据源 http://localhost:3100)

  ── 服务注册中心 ──────────────────────────────────
  Nacos   (控制台)         : http://localhost:8848/nacos  (账号/密码默认 nacos/nacos)
  · {otel_status}

  按 Ctrl+C 停止所有服务 (默认保留 OTel 容器，--stop 可连容器一起停)
""")
    print("=" * 60)

    # 阻塞等待，直到任一子进程退出或收到 Ctrl+C
    try:
        while True:
            for proc in processes:
                if proc.poll() is not None:
                    svc_name = SERVICES[processes.index(proc)]["name"]
                    warn(f"{svc_name} 已退出 (exit code {proc.returncode})")
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_all()


def stop_all():
    """通过端口查找并终止服务进程（独立停止模式）"""
    if sys.platform == "win32":
        os.system("")
    print()
    info("正在查找并停止 RecWeb 服务 ...")
    # 一次性抓 netstat, 建 port -> {pid} 映射。三处硬化(对应旧版三个坑):
    #  (1) 精确按本地地址尾部 ":port" 匹配, 避免子串误中(旧 `f":{port}" in line` 会让
    #      :5000 命中 :50000、:8200 命中 :182000 之类);
    #  (2) 收集同端口【所有】LISTENING PID(不止第一个)——debug reloader 父子双绑、
    #      新旧实例叠绑同端口时, 旧版 break 只杀一个会留残;
    #  (3) taskkill 加 /T 连进程树杀(reloader 派生的子 worker 也清掉, 杜绝残留占端口,
    #      这正是 sasrec api_server.py 等重启撞端口 exit 1 的根因之一)。
    # Windows netstat/taskkill 输出用 OEM 代码页, 这里只解析全 ASCII 字段, errors="ignore" 安全。
    port_pids = {}
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True,
            encoding="utf-8", errors="ignore"
        )
        for line in result.stdout.splitlines():
            if "LISTENING" not in line:
                continue
            parts = line.split()
            if len(parts) < 5 or ":" not in parts[1]:
                continue
            tail = parts[1].rsplit(":", 1)[-1]  # 本地地址 0.0.0.0:8200 / [::]:8200 -> 8200
            if tail.isdigit():
                port_pids.setdefault(int(tail), set()).add(parts[-1])
    except Exception as e:
        err(f"netstat 查询失败: {e}")

    killed = 0
    self_pid = str(os.getpid())  # 别误杀执行 --stop 的本进程
    for svc in SERVICES:
        port = svc["port"]
        pids = sorted(port_pids.get(port, set()) - {self_pid})
        if not pids:
            warn(f"{svc['name']} (port {port}) 未在运行")
            continue
        for pid in pids:
            r = subprocess.run(["taskkill", "/F", "/T", "/PID", pid],
                               capture_output=True, text=True,
                               encoding="utf-8", errors="ignore")
            if r.returncode == 0:
                ok(f"已停止 {svc['name']} (PID {pid}, port {port})")
                killed += 1
            else:
                warn(f"{svc['name']} (PID {pid}) 终止未成功(可能已退/属其它树): "
                     f"{(r.stdout or r.stderr).strip()[:80]}")
    print()
    ok(f"完成，共停止 {killed} 个进程(含同端口多 PID 与子进程树)")

    # 一并停掉 Docker OTel 栈（命名数据卷保留，不删数据）
    print()
    stop_otel_stack()

    # 一并停掉 Nacos 注册中心
    print()
    stop_nacos()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RecWeb 一键启动 / 停止")
    parser.add_argument("--stop", action="store_true", help="停止所有服务（含 Docker OTel 栈）")
    parser.add_argument("--no-docker", action="store_true",
                        help="跳过 Docker OTel 栈，只启动 Python 服务")
    args = parser.parse_args()

    if args.stop:
        stop_all()
    else:
        start_all(no_docker=args.no_docker)
