# -*- coding: utf-8 -*-
"""agentfault 采集执行后端 —— 让【同一个 runner】既能驱动本机隔离 harness,也能驱动 K8S 全栈。

为什么有这一层(2026-07-27,B 档)
--------------------------------------------------------------------------------
`(upstream batch)` 的 108 case 是在**本机隔离 harness**(只起 rec-agent 一个临时
进程 + 真实 sasrec:8200)采的。交付包里"agent 跑在 25 服务全栈内"这句话因此只能靠降级
措辞遮掩。B 档 = 在 K8S 全栈上用**同一套 9 combo** 重采一遍,把这个 overclaim 消掉。

做法只能是"同一个 runner + 可换执行后端",**绝不另起并行采集器**:
  - CSV schema 一份(新旧批次可直接对比);
  - GT 判定(注入台账 strict_trace)一份;
  - Tier-A / Who&When / A2P judge / infra 负例 四套 eval **一行都不用改**。

★★ 头号红线:`--backend local` 必须与改造前逐字节等价 ★★
--------------------------------------------------------------------------------
本模块保证等价性的三条**构造性**依据(可逐条核对,不需要花钱重采来证):

 (1) `LocalBackend.start_instance / stop_instance / slot_ready / sample_host / reset_ledger`
     是原 `agentfault_runner.start_instance / stop_instance / port_free / sample_host` 与
     `_bring_up` 里那句 ledger 截断的**函数体逐字搬迁** —— 除了把模块级的 `build_env`
     换成构造时注入进来的**同一个函数对象**之外,一个字符都没改。

 (2) `probe / wait_health / read_spans` 是**运行时转调** `injector_smoke` 的同名函数。
     注意这里刻意用"薄包装函数"而不是 `probe = staticmethod(ISM.probe)`:
     class 定义期绑定会把函数对象**冻死**,而 `tests_dev/test_p0_2_runner.py` 的 test_c
     正是靠 `ISM.probe = fake_probe` 这个 monkeypatch seam 做离线自检的(见该文件
     L202-222)。冻死绑定 = 自检会真的往 5199 端口发网络请求。运行时转调既保住了
     "调的是同一个 code object",又保住了那个 seam。

 (3) 新增的三类钩子在 LocalBackend 上一律是 no-op / 空集合:
       `sync_ledger()`          -> pass        (本机 ledger 就在本地盘上,无需同步)
       `extra_csv_columns()`    -> []          (CSV 列一个不加 -> 表头字节不变)
       `extra_row_fields()`     -> {}          (行内容一个字段不加)
       `summary_meta()`         -> None        (run_summary.json 不多写 key)
     serverlog 也不需要新钩子:本机 serverlog 是 Popen 直接把子进程 stdout 接到文件,
     stop_instance 里关句柄即可(K8S 侧才需要 `kubectl logs` 转储,已内聚进 K8sBackend
     自己的 start/stop,不往 runner 里加钩子 —— 见下方 §偏离说明 D2)。

  => local 路径新增的执行差异只剩"一次属性查找 + 一层函数调用"。

与勘察结论的偏离(全部有理由,勿当疏漏)
--------------------------------------------------------------------------------
 D1 `port_free` 更名为 `slot_ready`,返回 `(ok, reason)`。
    理由:K8S 侧该检查语义**反转**(本机 = 端口必须空着;K8S = 服务必须活着且是变体镜像)。
    继续叫 port_free 会是主动误导。只有 1 个调用点,改名成本为零。LocalBackend 里的
    函数体仍是原 port_free 逐字搬迁。

 D2 不加 `BACKEND.dump_serverlog()` 运行期钩子。
    理由:`stop_instance()` 在所有路径上都先于下一次 `start_instance()` 被调用
    (run_combo 的 per-rep 分支 L935-939 / 单实例分支 L945-947 / health 失败分支 L877),
    所以"rollout 前把旧 pod 日志抢救下来"完全可以内聚在 K8sBackend.stop_instance 里,
    再在 start_instance 开头补一次幂等兜底。少改 runner 一处,等价性面更小。

 D3 env 白名单是 **16 键**不是 13 键。
    多出来的 `AGENTFAULT_SUBLLM_MODEL / AGENTFAULT_HALLU_MODE / AGENTFAULT_DEBUG`
    是 `build_env()` **不显式设置**、靠"驱动 shell 里也没设"来落到注入器内部默认值的
    三个旋钮(agentfault_injector.py L149/L534/L245)。pod 是长命对象,上一轮实验完全
    可能给它挂过这些 env;不显式 unset 就会与本机口径悄悄分叉。故一律纳入白名单:
    build_env 给了就 set,没给就显式 `KEY-` unset。

 D4 不做"rep-1 injected==0 就中止 combo"的 fail-fast。
    理由:v2 实测 hallu 族本来就有 ~6% 的 `inject_failed`(副 LLM 拒答),按 rep-1 赌运气
    会误杀好 combo。真正要防的是"pod 装错 env",而那个在 `start_instance` 里用
    printenv 逐键比对 + loader 装载日志确认,是**确定性**检查,严格优于概率性 fail-fast。

 D5 `host_cpu_pct / host_mem_pct` 在 K8S 侧改由 Prometheus 取 —— 这是**口径变更**,
    所以按硬要求"必须在 CSV 里可区分":K8S 树的 CSV 尾部会多出
    `collect_backend / host_metric_source / k8s_pod_name / k8s_pod_restarts` 四列
    (local 树一列不多)。详见 `K8sBackend.extra_csv_columns` 的注释。

★三份审查后的修正(2026-07-27 第二轮;每条都标了"没修会怎样",别再改回去)
--------------------------------------------------------------------------------
 R1 [致命·静默丢数据] span 本地镜像改**按 combo 累积**,不再每 rep 覆盖。
    原实现 `_write_local(span_file, tail_增量)` 在 **per-rep 模式**(format 族)下错得很彻底:
    每 rep 都 rollout 换 pod → 新 emptyDir → 行号归 0 → 拉到的只有本 rep 的 span →
    覆盖写把前面 11 个 rep 的 span 全抹掉。而 `eval/whowhen/make_whowhen_cases.py` 与
    `eval/content_ctxdrift_track.py` 都是**按 `spans/<combo>.jsonl` + trace_id** 取工具调用
    原始参数的 → format 族 12 个 case 里 11 个拿到空 span,Who&When/A2P 输入静默退化。
    (本机同 combo 的该文件实测 1955 行 / 19.6MB;本机 exporter 是 open(...,"a") 追加,
     跨进程重启天然累积 —— K8S 侧必须自己把这条语义补回来。)
    修法 = `self._span_prefix`(本 combo 已落盘文本)+ 每 rep 结束推进 `handle.span_offset`。
 R2 [致命·必然阻断 resume] arm 校验的 `kubectl logs` 由 `--tail=400` 改 `--tail=-1`。
    "injector armed"/"OBSERVE-ONLY" 只在**解释器启动时**打一次;pod 的 readiness(10s)+
    liveness(20s)探针每分钟产 9 行 werkzeug access log → 400 行 ≈ 44 分钟。
    崩溃后 resume 同一个 combo 时,`set env` 算出的 KV 与 pod 上完全相同 → **不触发 rollout**
    → 沿用已存活数小时的老 pod → tail 400 里全是探针行 → 抛"注入器没挂上"把好 combo 判死,
    且每次 resume 都撞同一堵墙。
 R3 [O(n²) → O(n)] span 拉取改**每 rep 推进 offset**(与 R1 同一处修改)。
    原实现 offset 整个 combo 固定 → 第 N 个 rep 要把前 N 个 rep 的 span 全量再传一遍
    (单 combo 累计经 kubectl exec 传 ~0.5GB,还压着 tail 的 240s 硬超时 ——
     一旦踩线就是 BackendTransientError 作废整个 combo)。
    〔**未做**、已知残留成本〕稳定性判据每轮仍要 3 次 kubectl 调用(_running_pods +
    _pod_line_count + tail),典型 3 轮收敛 ≈ 9 次进程启动 ≈ 5-10s/rep。合并成一条 exec
    能省掉大半,但会削弱 `_pull_since` 的 pod 换身检测,**故意不动** —— 相对每 rep
    30-64s 的 LLM 主体开销,这点开销不值得拿正确性去换。
    台账(sync_ledger)的 offset 仍是整 combo 固定,这是**有意**的:_determine_gt 要在
    combo 内按 trace_id 回查,而台账一行几十字节,不存在体量问题。
 R4 [坏行永不重跑] apiserver 的 5xx 不再当成业务观测。经 kubectl proxy 时,Endpoints 为空/
    pod NotReady 会由 **API server 自己**回 `503 + {"kind":"Status"}`,不是 rec-agent 的响应。
    原实现"HTTPError 一律原样返回不重试" → 写出 note=no_trace_id_INVALID 的坏行 + journal
    → resume 门是 journal-exists → 永不重跑。现按"5xx 且响应体不是 rec-agent 的 JSON"判瞬时。
    (rec-agent 自己的 500 走 workflow.py:469-475,**必带** success/message/trace_id 三个键,
     所以"5xx + kind==Status 或 非 JSON"这个判据不会误伤真实业务 500。)
 R5 [整晚全废] 新增"span 真的在写"闸:`reset_ledger`(其调用点 = warmup 之后、第一个计数 rep
    之前)会断言 pod 内 spans.jsonl 相对基线**有增长**。exporter 落盘失败是静默的
    (local_span_exporter.py:105 `except: return FAILURE`),卷没挂上时每行 CSV 都是
    total_span_count=0 但 injected=1、GT 正确、照写照 journal —— 是唯一能报废一整晚且
    resume 也救不回来的模式。`--warmup 0` 时自动跳过该断言(没探针就没 span,不误报)。
 R6 遗留 `AGENT_FAULT_<Name>` / `AGENT_FAULT_DELAY_MS` 钩子纳入白名单(恒 unset)。
    workflow.py:90/106 真的会读它们(delay/error/garbage),本机 build_env L205-207 是显式
    pop 掉的 —— pod 侧不显式摘就是防护不对称。
 R7 `image_hint` 默认由 `agentfault` 收紧为 `agentfault-v2`:前者是子串匹配,会把 G1 用的
    旧 tag `:agentfault`(无 _filter_real_title、无 PVC)也放行。
 R8 preflight 补三条:Prometheus **可达性**与 cadvisor 序列分开报(原来 Prometheus 进程没起
    时报的是"target down?",把人指向错误方向);cadvisor 判据由 `count(up{...})`(target down
    时序列仍在 → 恒返 1,是**空闸**)改 `sum(up{...}) > 0`;新增 pod 时钟偏移检查
    (wallclock_sanity_ok 比的是 pod 时钟 vs 宿主时钟 ±5s,Docker Desktop VM 休眠后有已知漂移,
     一漂就是整树该列全 0)与 `/agentfault-data` 可写检查。
 R9 `sample_host` 拒绝非有限值:limit 序列缺失时 PromQL 除零得 `+Inf`,而 runner 的 `_r2()`
    只挡了 NaN → 会往 CSV 写字面量 "inf"。
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# 与 runner 同源:injector_smoke 是 probe/health/read_spans/常量 的单一真相源
_HERE = os.path.dirname(os.path.abspath(__file__))                 # .../agentfault/collect
_AGENTFAULT_DIR = os.path.dirname(_HERE)                           # .../agentfault
_INJECTOR_DIR = os.path.join(_AGENTFAULT_DIR, "injector")
if _INJECTOR_DIR not in sys.path:
    sys.path.insert(0, _INJECTOR_DIR)
import injector_smoke as ISM  # noqa: E402

VENV_PY = ISM.VENV_PY
SVC_DIR = ISM.SVC_DIR
NAN = float("nan")

# host 水位(与 runner 原来同一份 try-import;LocalBackend.sample_host 要用)
try:
    import psutil
except Exception:
    psutil = None


class BackendFatalError(RuntimeError):
    """执行环境**结构性**损坏 —— 重试没用,继续跑只会烧钱产废数据,必须硬停整轮。

    目前唯一的抛点是 `K8sBackend.reset_ledger` 的"span 真的在写吗"断言(见文件头 R5)。
    ★为什么要专门一个类而不是复用 RuntimeError:runner 的 `_bring_up` 里那句
      `BACKEND.reset_ledger(...)` 外面**裹着 try/except Exception -> [WARN] 继续**
      (本机语义:清台账失败只是个小事)。裸 RuntimeError 会被那句降级成一行警告,
      断言就白做了。runner 对本类**单独 re-raise**,main 里再硬停整轮(rc=5)。
      LocalBackend 永不抛它 → 本机路径的 try/except 行为逐字不变。
    """


class BackendTransientError(RuntimeError):
    """执行环境**瞬时**故障(kubectl proxy 抖 / pod 被换掉 / emptyDir 被清)。

    ★为什么要专门一个异常类:这类故障若被当成"正常返回"往下走,会产出一条
    `note=no_trace_id_INVALID` 或 `total_span_count=0` 的**坏行 + journal**,而 resume
    门是 journal-exists → 这条坏行**永远不会被重跑**。所以必须在 `append_csv` 之前
    (run_one_rep 内)抛出来,让 run_combo 整个冒泡到 main 的 `except Exception`:
    该 combo 记 ERROR、不写 CSV、不写 journal → 下次 resume 干净重跑。
    """


# ============================================================
# 基类:12 个 seam 的协议 + 一律给 no-op 默认实现
# ============================================================
class Backend(object):
    name = "base"
    needs_phase1_venv = False       # main 里的 VENV_PY FATAL 门是否适用

    def __init__(self, build_env=None):
        # ★build_env 由 runner 注入(依赖倒置),不在本模块重写一份 —— 注入语义必须单一真相源。
        #   K8sBackend 也调**同一个** build_env,只是把它产出的 env 过一遍白名单再喂 kubectl。
        self._build_env = build_env

    # ---- S1/S2/S3/S9:实例生命周期 ----
    def start_instance(self, combo, port, span_file, ledger_file, log_path,
                       subtype=None, field=None):
        raise NotImplementedError

    def stop_instance(self, handle):
        raise NotImplementedError

    def slot_ready(self, port, combo_id=""):
        """采集位置是否可用。返回 (ok: bool, reason: str)。

        ★reason 由**后端自己**拼完整(含 combo_id),不由 runner 再套一层 f-string ——
          这样 LocalBackend 能给出与改造前**逐字节相同**的那句
          `port {port} not free; refuse to start temp instance for {cid}`(回归审查 F2)。
        """
        return True, ""

    def sample_host(self, handle):
        return NAN, NAN

    # ---- S4/S5/S6:探针与 span ----
    def wait_health(self, port, timeout_s=None):
        raise NotImplementedError

    def probe(self, port, seq=None, top_k=None):
        raise NotImplementedError

    def read_spans(self, span_file, trace_id):
        raise NotImplementedError

    # ---- S7/S8:台账 ----
    def reset_ledger(self, ledger_file):
        raise NotImplementedError

    def sync_ledger(self, ledger_file):
        """把远端台账增量同步到本地 `ledger_file`。本机后端无需做事。

        ★这是勘察里"背景漏掉的头号 seam":`_determine_gt()` 读的是**本地** ledger_file
        (agentfault_runner L516/L528/L535),不同步 = 96 个 faulted case 全退化成负类
        (`no_ledger_match`),而且只会在 combo 末尾报一句 `[QC-FAIL] 0 faulted reps`。
        """
        return None

    # ---- 环境前检 & provenance ----
    def preflight(self):
        """返回问题列表(空 = 通过)。"""
        return []

    def extra_csv_columns(self):
        return []

    def extra_row_fields(self):
        return {}

    def summary_meta(self):
        return None


# ============================================================
# LocalBackend —— 原 runner 行为的逐字搬迁(改造前后行为同一)
# ============================================================
class LocalBackend(Backend):
    """本机隔离 harness:phase1 venv python 起一个临时 rec_agent 进程,打 127.0.0.1:<port>。

    ★本类**不新增任何行为**。每个方法上面都标了它搬自 agentfault_runner 的哪一段。
    """
    name = "local"
    needs_phase1_venv = True        # main 的 `if not os.path.exists(VENV_PY): return 2` 只对本机成立

    # ---- S1:搬自 agentfault_runner.start_instance(改造前 L220-226),逐字 ----
    def start_instance(self, combo, port, span_file, ledger_file, log_path,
                       subtype=None, field=None):
        env = self._build_env(combo, port, span_file, ledger_file,
                              subtype=subtype, field=field)
        logf = open(log_path, "w", encoding="utf-8")
        proc = subprocess.Popen([VENV_PY, "app.py"], cwd=SVC_DIR, env=env,
                                stdout=logf, stderr=subprocess.STDOUT)
        proc._logf = logf
        return proc

    # ---- S2:搬自 agentfault_runner.stop_instance(改造前 L229-244),逐字 ----
    def stop_instance(self, proc):
        if proc is None:
            return
        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        except Exception:
            pass
        try:
            if getattr(proc, "_logf", None):
                proc._logf.close()
        except Exception:
            pass

    # ---- S3:搬自 agentfault_runner.port_free(改造前 L247-252),逐字;只是把
    #      "端口空不空"的布尔包成 (ok, reason),好让 K8S 侧给出它自己的失败原因 ----
    def slot_ready(self, port, combo_id=""):
        try:
            with ISM._req(f"http://127.0.0.1:{port}/recommend/health", timeout=2):
                free = False
        except Exception:
            free = True
        if free:
            return True, ""
        # ★这句与改造前 run_combo 里那句 raise 的文本**逐字节相同**(回归审查 F2)
        return False, f"port {port} not free; refuse to start temp instance for {combo_id}"

    # ---- S9:搬自 agentfault_runner.sample_host(改造前 L384-395),逐字 ----
    #   注:host_cpu_pct 在 v2 的 108 行里是恒 0.0 的死列(psutil 首次 cpu_percent 预热 +
    #   0.2s 采样窗对一个刚回完 HTTP 的空闲进程必然量到 0);host_mem_pct 是**宿主全局**
    #   内存百分比(virtual_memory().percent),不是该进程的。这两条都是既有事实,
    #   B 档不改本机口径(改了就不是逐字节等价了)。
    def sample_host(self, proc):
        if psutil is None:
            return NAN, NAN
        try:
            ps = psutil.Process(proc.pid)
            ps.cpu_percent(interval=None)  # 预热
            time.sleep(0.2)
            cpu = ps.cpu_percent(interval=None)
            mem = psutil.virtual_memory().percent
            return cpu, mem
        except Exception:
            return NAN, NAN

    # ---- S4/S5/S6:运行时转调 ISM(见文件头 §依据(2):不可写成 staticmethod 冻死绑定)----
    def wait_health(self, port, timeout_s=None):
        if timeout_s is None:
            return ISM.wait_health(port)
        return ISM.wait_health(port, timeout_s=timeout_s)

    def probe(self, port, seq=None, top_k=None):
        return ISM.probe(port, seq=seq, top_k=top_k)

    def read_spans(self, span_file, trace_id):
        return ISM.read_spans(span_file, trace_id)

    # ---- S7:搬自 _bring_up 里 warmup 后那句台账截断(改造前 L887),逐字 ----
    #   (外层 try/except + [WARN] 打印仍留在 runner 的调用点,异常处理路径也不变)
    def reset_ledger(self, ledger_file):
        open(ledger_file, "w", encoding="utf-8").close()

    # ---- S8 / provenance:本机一律 no-op / 空 ----
    def sync_ledger(self, ledger_file):
        return None

    def preflight(self):
        return []

    def extra_csv_columns(self):
        return []       # ★CSV 表头一个字节都不动

    def extra_row_fields(self):
        return {}

    def summary_meta(self):
        return None     # ★run_summary.json 一个 key 都不加


# ============================================================
# K8sBackend —— 25 微服务全栈里的常驻 rec-agent pod
# ============================================================
class _PodHandle(object):
    """冒充本机的 `proc` 往 runner 里传(runner 只把它当不透明句柄传给
    sample_host / stop_instance,不碰它的属性)。"""

    __slots__ = ("pod", "log_path", "span_offset", "ledger_offset",
                 "combo_id", "logs_dumped")

    def __init__(self, pod, log_path, span_offset, ledger_offset, combo_id):
        self.pod = pod
        self.log_path = log_path
        self.span_offset = span_offset      # 本 combo 起始时 pod 内 spans.jsonl 的行数
        self.ledger_offset = ledger_offset  # warmup 后重置(= 本机"清台账"的等价物)
        self.combo_id = combo_id
        self.logs_dumped = False


class K8sBackend(Backend):
    """K8S 全栈后端。

    与本机后端的**唯一**语义差别都收敛在这个类里:
      - "起实例" = `kubectl set env` 白名单全量 set/unset + rollout + arm 校验
        (env 只能在 pod 创建时写入,这是 rollout 的唯一物理原因 —— 注入器本身所有
         per-combo 旋钮都是 call-time 读 env,见 agentfault_injector L108/L340/L398/L603);
      - "探针" 走 **kubectl proxy :8001 的 service proxy**,不用 port-forward
        (port-forward 绑定具体 pod,rollout 后立刻 `failed to find sandbox` 整个死掉,
         2026-07-27 dprobe 实证);
      - span/台账 从 pod 内 emptyDir 用 `tail -n +N` 增量拉回本地镜像文件,再交给
        **未改动**的 ISM.read_spans / _determine_gt 消费;
      - host 水位改从 Prometheus 取(pod 里没有 psutil 能看的本地进程)。

    ★subprocess 直调 kubectl(不过 shell)—— 天然规避 Git Bash 的 MSYS 路径改写
      (人工敲同样的命令必须带 `MSYS2_ARG_CONV_EXCL='*'`,否则 `/agentfault-data` 会被
       改写成 Windows 路径;G1 曾因此静默丢 10 个 case 的轨迹)。
    """
    name = "k8s"
    needs_phase1_venv = False       # K8S 侧跑的是镜像里的 python,phase1 venv 与它无关

    # pod 内落盘路径(与 patch_recagent_observe.ps1 的 emptyDir 挂载点一致)
    POD_SPAN = "/agentfault-data/spans.jsonl"
    POD_LEDGER = "/agentfault-data/ledger.jsonl"

    # R5 的等待窗:exporter 是 span-END 即时 flush(local_span_exporter.py:58),正常 <1s;
    # 给 30s 是为了容忍节点忙。离线自检会把它调小,别把它当"可调优参数"随手改。
    SPAN_WRITE_GRACE_S = 30

    # ★env 白名单(16 键)。runner 的 build_env 产出的是**整个 os.environ 副本**,
    #   绝不能整表往 pod 上搬 —— 其中至少三样东西会直接把 K8S 侧打坏:
    #     · `env.pop("SASREC_API_URL")` 是本机护栏(强制真实 8200)。K8S 上这个键必须
    #       保留 `http://sasrec:8200`,搬过去把它 unset 掉 = tools.py 回落 127.0.0.1:8200
    #       = pod 内连自己 = 工具全失败,B 档直接变 D 档。
    #     · OTEL_* 指 DEAD_OTLP(14318)是"不污染持久 Jaeger"的本机护栏;K8S 侧要的正好
    #       相反 —— 指真 collector 才算"跑在全栈里"(消 overclaim 的全部目的)。
    #     · PYTHONPATH / RECOMMENDATION_PORT / NACOS_ENABLED 都是宿主路径/本机口径。
    #   所以只取 AGENTFAULT_* + SPAN_FILE 这一小撮**注入语义**键。
    ENV_WHITELIST = (
        ("AGENTFAULT_INSTRUMENT", "AGENTFAULT_INJECT", "AGENTFAULT_OBSERVE")
        + tuple("AGENTFAULT_KIND_" + a for a in ISM.AGENT_NAMES)
        + ("AGENTFAULT_WRONG_ASIN", "AGENTFAULT_DROP_AGENT",
           "AGENTFAULT_FORMAT_SUBTYPE", "AGENTFAULT_FORMAT_FIELD",
           "AGENTFAULT_LEDGER", "SPAN_FILE",
           # 见文件头 D3:build_env 不显式设,靠"shell 里也没设"落到注入器默认值。
           # pod 是长命对象,必须显式 unset 才能保证与本机同口径。
           "AGENTFAULT_SUBLLM_MODEL", "AGENTFAULT_HALLU_MODE", "AGENTFAULT_DEBUG")
        # ★R6:**旧一代**黑盒钩子 AGENT_FAULT_<Name>=delay|error|garbage(注意是 AGENT_FAULT_
        #   不是 AGENTFAULT_)。它们不在注入器里,而是直接写在业务代码 workflow.py L90/L106,
        #   是 agentchaos 那批(内容层埋点之前的黑盒基线)留下的时延/异常/乱码钩子。
        #   本机 build_env L205-207 对它们是显式 pop 的;pod 侧若不摘,一个手工挂过的残留就能
        #   给 agent 加 5s 延迟而 CSV 里毫无标记。build_env 永远不会设它们 → 恒进 unsets。
        + tuple("AGENT_FAULT_" + a for a in ISM.AGENT_NAMES)
        + ("AGENT_FAULT_DELAY_MS",)
    )

    DEFAULT_KUBECTL = r"kubectl"

    def __init__(self, build_env=None, ns="recweb-chaos", deploy="rec-agent",
                 container="rec-agent", service="rec-agent", svc_port=5001,
                 selector="app=recommendation_agent",
                 proxy="http://127.0.0.1:8001", kubectl=None,
                 prom_url="http://localhost:9090", host_metrics="prom",
                 # ★R7:默认必须是 agentfault-v2 不是 agentfault —— 下面是**子串**匹配,
                 #   写 "agentfault" 会把 G1(single_recagent 15)用的旧 tag `:agentfault`
                 #   一起放行,而那个镜像没有 _filter_real_title、也没挂 PVC,口径与本机 v2 不同。
                 image_hint="agentfault-v2", rollout_timeout=300,
                 probe_timeout=300, health_timeout=300,
                 allow_inject_residue=False, skip_code_parity=False):
        Backend.__init__(self, build_env=build_env)
        self.ns = ns
        self.deploy = deploy
        self.container = container
        self.service = service
        self.svc_port = svc_port
        self.selector = selector
        self.proxy = proxy.rstrip("/")
        self.kubectl = kubectl or os.environ.get("KUBECTL") or self.DEFAULT_KUBECTL
        self.prom_url = prom_url.rstrip("/")
        self.host_metrics = host_metrics            # prom | none
        self.image_hint = image_hint
        self.rollout_timeout = rollout_timeout
        # ★probe 余量给到 300s:v2 实测 e2e max 63.9s,dprobe 48 case / 288 次 proxy 探针
        #   288/288 HTTP 200(最长 75.8s),API server 长请求不是瓶颈;但 K8S 侧多一次
        #   rollout 后的 title-cache 冷加载(实测 +3~8s),余量放宽不吃亏。
        #   注:这是本类自己的常量,**不改 ISM.PROBE_TIMEOUT_S**(那是本机口径)。
        self.probe_timeout = probe_timeout
        self.health_timeout = health_timeout
        self.allow_inject_residue = allow_inject_residue
        self.skip_code_parity = skip_code_parity

        self._cur = None                            # 当前 _PodHandle
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self._last_restarts = ""                    # 供 extra_row_fields 用
        # ★R1:本 combo 已落盘的 span 文本(不含本 rep 的增量)。per-rep 模式下每 rep 都换 pod、
        #   行号归 0,只有靠它把前面 rep 的 span 续在前面,盘上的 spans/<cid>.jsonl 才与本机
        #   采集树同形(本机 exporter 是 open(...,"a"),天然累积)。
        self._span_prefix = ""
        # ★R5:自 start_instance 起本后端发过几次探针 —— reset_ledger 用它判断"该不该断言
        #   spans.jsonl 已有增长"(--warmup 0 时一次探针都没有,断言会误报)。
        self._probes_since_start = 0

    # ------------------------------------------------------------------
    # kubectl / HTTP 原语
    # ------------------------------------------------------------------
    def _kc(self, args, timeout=120, stdin=None):
        """subprocess 直调 kubectl(★不过 shell:规避 MSYS 路径改写)。返回 (rc, out, err)。"""
        try:
            p = subprocess.run([self.kubectl] + args, input=stdin,
                               capture_output=True, text=True,
                               encoding="utf-8", errors="ignore", timeout=timeout)
        except Exception as e:
            return 127, "", f"{type(e).__name__}: {e}"
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()

    def _recommend_url(self, path=""):
        return (f"{self.proxy}/api/v1/namespaces/{self.ns}/services/"
                f"{self.service}:{self.svc_port}/proxy/recommend{path}")

    def _http(self, url, method="GET", body=None, timeout=30):
        data = body.encode("utf-8") if isinstance(body, str) else body
        r = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            r.add_header("Content-Type", "application/json")
        return self._opener.open(r, timeout=timeout)

    def _running_pods(self):
        rc, so, _ = self._kc(["get", "pods", "-n", self.ns, "-l", self.selector,
                              "--field-selector=status.phase=Running",
                              "-o", "jsonpath={.items[*].metadata.name}"], timeout=60)
        if rc != 0:
            return []
        return [p for p in so.split() if p.strip()]

    def _pod_line_count(self, pod, path):
        """pod 内文件行数;文件不存在返回 0(emptyDir 刚挂上时就是这种)。"""
        rc, so, _ = self._kc(["exec", "-n", self.ns, pod, "-c", self.container, "--",
                              "sh", "-c", f"[ -f {path} ] && wc -l < {path} || echo 0"],
                             timeout=90)
        try:
            return int(so.strip().splitlines()[-1])
        except Exception:
            return 0

    def _pull_since(self, handle, path, offset, what):
        """把 pod 内 `path` 从第 offset+1 行起的增量拉回来(文本)。

        ★同时做**pod 换身检测**:pod 名变了 或 当前行数 < offset,都意味着
        emptyDir 被清过(pod 重建)→ 本 rep 的轨迹已经永久丢失,必须抛
        BackendTransientError 让这条 rep 不落 CSV/journal(否则会写出一条
        total_span_count=0 的坏行,且 resume 永不重试)。
        """
        pod = None
        pods = self._running_pods()
        if len(pods) == 1:
            pod = pods[0]
        elif pods:
            raise BackendTransientError(
                f"{len(pods)} 个 Running rec-agent pod({pods}) —— rollout 未收敛/双 pod,"
                f"探针可能打到旧配置 pod;本 rep 作废等 resume 重跑")
        if not pod:
            raise BackendTransientError("找不到 Running 的 rec-agent pod(pod 正在重建?)")
        if pod != handle.pod:
            raise BackendTransientError(
                f"pod 在采集中途被换掉({handle.pod} -> {pod}):emptyDir 已清空,"
                f"本 rep 的 {what} 永久丢失,作废等 resume 重跑")
        cur = self._pod_line_count(pod, path)
        if cur < offset:
            raise BackendTransientError(
                f"{path} 行数 {cur} < 基线 {offset}:卷被清过(容器重建),{what} 已丢失")
        rc, so, se = self._kc(["exec", "-n", self.ns, pod, "-c", self.container, "--",
                               "sh", "-c", f"tail -n +{offset + 1} {path} 2>/dev/null || true"],
                              timeout=240)
        if rc != 0:
            raise BackendTransientError(f"拉取 {what} 失败(rc={rc}): {se[:200]}")
        return so

    @staticmethod
    def _write_local(path, text):
        """把**完整**的本地镜像内容写进 `path`(整文件覆盖)。

        ★调用方负责把 "已落盘前缀 + 本次增量" 拼好再传进来(见 R1 / read_spans)。
          这里之所以是覆盖而不是追加:read_spans 的稳定性判据要在同一个 rep 内反复
          重写同一段内容(连续两轮条数不变才算写完),追加会写出重复行。
        """
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
            if text and not text.endswith("\n"):
                f.write("\n")

    @staticmethod
    def _read_local(path):
        """读本地镜像文件;不存在返 ""。行尾统一补 \\n(拼接前缀时不能粘行)。"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                t = f.read()
        except Exception:
            return ""
        if t and not t.endswith("\n"):
            t += "\n"
        return t

    @staticmethod
    def _as_lines_text(text):
        """把 `_kc` 返回的(已 strip 的)多行文本规范成 "每行都以 \\n 结尾" 的形式,
        并返回 (规范文本, 行数)。行数用于推进 pod 侧行号基线 —— 必须与写进本地镜像的
        行数**严格一致**,否则下个 rep 会重复拉或漏拉(重复拉 = 同一条 span 在本地文件里
        出现两次 = aggregate_agent_spans 双计)。

        注:pod 内的 spans.jsonl 由 local_span_exporter.py:103 写
        `"\\n".join(lines) + "\\n"`(整批加锁一次写),所以不会有半行;`wc -l` 与
        splitlines() 计数一致。
        """
        lines = [l for l in text.splitlines()]
        if not lines:
            return "", 0
        return "".join(l + "\n" for l in lines), len(lines)

    def _dump_logs(self, handle):
        """把 pod 的 stderr/stdout 转存到 combo 的 .serverlog。

        必须在 rollout **之前**做:注入器的 `_err()` 全走 stderr(agentfault_injector
        里 30+ 处),是"为什么这个 rep 没注进去"的唯一线索;rollout 一换 pod 就没了。
        """
        if handle is None or handle.logs_dumped:
            return
        rc, so, _ = self._kc(["logs", "-n", self.ns, handle.pod, "-c", self.container,
                              "--tail=-1"], timeout=180)
        try:
            with open(handle.log_path, "a", encoding="utf-8") as f:
                f.write(f"\n===== kubectl logs {handle.pod} (combo {handle.combo_id}) "
                        f"rc={rc} @ {time.strftime('%Y-%m-%dT%H:%M:%S')} =====\n")
                f.write(so + "\n")
        except Exception:
            pass
        handle.logs_dumped = True

    # ------------------------------------------------------------------
    # S1:起实例 = set env + rollout + arm 校验
    # ------------------------------------------------------------------
    def env_ops(self, combo, port, subtype=None, field=None):
        """把 runner 的 build_env 产出过一遍白名单,算出 kubectl set env 的参数。

        单独成方法是为了能**离线自检**(tests_dev/test_backend_parity.py §C 不碰 kubectl
        就能验"本机专属 env 一个都没外泄到 pod 上")。返回 (sets, unsets, env)。
        """
        env = self._build_env(combo, port, self.POD_SPAN, self.POD_LEDGER,
                              subtype=subtype, field=field)
        sets, unsets = [], []
        for k in self.ENV_WHITELIST:
            v = env.get(k)
            if v is None:
                unsets.append(k + "-")      # kubectl set env 的 unset 语法
            else:
                sets.append(f"{k}={v}")
        return sets, unsets, env

    def start_instance(self, combo, port, span_file, ledger_file, log_path,
                       subtype=None, field=None):
        # 0) 幂等兜底:如果上一把 stop_instance 没跑到(异常路径),先抢救旧 pod 日志
        self._dump_logs(self._cur)
        self._cur = None

        # 1) 复用 runner 的 build_env(单一真相源),只是把 span/ledger 换成 pod 内路径
        sets, unsets, env = self.env_ops(combo, port, subtype=subtype, field=field)

        # 2) 一次 set env = 一次 rollout(白名单全量下发,不做增量 diff:
        #    残留 env 是 K8S 侧最阴的坑,宁可每次全量覆盖)
        rc, so, se = self._kc(["set", "env", f"deploy/{self.deploy}", "-n", self.ns,
                               "-c", self.container] + sets + unsets, timeout=180)
        if rc != 0:
            raise RuntimeError(f"kubectl set env 失败(rc={rc}): {se[:400]}")

        rc, so, se = self._kc(["rollout", "status", f"deploy/{self.deploy}", "-n", self.ns,
                               f"--timeout={self.rollout_timeout}s"],
                              timeout=self.rollout_timeout + 60)
        if rc != 0:
            raise RuntimeError(
                f"rollout 未收敛(rc={rc}): {se[:400]}  "
                f"-> kubectl describe pod -l {self.selector} -n {self.ns}")

        # 3) 单 pod 校验。deploy 若还是 RollingUpdate(maxSurge 25% + replicas 1 -> 向上
        #    取整 = 1),新 pod Ready 前旧 pod 仍在 Endpoints 里,经 Service proxy 的探针
        #    可能打到**旧配置 pod**,而且 span 会写进旧 pod 的 emptyDir 被丢。
        #    正解是把 strategy 改 Recreate(patch_recagent_collect 已做);这里再兜一道。
        pods = self._running_pods()
        if len(pods) != 1:
            raise RuntimeError(
                f"期望恰好 1 个 Running rec-agent pod,实得 {len(pods)}: {pods}  "
                f"-> deploy 的 strategy 应为 Recreate(见 patch 脚本 -Strategy)")
        pod = pods[0]

        # 4) arm 校验(确定性,替代 rep-1 fail-fast):逐键比对 printenv
        rc, so, se = self._kc(["exec", "-n", self.ns, pod, "-c", self.container,
                               "--", "printenv"], timeout=90)
        if rc != 0:
            raise RuntimeError(f"printenv 失败(rc={rc}): {se[:200]}")
        podenv = {}
        for line in so.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                podenv[k] = v
        bad = []
        for k in self.ENV_WHITELIST:
            want = env.get(k)
            got = podenv.get(k)
            if want is None:
                if got not in (None, ""):
                    bad.append(f"{k} 应 unset 实得 {got!r}")
            elif got != want:
                bad.append(f"{k} 应 {want!r} 实得 {got!r}")
        if bad:
            raise RuntimeError("pod env 与 combo 不符(装错 env,拒绝开采): " + "; ".join(bad))

        # 5) loader 装载日志确认(注入器/观察器真的挂上了)
        # ★R2:必须 --tail=-1(全量)不能 --tail=N。"injector armed"/"OBSERVE-ONLY" 只在
        #   **解释器启动时**打一次(sitecustomize.py:44/47、agentfault_injector.py:783/849),
        #   而 pod 的 readiness(10s)+liveness(20s)探针每分钟产 9 行 werkzeug access log ——
        #   400 行只覆盖最近 ~44 分钟。崩溃后 resume 同一 combo 时 set env 算出的 KV 与 pod
        #   上完全相同 → 不触发 rollout → 沿用已存活数小时的老 pod → 尾部全是探针行 →
        #   抛"注入器没挂上"把装得好好的 combo 判死,且每次 resume 都撞同一堵墙。
        rc, logs, _ = self._kc(["logs", "-n", self.ns, pod, "-c", self.container,
                                "--tail=-1"], timeout=180)
        loader = "\n".join(l for l in logs.splitlines() if "agentfault-loader" in l
                           or "agentfault injector armed" in l or "OBSERVE-ONLY" in l)
        if combo["faulted"]:
            if "injector armed" not in logs:
                raise RuntimeError(
                    "loader 日志里没有 'agentfault injector armed' —— 注入器没挂上,"
                    f"拒绝开采(装载日志: {loader[-600:]!r})")
        else:
            if "OBSERVE-ONLY" not in logs:
                raise RuntimeError(
                    "loader 日志里没有 'OBSERVE-ONLY' —— normal 臂的观察器没挂上,"
                    f"拒绝开采(装载日志: {loader[-600:]!r})")
            if "AGENTFAULT_OBSERVE ignored" in logs:
                raise RuntimeError(
                    "loader 报 'AGENTFAULT_OBSERVE ignored'(= INJECT 残留把 observer 顶掉),"
                    "normal 臂口径已污染,拒绝开采")
        if "armed openinference" not in logs and "already instrumented" not in logs:
            raise RuntimeError(
                "loader 没 arm openinference —— 内容层 span 会全空,拒绝开采")

        # 6) 记 span/台账行号基线。★不能假设 rollout 后一定是 0:若 set env 前后 env
        #    完全相同,kubectl 不会触发新 rollout,pod 是老的、文件里还有上个 combo 的行。
        span_off = self._pod_line_count(pod, self.POD_SPAN)
        ledger_off = self._pod_line_count(pod, self.POD_LEDGER)
        handle = _PodHandle(pod, log_path, span_off, ledger_off, combo["id"])
        self._cur = handle
        # ★R1:接上本 combo 已落盘的 span(per-rep 模式下这里是前 i-1 个 rep 的内容)。
        #   run_combo 在 combo 开头会 os.remove(span_file),所以 rep1 读到的是 ""。
        self._span_prefix = self._read_local(span_file)
        self._probes_since_start = 0                # R5:新实例,探针计数归零
        print(f"  [k8s] pod={pod} span_off={span_off} ledger_off={ledger_off} "
              f"set={len(sets)} unset={len(unsets)} "
              f"local_span_prefix={self._span_prefix.count(chr(10))}行", flush=True)
        return handle

    # ------------------------------------------------------------------
    # S2:停实例 = 只转存日志,**不还原镜像/不摘 env**
    # ------------------------------------------------------------------
    def stop_instance(self, handle):
        """K8S 侧的 pod 是常驻的,"停"只意味着"这个 combo 的窗关了"。

        ★绝不在这里 restore:emptyDir 随 pod 重建即清空,还没拉走的轨迹会永久丢。
          整轮采完由人手跑 restore_recagent_stock.ps1 -ConfirmedCollected。
        """
        if handle is None:
            return
        self._dump_logs(handle)
        if self._cur is handle:
            self._cur = None

    # ------------------------------------------------------------------
    # S3:位置可用性(语义与本机反转:服务必须**在**)
    # ------------------------------------------------------------------
    def slot_ready(self, port, combo_id=""):
        tag = f"(combo {combo_id})" if combo_id else ""
        pods = self._running_pods()
        if len(pods) != 1:
            return False, f"Running rec-agent pod 数 = {len(pods)}(期望 1): {pods} {tag}"
        rc, img, _ = self._kc(["get", "deploy", self.deploy, "-n", self.ns, "-o",
                               "jsonpath={.spec.template.spec.containers[0].image}"],
                              timeout=60)
        if rc != 0:
            return False, f"读不到 deploy/{self.deploy} 的 image(rc={rc})"
        if self.image_hint and self.image_hint not in img:
            return False, (f"rec-agent 镜像 {img!r} 不含 {self.image_hint!r} —— "
                           f"先跑 patch_recagent_collect.ps1 切变体镜像")
        # B 档 = agent-only。ns 里任何遗留 Chaos CRD 都会把它污染成跨层的 C/D 档。
        rc, so, _ = self._kc(["get", "podchaos,networkchaos,stresschaos", "-n", self.ns,
                              "--no-headers", "--ignore-not-found"], timeout=60)
        if rc == 0 and so.strip():
            return False, ("ns 里有遗留 Chaos CRD(B 档必须 agent-only):\n" + so[:400])
        if not self.wait_health(port, timeout_s=60):
            return False, f"经 proxy 探 {self._recommend_url('/health')} 不健康"
        return True, ""

    # ------------------------------------------------------------------
    # S4/S5:探针(走 kubectl proxy 的 service proxy)
    # ------------------------------------------------------------------
    def wait_health(self, port, timeout_s=None):
        url = self._recommend_url("/health")
        deadline = time.time() + (timeout_s if timeout_s is not None else self.health_timeout)
        while time.time() < deadline:
            try:
                with self._http(url, timeout=15) as r:
                    if r.status == 200:
                        return True
            except Exception:
                pass
            time.sleep(2.0)
        return False

    def probe(self, port, seq=None, top_k=None):
        """与 ISM.probe 同返回契约 (status, e2e_ms, json)。

        ★与本机的唯一差别:driver 层失败(status=-1,多半是 kubectl proxy 抖)会**退避重试**,
          重试耗尽则抛 BackendTransientError。为什么不能像本机那样把 -1 原样返回:
          -1 → resp 里没有 trace_id → run_one_rep 重试 2 次仍空 → 写出一条
          `note=no_trace_id_INVALID` 的坏行 + journal → resume 门永不重跑,一路把整个
          combo 烧完(而且是花了钱的)。抛异常则该 rep 不落任何盘,下次 resume 干净重跑。
        """
        url = self._recommend_url()
        item_sequence = seq if seq else ISM.PROBE_SEQ
        tk = top_k if top_k is not None else ISM.PROBE_TOPK
        body = json.dumps({"item_sequence": item_sequence, "top_k": tk})
        last_err = ""
        self._probes_since_start += 1               # R5:给 reset_ledger 的断言用
        for attempt in range(3):
            t0 = time.time()
            try:
                with self._http(url, method="POST", body=body,
                                timeout=self.probe_timeout) as r:
                    raw = r.read().decode("utf-8", "ignore")
                    e2e = (time.time() - t0) * 1000.0
                    try:
                        j = json.loads(raw)
                    except Exception:
                        j = None
                    return r.status, e2e, j
            except urllib.error.HTTPError as e:
                e2e = (time.time() - t0) * 1000.0
                raw = ""
                try:
                    raw = e.read().decode("utf-8", "ignore")
                except Exception:
                    pass
                try:
                    j = json.loads(raw)
                except Exception:
                    j = None
                # ★R4:分清"rec-agent 自己的 5xx"(= 真实业务观测,必须原样入表)与
                #   "API server / service proxy 的 5xx"(= 执行环境瞬时故障,绝不能入表)。
                #   判据:rec-agent 的 500 走 workflow.py:469-475,**必带** success/message/
                #   trace_id 三个键的 JSON;而 Endpoints 为空 / pod NotReady 时 apiserver 回的是
                #   `503 + {"kind":"Status", ...}`(或干脆非 JSON 文本)。
                #   不区分的后果:写出 note=no_trace_id_INVALID 的坏行 + journal,而 resume 门是
                #   journal-exists → 这条花了钱的坏行**永远不会被重跑**。
                if e.code >= 500 and self._is_infra_5xx(j, raw):
                    last_err = (f"apiserver/service-proxy {e.code}: "
                                f"{(raw or '')[:200]!r}")
                    if attempt < 2:
                        print(f"    [k8s] 基础设施 {e.code}(不是 rec-agent 的响应,第 "
                              f"{attempt + 1} 次): {last_err} -> 退避重试", flush=True)
                        time.sleep(8 * (attempt + 1))
                        continue
                    break
                # 业务层非 200(含 rec-agent 自己的 500)= 真实观测,原样返回不重试(与本机一致)
                return e.code, e2e, j
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                if attempt < 2:
                    print(f"    [k8s] probe driver error(第 {attempt + 1} 次): "
                          f"{last_err} -> 退避重试", flush=True)
                    time.sleep(8 * (attempt + 1))
        raise BackendTransientError(
            f"probe 经 {url} 连续 3 次驱动层失败: {last_err}  "
            f"-> 检查 `kubectl proxy --port=8001` 是否还活着 / pod 是否在 CrashLoop")

    @staticmethod
    def _is_infra_5xx(parsed, raw):
        """这个 5xx 是不是"基础设施回的"(而非 rec-agent 自己回的)?见 R4。"""
        if isinstance(parsed, dict):
            if parsed.get("kind") == "Status":          # apiserver 的 Status 对象
                return True
            # rec-agent 的错误响应必有这三个键之一;都没有 = 不是它回的
            return not any(k in parsed for k in ("trace_id", "success", "message"))
        # 非 JSON(nginx/apiserver 的纯文本或 HTML)—— rec-agent 一律 jsonify,不会走到这
        return True

    # ------------------------------------------------------------------
    # S6:span 回收(pod emptyDir -> 本地镜像 -> 未改动的解析逻辑)
    # ------------------------------------------------------------------
    def read_spans(self, span_file, trace_id):
        """拉本 rep 的增量,**续写**在本地 `spans/<cid>.jsonl` 已有内容之后,再按 trace_id 过滤。

        稳定性判据抄 ISM.read_spans(连续两轮条数不变且 >0 即认为写完),只是每轮都
        重新从 pod 拉一次(本地文件不会自己长)。

        ★R1/R3 —— 本方法有两条**必须一起成立**的不变量,改动前先读文件头 R1:
          (a) 本地文件 = `self._span_prefix`(本 combo 前面 rep 的全部 span)+ 本 rep 增量。
              per-rep 模式(format 族)每 rep 都 rollout 换 pod、pod 内行号归 0,若像最初那样
              直接覆盖写,12 个 rep 的 span 文件最后只剩第 12 个 —— 而 whowhen/A2P/内容轨
              三套 eval 都按 `spans/<combo>.jsonl` + trace_id 取数,会**静默**退化。
          (b) 收尾把 `handle.span_offset` 推进到本 rep 末尾,下个 rep 只拉自己的增量。
              不推进 = 第 N 个 rep 要把前 N 个 rep 的 span 全量再传一遍(O(n²),单 combo
              累计经 kubectl exec 传 ~0.5GB,还压着 tail 的 240s 硬超时)。
        """
        handle = self._cur
        if handle is None:
            return []
        prefix = self._span_prefix
        last, stable = [], 0
        final_text, final_n = "", 0

        def _finish(result):
            # 增量并进前缀 + 推进 pod 侧行号基线(两者必须同一份 final_text,见不变量 (b))
            self._span_prefix = prefix + final_text
            handle.span_offset += final_n
            return result

        for _ in range(8):
            raw = self._pull_since(handle, self.POD_SPAN, handle.span_offset, "agent span")
            final_text, final_n = self._as_lines_text(raw)
            self._write_local(span_file, prefix + final_text)
            spans = []
            for line in final_text.splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    rec = json.loads(s)
                except Exception:
                    continue
                if rec.get("trace_id") == trace_id:
                    spans.append(rec)
            if len(spans) == len(last) and len(spans) > 0:
                stable += 1
                if stable >= 2:
                    return _finish(spans)
            else:
                stable = 0
            last = spans
            time.sleep(0.8)
        return _finish(last)

    # ------------------------------------------------------------------
    # S7/S8:台账
    # ------------------------------------------------------------------
    def reset_ledger(self, ledger_file):
        """本机是把台账文件清空;K8S 侧不能动 pod 里的文件(别的 rep 还要用它的行号),
        改为**把行号基线抬到当前值** —— 效果等价:warmup 期间落的注入记录(尤其空
        trace_id 的)不会进入后续任何一个 rep 的 GT 判定。同时清空本地镜像。

        ★R5:这里顺带做"span 真的在写"的硬闸。为什么挂在这个方法上 —— 它在 runner 里的
          调用点(run_combo._bring_up 末尾)恰好是"warmup 探针已跑完、第一个计数 rep 还没开始"
          的**唯一**时刻,是花钱之前最后一次能确认 `/agentfault-data` 真的通的机会。
          不查的后果:exporter 落盘失败是静默的(local_span_exporter.py:105
          `except: return SpanExportResult.FAILURE`),卷没挂上时每行 CSV 都是
          total_span_count=0 / span_*_present=0,但 injected=1、GT 正确、照写 CSV 照写 journal
          → 一整晚 108 行全废,而且 resume 门是 journal-exists,**重跑也救不回来**。
          `--warmup 0` 时一次探针都没发过,不做该断言(否则必然误报)。
        """
        handle = self._cur
        if handle is None:
            self._write_local(ledger_file, "")
            return
        if self._probes_since_start > 0:
            # 先分清"卷没挂"(结构性,硬停)和"pod 中途被换掉"(瞬时,作废本 combo 等 resume)——
            # 后者也会表现成"行数没涨"(新 emptyDir 是空的),但硬停整轮就过头了。
            pods = self._running_pods()
            if len(pods) != 1 or pods[0] != handle.pod:
                raise BackendTransientError(
                    f"warmup 期间 pod 变了({handle.pod} -> {pods}):本 combo 的窗已经不干净,"
                    f"作废等 resume 重跑")
            deadline = time.time() + self.SPAN_WRITE_GRACE_S
            n = self._pod_line_count(handle.pod, self.POD_SPAN)
            while n <= handle.span_offset and time.time() < deadline:
                time.sleep(3.0)
                n = self._pod_line_count(handle.pod, self.POD_SPAN)
            if n <= handle.span_offset:
                # ★必须是 BackendFatalError:普通异常会被 runner 的
                #   `except Exception -> [WARN] ledger truncate failed` 吞成一行警告。
                raise BackendFatalError(
                    f"warmup 跑了 {self._probes_since_start} 次探针,但 pod 内 "
                    f"{self.POD_SPAN} 行数仍是 {n}(基线 {handle.span_offset})—— "
                    f"内容层 span 根本没落盘,继续采只会得到 108 行 total_span_count=0 的废数据。"
                    f"查:kubectl exec -n {self.ns} {handle.pod} -c {self.container} -- "
                    f"sh -c 'ls -la /agentfault-data'(卷没挂?)以及 kubectl logs 里的 "
                    f"'[agentfault-loader] armed openinference'")
        handle.ledger_offset = self._pod_line_count(handle.pod, self.POD_LEDGER)
        self._write_local(ledger_file, "")

    def sync_ledger(self, ledger_file):
        """★关键钩子:把 pod 的 /agentfault-data/ledger.jsonl 增量落到本地 ledger_file,
        使**未改动的** `_determine_gt()` 能读到台账。不做 = 96 个 faulted case 全变负类。"""
        handle = self._cur
        if handle is None:
            return
        text = self._pull_since(handle, self.POD_LEDGER, handle.ledger_offset, "注入台账")
        self._write_local(ledger_file, text)

    # ------------------------------------------------------------------
    # S9:host 水位 —— 改从 Prometheus 取(口径变更,已在 CSV 里标出)
    # ------------------------------------------------------------------
    def _prom_instant(self, expr):
        """Prometheus 即时查询。取不到/无序列/非有限值一律返 None(见 R9)。"""
        url = f"{self.prom_url}/api/v1/query?query={urllib.parse.quote(expr)}"
        try:
            with self._opener.open(url, timeout=20) as r:
                j = json.loads(r.read().decode("utf-8", "ignore"))
        except Exception:
            return None
        res = ((j.get("data") or {}).get("result") or [])
        if not res:
            return None
        try:
            v = float(res[0]["value"][1])
        except Exception:
            return None
        # ★R9:limit 序列缺失时 mem 表达式会除零得 +Inf;PromQL 也可能回 NaN。
        #   runner 的 _r2() 只挡了 NaN → +Inf 会被写成字面量 "inf" 塞进 CSV 给下游 eval。
        if v != v or v in (float("inf"), float("-inf")):
            return None
        return v

    def _prom_up(self):
        """Prometheus 本身活着吗(与"某个 target 是否 up"是两件事,见 R8)。"""
        try:
            with self._opener.open(f"{self.prom_url}/-/ready", timeout=10) as r:
                return r.status == 200
        except Exception:
            return False

    def sample_host(self, handle):
        """pod 里没有本机进程可以让 psutil 看,只能走 cadvisor。

        口径(与本机**不同**,故 CSV 里加 `host_metric_source` 标出):
          host_cpu_pct = 该 pod 容器 CPU 用量(core/s)× 100 = "占一个核的百分比"
                         (psutil.Process.cpu_percent 也是这个量纲,可粗比)
          host_mem_pct = working_set / memory limit × 100 = "占 limit 的百分比"
                         (本机那一列是 **宿主全局** virtual_memory().percent,不同物,勿混比)
        ★绝不返回 NaN:`_r2(NaN)` 会往 CSV 写字面量 "nan" 字符串,给下游 eval 塞脏值。
          取不到就返 None → `_r2(None)` → 空串。
        """
        if self.host_metrics != "prom" or handle is None:
            return None, None
        pod = handle.pod
        cpu = self._prom_instant(
            f'sum(rate(container_cpu_usage_seconds_total{{namespace="{self.ns}",'
            f'pod="{pod}",container="{self.container}"}}[1m])) * 100')
        mem = self._prom_instant(
            f'sum(container_memory_working_set_bytes{{namespace="{self.ns}",'
            f'pod="{pod}",container="{self.container}"}}) / '
            f'sum(container_spec_memory_limit_bytes{{namespace="{self.ns}",'
            f'pod="{pod}",container="{self.container}"}}) * 100')
        return cpu, mem

    # ------------------------------------------------------------------
    # provenance:CSV 追加列(★只在 K8S 树出现,local 树一列不多)
    # ------------------------------------------------------------------
    def extra_csv_columns(self):
        """追加在 COLS **末尾**的 4 列。

        为什么必须有:硬要求"CSV schema 不许变;若 K8S 下某列语义不同必须在 CSV 里
        可区分"。`host_cpu_pct/host_mem_pct` 两列在两个后端下语义不同(见 sample_host),
        所以每一行都自带 `collect_backend` + `host_metric_source` 两个口径标签。
        另两列是排障用的 provenance(pod 换身/OOM 重启会解释异常行)。

        下游安全性:四套 eval 与 make_agentchaos_features 都是**按列名取列**、忽略额外列
        (runner csv_columns() 的注释已写明这条),所以尾部加列不破坏任何消费方。
        local 后端返回 [] → 表头字节完全不变 → agentfault_v2 仍可逐字节复现。
        """
        return ["collect_backend", "host_metric_source", "k8s_pod_name", "k8s_pod_restarts"]

    def extra_row_fields(self):
        pod = self._cur.pod if self._cur else ""
        restarts = ""
        if pod:
            rc, so, _ = self._kc(
                ["get", "pod", pod, "-n", self.ns, "-o",
                 "jsonpath={.status.containerStatuses[0].restartCount}"], timeout=45)
            if rc == 0 and so.strip().isdigit():
                restarts = int(so.strip())
        return {
            "collect_backend": "k8s",
            # 容器 CPU 占单核百分比 / 内存占 limit 百分比 —— 与 local 的
            # (进程 cpu_percent / 宿主全局内存%)不是同一个量,跨树比这两列前先读这一列。
            "host_metric_source": ("prom_container" if self.host_metrics == "prom" else "none"),
            "k8s_pod_name": pod,
            "k8s_pod_restarts": restarts,
        }

    def summary_meta(self):
        rc, img, _ = self._kc(["get", "deploy", self.deploy, "-n", self.ns, "-o",
                               "jsonpath={.spec.template.spec.containers[0].image}"],
                              timeout=60)
        rc2, sha, _ = self._kc(["exec", "-n", self.ns, f"deploy/{self.deploy}",
                                "-c", self.container, "--",
                                "printenv", "RECWEB_SRC_GIT_SHA"], timeout=60)
        return {
            "backend": "k8s",
            "namespace": self.ns,
            "deployment": self.deploy,
            "image": img if rc == 0 else "?",
            "src_git_sha": sha.strip() if rc2 == 0 else "",
            "probe_path": f"kubectl proxy -> svc/{self.service}:{self.svc_port} (不用 port-forward)",
            "prometheus": self.prom_url,
            "host_metric_source": ("prom_container" if self.host_metrics == "prom" else "none"),
            "otlp": "pod 沿用 stock OTEL_EXPORTER_OTLP_ENDPOINT(真 collector),"
                    "与 traditional 255 同一条链路;两条线靠时间窗隔离,不改 service.name",
        }

    # ------------------------------------------------------------------
    # preflight
    # ------------------------------------------------------------------
    def preflight(self):
        bad = []
        rc, _, se = self._kc(["get", "nodes"], timeout=60)
        if rc != 0:
            bad.append(f"K8S API 不可达({se[:160]}) —— kubectl={self.kubectl}")
            return bad          # 后面全依赖 kubectl,直接短路

        # kubectl proxy:探针唯一通道,同时 Prometheus 的 cadvisor/kube-state 两个 target
        # 也经 host.docker.internal:8001 抓 —— proxy 不起则 metrics 全空
        try:
            self._opener.open(f"{self.proxy}/api/v1/namespaces/{self.ns}/pods?limit=1",
                              timeout=10).read()
        except Exception as e:
            bad.append(f"kubectl proxy {self.proxy} 不通({e}) -> "
                       f"kubectl proxy --port=8001 --address=0.0.0.0 --accept-hosts='.*'")

        if self.host_metrics == "prom":
            # ★R8 / 复现审查③:Prometheus **进程没起** 与 **cadvisor target down** 是两件事,
            #   分开报 —— 原来合成一条"target down?",会把"你压根没起 OTel 栈"的人指向查 target。
            if not self._prom_up():
                bad.append(
                    f"Prometheus {self.prom_url} 不可达 —— host_cpu_pct/host_mem_pct 会全空。"
                    f"起它:docker compose -f ops/docker-compose.otel.yml up -d"
                    f"(或 python start_all.py);确实不要 host 水位则加 --k8s-host-metrics none")
            # ★原判据 `count(up{job="cadvisor"})` 是**空闸**:target down 时 up 序列仍在、
            #   值为 0,count() 恒返 1(实测)。必须用 sum(up)>0。
            elif (self._prom_instant('sum(up{job="cadvisor"})') or 0) <= 0:
                bad.append("Prometheus 的 cadvisor target 是 down(sum(up)=0)-> "
                           "host_cpu_pct/host_mem_pct 会全空。cadvisor/kube-state-metrics 两个 "
                           "target 都经 host.docker.internal:8001 抓,先确认 kubectl proxy 常驻 "
                           "且 12-kube-state-metrics.yaml 已 apply;或改 --k8s-host-metrics none")

        pods = self._running_pods()
        if len(pods) != 1:
            bad.append(f"Running rec-agent pod 数 = {len(pods)}(期望 1): {pods}")
            return bad
        pod = pods[0]

        rc, img, _ = self._kc(["get", "deploy", self.deploy, "-n", self.ns, "-o",
                               "jsonpath={.spec.template.spec.containers[0].image}"],
                              timeout=60)
        if self.image_hint and self.image_hint not in (img or ""):
            bad.append(f"rec-agent 镜像 {img!r} 不含 {self.image_hint!r} -> "
                       f"先跑 k8s/patch_recagent_collect.ps1")

        rc, strat, _ = self._kc(["get", "deploy", self.deploy, "-n", self.ns, "-o",
                                 "jsonpath={.spec.strategy.type}"], timeout=60)
        if (strat or "").strip() != "Recreate":
            bad.append(f"deploy strategy={strat!r} 非 Recreate:RollingUpdate 下 replicas=1 的"
                       f" maxSurge 向上取整=1,新旧 pod 会同时在 Endpoints 里,探针可能打到"
                       f"旧配置 pod 且 span 写进旧 emptyDir 丢掉 -> patch 脚本会设 Recreate")

        rc, so, _ = self._kc(["exec", "-n", self.ns, pod, "-c", self.container,
                              "--", "printenv"], timeout=90)
        podenv = dict(l.partition("=")[::2] for l in so.splitlines() if "=" in l)
        if not podenv.get("DEEPSEEK_API_KEY"):
            # ★复现审查②:全仓原本没有一处给出 deepseek-env 的**创建命令**与键名清单
            #   (它是 spike 遗产,在维护者机器上早就在 ns 里)。这里直接给出可粘贴的命令。
            bad.append(
                "pod 内无 DEEPSEEK_API_KEY -> envFrom secretRef deepseek-env 没挂上/没建。建它"
                "(键名取自 .env.example L23-25,workflow.py L62-66 与 injector L132-136 都读这三个):\n"
                f"      kubectl create secret generic deepseek-env -n {self.ns} \\\n"
                "        --from-literal=DEEPSEEK_API_KEY=<你的 key> \\\n"
                "        --from-literal=DEEPSEEK_API_BASE=https://api.deepseek.com/v1 \\\n"
                "        --from-literal=DEEPSEEK_MODEL=deepseek-chat\n"
                "      建完重跑 patch_recagent_collect.ps1(它负责挂 envFrom)")
        if podenv.get("AGENTFAULT_INJECT") and not self.allow_inject_residue:
            bad.append("★pod 上挂着 AGENTFAULT_INJECT 残留(上一轮没收干净)。"
                       "首次开采必须从干净态起;若这是崩溃后 resume,加 "
                       "--k8s-allow-inject-residue 放行")
        if "/app/agentfault/injector/loader" not in (podenv.get("PYTHONPATH") or ""):
            bad.append("pod 的 PYTHONPATH 不含 injector/loader -> 不是 agentfault 变体镜像")

        # 口径校验:候选过滤 + 真标题。只补代码不挂数据比现状更糟(候选会被全滤光)。
        if not self.skip_code_parity:
            rc, so, _ = self._kc(
                ["exec", "-n", self.ns, pod, "-c", self.container, "--", "sh", "-c",
                 "grep -c _filter_real_title /app/services/recommendation_agent/agents/tools.py "
                 "2>/dev/null || echo 0"], timeout=90)
            n = 0
            try:
                n = int((so or "0").strip().splitlines()[-1])
            except Exception:
                pass
            if n < 1:
                bad.append("pod 内 tools.py 无 _filter_real_title -> 镜像是 2026-07-19 的旧代码"
                           "快照,与本机 v2 不同口径(候选不过滤占位符)。重 build 变体镜像"
                           "(Dockerfile.agentfault 的 --build-context repo=<仓库根>)")
            rc, so, _ = self._kc(
                ["exec", "-n", self.ns, pod, "-c", self.container, "--", "sh", "-c",
                 "stat -c %s /app/shared/data/electronics.item 2>/dev/null || echo 0"],
                timeout=90)
            sz = 0
            try:
                sz = int((so or "0").strip().splitlines()[-1])
            except Exception:
                pass
            if sz < 200 * 1024 * 1024:
                bad.append(f"pod 内 electronics.item 缺失/过小({sz} B,期望 266818680)-> "
                           f"先跑 k8s/load_recagent_data.ps1 灌 PVC。★只补代码不挂数据比"
                           f"现状更糟:_filter_real_title 会把候选全滤光")

            # ★2026-07-27 加两道:候选侧【语义】口径。上面两道只证明"过滤器在、数据在",
            #   **不证明 agent 真看到了商品标题** —— 实测踩过:B 档前两轮的候选面
            #   46 个 distinct 候选**全是"未知商品"**,而 v2 是真标题(方向正好相反),
            #   B1/B2/B3 三道闸全 PASS 却毫无察觉("未知商品" != "Product_<id>",绕过了占位符判据)。
            #   根因不在 rec-agent:tools.py 渲染用的是 **sasrec 响应里的 rec['title']**,
            #   而 K8S 的 sasrec pod 没挂 electronics.item => item_info 空 => title=None
            #   => 落到 `or "未知商品"`。本机之所以没暴露:宿主 shared/data/ 里那份文件在。
            # (a) 过采倍数:×3 时实测约 7% 的调用只拿到 4 个候选(SASRec top-K 里占位符约占一半)
            rc, so, _ = self._kc(
                ["exec", "-n", self.ns, pod, "-c", self.container, "--", "sh", "-c",
                 "grep -c 'top_k \* 10' "
                 "/app/services/recommendation_agent/agents/tools.py 2>/dev/null || echo 0"],
                timeout=90)
            try:
                n_fetch = int((so or "0").strip().splitlines()[-1])
            except Exception:
                n_fetch = 0
            if n_fetch < 1:
                bad.append("pod 内 tools.py 的过采倍数还是旧的 ×3(找不到 `top_k * 10`)-> "
                           "约 7% 的调用会只拿到 4 个候选(实测 v2 7.0%/B档首轮 6.8%)。"
                           "重 build 变体镜像")
            # (b) ★端到端:从 rec-agent pod 里探真 sasrec,断言 title 非 null。
            #     这道是从**正确的观测点**(同一条集群网络路径)验"候选到底有没有语义"。
            #     一次 SASRec 推理约 0.04s、不花钱(不碰 LLM)。
            probe = ('{"item_sequence":["015600206X","6300215695","0446673145"],"top_k":5}')
            rc, so, _ = self._kc(
                ["exec", "-n", self.ns, pod, "-c", self.container, "--", "sh", "-c",
                 "curl -s --max-time 60 -X POST %s/recommend "
                 "-H 'Content-Type: application/json' -d '%s'"
                 % ((podenv.get("SASREC_API_URL") or "http://sasrec:8200").rstrip("/"),
                    probe)], timeout=120)
            body = (so or "")
            if '"title"' not in body:
                bad.append("从 pod 内探 sasrec /recommend:响应里没有 title 字段(前 200 字符: %r)"
                           " -> 响应结构与 api_server.py:455 不符,先看原文" % body[:200])
            elif '"title": null' in body or '"title":null' in body:
                bad.append("★从 pod 内探 sasrec /recommend:title=null -> sasrec 的 item_info "
                           "是空的(它没挂 electronics.item)。候选面会全变'未知商品',"
                           "agent 拿不到任何商品语义。修:powershell -File "
                           "scripts/chaos/agentfault/k8s/patch_sasrec_itemfile.ps1"
                           "(采完记得 restore_sasrec_stock.ps1 还原)")

        # ★R8:/agentfault-data 可写。patch 脚本查过一次,但那是"很久以前的一次性动作" ——
        #   `kubectl replace` / 人手 patch 可以把卷摘掉而镜像不变。卷没挂 = 整轮 span 全空
        #   (静默,见 reset_ledger 的 R5 注释)。这里是花钱前的第二道。
        rc, so, _ = self._kc(
            ["exec", "-n", self.ns, pod, "-c", self.container, "--", "sh", "-c",
             "touch /agentfault-data/.preflight 2>/dev/null && rm -f /agentfault-data/.preflight "
             "&& echo WRITABLE || echo NOPE"], timeout=90)
        if "WRITABLE" not in (so or ""):
            bad.append("pod 内 /agentfault-data 不可写(emptyDir 没挂上)-> span 与注入台账"
                       "都会静默丢光。重跑 k8s/patch_recagent_collect.ps1")

        # ★R8:pod 时钟 vs 宿主时钟。run_one_rep 的 wallclock_sanity_ok(runner L831-837)
        #   用宿主的 t0/t1 ±5s 去框 span 的 start_unix_nano,而 span 的时间戳出自 **pod**。
        #   Docker Desktop 的 WSL2 VM 休眠后有已知时钟漂移,一漂就是整树该列全 0(而且看不出因)。
        rc, so, _ = self._kc(["exec", "-n", self.ns, pod, "-c", self.container, "--",
                              "date", "+%s"], timeout=60)
        try:
            skew = abs(int((so or "0").strip().splitlines()[-1]) - int(time.time()))
        except Exception:
            skew = None
        if skew is not None and skew > 3:
            bad.append(f"pod 时钟与宿主差 {skew}s(>3s)-> wallclock_sanity_ok 会整树全 0"
                       f"(该列用宿主 t0/t1±5s 框 pod 侧 span 时间戳)。修:Docker Desktop"
                       f" 重启 WSL2(wsl --shutdown)让 VM 重新对时")

        # B 档必须 agent-only
        rc, so, _ = self._kc(["get", "podchaos,networkchaos,stresschaos", "-n", self.ns,
                              "--no-headers", "--ignore-not-found"], timeout=60)
        if rc == 0 and so.strip():
            bad.append("ns 里有遗留 Chaos CRD(B 档必须 agent-only):\n" + so[:400])

        # 下游 sasrec 必须从 **pod 内** 探(宿主 loopback 探不算数,dprobe 铁律 2)
        rc, so, _ = self._kc(["exec", "-n", self.ns, pod, "-c", self.container, "--",
                              "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                              "--max-time", "8", "http://sasrec:8200/health"], timeout=60)
        if (so or "").strip() != "200":
            bad.append(f"pod 内探 http://sasrec:8200/health 非 200(得 {so!r})")
        return bad


# ============================================================
# 工厂
# ============================================================
def make_backend(name, build_env=None, **kw):
    if name == "local":
        return LocalBackend(build_env=build_env)
    if name == "k8s":
        return K8sBackend(build_env=build_env, **kw)
    raise ValueError(f"unknown backend {name!r} (local|k8s)")
