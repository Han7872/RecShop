# -*- coding: utf-8 -*-
"""agentfault 注入器 loader(auto-imported;必须叫 sitecustomize.py 且在 PYTHONPATH 最前）。

CPython 在 site 初始化时 import 一次本文件,早于 app.py/workflow.py import langchain。
本 loader 干三件事(都 env-gated,shell 默认不触发 -> venv python 仍是干净解释器):
  1. AGENTFAULT_INSTRUMENT=1 -> arm openinference 内容层埋点(复用 Phase1 已验通姿势:
     minimal instrument,靠 ProxyTracerProvider late-bind 到 app.py 真 provider)。
     => 注入后的内容会经 on_llm_end 落进 SPAN_FILE,可核"内容层看得见语义故障"。
  2. AGENTFAULT_INJECT=1 -> install() 注入器(patch ChatOpenAI._generate)。
  3. AGENTFAULT_OBSERVE=1 -> install_observer() **只观测不注入**(patch ChatOpenAI._generate
     成最小 wrapper,只发 agentfault.resolved_input span 后原样调原函数)。给 clean(normal)
     基线用:基线也有 resolved_input 轨迹 -> 可测结构化检测器在无故障运行上的误报率。
     **零注入保证不变**:该路径不碰 messages、不写台账、不做任何后处理。

三者独立开关。采集正常基线开 (1)+(3);注入采集开 (1)+(2)。
★(2) 与 (3) 互斥且 (2) 优先:INJECT=1 时**不** arm observer —— install() 的 patched_generate
内部本就在 pre-call 钩子之后发 resolved_input,再叠 observer 会双发、且多发一次"删除前"的
名单。故注入路径保持与今天逐字节一致(injector 侧代码零改动;install_observer 自身也再查一次
AGENTFAULT_INJECT 作双保险)。

顺序:先 arm openinference(callback 级),再 install 注入器(_generate 级)。运行时
调用栈 = generate -> _generate(patched:orig 后注入) -> on_llm_end(见注入后 result)。
=> openinference 捕到的是**注入后**内容(on-thesis:内容层可见语义故障)。

SAFETY:整体 try 包裹,任何失败打 stderr 并吞掉,绝不阻断 app.py 启动。
"""
import os
import sys
import traceback


def _err(msg):
    try:
        sys.stderr.write("[agentfault-loader] " + msg + "\n")
        sys.stderr.flush()
    except Exception:
        pass


def _arm_openinference():
    from openinference.instrumentation.langchain import LangChainInstrumentor
    li = LangChainInstrumentor()
    if li.is_instrumented_by_opentelemetry:  # BOOLEAN PROPERTY in this rev
        _err("openinference already instrumented (skip).")
        return
    li.instrument()  # NO tracer_provider kwarg -> proxy late-bind (Phase1-proven)
    _err("armed openinference-langchain (minimal; proxy late-bind).")


def _arm_injector():
    # 注入器与本 loader 同目录树:loader/ 的上级是 injector/
    here = os.path.dirname(os.path.abspath(__file__))
    injector_dir = os.path.dirname(here)  # .../injector
    if injector_dir not in sys.path:
        sys.path.insert(0, injector_dir)
    import agentfault_injector
    agentfault_injector.install()


def _arm_observer():
    """只观测不注入(clean 基线):同目录树 import injector,调 install_observer()。"""
    here = os.path.dirname(os.path.abspath(__file__))
    injector_dir = os.path.dirname(here)  # .../injector
    if injector_dir not in sys.path:
        sys.path.insert(0, injector_dir)
    import agentfault_injector
    agentfault_injector.install_observer()


def _main():
    if os.environ.get("AGENTFAULT_INSTRUMENT", "").strip() == "1":
        try:
            _arm_openinference()
        except Exception:
            _err("openinference arm crashed (ignored):\n" + traceback.format_exc())
    injecting = os.environ.get("AGENTFAULT_INJECT", "").strip() == "1"
    if injecting:
        try:
            _arm_injector()
        except Exception:
            _err("injector arm crashed (ignored):\n" + traceback.format_exc())
    # 只观测不注入(clean 基线)。INJECT=1 时让位:注入器自己就发 resolved_input,不叠加。
    if os.environ.get("AGENTFAULT_OBSERVE", "").strip() == "1":
        if injecting:
            _err("AGENTFAULT_OBSERVE ignored (AGENTFAULT_INJECT=1 owns emission).")
        else:
            try:
                _arm_observer()
            except Exception:
                _err("observer arm crashed (ignored):\n" + traceback.format_exc())


try:
    _main()
except Exception:
    _err("loader crashed (ignored):\n" + traceback.format_exc())
