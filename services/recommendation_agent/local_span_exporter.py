"""自包含本地 JSONL span exporter（TASK-X agent 故障注入实验用）。

为什么需要它（见 (项目文档) telemetry_method 决策）：
- 持久栈 Jaeger 在跑、临时实例 OTEL_SERVICE_NAME 与持久栈同名（app.py:40 setdefault
  =recommendation_agent）→ Jaeger 查询会混采持久+临时，无法干净归窗。
- 过夜无人值守要求"每窗即时落盘、中断保住已完成窗"，Jaeger 事后查不满足。
- 故给临时实例额外挂一个 env 门控（SPAN_FILE）的本地 JSONL FileSpanExporter，
  用 **SimpleSpanProcessor（非 Batch）** 保证每条 span 在 END 时即时 flush 落盘。

归窗（TASK-X must-fix #1）：不靠 wall-clock 时窗（SimpleSpanProcessor 在 span END 时
export，根 Flask server span start 早于探针 t0、子 span 可能晚于 t1）。endpoint 回写
trace_id 进 /recommend 响应 JSON，runner 按 trace_id 从本 JSONL 精确捞该窗全部 span。

每行一个 JSON 对象，字段：
  trace_id(032x) / span_id(016x) / parent_span_id(016x or "") / name /
  start_unix_nano / end_unix_nano / duration_ms / status_code / kind /
  attributes{recweb.agent.name, recweb.agent.fault, recweb.agent.fault.delay_ms, http.*, ...}

线程安全：module 级 Lock 串行化写（dev server 单线程仍按纪律加锁，防 instrumentation 多线程）。
"""

import json
import os
import threading

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

_WRITE_LOCK = threading.Lock()


def _fmt_trace_id(tid):
    try:
        return format(tid, "032x")
    except Exception:
        return ""


def _fmt_span_id(sid):
    try:
        if sid in (None, 0):
            return ""
        return format(sid, "016x")
    except Exception:
        return ""


def _serialize_attributes(attrs):
    """把 span attributes 转成 JSON 可序列化的 dict（只保留我们关心的标量/字符串）。"""
    out = {}
    if not attrs:
        return out
    for k, v in attrs.items():
        try:
            if isinstance(v, (str, int, float, bool)):
                out[k] = v
            elif isinstance(v, (list, tuple)):
                out[k] = [x for x in v if isinstance(x, (str, int, float, bool))]
            else:
                out[k] = str(v)
        except Exception:
            out[k] = "<unserializable>"
    return out


class LocalJSONLSpanExporter(SpanExporter):
    """把每条 ReadableSpan 序列化为一行 JSON append 到 file_path。"""

    def __init__(self, file_path):
        self._file_path = file_path
        d = os.path.dirname(file_path)
        if d:
            os.makedirs(d, exist_ok=True)

    def export(self, spans):
        lines = []
        for sp in spans:
            try:
                ctx = sp.get_span_context()
                parent = sp.parent
                status = sp.status
                rec = {
                    "trace_id": _fmt_trace_id(ctx.trace_id),
                    "span_id": _fmt_span_id(ctx.span_id),
                    "parent_span_id": _fmt_span_id(parent.span_id) if parent else "",
                    "name": sp.name,
                    "start_unix_nano": sp.start_time,
                    "end_unix_nano": sp.end_time,
                    "duration_ms": ((sp.end_time - sp.start_time) / 1e6)
                    if (sp.end_time and sp.start_time) else None,
                    "status_code": status.status_code.name if status else "UNSET",
                    "kind": sp.kind.name if sp.kind is not None else None,
                    "attributes": _serialize_attributes(sp.attributes),
                }
                lines.append(json.dumps(rec, ensure_ascii=False))
            except Exception:
                # 单条 span 序列化失败不连累整批
                continue
        if not lines:
            return SpanExportResult.SUCCESS
        try:
            with _WRITE_LOCK:
                with open(self._file_path, "a", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self):
        return None

    def force_flush(self, timeout_millis=30000):
        return True
