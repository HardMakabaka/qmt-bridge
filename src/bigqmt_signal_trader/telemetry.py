"""Small, dependency-free JSONL tracing for the Big QMT bridge.

The module deliberately never raises into business code.  It is also safe to
ship into QMT's Python 3.6 runtime: startup is lazy and all storage is local.
"""

from __future__ import print_function

import datetime
import atexit
import functools
import inspect
import json
import math
import os
import queue
import re
import threading
import time
import uuid
from contextlib import contextmanager
from time import monotonic as _monotonic

try:
    import contextvars
except ImportError:  # Python 3.6 in embedded QMT
    contextvars = None


_SCHEMA_VERSION = 1
_DEFAULT_MAX_BYTES = 5 * 1024 * 1024
_DEFAULT_BACKUP_COUNT = 3
_DEFAULT_QUEUE_SIZE = 4096
_MAX_TEXT = 512
_MAX_FIELDS = 48
_MAX_ITEMS = 24
_SENSITIVE = re.compile(r"(secret|password|passwd|token|api.?key|authorization|cookie|account)", re.I)
_TRACE_KEYS = ("trace_id", "span_id", "parent_span_id", "http_request_id", "rpc_request_id", "runtime_generation")
_TRACE_VALUE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
_STOP = object()


def _env_enabled():
    return str(os.environ.get("QMT_BRIDGE_TRACE_ENABLED", "true")).strip().lower() not in (
        "0", "false", "no", "off",
    )


def _default_directory():
    configured = os.environ.get("QMT_BRIDGE_TRACE_DIR")
    if configured:
        return configured
    root = os.environ.get("LOCALAPPDATA")
    if root:
        return os.path.join(root, "QmtBridge", "traces")
    return os.path.join(os.path.expanduser("~"), ".qmt_bridge", "traces")


def _safe_role(value):
    text = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value or "bridge"))
    return text[:64] or "bridge"


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _sensitive_key(key):
    lowered = str(key).lower()
    if lowered in ("account_enabled", "accounts_enabled", "account_allowed", "account_required", "account_count"):
        return False
    return bool(_SENSITIVE.search(lowered))


def _safe_value(value, depth=0):
    if depth > 3:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value[:_MAX_TEXT]
    if isinstance(value, bytes):
        return "<bytes:%d>" % len(value)
    if isinstance(value, (list, tuple)):
        return [_safe_value(item, depth + 1) for item in value[:_MAX_ITEMS]]
    if isinstance(value, dict):
        out = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_ITEMS:
                out["_truncated"] = True
                break
            key = str(key)[:96]
            out[key] = "[REDACTED]" if _sensitive_key(key) else _safe_value(item, depth + 1)
        return out
    # Avoid repr(): model and native QMT objects can expose secrets or payloads.
    return "<%s>" % type(value).__name__


def _safe_fields(fields):
    out = {}
    for index, (key, value) in enumerate((fields or {}).items()):
        if index >= _MAX_FIELDS:
            out["fields_truncated"] = True
            break
        key = str(key)[:96]
        out[key] = "[REDACTED]" if _sensitive_key(key) else _safe_value(value)
    return out


class _ContextStore(object):
    def __init__(self):
        self._local = threading.local()
        self._var = contextvars.ContextVar("qmt_bridge_trace_context", default={}) if contextvars else None

    def get(self):
        if self._var is not None:
            return dict(self._var.get() or {})
        return dict(getattr(self._local, "value", {}) or {})

    def set(self, value):
        value = dict(value or {})
        if self._var is not None:
            return self._var.set(value)
        previous = self.get()
        self._local.value = value
        return previous

    def reset(self, token):
        if self._var is not None:
            self._var.reset(token)
        else:
            self._local.value = dict(token or {})


class _Telemetry(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.context = _ContextStore()
        self.enabled = _env_enabled()
        self.directory = _default_directory()
        self.role = _safe_role(os.environ.get("QMT_BRIDGE_TRACE_ROLE", "bridge"))
        self.max_bytes = _DEFAULT_MAX_BYTES
        self.backup_count = _DEFAULT_BACKUP_COUNT
        self.queue_size = _DEFAULT_QUEUE_SIZE
        self.process_instance_id = uuid.uuid4().hex
        self.events = None
        self.thread = None
        self.stop_requested = False
        self.file_handle = None
        self.file_path = None
        self.counters = {"emitted": 0, "written": 0, "dropped": 0, "queue_full": 0,
                         "critical_dropped": 0, "write_failures": 0,
                         "critical_write_failures": 0, "rejected_shutdown": 0, "loss_events": 0}
        self.pending_loss = {"dropped": 0, "critical_dropped": 0, "write_failures": 0,
                             "critical_write_failures": 0}
        self.last_failure_type = None

    def configure(self, enabled=None, directory=None, role=None, max_bytes=None, backup_count=None, queue_size=None):
        if not self.shutdown(1.0):
            return False
        with self.lock:
            self.enabled = self.enabled if enabled is None else bool(enabled)
            self.directory = str(directory) if directory is not None else self.directory
            self.role = _safe_role(role) if role is not None else self.role
            self.max_bytes = max(1024, int(max_bytes)) if max_bytes is not None else self.max_bytes
            self.backup_count = max(0, int(backup_count)) if backup_count is not None else self.backup_count
            self.queue_size = max(1, int(queue_size)) if queue_size is not None else self.queue_size
            self.events = None
            self.thread = None
            self.stop_requested = False
            self.file_handle = None
            self.file_path = None
            self.counters = {"emitted": 0, "written": 0, "dropped": 0, "queue_full": 0,
                             "critical_dropped": 0, "write_failures": 0,
                             "critical_write_failures": 0, "rejected_shutdown": 0, "loss_events": 0}
            self.pending_loss = {"dropped": 0, "critical_dropped": 0, "write_failures": 0,
                                 "critical_write_failures": 0}
            self.last_failure_type = None
            return True

    def _start_locked(self):
        if not self.enabled:
            return False
        if self.thread is not None and self.thread.is_alive():
            return True
        try:
            self.events = queue.Queue(maxsize=self.queue_size)
            self.stop_requested = False
            self.thread = threading.Thread(target=self._writer, name="qmt-telemetry", daemon=True)
            self.thread.start()
            return True
        except Exception:
            self.counters["write_failures"] += 1
            self.pending_loss["write_failures"] += 1
            return False

    def _event(self, event_name, fields):
        context = self.context.get()
        event = {
            "schema_version": _SCHEMA_VERSION,
            "timestamp_utc": _utc_now(),
            "process_instance_id": self.process_instance_id,
            "service_role": self.role,
            "event_name": str(event_name)[:160],
            "outcome": "success",
        }
        event.update(_safe_fields(context))
        event.update(_safe_fields(fields))
        for key in ("trace_id", "span_id", "parent_span_id"):
            event.setdefault(key, context.get(key))
        return event

    def emit(self, event_name, critical=False, fields=None):
        try:
            with self.lock:
                if self.stop_requested:
                    self.counters["rejected_shutdown"] += 1
                    return False
                if not self.enabled or not self._start_locked():
                    return False
                event = self._event(event_name, fields)
                event["critical"] = bool(critical)
                try:
                    self.events.put_nowait(event)
                    self.counters["emitted"] += 1
                    return True
                except queue.Full:
                    self.counters["dropped"] += 1
                    self.counters["queue_full"] += 1
                    self.pending_loss["dropped"] += 1
                    if critical:
                        self.counters["critical_dropped"] += 1
                        self.pending_loss["critical_dropped"] += 1
                    return False
        except Exception:
            return False

    def _open_file(self):
        if self.file_handle is not None:
            return self.file_handle
        if not os.path.isdir(self.directory):
            os.makedirs(self.directory, exist_ok=True)
        name = "%s-%s-%s.jsonl" % (self.role, os.getpid(), self.process_instance_id[:12])
        self.file_path = os.path.join(self.directory, name)
        self.file_handle = open(self.file_path, "a", encoding="utf-8")
        return self.file_handle

    def _rotate(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None
        if self.backup_count:
            for number in range(self.backup_count, 0, -1):
                old = "%s.%d" % (self.file_path, number)
                new = "%s.%d" % (self.file_path, number + 1)
                if number == self.backup_count and os.path.exists(old):
                    os.remove(old)
                elif os.path.exists(old):
                    os.rename(old, new)
            if os.path.exists(self.file_path):
                os.rename(self.file_path, "%s.1" % self.file_path)
        elif os.path.exists(self.file_path):
            os.remove(self.file_path)

    def _write_one(self, event):
        line = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        handle = self._open_file()
        if handle.tell() and handle.tell() + len(line.encode("utf-8")) > self.max_bytes:
            self._rotate()
            handle = self._open_file()
        handle.write(line)
        handle.flush()
        with self.lock:
            self.counters["written"] += 1

    def _write_loss_if_needed(self):
        with self.lock:
            loss = dict(self.pending_loss)
        if not any(loss.values()):
            return
        event = self._event("telemetry.loss", {"outcome": "partial", "loss": loss})
        self._write_one(event)
        with self.lock:
            # Preserve failures recorded while the writer was persisting loss.
            for key, value in loss.items():
                self.pending_loss[key] = max(0, self.pending_loss.get(key, 0) - value)
            self.counters["loss_events"] += 1

    def _writer(self):
        while True:
            event = self.events.get()
            try:
                if event is _STOP:
                    return
                try:
                    self._write_loss_if_needed()
                    self._write_one(event)
                except Exception as exc:
                    failure_type = type(exc).__name__
                    with self.lock:
                        self.counters["write_failures"] += 1
                        self.pending_loss["write_failures"] += 1
                        if event.get("critical"):
                            self.counters["critical_write_failures"] += 1
                            self.pending_loss["critical_write_failures"] += 1
                        self.last_failure_type = failure_type
            finally:
                self.events.task_done()

    def flush(self, timeout):
        deadline = _monotonic() + max(0.0, float(timeout))
        try:
            while self.events is not None and getattr(self.events, "unfinished_tasks", 0):
                if _monotonic() >= deadline:
                    return False
                time.sleep(0.005)
            return True
        except Exception:
            return False

    def shutdown(self, timeout):
        timeout = max(0.0, float(timeout))
        deadline = _monotonic() + timeout
        with self.lock:
            self.stop_requested = True
            events, thread = self.events, self.thread
        self.flush(max(0.0, deadline - _monotonic()))
        with self.lock:
            events, thread = self.events, self.thread
            if events is not None and thread is not None and thread.is_alive():
                try:
                    events.put(_STOP, timeout=max(0.0, deadline - _monotonic()))
                except queue.Full:
                    return False
        if thread is not None and thread.is_alive():
            thread.join(max(0.0, deadline - _monotonic()))
        with self.lock:
            stopped = thread is None or not thread.is_alive()
            if stopped and self.file_handle is not None:
                try:
                    self.file_handle.close()
                except Exception:
                    pass
                self.file_handle = None
            if stopped:
                self.events = None
                self.thread = None
            return stopped

    def stats(self):
        with self.lock:
            result = dict(self.counters)
            result.update({"enabled": self.enabled, "directory": self.directory, "role": self.role,
                           "queue_size": self.queue_size, "queue_depth": self.events.qsize() if self.events else 0,
                           "writer_alive": bool(self.thread and self.thread.is_alive()),
                           "process_instance_id": self.process_instance_id,
                           "last_failure_type": self.last_failure_type,
                           "pending_loss": dict(self.pending_loss)})
            return result


_telemetry = _Telemetry()


def configure(enabled=None, directory=None, role=None, max_bytes=None, backup_count=None, queue_size=None):
    """Reset telemetry configuration. File creation remains lazy until emit."""
    try:
        return _telemetry.configure(enabled, directory, role, max_bytes, backup_count, queue_size)
    except Exception:
        return False


def refresh_from_env():
    """Explicitly reload enablement, destination and role from environment."""
    return configure(
        enabled=_env_enabled(), directory=_default_directory(),
        role=os.environ.get("QMT_BRIDGE_TRACE_ROLE", "bridge"),
    )


def emit(event_name, critical=False, **fields):
    return _telemetry.emit(event_name, critical=critical, fields=fields)


@contextmanager
def bind_context(**fields):
    current = _telemetry.context.get()
    current.update(_safe_fields(fields))
    token = _telemetry.context.set(current)
    try:
        yield current
    finally:
        _telemetry.context.reset(token)


def current_context():
    return _telemetry.context.get()


@contextmanager
def span(name, **fields):
    parent = _telemetry.context.get()
    context = dict(parent)
    context["trace_id"] = str(parent.get("trace_id") or uuid.uuid4().hex)
    context["parent_span_id"] = str(parent.get("span_id") or "") or None
    context["span_id"] = uuid.uuid4().hex[:16]
    context.update(_safe_fields(fields))
    token = _telemetry.context.set(context)
    result = {}
    started = _monotonic()
    start_fields = dict(context)
    start_fields["outcome"] = "started"
    emit(str(name) + ".start", **start_fields)
    try:
        yield result
    except BaseException as exc:
        if type(exc).__name__ == "CancelledError":
            default_outcome = "canceled"
        elif isinstance(exc, Exception):
            default_outcome = "error"
        else:
            default_outcome = "aborted"
        result["outcome"] = result.get("outcome") or default_outcome
        result["error_type"] = type(exc).__name__
        raise
    finally:
        result["duration_ms"] = round((_monotonic() - started) * 1000.0, 3)
        end_fields = dict(context)
        end_fields.update(_safe_fields(result))
        emit(str(name) + ".end", **end_fields)
        _telemetry.context.reset(token)


def inject_trace(payload):
    result = dict(payload) if isinstance(payload, dict) else {}
    context = current_context()
    trace = {key: str(context[key]) for key in _TRACE_KEYS if context.get(key)}
    if trace:
        result["trace"] = trace
    return result


def extract_trace(payload):
    raw = payload.get("trace") if isinstance(payload, dict) else None
    if not isinstance(raw, dict):
        return {}
    return {key: str(raw[key]) for key in _TRACE_KEYS
            if raw.get(key) and _TRACE_VALUE.match(str(raw[key]))}


def traced(name, **fields):
    def decorate(function):
        if inspect.iscoroutinefunction(function):
            @functools.wraps(function)
            async def async_wrapper(*args, **kwargs):
                with span(name, **fields):
                    return await function(*args, **kwargs)
            return async_wrapper

        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            with span(name, **fields):
                return function(*args, **kwargs)
        return wrapper
    return decorate


def flush(timeout=1.0):
    """Wait for queue drain only; local JSONL flushing does not imply fsync durability."""
    return _telemetry.flush(timeout)


def shutdown(timeout=1.0):
    return _telemetry.shutdown(timeout)


def stats():
    return _telemetry.stats()


# Client-only scripts have no server lifespan to own shutdown. Registering a
# bounded handler does not create files at import time, but gives queued events
# one last chance to reach local JSONL on normal interpreter exit.
atexit.register(shutdown, 1.0)
