import asyncio
import json
import math
import os
import subprocess
import sys
import threading

import pytest

from bigqmt_signal_trader import telemetry


@pytest.fixture(autouse=True)
def isolated_telemetry(tmp_path):
    telemetry.shutdown(0.2)
    assert telemetry.configure(enabled=True, directory=str(tmp_path), role="test", queue_size=32, max_bytes=300, backup_count=1)
    yield tmp_path
    telemetry.shutdown(1.0)
    telemetry.configure(enabled=False)


def _events(directory):
    out = []
    for path in directory.glob("*.jsonl*"):
        out.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line)
    return out


def test_span_context_trace_and_exception(isolated_telemetry):
    with telemetry.bind_context(http_request_id="h1"):
        with telemetry.span("request", account_id="secret") as result:
            result["outcome"] = "partial"
        with pytest.raises(ValueError):
            with telemetry.span("broken"):
                raise ValueError("unchanged")
    assert telemetry.flush(1.0)
    events = _events(isolated_telemetry)
    completed = [event for event in events if event["event_name"] == "request.end"][0]
    failed = [event for event in events if event["event_name"] == "broken.end"][0]
    assert completed["trace_id"] and completed["span_id"]
    assert completed["http_request_id"] == "h1"
    assert completed["account_id"] == "[REDACTED]"
    assert completed["duration_ms"] >= 0
    assert failed["outcome"] == "error" and failed["error_type"] == "ValueError"


def test_span_accepts_outcome_and_records_async_cancellation(isolated_telemetry):
    with telemetry.span("already-set", outcome="partial"):
        pass

    async def canceled():
        with telemetry.span("cancel"):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(canceled())
    assert telemetry.flush(1.0)
    events = _events(isolated_telemetry)
    assert [e for e in events if e["event_name"] == "already-set.start"][0]["outcome"] == "started"
    assert [e for e in events if e["event_name"] == "cancel.end"][0]["outcome"] == "canceled"


def test_context_async_and_threads_are_isolated(isolated_telemetry):
    async def child():
        return telemetry.current_context()["trace_id"]

    with telemetry.bind_context(trace_id="trace-main"):
        assert asyncio.run(child()) == "trace-main"
        observed = []
        thread = threading.Thread(target=lambda: observed.append(telemetry.current_context()))
        thread.start()
        thread.join()
    assert observed == [{}]


def test_observability_clock_isolated_from_business_time_monkeypatch(monkeypatch, isolated_telemetry):
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: (_ for _ in ()).throw(AssertionError("business clock")))
    with telemetry.span("isolated-clock"):
        pass
    assert telemetry.flush(1.0)


def test_inject_extract_and_decorator(isolated_telemetry):
    with telemetry.bind_context(trace_id="trace-1", span_id="span-1", account_id="hidden"):
        payload = telemetry.inject_trace({"method": "ping"})
    assert payload == {"method": "ping", "trace": {"trace_id": "trace-1", "span_id": "span-1"}}
    assert telemetry.extract_trace({"trace": {"trace_id": "ok", "account_id": "ignored", "bad": "x"}}) == {"trace_id": "ok"}

    @telemetry.traced("decorated")
    def works(value):
        return value + 1

    assert works(1) == 2

    @telemetry.traced("async-decorated")
    async def async_works(value):
        return value + 1

    assert asyncio.run(async_works(2)) == 3


def test_rotation_and_safe_values(isolated_telemetry):
    for index in range(20):
        telemetry.emit("event", item=index, payload="x" * 1000, password="nope", account_enabled=True,
                       nan=math.nan, arbitrary=object())
    assert telemetry.flush(1.0)
    paths = list(isolated_telemetry.glob("*.jsonl*"))
    assert any(path.suffix == ".1" for path in paths)
    event = _events(isolated_telemetry)[0]
    assert event["password"] == "[REDACTED]"
    assert event["arbitrary"] == "<object>"
    assert len(event["payload"]) == 512
    assert event["nan"] is None and event["account_enabled"] is True


def test_queue_pressure_write_failure_and_shutdown(monkeypatch, isolated_telemetry):
    # A synthetic live writer keeps the queue full without a timing race.
    telemetry.shutdown(1.0)
    telemetry.configure(directory=str(isolated_telemetry), role="pressure", queue_size=1)
    class AliveWriter:
        def is_alive(self):
            return True
    with telemetry._telemetry.lock:
        telemetry._telemetry.events = __import__("queue").Queue(maxsize=1)
        telemetry._telemetry.thread = AliveWriter()
        telemetry._telemetry.events.put_nowait({"occupied": True})
    assert telemetry.emit("dropped", critical=True) is False
    assert telemetry.stats()["queue_full"] == 1
    with telemetry._telemetry.lock:
        telemetry._telemetry.events = None
        telemetry._telemetry.thread = None

    telemetry.configure(directory=str(isolated_telemetry), role="failure")
    original_write = telemetry._telemetry._write_one
    calls = []
    def fail_once(event):
        calls.append(event)
        if len(calls) == 1:
            raise OSError("disk")
        return original_write(event)
    monkeypatch.setattr(telemetry._telemetry, "_write_one", fail_once)
    assert telemetry.emit("write-fails")
    assert telemetry.flush(1.0)
    assert telemetry.emit("recovered")
    assert telemetry.flush(1.0)
    assert telemetry.stats()["write_failures"] >= 1
    assert telemetry.stats()["loss_events"] == 1
    assert any(event["event_name"] == "telemetry.loss" for event in _events(isolated_telemetry))


def test_partial_config_env_disable_and_shutdown_rejection(monkeypatch, isolated_telemetry):
    before = telemetry.stats()
    telemetry.configure(role="new-role")
    after = telemetry.stats()
    assert after["directory"] == before["directory"] and after["role"] == "new-role"
    telemetry.shutdown(1.0)
    assert telemetry.emit("after-close") is False
    assert telemetry.stats()["rejected_shutdown"] >= 1
    monkeypatch.setenv("QMT_BRIDGE_TRACE_ENABLED", "false")
    monkeypatch.setenv("QMT_BRIDGE_TRACE_DIR", str(isolated_telemetry / "env-traces"))
    assert telemetry.refresh_from_env()
    assert telemetry.stats()["enabled"] is False
    assert telemetry.stats()["directory"].endswith("env-traces")
    assert telemetry.emit("env-off") is False


def test_threads_do_not_corrupt_pending_loss_or_close_race(isolated_telemetry):
    produced = []
    def produce():
        for index in range(100):
            produced.append(telemetry.emit("threaded", item=index, critical=index % 10 == 0))
    threads = [threading.Thread(target=produce) for _ in range(4)]
    for thread in threads:
        thread.start()
    telemetry.shutdown(1.0)
    for thread in threads:
        thread.join()
    assert all(value in (True, False) for value in produced)
    assert telemetry.stats()["writer_alive"] is False


def test_atexit_drains_client_only_process(tmp_path):
    trace_dir = tmp_path / "child-traces"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join([str(__import__("pathlib").Path("src").resolve()), environment.get("PYTHONPATH", "")])
    environment["QMT_BRIDGE_TRACE_ENABLED"] = "true"
    environment["QMT_BRIDGE_TRACE_DIR"] = str(trace_dir)
    result = subprocess.run(
        [sys.executable, "-S", "-c", "from bigqmt_signal_trader.telemetry import emit; emit('child.exit')"],
        env=environment, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert any(event["event_name"] == "child.exit" for event in _events(trace_dir))


def test_failed_write_exposes_pending_audit_gap(monkeypatch, isolated_telemetry):
    monkeypatch.setattr(telemetry._telemetry, "_write_one", lambda _event: (_ for _ in ()).throw(OSError("disk")))
    assert telemetry.emit("one-failure")
    assert telemetry.flush(1.0)
    details = telemetry.stats()
    assert details["write_failures"] >= 1
    assert details["pending_loss"]["write_failures"] >= 1
