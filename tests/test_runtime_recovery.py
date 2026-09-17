"""Deterministic lifecycle tests; no QMT process or broker requests involved."""

import asyncio
import threading

from fastapi import APIRouter
from fastapi.testclient import TestClient
import pytest
from types import SimpleNamespace

from qmt_bridge.server.bigqmt import BigQmtConfigurationError
from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import NativePayloadError
from qmt_bridge.server.runtime_controller import RuntimeController


class _Runtime:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


def test_transport_failure_recovers_once_without_replaying_work():
    attempts = []
    runtime = _Runtime()

    def factory(_settings):
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError("bridge unavailable")
        return runtime

    async def no_sleep(_delay):
        await asyncio.sleep(0)

    async def scenario():
        controller = RuntimeController(Settings(), runtime_factory=factory, sleep=no_sleep)
        await controller.start()
        for _ in range(50):
            if controller.runtime is runtime:
                break
            await asyncio.sleep(0)
        assert controller.runtime is runtime
        assert len(attempts) == 2
        assert controller.status()["recovering"] is False
        await controller.close()

    asyncio.run(scenario())
    assert runtime.closed == 1


def test_configuration_error_does_not_schedule_retries():
    attempts = []

    def factory(_settings):
        attempts.append(1)
        raise BigQmtConfigurationError("bad loopback endpoint")

    async def scenario():
        controller = RuntimeController(Settings(), runtime_factory=factory)
        await controller.start()
        await asyncio.sleep(0)
        assert controller.runtime is None
        assert controller.configuration_error is True
        assert controller.recovering is False
        assert len(attempts) == 1
        await controller.close()

    asyncio.run(scenario())


def test_observed_transport_loss_invalidates_realtime_callbacks_before_reconnect():
    first, second = _Runtime(), _Runtime()
    created = []
    invalidated = []

    class Hub:
        async def invalidate(self, *, reason):
            invalidated.append(reason)

    def factory(_settings):
        created.append(1)
        return first if len(created) == 1 else second

    async def no_sleep(_delay):
        await asyncio.sleep(0)

    async def scenario():
        state = SimpleNamespace(realtime_quote_hub=Hub())
        controller = RuntimeController(
            Settings(), runtime_factory=factory, sleep=no_sleep, state=state
        )
        await controller.start()
        controller.mark_transport_unavailable(OSError("socket reset"))
        for _ in range(50):
            if controller.runtime is second:
                break
            await asyncio.sleep(0)
        assert controller.runtime is second
        assert invalidated == ["native_runtime_replaced"]
        await controller.close()

    asyncio.run(scenario())


def test_account_manager_failure_closes_new_runtime_before_retry():
    runtime = _Runtime()
    disconnected = []

    class FailingManager:
        def __init__(self, **_kwargs):
            pass

        def connect(self, **_kwargs):
            raise OSError("event endpoint unavailable")

        def disconnect(self):
            disconnected.append(True)

    async def scenario():
        controller = RuntimeController(
            Settings(account_enabled=True, trading_account_id="account"),
            runtime_factory=lambda _settings: runtime,
            manager_factory=FailingManager,
        )
        # Exercise one connection attempt directly, avoiding a background retry.
        assert await controller._connect_once() is False
        assert runtime.closed == 1
        assert disconnected == [True]
        assert controller.runtime is None
        assert controller.configuration_error is False
        await controller.close()

    asyncio.run(scenario())


def test_close_drains_blocked_factory_and_disposes_late_runtime():
    entered = threading.Event()
    release = threading.Event()
    runtime = _Runtime()

    def blocked_factory(_settings):
        entered.set()
        assert release.wait(timeout=2)
        return runtime

    async def scenario():
        controller = RuntimeController(Settings(rpc_timeout_seconds=1), runtime_factory=blocked_factory)
        starting = asyncio.create_task(controller.start())
        assert await asyncio.to_thread(entered.wait, 1)
        closing = asyncio.create_task(controller.close())
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(closing, timeout=2)
        await asyncio.wait_for(starting, timeout=2)
        assert controller.runtime is None
        assert runtime.closed == 1

    asyncio.run(scenario())


def test_download_restore_starts_only_after_runtime_and_is_nonfatal():
    runtime = _Runtime()
    restored = []
    state = SimpleNamespace()

    def locked_state_dir():
        restored.append(True)
        raise RuntimeError("download state owned by another process")

    async def scenario():
        controller = RuntimeController(
            Settings(),
            runtime_factory=lambda _settings: runtime,
            download_start=locked_state_dir,
            state=state,
        )
        await controller.start()
        assert controller.runtime is runtime
        assert restored == [True]
        assert "owned by another process" in state.download_jobs_error
        await controller.close()

    asyncio.run(scenario())


def test_actual_rpc_transport_failure_triggers_controller_recovery():
    """The recovery signal originates in BigQmtRpcClient, not a direct call."""
    from bigqmt_signal_trader.rpc_client import BigQmtRpcClient

    class FailingTransport:
        def send_request(self, _request, _timeout):
            raise OSError("ZMQ connection reset")

    class ActualRuntime(_Runtime):
        def __init__(self):
            super().__init__()
            self.client = BigQmtRpcClient(
                account_id="acct",
                redis_config={"transport": "zmq"},
            )
            self.client._transport_instance = FailingTransport()

        def set_transport_failure_callback(self, callback):
            self.client.set_transport_failure_callback(callback)

        def close(self):
            self.client.set_transport_failure_callback(None)
            super().close()

    first, second = ActualRuntime(), _Runtime()
    calls = []

    def factory(_settings):
        calls.append(True)
        return first if len(calls) == 1 else second

    async def no_sleep(_delay):
        await asyncio.sleep(0)

    async def scenario():
        controller = RuntimeController(Settings(), runtime_factory=factory, sleep=no_sleep)
        await controller.start()
        with pytest.raises(OSError, match="connection reset"):
            first.client.call("snapshot", force_rpc=True)
        for _ in range(50):
            if controller.runtime is second:
                break
            await asyncio.sleep(0)
        assert controller.runtime is second
        assert len(calls) == 2
        await controller.close()

    asyncio.run(scenario())


def test_native_unsupported_response_does_not_trigger_transport_observer():
    from bigqmt_signal_trader.rpc_client import BigQmtRpcClient

    class UnsupportedTransport:
        def send_request(self, _request, _timeout):
            return {"ok": False, "error_type": "NotImplementedError", "error": "unsupported"}

    observed = []
    client = BigQmtRpcClient(account_id="acct", redis_config={"transport": "zmq"})
    client._transport_instance = UnsupportedTransport()
    client.set_transport_failure_callback(observed.append)
    with pytest.raises(NotImplementedError, match="unsupported"):
        client.call("unsupported_method", force_rpc=True)
    assert observed == []


def test_app_scoped_settings_authenticate_data_and_all_mutations():
    """A factory-local Settings must win over the process singleton."""
    from qmt_bridge.server.app import create_app

    app = create_app(Settings(api_key="test-key", require_auth_for_data=True))
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/api/meta/health").status_code == 401
        assert client.get("/api/meta/health", headers={"X-API-Key": "test-key"}).status_code == 200
        # Sector removal is a data-router write, not an account route, and must
        # not bypass authentication merely because data reads are configurable.
        assert client.delete("/api/sector/remove", params={"sector_name": "x"}).status_code == 401


def test_native_payload_error_is_stable_bad_gateway_response():
    from qmt_bridge.server.app import create_app

    app = create_app(Settings())
    router = APIRouter()

    @router.get("/_native-payload-fixture")
    def fixture():
        raise NativePayloadError("malformed native reply")

    app.include_router(router)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/_native-payload-fixture")
    assert response.status_code == 502
    assert response.json() == {
        "status": "error",
        "reason_code": "invalid_native_payload",
        "message": "malformed native reply",
    }
