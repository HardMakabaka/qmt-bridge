import asyncio

from qmt_bridge.server.config import Settings
from qmt_bridge.server.notify.base import NotifierBackend, NotifierManager
from bigqmt_signal_trader.telemetry import bind_context


class _Backend(NotifierBackend):
    async def start(self):
        return None

    async def stop(self):
        return None

    async def send(self, event):
        return None

    def name(self):
        return "test-backend"


class _BlockingBackend(_Backend):
    def __init__(self):
        self.gate = asyncio.Event()

    async def send(self, event):
        await self.gate.wait()


def test_notify_filter_enqueue_and_direct_test_dispatch_are_traced(trace_events):
    async def scenario():
        manager = NotifierManager(Settings(notify_event_types="order"))
        manager._backends = [_Backend()]
        await manager.start()
        await manager.dispatch({"type": "trade"})
        assert manager.submit({"type": "order"})
        await asyncio.sleep(0)
        await manager.dispatch({"type": "test"}, bypass_filter=True)
        await manager.stop()

    asyncio.run(scenario())
    names = [event["event_name"] for event in trace_events()]
    assert "notify.filter" in names
    assert "notify.enqueue" in names
    assert "notify.send.start" in names
    assert "notify.backend_started" in names


def test_notify_shutdown_records_purged_and_inflight_unknown(trace_events):
    async def scenario():
        manager = NotifierManager(Settings())
        manager._backends = [_BlockingBackend()]
        await manager.start()
        assert manager.submit({"type": "order"})
        assert manager.submit({"type": "order"})
        await asyncio.sleep(0)
        await manager.stop()

    asyncio.run(scenario())
    event = next(event for event in trace_events() if event["event_name"] == "notify.shutdown_drop")
    assert event["outcome"] == "unknown"
    assert event["inflight_canceled_unknown"] is True
    assert event["purged_events"] == 1


def test_notifier_worker_restores_submitter_trace(trace_events):
    async def scenario():
        manager = NotifierManager(Settings())
        manager._backends = [_Backend()]
        await manager.start()
        with bind_context(trace_id="trace-submit", span_id="span-submit"):
            assert manager.submit({"type": "order"})
        await asyncio.sleep(0)
        await manager.stop()

    asyncio.run(scenario())
    send = next(event for event in trace_events() if event["event_name"] == "notify.send.start")
    assert send["trace_id"] == "trace-submit"
