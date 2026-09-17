import asyncio

from qmt_bridge.server.config import Settings
from qmt_bridge.server.notify.base import NotifierBackend, NotifierManager


class _SlowBackend(NotifierBackend):
    def __init__(self):
        self.started = False
        self.stopped = False
        self.sent = []
        self.gate = asyncio.Event()

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send(self, event):
        await self.gate.wait()
        self.sent.append(event)

    def name(self):
        return "slow"


def test_notifier_submit_is_bounded_and_stop_cancels_slow_worker():
    async def scenario():
        manager = NotifierManager(Settings())
        backend = _SlowBackend()
        manager._backends = [backend]
        await manager.start()
        assert manager.submit({"type": "order"}) is True
        await asyncio.sleep(0)
        for number in range(300):
            manager.submit({"type": "order", "number": number})
        assert manager.dropped_events > 0
        worker = manager._worker
        await manager.stop()
        assert worker.done()
        assert backend.stopped is True

    asyncio.run(scenario())
