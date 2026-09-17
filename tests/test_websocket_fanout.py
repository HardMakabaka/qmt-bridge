import asyncio

from qmt_bridge.server.ws.quote_hub import RealtimeQuoteHub, WholeQuoteHub


def test_whole_quote_hub_shares_polling_and_releases_last_subscriber():
    calls = []

    def get_full_tick(*, code_list):
        calls.append(code_list)
        return {code_list[0]: {"lastPrice": 1}}

    async def scenario():
        hub = WholeQuoteHub(get_full_tick)
        key1, one = await hub.subscribe(["000001.SZ"], 3)
        key2, two = await hub.subscribe(["000001.SZ"], 3)
        await asyncio.sleep(0.01)
        assert calls == [["000001.SZ"]]
        assert one.queue.get_nowait() == two.queue.get_nowait()
        await hub.unsubscribe(key1, one)
        assert key2 in hub._groups
        await hub.unsubscribe(key2, two)
        assert not hub._groups

    asyncio.run(scenario())


def test_realtime_hub_reference_counts_native_subscriptions():
    class Facade:
        def __init__(self):
            self.callbacks = []
            self.subscribed = []
            self.unsubscribed = []

        def subscribe_quote(self, code, *, period, callback):
            self.subscribed.append((code, period))
            self.callbacks.append(callback)
            return len(self.subscribed)

        def unsubscribe_quote(self, seq):
            self.unsubscribed.append(seq)

    async def scenario():
        facade = Facade()
        hub = RealtimeQuoteHub(facade)
        ids_one, one = await hub.subscribe(["000001.SZ"], "tick")
        ids_two, two = await hub.subscribe(["000001.SZ"], "tick")
        assert ids_one == ids_two == [1]
        assert facade.subscribed == [("000001.SZ", "tick")]
        facade.callbacks[0]({"code": "000001.SZ", "data": {"lastPrice": 10}})
        await asyncio.sleep(0)
        assert one.queue.get_nowait()["data"]["lastPrice"] == 10
        assert two.queue.get_nowait()["data"]["lastPrice"] == 10
        await hub.unsubscribe(["000001.SZ"], "tick", one)
        assert not facade.unsubscribed
        await hub.unsubscribe(["000001.SZ"], "tick", two)
        assert facade.unsubscribed == [1]

    asyncio.run(scenario())


def test_whole_quote_hub_notifies_and_restarts_after_upstream_failure():
    calls = 0

    def get_full_tick(*, code_list):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("QMT disconnected")
        return {code_list[0]: {"lastPrice": 10}}

    async def scenario():
        hub = WholeQuoteHub(get_full_tick)
        key, failed = await hub.subscribe(["000001.SZ"], 3)
        await asyncio.sleep(0.01)
        assert failed.queue.get_nowait()["reason"] == "native_whole_quote_unavailable"
        assert key not in hub._groups
        key, recovered = await hub.subscribe(["000001.SZ"], 3)
        await asyncio.sleep(0.01)
        assert recovered.queue.get_nowait()["000001.SZ"]["lastPrice"] == 10
        await hub.unsubscribe(key, recovered)

    asyncio.run(scenario())
