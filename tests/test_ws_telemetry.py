import asyncio

from qmt_bridge.server.ws.quote_hub import WholeQuoteHub
from bigqmt_signal_trader.telemetry import bind_context


def test_quote_hub_subscription_refcount_and_latest_replacement_are_traced(trace_events):
    async def scenario():
        def get_full_tick(*, code_list):
            return {code_list[0]: {"lastPrice": 1}}

        hub = WholeQuoteHub(get_full_tick)
        key, subscriber = await hub.subscribe(["000001.SZ"], 3)
        subscriber.offer({"first": 1})
        subscriber.offer({"latest": 2})
        await hub.unsubscribe(key, subscriber)

    asyncio.run(scenario())
    events = trace_events()
    names = [event["event_name"] for event in events]
    assert "ws.whole_quote.subscribe" in names
    assert "ws.whole_quote.unsubscribe" in names
    unsubscribe = next(event for event in events if event["event_name"] == "ws.whole_quote.unsubscribe")
    assert unsubscribe["latest_replaced"] >= 1


def test_whole_quote_sampler_uses_independent_group_trace(trace_events):
    async def scenario():
        def get_full_tick(*, code_list):
            return {code_list[0]: {"lastPrice": 1}}

        hub = WholeQuoteHub(get_full_tick)
        with bind_context(trace_id="client-trace", span_id="client-span", ws_session_id="client-session"):
            key, subscriber = await hub.subscribe(["000001.SZ"], 3)
        await asyncio.sleep(0.01)
        await hub.unsubscribe(key, subscriber)

    asyncio.run(scenario())
    poll = next(event for event in trace_events() if event["event_name"] == "ws.whole_quote.native_poll.start")
    assert poll["trace_id"] != "client-trace"
    assert poll["caused_by_trace_link"] == "client-trace"
    assert not poll.get("ws_session_id")
import asyncio
from types import SimpleNamespace

from qmt_bridge.server.config import Settings
from qmt_bridge.server.ws import trade_callback


class _FailingTradeSocket:
    def __init__(self):
        self.headers = {"X-API-Key": "key"}
        self.app = SimpleNamespace(state=SimpleNamespace(settings=Settings(api_key="key")))
        self.accepted = False
        self.receive_cancelled = False

    async def accept(self):
        self.accepted = True

    async def send_json(self, _event):
        raise RuntimeError("peer write failed")

    async def receive_text(self):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.receive_cancelled = True
            raise

    async def close(self, **_kwargs):
        return None


def test_trade_send_failure_ends_blocked_receiver_and_removes_listener(trace_events):
    async def scenario():
        ws = _FailingTradeSocket()
        task = asyncio.create_task(trade_callback.ws_trade(ws))
        for _ in range(20):
            if trade_callback._trade_listeners:
                break
            await asyncio.sleep(0)
        trade_callback.publish_trade_event({"type": "order", "data": {}})
        await asyncio.wait_for(task, timeout=1)
        assert not trade_callback._trade_listeners
        assert ws.receive_cancelled

    asyncio.run(scenario())
    send_errors = [event for event in trace_events() if event["event_name"] == "ws.trade.send" and event.get("outcome") == "error"]
    assert send_errors


def test_trade_publish_extracts_nested_execution_cursor(trace_events):
    trade_callback.publish_trade_event({
        "type": "trade",
        "data": {"cursor": {"epoch": "epoch-1", "sequence": 7}},
    })
    event = next(event for event in trace_events() if event["event_name"] == "ws.trade.publish")
    assert event["event_epoch"] == "epoch-1"
    assert event["event_sequence"] == 7
