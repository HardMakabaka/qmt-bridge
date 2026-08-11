from __future__ import annotations

import json
import socket
import time
from types import SimpleNamespace

import zmq

from bigqmt_signal_trader.exec_events import (
    EventReplayBuffer,
    ZmqExecutionEventPublisher,
)
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader


def test_event_replay_buffer_assigns_monotonic_cursor_and_replays_after_cursor() -> None:
    # Given: a bounded event buffer for one embedded QMT process epoch.
    buffer = EventReplayBuffer(maxlen=3, epoch="epoch-1")

    # When: four events are published and the oldest event is evicted.
    for index in range(4):
        buffer.append({"event_type": "trade", "trade_id": str(index)})

    replay = buffer.events_since({"epoch": "epoch-1", "sequence": 2})

    # Then: cursors are monotonic and replay contains only events after the cursor.
    assert buffer.cursor() == {"epoch": "epoch-1", "sequence": 4}
    assert [event["cursor"]["sequence"] for event in replay["events"]] == [3, 4]
    assert replay["gap"] is False


def test_event_replay_buffer_reports_gap_for_stale_or_foreign_cursor() -> None:
    # Given: a replay buffer whose retained sequence starts at three.
    buffer = EventReplayBuffer(maxlen=2, epoch="epoch-1")
    for index in range(4):
        buffer.append({"event_type": "order", "order_id": str(index)})

    # When: clients request an evicted cursor or a cursor from an old process epoch.
    stale = buffer.events_since({"epoch": "epoch-1", "sequence": 1})
    restarted = buffer.events_since({"epoch": "old-epoch", "sequence": 99})

    # Then: both responses fail closed with an explicit replay gap.
    assert stale["gap"] is True
    assert restarted["gap"] is True
    assert [event["cursor"]["sequence"] for event in stale["events"]] == [3, 4]


def test_zmq_execution_event_publisher_uses_independent_loopback_pub_sub() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    endpoint = f"tcp://127.0.0.1:{port}"
    publisher = ZmqExecutionEventPublisher(
        bind_address=endpoint,
        replay_buffer=EventReplayBuffer(epoch="epoch-zmq"),
    )
    subscriber = zmq.Context.instance().socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.RCVTIMEO, 2000)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")

    try:
        publisher.start()
        subscriber.connect(endpoint)
        time.sleep(0.1)
        published = publisher.publish(
            {"event_type": "trade", "account_id": "acct-1", "trade_id": "T1"}
        )
        received = json.loads(subscriber.recv().decode("utf-8"))
    finally:
        subscriber.close(linger=0)
        publisher.close()

    assert received == published
    assert received["cursor"] == {"epoch": "epoch-zmq", "sequence": 1}


def test_trader_replays_after_cursor_and_deduplicates_live_event() -> None:
    event = {
        "event_type": "trade",
        "account_id": "acct-1",
        "trade_id": "T2",
        "cursor": {"epoch": "epoch-1", "sequence": 2},
    }

    class ReplayClient:
        account_id = "acct-1"
        execution_event_config = {"transport": "zmq", "zmq": {}}

        def call(self, method, params=None):
            assert method == "get_events_since"
            assert params == {"cursor": {"epoch": "epoch-1", "sequence": 1}}
            return {"events": [event], "gap": False}

    class Callback:
        def __init__(self) -> None:
            self.trades = []

        def on_stock_trade(self, trade) -> None:
            self.trades.append(trade)

    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = ReplayClient()
    trader.callback = Callback()
    trader._event_cursor = {"epoch": "epoch-1", "sequence": 1}

    trader._replay_execution_events()
    trader._dispatch_event(event)

    assert len(trader.callback.trades) == 1
    assert trader.callback.trades[0].trade_id == "T2"
    assert trader._event_cursor == {"epoch": "epoch-1", "sequence": 2}


def test_trader_initial_subscription_replays_retained_events() -> None:
    event = {
        "event_type": "order",
        "account_id": "acct-1",
        "order_id": "O1",
        "cursor": {"epoch": "epoch-1", "sequence": 1},
    }

    class ReplayClient:
        account_id = "acct-1"
        execution_event_config = {"transport": "zmq", "zmq": {}}

        def call(self, method, params=None):
            assert method == "get_events_since"
            assert params == {"cursor": {"epoch": "", "sequence": 0}}
            return {
                "cursor": {"epoch": "epoch-1", "sequence": 1},
                "events": [event],
                "gap": False,
            }

    class Callback:
        def __init__(self) -> None:
            self.orders = []

        def on_stock_order(self, order) -> None:
            self.orders.append(order)

    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = ReplayClient()
    trader.callback = Callback()

    trader._replay_execution_events()

    assert [order.order_id for order in trader.callback.orders] == ["O1"]
    assert trader._event_cursor == {"epoch": "epoch-1", "sequence": 1}


def test_trader_ignores_other_account_events_but_advances_cursor() -> None:
    class Client:
        account_id = "acct-1"

    class Callback:
        def __init__(self) -> None:
            self.trades = []

        def on_stock_trade(self, trade) -> None:
            self.trades.append(trade)

    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = Client()
    trader.callback = Callback()
    trader._dispatch_event(
        {
            "event_type": "trade",
            "account_id": "acct-2",
            "trade_id": "T-foreign",
            "cursor": {"epoch": "epoch-1", "sequence": 3},
        }
    )

    assert trader.callback.trades == []
    assert trader._event_cursor == {"epoch": "epoch-1", "sequence": 3}


def test_zmq_listener_periodically_repairs_dropped_pub_events(monkeypatch) -> None:
    calls = []

    class ReplayClient:
        account_id = "acct-1"

        def call(self, method, params=None):
            calls.append((method, params))
            return {
                "cursor": {"epoch": "epoch-1", "sequence": 1},
                "events": [],
                "gap": False,
            }

    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = ReplayClient()
    trader._event_cursor = {"epoch": "epoch-1", "sequence": 1}
    trader._event_running = True

    class Socket:
        recv_calls = 0

        def setsockopt(self, *_args):
            return None

        def connect(self, _endpoint):
            return None

        def recv(self):
            self.recv_calls += 1
            time.sleep(0.01)
            if self.recv_calls > 1:
                trader._event_running = False
            raise zmq.Again()

        def close(self, linger=0):
            return linger

    socket_instance = Socket()
    context = SimpleNamespace(socket=lambda _kind: socket_instance)
    monkeypatch.setattr(zmq, "Context", SimpleNamespace(instance=lambda: context))

    trader._event_loop_zmq(
        {"zmq": {"connect_address": "tcp://127.0.0.1:15561", "replay_interval_seconds": 0.001}}
    )

    assert [method for method, _params in calls] == [
        "get_events_since",
        "get_events_since",
    ]


def test_trader_repairs_detected_live_sequence_gap_before_dispatch() -> None:
    replay_events = [
        {
            "event_type": "trade",
            "account_id": "acct-1",
            "trade_id": "T2",
            "cursor": {"epoch": "epoch-1", "sequence": 2},
        },
        {
            "event_type": "trade",
            "account_id": "acct-1",
            "trade_id": "T3",
            "cursor": {"epoch": "epoch-1", "sequence": 3},
        },
    ]

    class ReplayClient:
        account_id = "acct-1"

        def call(self, method, params=None):
            assert method == "get_events_since"
            assert params == {"cursor": {"epoch": "epoch-1", "sequence": 1}}
            return {
                "cursor": {"epoch": "epoch-1", "sequence": 3},
                "events": replay_events,
                "gap": False,
            }

    class Callback:
        def __init__(self) -> None:
            self.trades = []

        def on_stock_trade(self, trade) -> None:
            self.trades.append(trade)

    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = ReplayClient()
    trader.callback = Callback()
    trader._event_cursor = {"epoch": "epoch-1", "sequence": 1}

    trader._dispatch_event(replay_events[-1])

    assert [trade.trade_id for trade in trader.callback.trades] == ["T2", "T3"]
    assert trader._event_cursor == {"epoch": "epoch-1", "sequence": 3}
