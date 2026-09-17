import threading
import time

import pytest

from bigqmt_signal_trader.data_client import BigQmtDataClient
from qmt_bridge.server import helpers


def test_budgeted_xtdata_call_does_not_invoke_native_after_lock_wait_expires():
    entered = threading.Event()
    release = threading.Event()
    invoked = []

    def hold_transport_lock():
        with helpers._XTDATA_TRANSPORT._call_lock:
            entered.set()
            release.wait(1)

    holder = threading.Thread(target=hold_transport_lock)
    holder.start()
    assert entered.wait(1)
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            helpers._call_xtdata_serialized_with_budget(
                0.04, lambda _remaining: invoked.append(True)
            )
    finally:
        release.set()
        holder.join(1)

    assert invoked == []
    assert time.monotonic() - started < 0.15


def test_budgeted_xtdata_call_passes_only_remaining_budget_to_native_callback():
    entered = threading.Event()
    release = threading.Event()
    received = []

    def hold_transport_lock():
        with helpers._XTDATA_TRANSPORT._call_lock:
            entered.set()
            release.wait(1)

    holder = threading.Thread(target=hold_transport_lock)
    holder.start()
    assert entered.wait(1)

    def release_later():
        time.sleep(0.03)
        release.set()

    releaser = threading.Thread(target=release_later)
    releaser.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            helpers._call_xtdata_serialized_with_budget(
                0.12,
                lambda remaining: (
                    received.append(remaining),
                    time.sleep(remaining),
                    (_ for _ in ()).throw(TimeoutError("native fixture timeout")),
                )[-1],
            )
    finally:
        release.set()
        holder.join(1)
        releaser.join(1)

    elapsed = time.monotonic() - started
    assert len(received) == 1
    assert 0 < received[0] < 0.12
    assert elapsed < 0.16


def test_scoped_minute_read_passes_budget_as_client_timeout_not_rpc_params():
    class Client:
        def __init__(self):
            self.calls = []

        def call(self, method, params=None, **kwargs):
            self.calls.append((method, params, kwargs))
            return {"000300.SH": []}

    client = Client()
    market_data = BigQmtDataClient(client)

    result = market_data.get_market_data_ex_scoped(
        ["000300.SH"], "20260911110000", "20260911110200", count=3,
        timeout_seconds=0.75,
    )

    assert result == {"000300.SH": []}
    assert client.calls == [(
        "get_market_data_ex_scoped",
        {
            "stock_list": ["000300.SH"],
            "start_time": "20260911110000",
            "end_time": "20260911110200",
            "count": 3,
        },
        {"timeout_seconds": 0.75},
    )]


def test_scoped_minute_read_keeps_existing_client_call_when_no_budget_is_supplied():
    class Client:
        def __init__(self):
            self.calls = []

        def call(self, method, params=None, **kwargs):
            self.calls.append((method, params, kwargs))
            return {}

    client = Client()
    BigQmtDataClient(client).get_market_data_ex_scoped(
        ["000300.SH"], "20260911110000", "20260911110200"
    )

    assert client.calls[0][2] == {}
    assert "timeout_seconds" not in client.calls[0][1]
