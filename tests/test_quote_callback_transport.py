from types import SimpleNamespace

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.data_client import BigQmtDataClient


class NativeContext:
    def __init__(self):
        self.calls = []
        self.callback = None

    def subscribe_quote(self, stock_code, **kwargs):
        self.calls.append((stock_code, kwargs))
        self.callback = kwargs["callback"]
        return 73

    def unsubscribe_quote(self, seq):
        self.calls.append(("unsubscribe", seq))
        return 0


def _handlers(context):
    provider = BigQmtMarketDataProvider(context)
    return BigQmtRpcHandlers(
        account_id="acct-1",
        market_data=provider,
        position_provider=SimpleNamespace(),
    )


def test_rpc_subscription_invokes_embedded_context_callback(monkeypatch):
    # Given: an embedded ContextInfo with native subscription capability.
    context = NativeContext()
    published = []
    monkeypatch.setattr(
        "bigqmt_signal_trader.exec_events.publish_transient_zmq_event",
        lambda event: published.append(event),
    )

    # When: qmt-server requests a quote subscription and the terminal calls back.
    seq = _handlers(context).handle(
        "subscribe_quote", {"stock_code": "000001.SZ", "period": "tick"}
    )
    context.callback({"000001.SZ": {"lastPrice": 10.5}})

    # Then: the real native seq is returned and a JSON-safe quote event is published.
    assert seq == 73
    assert context.calls[0][0] == "000001.SZ"
    assert context.calls[0][1]["period"] == "tick"
    assert callable(context.calls[0][1]["callback"])
    assert published[0]["event_type"] == "quote"
    assert published[0]["code"] == "000001.SZ"
    assert published[0]["data"]["lastPrice"] == 10.5
    assert published[0]["source_at"]


def test_native_quote_callback_ignores_invalid_payload(monkeypatch):
    # Given: a valid native subscription and a capturing publisher.
    context = NativeContext()
    published = []
    monkeypatch.setattr(
        "bigqmt_signal_trader.exec_events.publish_transient_zmq_event",
        lambda event: published.append(event),
    )
    _handlers(context).handle(
        "subscribe_quote", {"stock_code": "000001.SZ", "period": "tick"}
    )

    # When: QMT supplies malformed callback data.
    context.callback(None)
    context.callback([])

    # Then: the subscription remains safe and no invalid event crosses transport.
    assert published == []


class RpcClientFake:
    def __init__(self):
        self.calls = []
        self.quote_callback = None

    def call(self, method, params=None):
        self.calls.append((method, params))
        return 73 if method == "subscribe_quote" else 0

    def register_quote_callback(self, seq, code, callback):
        self.quote_callback = callback

    def unregister_quote_callback(self, seq):
        self.quote_callback = None


def test_xtdata_subscription_uses_rpc_seq_and_local_event_dispatch():
    # Given: qmt-server's RPC client and a local callback.
    client = RpcClientFake()
    received = []
    market_data = BigQmtDataClient(client)

    # When: it subscribes, receives a transported quote, then unsubscribes.
    seq = market_data.subscribe_quote("000001.SZ", period="tick", callback=received.append)
    client.quote_callback({"code": "000001.SZ", "data": {"lastPrice": 10.5}})
    result = market_data.unsubscribe_quote(seq)

    # Then: it uses the terminal seq, dispatches locally, and releases that seq over RPC.
    assert seq == 73
    assert received[0]["data"]["lastPrice"] == 10.5
    assert client.calls == [
        ("subscribe_quote", {"stock_code": "000001.SZ", "period": "tick"}),
        ("unsubscribe_quote", {"seq": 73}),
    ]
    assert result == 0


def test_rpc_client_keeps_duplicate_code_callbacks_independent(monkeypatch):
    # Given: two native subscription ids for the same security.
    client = RpcClientFake()
    received_first = []
    received_second = []
    client._quote_callbacks = {}
    client._quote_subscription_codes = {}
    monkeypatch.setattr(
        client, "_start_quote_event_listener", lambda: None, raising=False
    )

    # When: each subscription is registered and the first is released.
    from bigqmt_signal_trader.rpc_client import BigQmtRpcClient

    BigQmtRpcClient.register_quote_callback(
        client, 73, "000001.SZ", received_first.append
    )
    BigQmtRpcClient.register_quote_callback(
        client, 74, "000001.SZ", received_second.append
    )
    BigQmtRpcClient.unregister_quote_callback(client, 73)

    # Then: releasing one id cannot remove the other client's callback.
    assert client._quote_callbacks == {74: received_second.append}
    assert client._quote_subscription_codes == {74: "000001.SZ"}
