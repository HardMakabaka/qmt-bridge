from bigqmt_signal_trader.data_client import BigQmtDataClient
import pandas as pd
import pytest
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import READ_METHODS, MARKET_DATA_METHODS


class MinuteSubscriptionContext:
    def __init__(self):
        self.active = {}
        self.cached = set()
        self.next_seq = 0
        self.reads = []
        self.fail_read = False

    def subscribe_quote(self, stock_code, **kwargs):
        self.next_seq += 1
        self.active[self.next_seq] = stock_code
        return self.next_seq

    def unsubscribe_quote(self, seq):
        del self.active[seq]

    def get_market_data_ex_ori(self, fields, stock_code, period, start_time,
                               end_time, count, dividend_type, fill_data=True,
                               subscribe=True):
        self.reads.append((subscribe, fill_data, set(self.active.values())))
        if self.fail_read:
            raise RuntimeError("read failed")
        visible = set(self.cached)
        if subscribe:
            visible.update(self.active.values())
            self.cached.update(self.active.values())
        return {code: [{"stime": end_time, "close": 10, "volume": 3}]
                if code in visible else [] for code in stock_code}


def test_scoped_minute_rpc_is_read_only_and_facade_routes_to_it():
    assert "get_market_data_ex_scoped" in READ_METHODS & MARKET_DATA_METHODS
    client = RecordingClient()
    BigQmtDataClient(client).get_market_data_ex_scoped(stock_list=["000001.SZ"],
                                                start_time="20260910093000", end_time="20260910093200", count=3)
    assert client.calls[-1][0] == "get_market_data_ex_scoped"


@pytest.mark.parametrize("fail_read", [False, True])
def test_scoped_minute_reads_hold_native_subscription_and_always_release(fail_read):
    context = MinuteSubscriptionContext()
    context.fail_read = fail_read
    provider = BigQmtMarketDataProvider(context)
    request = dict(stock_list=["000001.SZ", "600000.SH"], start_time="20260910093000",
                   end_time="20260910093200", count=3)
    if fail_read:
        with pytest.raises(RuntimeError, match="read failed"):
            provider.get_market_data_ex_scoped(**request)
    else:
        for _ in range(3):
            data = provider.get_market_data_ex_scoped(**request)
            assert data["000001.SZ"]["records"][0]["stime"] == request["end_time"]
            assert data["000001.SZ"]["records"][0]["close"] == 10
    assert context.active == {}
    if fail_read:
        assert context.next_seq == 0
    else:
        # The cold call subscribes once; both warm calls use the cache-only read.
        assert context.next_seq == 2
        assert context.reads[0] == (False, False, set())
        assert context.reads[1] == (
            True, False, {"000001.SZ", "600000.SH"}
        )
        assert context.reads[2:] == [
            (False, False, set()),
            (False, False, set()),
        ]


def test_scoped_minute_rejects_oversized_batch_before_subscribing():
    context = MinuteSubscriptionContext()
    with pytest.raises(ValueError, match="batch_limit"):
        BigQmtMarketDataProvider(context).get_market_data_ex_scoped(
            stock_list=["%06d.SZ" % index for index in range(21)],
            start_time="20260910093000", end_time="20260910093200", count=3)
    assert context.next_seq == 0


@pytest.mark.parametrize("end_time", ["20000101150000", "20990101150000"])
def test_explicit_window_read_does_not_acquire_implicit_live_subscriptions(end_time):
    context = MinuteSubscriptionContext()
    BigQmtMarketDataProvider(context).get_market_data_ex(
        stock_list=["000001.SZ"], start_time=end_time[:8] + "093000", end_time=end_time)
    assert context.reads == [(False, True, set())]


def test_scoped_minute_subscribes_only_cache_misses_and_merges_ready_rows():
    context = MinuteSubscriptionContext()
    context.cached.add("000001.SZ")

    data = BigQmtMarketDataProvider(context).get_market_data_ex_scoped(
        ["000001.SZ", "600000.SH"],
        "20260910093000",
        "20260910093200",
        3,
    )

    assert context.next_seq == 1
    assert context.reads == [
        (False, False, set()),
        (True, False, {"600000.SH"}),
    ]
    assert data["000001.SZ"]["records"][0]["stime"] == "20260910093200"
    assert data["600000.SH"]["records"][0]["close"] == 10


def test_scoped_minute_waits_for_subscription_payload_before_release(monkeypatch):
    context = MinuteSubscriptionContext()
    read = context.get_market_data_ex_ori

    def delayed(**kwargs):
        data = read(**kwargs)
        return {code: [] for code in data} if len(context.reads) < 3 else data

    context.get_market_data_ex_ori = delayed
    monkeypatch.setattr("bigqmt_signal_trader.adapters.market_bigqmt.time.sleep", lambda _delay: None)
    BigQmtMarketDataProvider(context).get_market_data_ex_scoped(
        ["000001.SZ"], "20260910093000", "20260910093200", 3)
    assert len(context.reads) == 3
    assert context.active == {}


def test_scoped_minute_partial_subscribe_failure_releases_earlier_owned_ids():
    context = MinuteSubscriptionContext()
    subscribe = context.subscribe_quote
    context.subscribe_quote = lambda code, **kwargs: 0 if context.active else subscribe(code, **kwargs)
    with pytest.raises(RuntimeError, match="native_minute_subscription_unavailable"):
        BigQmtMarketDataProvider(context).get_market_data_ex_scoped(
            ["000001.SZ", "600000.SH"], "20260910093000", "20260910093200", 3)
    assert context.active == {}


def test_scoped_minute_empty_subscription_has_bounded_wait_and_releases(monkeypatch):
    context = MinuteSubscriptionContext()
    context.get_market_data_ex_ori = lambda **_kwargs: {"000001.SZ": []}
    times = iter([0.0, 1.0, 2.0])
    monkeypatch.setattr("bigqmt_signal_trader.adapters.market_bigqmt.time.monotonic", lambda: next(times))
    monkeypatch.setattr("bigqmt_signal_trader.adapters.market_bigqmt.time.sleep", lambda _delay: None)
    result = BigQmtMarketDataProvider(context).get_market_data_ex_scoped(
        ["000001.SZ"], "20260910093000", "20260910093200", 3)
    assert result["000001.SZ"]["records"] == []
    assert context.active == {}


@pytest.mark.parametrize("method", ["get_market_data_ex", "get_market_data_ex_ori"])
def test_bigqmt_keyword_shape_preserves_fill_data_false(method):
    class Context:
        def get_market_data_ex(self, fields, stock_code, period, start_time,
                               end_time, count, dividend_type, fill_data=True):
            return fill_data

        get_market_data_ex_ori = get_market_data_ex

    provider = BigQmtMarketDataProvider(Context())
    shapes = provider._market_data_shapes(method, fill_data=False)
    assert provider._call_first_supported(shapes) is False


def test_no_fill_drops_suspended_zero_activity_padding_before_cache_write(monkeypatch):
    frame = pd.DataFrame({"close": [10, 10, 11], "volume": [0, 0, 2],
                          "amount": [0, 0, 22], "suspendFlag": [1, 0, 1]})
    class Client:
        def call(self, *_args, **_kwargs):
            return {"000001.SZ": frame}

    facade = BigQmtDataClient(Client())
    cached = []
    class Cache:
        def write(self, _code, _period, value, **_kwargs):
            cached.append(value)
    monkeypatch.setattr(facade, "_local_cache", lambda: Cache())
    actual = facade.get_market_data_ex(stock_list=["000001.SZ"], fill_data=False)
    assert actual["000001.SZ"].index.tolist() == [1, 2]
    assert cached[0].index.tolist() == [1, 2]
    assert len(facade.get_market_data_ex(fill_data=True)["000001.SZ"]) == 3
    assert len(frame) == 3


def test_bigqmt_download_uses_the_native_single_symbol_contract(monkeypatch):
    facade = BigQmtDataClient(RecordingClient())
    monkeypatch.setattr(facade, "get_market_data_ex", lambda **_kwargs: pytest.fail("cache read is not a download"))
    facade.download_history_data("600519.SH", "1d")
    assert facade.client.calls[-1] == (
        "download_history_data",
        {"stock_code": "600519.SH", "period": "1d", "start_time": "", "end_time": ""},
    )


class RecordingClient:
    local_cache_config = {}

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def call(self, method, params=None, **_kwargs):
        self.calls.append((method, params or {}))
        if method == "get_market_data_ex":
            return {"000001.SZ": "frame"}
        if method == "get_instrument_detail":
            return {"code": params["code"]}
        if method == "call_formula":
            return {"stock": params["stock_code"]}
        return {"method": method, "params": params or {}}


def test_data_facade_exposes_every_verified_native_adapter() -> None:
    # Given: the real Big QMT client facade class.
    required = {
        "download_cb_data",
        "download_history_contracts",
        "download_index_weight",
        "download_sector_data",
        "get_basket",
        "get_bvol",
        "get_contract_expire_date",
        "get_contract_multiplier",
        "get_etf_iopv",
        "get_hkt_exchange_rate",
        "get_industry_name_of_stock",
        "get_last_volume",
        "get_market_time",
        "get_open_date",
        "get_risk_free_rate",
        "get_svol",
        "get_total_share",
        "getfindata",
        "is_suspended_stock",
    }

    # When: its public methods are inspected.
    actual = set(dir(BigQmtDataClient))

    # Then: every verified provider capability is callable through the facade.
    assert required <= actual


def test_market_data3_uses_the_verified_market_data_ex_contract() -> None:
    # Given: a recording RPC client and a facade.
    client = RecordingClient()
    facade = BigQmtDataClient(client)

    # When: market_data3 is requested.
    result = facade.get_market_data3(
        field_list=["close"], stock_list=["000001.SZ"], period="1d"
    )

    # Then: it uses the installed version's market_data_ex RPC shape.
    assert result == {"000001.SZ": "frame"}
    assert client.calls == [
        (
            "get_market_data_ex",
            {
                "field_list": ["close"],
                "stock_list": ["000001.SZ"],
                "period": "1d",
                "start_time": "",
                "end_time": "",
                "count": -1,
                "dividend_type": "none",
                "fill_data": True,
            },
        )
    ]


def test_derived_facade_methods_preserve_per_item_results() -> None:
    # Given: a facade backed by deterministic RPC responses.
    client = RecordingClient()
    facade = BigQmtDataClient(client)

    # When: batch details and batch formulas are requested.
    details = facade.get_instrument_detail_list(["000001.SZ", "600000.SH"])
    formulas = facade.call_formula_batch(
        "MA", ["000001.SZ", "600000.SH"], "1d"
    )

    # Then: each requested security keeps its own result.
    assert details == {
        "000001.SZ": {"code": "000001.SZ"},
        "600000.SH": {"code": "600000.SH"},
    }
    assert formulas == {
        "000001.SZ": {"status": "ok", "data": {"stock": "000001.SZ"}},
        "600000.SH": {"status": "ok", "data": {"stock": "600000.SH"}},
    }


def test_period_and_trading_period_are_safe_derived_capabilities() -> None:
    # Given: a facade whose RPC records the requested native method.
    client = RecordingClient()
    facade = BigQmtDataClient(client)

    # When: supported periods and a security's trading period are requested.
    periods = facade.get_period_list()
    trading_period = facade.get_trading_period("000001.SZ")

    # Then: periods are explicit and trading time uses get_trade_times.
    assert {"tick", "1m", "5m", "1d"} <= set(periods)
    assert trading_period["method"] == "get_trade_times"
