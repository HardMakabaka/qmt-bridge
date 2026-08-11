from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


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
    actual = set(dir(BigQmtXtData))

    # Then: every verified provider capability is callable through the facade.
    assert required <= actual


def test_market_data3_uses_the_verified_market_data_ex_contract() -> None:
    # Given: a recording RPC client and a facade.
    client = RecordingClient()
    facade = BigQmtXtData(client)

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
    facade = BigQmtXtData(client)

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
    facade = BigQmtXtData(client)

    # When: supported periods and a security's trading period are requested.
    periods = facade.get_period_list()
    trading_period = facade.get_trading_period("000001.SZ")

    # Then: periods are explicit and trading time uses get_trade_times.
    assert {"tick", "1m", "5m", "1d"} <= set(periods)
    assert trading_period["method"] == "get_trade_times"
