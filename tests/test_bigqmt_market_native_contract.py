"""Focused Big QMT-only contracts; no MiniQMT signature fallback."""

import pandas as pd

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider


def test_local_data_uses_scalar_context_signature_and_converts_time_mapping():
    class Context:
        def __init__(self):
            self.calls = []

        def get_local_data(self, stock_code, start_time, end_time, period, divid_type, count):
            self.calls.append((stock_code, start_time, end_time, period, divid_type, count))
            return {
                "20260910150000": {"open": 10, "close": 11},
                "20260911150000": {"open": 11, "close": 12},
            }

    context = Context()
    result = BigQmtMarketDataProvider(context).get_local_data(
        stock_list=["000001.SZ", "600000.SH"], start_time="20260910",
        end_time="20260911", period="1d", dividend_type="front", count=2,
    )

    assert context.calls == [
        ("000001.SZ", "20260910", "20260911", "1d", "front", 2),
        ("600000.SH", "20260910", "20260911", "1d", "front", 2),
    ]
    assert isinstance(result["000001.SZ"], pd.DataFrame)
    assert result["000001.SZ"].index.tolist() == ["20260910150000", "20260911150000"]
    assert result["000001.SZ"]["close"].tolist() == [11, 12]


def test_market_data_ex_uses_one_canonical_context_call_without_fallback():
    class Context:
        def __init__(self):
            self.calls = []

        def get_market_data_ex(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {"000001.SZ": []}

    context = Context()
    result = BigQmtMarketDataProvider(context).get_market_data_ex(
        field_list=["close"], stock_list=["000001.SZ"], period="1d",
        start_time="20260901", end_time="20260910", count=3,
        dividend_type="none", fill_data=False,
    )

    assert result == {"000001.SZ": []}
    assert context.calls == [(
        (),
        {
            "fields": ["close"], "stock_code": ["000001.SZ"], "period": "1d",
            "start_time": "20260901", "end_time": "20260910", "count": 3,
            "dividend_type": "none", "fill_data": False, "subscribe": False,
        },
    )]
