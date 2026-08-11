import sys
from pathlib import Path
from types import SimpleNamespace


OVERLAY = Path(__file__).resolve().parents[1] / "src" / "mecostock_bigqmt_mode_overlay.py"


def test_mode_overlay_exposes_current_qmt_request_id() -> None:
    calls = []

    class FakeHandlers:
        def _handle_ping(self, params):
            return {"pong": True}

    module_name = "bigqmt_signal_trader.redis_rpc"
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = SimpleNamespace(BigQmtRpcHandlers=FakeHandlers)
    namespace = {
        "__name__": "mecostock_overlay_test",
        "init": lambda context: calls.append(("init", context)),
        "adjust": lambda context: calls.append(("adjust", context)),
        "handlebar": lambda context: calls.append(("handlebar", context)),
    }
    try:
        exec(compile(OVERLAY.read_bytes(), str(OVERLAY), "exec"), namespace, namespace)
        context = SimpleNamespace(context=SimpleNamespace(request_id="qmt-request-17"))

        namespace["init"](context)
        first = FakeHandlers()._handle_ping({})

        context.context.request_id = "qmt-request-18"
        namespace["adjust"](context)
        second = FakeHandlers()._handle_ping({})
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous

    assert first["qmt_request_id"] == "qmt-request-17"
    assert second["qmt_request_id"] == "qmt-request-18"
    assert [item[0] for item in calls] == ["init", "adjust"]


def test_mode_overlay_skips_unreachable_native_sector_list() -> None:
    # Given: the vendored provider would call the unreachable native quote service.
    native_calls: list[str] = []

    class FakeHandlers:
        def _handle_ping(self, params):
            return {"pong": True}

    class FakeMarketProvider:
        _FALLBACK_SECTORS = ("沪深A股", "沪深ETF")

        def get_sector_list(self):
            native_calls.append("get_sector_list")
            return ["native-result-must-not-be-used"]

    rpc_module_name = "bigqmt_signal_trader.redis_rpc"
    market_module_name = "bigqmt_signal_trader.adapters.market_bigqmt"
    previous_rpc = sys.modules.get(rpc_module_name)
    previous_market = sys.modules.get(market_module_name)
    sys.modules[rpc_module_name] = SimpleNamespace(BigQmtRpcHandlers=FakeHandlers)
    sys.modules[market_module_name] = SimpleNamespace(
        BigQmtMarketDataProvider=FakeMarketProvider,
    )
    namespace = {
        "__name__": "mecostock_overlay_sector_test",
        "init": lambda _context: None,
        "adjust": lambda _context: None,
        "handlebar": lambda _context: None,
    }
    try:
        exec(compile(OVERLAY.read_bytes(), str(OVERLAY), "exec"), namespace, namespace)

        # When: the MeCoStock overlay initializes the embedded runtime.
        namespace["init"](SimpleNamespace())
        sectors = FakeMarketProvider().get_sector_list()
    finally:
        if previous_rpc is None:
            sys.modules.pop(rpc_module_name, None)
        else:
            sys.modules[rpc_module_name] = previous_rpc
        if previous_market is None:
            sys.modules.pop(market_module_name, None)
        else:
            sys.modules[market_module_name] = previous_market

    # Then: sector enumeration returns immediately without invoking native xtdata.
    assert native_calls == []
    assert sectors == ["沪深A股", "沪深ETF"]
