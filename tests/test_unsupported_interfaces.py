from qmt_bridge.server.app import create_app
from qmt_bridge.server.config import Settings


def test_miniqmt_only_routes_are_absent_not_emulated() -> None:
    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    paths = app.openapi()["paths"]
    removed = {
        "/api/financial/field", "/api/formula/generate_index", "/api/futures/sec_main_contract",
        "/api/sector/create_folder", "/api/sector/remove_stocks", "/api/sector/reset",
        "/api/tick/l2_thousand_quote", "/api/tick/l2_thousand_orderbook",
        "/api/tick/l2_thousand_trade", "/api/trading/order_async", "/api/trading/cancel_async",
    }
    assert removed.isdisjoint(paths)
