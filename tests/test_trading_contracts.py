from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, StockAccount
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from qmt_bridge.server.deps import get_trader_manager
from qmt_bridge.server.routers.smt import router as smt_router
from qmt_bridge.server.security import require_api_key
from qmt_bridge.server.trading.manager import TradingWriteDisabled, XtTraderManager


class RecordingTrader:
    def __init__(self) -> None:
        self.callback = None
        self.calls: list[tuple[object, ...]] = []

    def register_callback(self, callback) -> int:
        self.callback = callback
        self.calls.append(("register_callback",))
        return 0

    def start(self) -> int:
        self.calls.append(("start",))
        return 0

    def connect(self) -> int:
        self.calls.append(("connect",))
        return 0

    def subscribe(self, account) -> int:
        self.calls.append(("subscribe", account.account_id))
        return 0

    def stop(self) -> int:
        self.calls.append(("stop",))
        return 0

    def order_stock(self, *args):
        self.calls.append(("order_stock", *args))
        return "unsafe-normal-order"

    def fund_transfer(self, *args):
        self.calls.append(("fund_transfer", *args))
        return "unsafe-generic-transfer"

    def query_smt_secu_rate(self, *args):
        self.calls.append(("query_smt_secu_rate", *args))
        return ["rate"]


class RecordingRuntime:
    def __init__(self, trader: RecordingTrader) -> None:
        self.trader = trader
        self.ping_payload = {
            "allow_order_methods": True,
            "terminal_real_mode": True,
        }

    def new_trader(self) -> RecordingTrader:
        return self.trader

    def new_account(self, account_id: str):
        return SimpleNamespace(account_id=account_id)

    def probe(self):
        return self.ping_payload


class RecordingClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.account_id = "acct-1"
        self.fail = fail
        self.calls: list[tuple[str, dict, str]] = []

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        del timeout_seconds
        self.calls.append((method, params or {}, account_id or ""))
        if self.fail:
            raise TimeoutError("provider offline")
        return [{"method": method}]


def _facade(client: RecordingClient) -> BigQmtXtTrader:
    trader = BigQmtXtTrader(account_id="acct-1", redis_config={})
    trader.client = client
    return trader


def test_manager_binds_callback_loop_and_starts_zmq_event_subscription() -> None:
    # Given: a connected Big QMT runtime and an explicit FastAPI event loop token.
    trader = RecordingTrader()
    manager = XtTraderManager(RecordingRuntime(trader), account_id="acct-1")
    event_loop = object()

    # When: the manager establishes the account bridge.
    manager.connect(event_loop=event_loop)

    # Then: callbacks are bound before the ZMQ listener is started and subscribed.
    assert trader.callback._loop is event_loop
    assert trader.calls == [
        ("register_callback",),
        ("start",),
        ("connect",),
        ("subscribe", "acct-1"),
    ]


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("credit_order", ("000001.SZ", 23, 100)),
        ("smt_order", ("000001.SZ", 23, 100)),
        ("bank_transfer", (0, 1000.0)),
        ("ctp_fund_transfer", (0, 1000.0)),
    ],
)
def test_specialized_writes_never_fall_back_to_normal_order_or_generic_fund_transfer(
    method: str,
    args: tuple[object, ...],
) -> None:
    # Given: a provider exposing only the unsafe generic write methods.
    trader = RecordingTrader()
    manager = XtTraderManager(RecordingRuntime(trader), account_id="acct-1")
    manager._trader = trader
    manager._account = SimpleNamespace(account_id="acct-1")

    # When: a specialized write capability is requested.
    result = getattr(manager, method)(*args)

    # Then: the bridge reports unsupported and performs no provider write.
    assert result["status"] == "unsupported"
    assert result["retryable"] is False
    assert not any(call[0] in {"order_stock", "fund_transfer"} for call in trader.calls)


def test_normal_order_keeps_the_verified_order_stock_contract() -> None:
    trader = RecordingTrader()
    manager = XtTraderManager(RecordingRuntime(trader), account_id="acct-1")
    manager._trader = trader
    manager._account = SimpleNamespace(account_id="acct-1")

    result = manager.order("000001.SZ", 23, 100)

    assert result == "unsafe-normal-order"
    assert trader.calls[0][0] == "order_stock"


def test_smt_rate_contract_forwards_all_provider_parameters() -> None:
    # Given: an SMT-capable provider and the six-argument Big QMT contract.
    trader = RecordingTrader()
    manager = XtTraderManager(RecordingRuntime(trader), account_id="acct-1")
    manager._trader = trader
    manager._account = SimpleNamespace(account_id="acct-1")

    # When: the bridge queries a security rate.
    result = manager.query_smt_secu_rate(
        stock_code="600000.SH",
        max_term=30,
        fare_way=1,
        credit_type=2,
        trade_type=3,
    )

    # Then: no parameter is dropped or reinterpreted.
    assert result == ["rate"]
    assert trader.calls == [
        (
            "query_smt_secu_rate",
            manager._account,
            "600000.SH",
            30,
            1,
            2,
            3,
        )
    ]


def test_smt_cancel_http_contract_accepts_client_json_body() -> None:
    calls = []
    manager = SimpleNamespace(
        cancel_smt_order=lambda **kwargs: calls.append(kwargs) or 0,
    )
    app = FastAPI()
    app.include_router(smt_router)
    app.dependency_overrides[get_trader_manager] = lambda: manager
    app.dependency_overrides[require_api_key] = lambda: None

    response = TestClient(app).post(
        "/api/smt/cancel",
        json={"order_id": 42, "account_id": "acct-1"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "data": 0}
    assert calls == [{"order_id": 42, "account_id": "acct-1"}]


def test_bigqmt_facade_propagates_provider_failures_instead_of_returning_empty_data() -> None:
    # Given: a transport timeout from the Big QMT provider.
    facade = _facade(RecordingClient(fail=True))

    # When/Then: the failure remains distinguishable from a valid empty account result.
    with pytest.raises(TimeoutError, match="provider offline"):
        facade.query_account_infos(StockAccount("acct-1"))


def test_bigqmt_facade_uses_verified_ipo_methods() -> None:
    # Given: a recording Big QMT provider client.
    client = RecordingClient()
    facade = _facade(client)
    account = StockAccount("acct-1")

    # When: IPO data and purchase limits are queried.
    facade.query_ipo_data(account)
    facade.query_new_purchase_limit(account)

    # Then: each facade method targets its exact provider function.
    assert [call[0] for call in client.calls] == [
        "get_ipo_data",
        "get_new_purchase_limit",
    ]


def _handlers(*, qmt_api=None, order_gateway=None) -> BigQmtRpcHandlers:
    position_provider = SimpleNamespace(
        get_asset=lambda _account_id: {},
        get_positions=lambda _account_id: {},
    )
    return BigQmtRpcHandlers(
        account_id="acct-1",
        market_data=SimpleNamespace(),
        position_provider=position_provider,
        order_gateway=order_gateway,
        qmt_api=qmt_api,
    )


def test_rpc_missing_global_and_trade_detail_capabilities_are_not_empty_results() -> None:
    handlers = _handlers()

    with pytest.raises(NotImplementedError, match="get_ipo_data"):
        handlers.handle("get_ipo_data", {"account_id": "acct-1"})
    with pytest.raises(NotImplementedError, match="get_trade_detail_data"):
        handlers.handle("query_account_infos", {"account_id": "acct-1"})


def test_rpc_provider_failure_is_not_swallowed_as_empty_data() -> None:
    def offline(_account_id):
        raise TimeoutError("provider offline")

    handlers = _handlers(qmt_api={"get_ipo_data": offline})

    with pytest.raises(TimeoutError, match="provider offline"):
        handlers.handle("get_ipo_data", {"account_id": "acct-1"})


@pytest.mark.parametrize(
    "method",
    [
        "query_account_status",
        "query_appointment_info",
        "query_smt_secu_info",
        "query_smt_secu_rate",
    ],
)
def test_unverified_trading_query_aliases_are_explicitly_unsupported(method: str) -> None:
    handlers = _handlers(
        qmt_api={
            "get_ipo_data": lambda _account_id: ["ipo"],
            "get_option_subject_position": lambda _account_id: ["option"],
            "get_comb_option": lambda _account_id: ["combination"],
        }
    )

    with pytest.raises(NotImplementedError, match=method):
        handlers.handle(method, {"account_id": "acct-1"})


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("order", ("000001.SZ", 23, 100)),
        ("cancel_order", (1,)),
        ("cancel_order_sysid", ("sys-1", 0)),
        ("credit_order", ("000001.SZ", 23, 100)),
        ("fund_transfer", (0, 100.0)),
        ("ctp_fund_transfer", (0, 100.0)),
        ("bank_transfer", (0, 100.0)),
        ("smt_order", ("000001.SZ", 23, 100)),
        ("smt_negotiate_order_async", ("000001.SZ", 23, 100)),
        ("cancel_smt_order", (1,)),
        ("order_async", ("000001.SZ", 23, 100)),
        ("cancel_order_async", (1,)),
        ("cancel_order_sysid_async", ("sys-1", 0)),
        ("ctp_transfer_option_to_future", (100.0,)),
        ("ctp_transfer_future_to_option", (100.0,)),
        (
            "sync_transaction_from_external",
            ("append", "DEAL", [], "STOCK"),
        ),
    ],
)
def test_bridge_write_gate_blocks_every_write_surface(
    method: str,
    args: tuple[object, ...],
) -> None:
    trader = RecordingTrader()
    manager = XtTraderManager(
        RecordingRuntime(trader),
        account_id="acct-1",
        order_writes_enabled=False,
    )
    manager._trader = trader
    manager._account = SimpleNamespace(account_id="acct-1")

    with pytest.raises(TradingWriteDisabled, match="bridge_order_writes_disabled"):
        getattr(manager, method)(*args)

    assert not any(
        call[0]
        in {
            "order_stock",
            "fund_transfer",
            "credit_order",
            "smt_order",
            "bank_transfer",
        }
        for call in trader.calls
    )
