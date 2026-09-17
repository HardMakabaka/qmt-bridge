from __future__ import annotations

from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.trading_client import BigQmtTradingClient
from bigqmt_signal_trader.models import (
    CancelResult,
    OrderSnapshot,
    OrderSubmitResult,
    TradeSnapshot,
)
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from qmt_bridge.server.routers import credit, fund, trading
from qmt_bridge.server.trading.manager import BigQmtTradingManager, TradingWriteDisabled


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

    def stop(self) -> int:
        self.calls.append(("stop",))
        return 0

    def submit_order(self, **kwargs):
        self.calls.append(("submit_order", kwargs))
        return {"order_sys_id": "9001"}


class RecordingRuntime:
    def __init__(self, trader: RecordingTrader) -> None:
        self.trader = trader
        self.ping_payload = {
            "allow_order_methods": True,
            "terminal_real_mode": True,
        }

    def new_trader(self) -> RecordingTrader:
        return self.trader

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


def _facade(client: RecordingClient) -> BigQmtTradingClient:
    return BigQmtTradingClient(account_id="acct-1", client=client)


def test_manager_binds_callback_loop_and_starts_zmq_event_subscription() -> None:
    # Given: a connected Big QMT runtime and an explicit FastAPI event loop token.
    trader = RecordingTrader()
    manager = BigQmtTradingManager(RecordingRuntime(trader), account_id="acct-1")
    event_loop = object()

    # When: the manager establishes the account bridge.
    manager.connect(event_loop=event_loop)

    # Then: callbacks are bound before the direct client starts and connects.
    assert trader.callback._loop is event_loop
    assert trader.calls == [
        ("register_callback",),
        ("start",),
        ("connect",),
    ]


def test_normal_order_uses_the_native_submit_order_contract() -> None:
    trader = RecordingTrader()
    manager = BigQmtTradingManager(RecordingRuntime(trader), account_id="acct-1")
    manager._trader = trader

    result = manager.order("000001.SZ", 23, 100)

    assert result == "9001"
    assert trader.calls[0] == (
        "submit_order",
        {
            "stock_code": "000001.SZ", "order_type": 23, "order_volume": 100,
            "price_type": 5, "price": 0.0, "strategy_name": "", "order_remark": "",
            "account_id": "acct-1",
        },
    )


def test_bigqmt_trading_client_propagates_provider_failures_instead_of_returning_empty_data() -> None:
    # Given: a transport timeout from the Big QMT provider.
    facade = _facade(RecordingClient(fail=True))

    # When/Then: the failure remains distinguishable from a valid empty account result.
    with pytest.raises(TimeoutError, match="provider offline"):
        facade.query_asset()


def test_retained_account_routes_use_only_native_domain_methods() -> None:
    class NativeClient:
        def __init__(self):
            self.calls = []

        def query_positions(self, *, account_id=""):
            self.calls.append(("query_positions", account_id))
            return []

        def query_asset(self, *, account_id=""):
            self.calls.append(("query_asset", account_id))
            return {"cash": 1000.0}

        def query_order(self, order_id, *, account_id=""):
            self.calls.append(("query_order", order_id, account_id))
            return None

        def query_trade(self, trade_id, *, account_id=""):
            self.calls.append(("query_trade", trade_id, account_id))
            return None

        def query_position(self, stock_code, *, account_id=""):
            self.calls.append(("query_position", stock_code, account_id))
            return None

        def query_extension(self, method, params=None, *, account_id=""):
            self.calls.append(("query_extension", method, params or {}, account_id))
            return []

    client = NativeClient()
    manager = BigQmtTradingManager(account_id="acct-1")
    manager._trader = client

    credit.query_credit_positions(manager=manager)
    credit.query_credit_asset(manager=manager)
    credit.query_credit_debt(manager=manager)
    credit.query_slo_stocks(manager=manager)
    credit.query_fin_stocks(manager=manager)
    credit.query_credit_subjects(manager=manager)
    credit.query_credit_assure(manager=manager)
    fund.query_available_fund(manager=manager)
    trading.get_account_status(manager=manager)
    trading.get_account_info(manager=manager)
    trading.query_single_order(1, manager=manager)
    trading.query_single_trade(2, manager=manager)
    trading.query_single_position("000001.SZ", manager=manager)
    trading.query_position_statistics(manager=manager)
    trading.query_new_purchase_limit(manager=manager)
    trading.query_ipo_data(manager=manager)
    trading.query_account_infos(manager=manager)

    assert client.calls == [
        ("query_positions", "acct-1"),
        ("query_asset", "acct-1"),
        ("query_extension", "query_stk_compacts", {}, "acct-1"),
        ("query_extension", "query_credit_slo_code", {}, "acct-1"),
        ("query_extension", "query_credit_subjects", {}, "acct-1"),
        ("query_extension", "query_credit_subjects", {}, "acct-1"),
        ("query_extension", "query_credit_assure", {}, "acct-1"),
        ("query_asset", "acct-1"),
        ("query_extension", "query_account_status", {}, "acct-1"),
        ("query_extension", "query_account_infos", {}, "acct-1"),
        ("query_order", 1, "acct-1"),
        ("query_trade", 2, "acct-1"),
        ("query_position", "000001.SZ", "acct-1"),
        ("query_extension", "query_position_statistics", {}, "acct-1"),
        ("query_extension", "get_new_purchase_limit", {}, "acct-1"),
        ("query_extension", "get_ipo_data", {}, "acct-1"),
        ("query_extension", "query_account_infos", {}, "acct-1"),
    ]


def _handlers(*, qmt_api=None, order_gateway=None, allow_order_methods=False) -> BigQmtRpcHandlers:
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
        allow_order_methods=allow_order_methods,
    )


class ReconciliationGateway:
    def __init__(self, identity_results):
        self.identity_results = iter(identity_results)
        self.submit_calls = []

    def query_submission_identities_strict(self, _account_id, _strategy_name):
        return next(self.identity_results)

    def submit(self, request):
        self.submit_calls.append(request)
        return OrderSubmitResult(
            status="SUBMITTED",
            user_order_id=request.remark,
            order_sys_id=None,
        )


class CancelReconciliationGateway:
    def __init__(self, order_results, cancel_result=None):
        self.order_results = iter(order_results)
        self.cancel_result = cancel_result or CancelResult(True)
        self.cancel_calls = []

    def query_orders_strict(self, _account_id, _strategy_name):
        result = next(self.order_results)
        if isinstance(result, Exception):
            raise result
        return result

    def cancel(self, order_ref):
        self.cancel_calls.append(order_ref)
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result


def _rpc_order_params():
    return {
        "account_id": "acct-1",
        "stock_code": "300308.SZ",
        "order_type": 23,
        "order_volume": 100,
        "price_type": 11,
        "price": 10.0,
        "strategy_name": "MeCoStock",
        "order_remark": "meco00000000000000000001",
        "require_idempotency_check": True,
    }


def _rpc_order(order_sys_id="9001", stock_code="300308.SZ"):
    return OrderSnapshot(
        order_sys_id=order_sys_id,
        user_order_id="meco00000000000000000001",
        stock_code=stock_code,
        action="BUY",
        volume=100,
        traded_volume=0,
        status="50",
        price=10.0,
        strategy_name="MeCoStock",
        remark="meco00000000000000000001",
    )


def test_rpc_single_order_returns_the_exact_reconciled_broker_id() -> None:
    # Given: passorder has no return receipt but QMT later exposes the exact remark.
    gateway = ReconciliationGateway([([], []), ([_rpc_order()], [])])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: the RPC handles one guarded logical submission.
    result = handlers.handle("submit_order", _rpc_order_params())

    # Then: the broker id is written into the response after one provider write.
    assert len(gateway.submit_calls) == 1
    assert result.order_sys_id == "9001"
    assert result.user_order_id == "meco00000000000000000001"
    assert result.status == "CONFIRMED"


def test_rpc_single_order_survives_process_restart_without_duplicate_submit() -> None:
    # Given: QMT already contains an order from a previous server process.
    gateway = ReconciliationGateway([([_rpc_order()], [])])
    restarted_handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: the same stable submission is retried after restart.
    result = restarted_handlers.handle("submit_order", _rpc_order_params())

    # Then: the existing broker order is returned without calling passorder.
    assert gateway.submit_calls == []
    assert result.order_sys_id == "9001"
    assert result.status == "IDEMPOTENT"


def test_rpc_single_order_reconciles_from_trade_identity_after_restart() -> None:
    # Given: the order row is gone but an execution retains the stable identity.
    trade = TradeSnapshot(
        trade_id="trade-1",
        order_sys_id="9001",
        stock_code="300308.SZ",
        action="BUY",
        volume=100,
        price=10.0,
        user_order_id="meco00000000000000000001",
    )
    gateway = ReconciliationGateway([([], [trade])])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: the logical order is retried after service recovery.
    result = handlers.handle("submit_order", _rpc_order_params())

    # Then: the execution proves prior submission and passorder is not replayed.
    assert gateway.submit_calls == []
    assert result.order_sys_id == "9001"
    assert result.status == "IDEMPOTENT"


def test_rpc_single_order_rejects_conflicting_reuse_of_client_identity() -> None:
    # Given: the logical identity already belongs to a different security.
    gateway = ReconciliationGateway([([_rpc_order(stock_code="510300.SH")], [])])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When/Then: the conflict fails closed without calling passorder.
    with pytest.raises(ValueError, match="CLIENT_SUBMIT_ID_CONFLICT"):
        handlers.handle("submit_order", _rpc_order_params())

    assert gateway.submit_calls == []


def test_rpc_confirmed_journal_rejects_conflicting_reuse_in_same_process() -> None:
    # Given: one handler has confirmed and journaled the first logical order.
    gateway = ReconciliationGateway([([], []), ([_rpc_order()], [])])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)
    handlers.handle("submit_order", _rpc_order_params())
    conflicting = dict(_rpc_order_params(), stock_code="510300.SH")

    # When/Then: an in-process identity conflict is not hidden by the journal.
    with pytest.raises(ValueError, match="CLIENT_SUBMIT_ID_CONFLICT"):
        handlers.handle("submit_order", conflicting)

    assert len(gateway.submit_calls) == 1


def test_rpc_unconfirmed_journal_reconciles_later_without_resubmitting(monkeypatch) -> None:
    # Given: the order is initially invisible, then appears on a logical retry.
    gateway = ReconciliationGateway(
        [([], []), ([], []), ([], []), ([], []), ([_rpc_order()], [])]
    )
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)
    monkeypatch.setattr("bigqmt_signal_trader.redis_rpc.time.sleep", lambda _seconds: None)

    # When: the same handler receives the retry after its first unconfirmed result.
    first = handlers.handle("submit_order", _rpc_order_params())
    second = handlers.handle("submit_order", _rpc_order_params())

    # Then: QMT is queried again and passorder remains a one-shot operation.
    assert first.status == "SUBMITTED_UNCONFIRMED"
    assert len(gateway.submit_calls) == 1
    assert second.order_sys_id == "9001"
    assert second.status == "IDEMPOTENT"


def test_rpc_cancel_retry_returns_existing_canceled_state_without_provider_call() -> None:
    # Given: a prior process already moved the broker order to canceled.
    gateway = CancelReconciliationGateway([[_rpc_order(order_sys_id="168")]])
    gateway.order_results = iter([[
        OrderSnapshot(
            order_sys_id="168",
            user_order_id="meco00000000000000000001",
            stock_code="300308.SZ",
            action="BUY",
            volume=100,
            traded_volume=0,
            status="54",
        )
    ]])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: the same cancel is retried after service recovery.
    result = handlers.handle("cancel_order", {
        "account_id": "acct-1",
        "order_sysid": "168",
    })

    # Then: cancellation is acknowledged without calling the provider again.
    assert result.success is True
    assert gateway.cancel_calls == []
    assert result.message == "order already cancel-acknowledged"


def test_rpc_cancel_retry_does_not_touch_a_filled_order() -> None:
    # Given: the broker order has already reached its filled terminal state.
    gateway = CancelReconciliationGateway([[
        OrderSnapshot(
            order_sys_id="168",
            user_order_id="meco00000000000000000001",
            stock_code="300308.SZ",
            action="BUY",
            volume=100,
            traded_volume=100,
            status="56",
        )
    ]])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: the cancel request is retried after reconnecting.
    result = handlers.handle("cancel_order", {
        "account_id": "acct-1",
        "order_sysid": "168",
    })

    # Then: the filled state is returned without another provider cancel.
    assert result.success is False
    assert gateway.cancel_calls == []
    assert result.message == "order is terminal and not cancelable: 56"


def test_rpc_cancel_still_sends_once_when_reconciliation_is_unavailable() -> None:
    # Given: the embedded runtime cannot query current broker state.
    gateway = CancelReconciliationGateway([TimeoutError("query offline")])
    handlers = _handlers(order_gateway=gateway, allow_order_methods=True)

    # When: cancellation is sent using the stable broker order identity.
    result = handlers.handle("cancel_order", {
        "account_id": "acct-1",
        "order_sysid": "168",
    })

    # Then: the provider receives exactly one risk-reducing cancel operation.
    assert result.success is True
    assert len(gateway.cancel_calls) == 1


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
    manager = BigQmtTradingManager(
        RecordingRuntime(trader),
        account_id="acct-1",
        order_writes_enabled=False,
    )
    manager._trader = trader

    with pytest.raises(TradingWriteDisabled, match="bridge_order_writes_disabled"):
        getattr(manager, method)(*args)

    assert not any(
        call[0]
        in {
            "submit_order",
        }
        for call in trader.calls
    )
