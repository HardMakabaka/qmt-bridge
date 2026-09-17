from types import SimpleNamespace

import pytest

from qmt_bridge.server.app import create_app
from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import _call_xtdata_optional, _status_payload


def test_registry_covers_every_public_http_and_websocket_operation() -> None:
    # Given: the complete application surface with trading routes enabled.
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    openapi = app.openapi()

    # When: the runtime capability registry is built.
    registry = build_capability_registry(app, runtime=None)

    # Then: every public operation has one registry record.
    expected_http_operations = sum(
        method.lower() in {"get", "post", "put", "patch", "delete"}
        for path_item in openapi["paths"].values()
        for method in path_item
    )
    websocket_paths = {item.path for item in registry if item.surface == "websocket"}
    assert websocket_paths == {
        "/ws/formula",
        "/ws/realtime",
        "/ws/trade",
        "/ws/whole_quote",
    }
    assert len(registry) == expected_http_operations + len(websocket_paths)
    assert len({(item.method, item.path) for item in registry}) == len(registry)


def test_registry_marks_known_unverified_native_capabilities_unsupported() -> None:
    # Given: a runtime facade that lacks unverified L2 and formula push methods.
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    app.state.trader_manager = SimpleNamespace()
    runtime = SimpleNamespace(market_data=SimpleNamespace(), ping_payload={"rpc_revision": "test"})

    # When: capabilities are probed against that runtime.
    registry = build_capability_registry(app, runtime=runtime)
    indexed = {(item.method, item.path): item for item in registry}

    # Then: native but unverified capabilities remain explicit; MiniQMT shims do not.
    assert indexed[("POST", "/api/download/jobs")].status == "unsupported"
    assert indexed[("POST", "/api/download/jobs")].reason_code == "native_history_download_unavailable"
    assert indexed[("GET", "/api/download/jobs/{job_id}")].status != "unsupported"
    assert indexed[("WEBSOCKET", "/ws/formula")].status == "unsupported"
    assert ("GET", "/api/tick/l2_thousand_quote") not in indexed
    assert ("WEBSOCKET", "/ws/l2_thousand") not in indexed


def test_registry_excludes_miniqmt_only_account_surface() -> None:
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    app.state.trader_manager = SimpleNamespace()
    runtime = SimpleNamespace(market_data=SimpleNamespace(), ping_payload={"rpc_revision": "test"})
    registry = build_capability_registry(app, runtime=runtime)
    indexed = {(item.method, item.path): item for item in registry}

    retained_unverified_paths = {
        "/api/smt/appointment",
        "/api/smt/secu_info",
        "/api/smt/secu_rate",
    }

    actual_paths = {
        item.path
        for item in registry
        if item.domain in {"bank", "credit", "fund", "smt", "trading"}
        and item.status == "unsupported"
    }
    assert actual_paths == retained_unverified_paths
    removed_paths = {
        "/api/bank/available_amount", "/api/bank/balance", "/api/bank/banks",
        "/api/bank/status", "/api/bank/transfer_in", "/api/bank/transfer_limit",
        "/api/bank/transfer_out", "/api/bank/transfer_records", "/api/credit/available_amount",
        "/api/credit/order", "/api/fund/ctp_balance", "/api/fund/ctp_future_to_option",
        "/api/fund/ctp_option_to_future", "/api/fund/ctp_transfer_in", "/api/fund/ctp_transfer_out",
        "/api/fund/transfer", "/api/fund/transfer_records", "/api/smt/cancel",
        "/api/smt/compact", "/api/smt/negotiate_order_async", "/api/smt/order",
        "/api/smt/quoter", "/api/trading/cancel_async", "/api/trading/com_fund",
        "/api/trading/com_position", "/api/trading/export_data", "/api/trading/order_async",
        "/api/trading/query_data",
    }
    assert removed_paths.isdisjoint({item.path for item in registry})
    assert indexed[("POST", "/api/trading/sync_transaction")].status == "ok"
    assert indexed[("GET", "/api/trading/account_status")].mode == "derived"


def test_registry_marks_runtime_dependent_routes_unavailable_without_runtime() -> None:
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings())
    registry = build_capability_registry(app, runtime=None)
    indexed = {(item.method, item.path): item for item in registry}

    assert indexed[("GET", "/api/history")].status == "unavailable"
    assert indexed[("GET", "/api/meta/health")].status == "ok"
    assert indexed[("GET", "/api/meta/recovery-status")].status == "ok"


@pytest.mark.parametrize("available,expected", [(True, "ok"), (False, "unsupported"), (None, "unsupported"), ("true", "unsupported")])
def test_history_download_requires_native_global_function_attestation(available, expected) -> None:
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings())
    runtime = SimpleNamespace(
        market_data=SimpleNamespace(download_history_data=lambda *args: None),
        ping_payload={"native_history_download_available": available},
    )
    indexed = {(item.method, item.path): item for item in build_capability_registry(app, runtime)}
    assert indexed[("POST", "/api/download/jobs")].status == expected
    assert indexed[("POST", "/api/download/jobs")].mode == "derived"
    assert indexed[("POST", "/api/download")].status == expected
    assert indexed[("POST", "/api/download")].provider_method == "download_history_data"


def test_registry_marks_account_routes_unavailable_without_connected_manager() -> None:
    from qmt_bridge.server.capabilities import build_capability_registry

    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    app.state.trader_manager = None
    runtime = SimpleNamespace(market_data=SimpleNamespace(), ping_payload={"rpc_revision": "test"})
    registry = build_capability_registry(app, runtime=runtime)
    indexed = {(item.method, item.path): item for item in registry}

    sync = indexed[("POST", "/api/trading/sync_transaction")]
    assert sync.status == "unavailable"
    assert sync.reason_code == "bigqmt_account_runtime_unavailable"
    assert indexed[("GET", "/api/trading/health")].status == "ok"


def test_optional_call_returns_stable_machine_readable_failure_contract() -> None:
    # Given: a provider without the requested native method.
    provider = SimpleNamespace()

    # When: an optional provider capability is called.
    payload = _call_xtdata_optional(provider, "get_total_share", "000001.SZ")

    # Then: the response distinguishes unsupported from empty business data.
    assert payload == {
        "status": "unsupported",
        "data": None,
        "reason": "xtdata_get_total_share_missing",
        "reason_code": "xtdata_get_total_share_missing",
        "message": "xtdata_get_total_share_missing",
        "capability": "get_total_share",
        "provider": "bigqmt",
        "retryable": False,
        "details": {},
        "function": "get_total_share",
    }


def test_unavailable_status_is_retryable_but_unsupported_is_not() -> None:
    # Given: the two stable provider failure classes.
    unsupported = _status_payload(
        "unsupported", reason="native_method_missing", function="native_method"
    )
    unavailable = _status_payload(
        "unavailable", reason="terminal_offline", function="native_method"
    )

    # When/Then: clients can make a deterministic retry decision.
    assert unsupported["retryable"] is False
    assert unavailable["retryable"] is True
