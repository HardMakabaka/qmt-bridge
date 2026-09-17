from __future__ import annotations

from dataclasses import asdict, dataclass
from collections.abc import Iterable, Iterator
from typing import Literal, Protocol, cast, runtime_checkable

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute, APIWebSocketRoute
from starlette.routing import BaseRoute

CapabilityMode = Literal["native", "derived", "unsupported"]
CapabilityStatus = Literal["ok", "unavailable", "unsupported"]
CapabilitySurface = Literal["http", "websocket"]


class RuntimeFacade(Protocol):
    market_data: ProviderFacade | None


class ProviderFacade(Protocol):
    def __dir__(self) -> list[str]: ...


@runtime_checkable
class IncludedRouter(Protocol):
    original_router: APIRouter


@dataclass(frozen=True, slots=True)
class CapabilitySpec:
    name: str
    surface: CapabilitySurface
    domain: str
    path: str
    method: str
    provider_method: str | None
    mode: CapabilityMode
    write_effect: bool
    thread_affinity: str
    status: CapabilityStatus
    reason_code: str | None

    def to_payload(self) -> dict[str, str | bool | None]:
        return asdict(self)


_UNSUPPORTED_PATHS = {
    "/api/smt/appointment": "bigqmt_query_appointment_info_unverified",
    "/api/smt/secu_info": "bigqmt_query_smt_secu_info_unverified",
    "/api/smt/secu_rate": "bigqmt_query_smt_secu_rate_unverified",
    "/api/download/ipo_data": "xtdata_download_ipo_data_missing",
    "/api/download/option_data": "xtdata_download_option_data_missing",
    "/api/market/fullspeed_orderbook": "xtdata_get_fullspeed_orderbook_missing",
    "/api/market/transactioncount": "xtdata_get_transactioncount_missing",
    "/ws/formula": "bigqmt_formula_push_not_verified",
}

_DERIVED_PATHS = {
    "/api/download/jobs": "download_history_data",
    "/api/instrument/batch_detail": "get_instrument_detail_list",
    "/api/market/market_data3": "get_market_data3",
    "/api/market/full_kline": "get_full_kline",
    "/api/market/minute_tail": "get_market_data_ex",
    "/api/meta/periods": "get_period_list",
    "/api/calendar/trading_period": "get_trading_period",
    "/api/formula/call_batch": "call_formula_batch",
    "/api/financial/table_list": "get_financial_table_list",
    "/api/trading/account_status": None,
}

_PROVIDER_METHODS = {
    **_DERIVED_PATHS,
    "/api/download": "download_history_data",
    "/api/instrument/total_share": "get_total_share",
    "/api/financial/findata": "getfindata",
    "/api/etf/iopv": "get_etf_iopv",
    "/api/utility/industry_name": "get_industry_name_of_stock",
    "/api/utility/market_time": "get_market_time",
    "/api/instrument/is_suspended": "is_suspended_stock",
}

_WRITE_SEGMENTS = (
    "/order",
    "/cancel",
    "/transfer",
    "/sync",
    "/download",
    "/create",
    "/remove",
    "/reset",
)

_ACCOUNT_DOMAINS = {"credit", "fund", "smt", "trading"}

_RUNTIME_INDEPENDENT_PATHS = {
    "/api/meta/capabilities",
    "/api/meta/connection_status",
    "/api/meta/health",
    "/api/meta/readiness",
    "/api/meta/recovery-status",
    "/api/trading/health",
}


def _domain(path: str) -> str:
    parts = path.strip("/").split("/")
    return parts[1] if len(parts) > 1 else "meta"


def _write_effect(method: str, path: str) -> bool:
    return method in {"POST", "DELETE"} and any(segment in path for segment in _WRITE_SEGMENTS)


def _probe_status(
    path: str,
    provider_method: str | None,
    runtime: RuntimeFacade | None,
    account_runtime_available: bool,
) -> tuple[CapabilityStatus, str | None]:
    if path in {"/api/download/jobs", "/api/download"}:
        # Only the currently attached native model can attest that its QMT
        # global function was actually bound. A Python method's presence is
        # not evidence that the installed embedded runtime supports download.
        payload: object = getattr(runtime, "ping_payload", None)
        available = (
            isinstance(payload, dict)
            and cast(dict[str, object], payload).get("native_history_download_available") is True
        )
        if not available:
            return "unsupported", "native_history_download_unavailable"
    unsupported_reason = _UNSUPPORTED_PATHS.get(path)
    if unsupported_reason is not None:
        return "unsupported", unsupported_reason
    if (
        _domain(path) in _ACCOUNT_DOMAINS
        and path != "/api/trading/health"
        and not account_runtime_available
    ):
        return "unavailable", "bigqmt_account_runtime_unavailable"
    if runtime is None and path not in _RUNTIME_INDEPENDENT_PATHS:
        return "unavailable", "bigqmt_runtime_unavailable"
    if provider_method is None:
        return "ok", None
    if runtime is None or runtime.market_data is None:
        return "unavailable", "bigqmt_runtime_unavailable"
    if provider_method not in dir(runtime.market_data):
        return "unsupported", f"xtdata_{provider_method}_missing"
    return "ok", None


def _iter_routes(routes: Iterable[BaseRoute]) -> Iterator[APIRoute | APIWebSocketRoute]:
    for route in routes:
        if isinstance(route, (APIRoute, APIWebSocketRoute)):
            yield route
        elif isinstance(route, IncludedRouter):
            yield from _iter_routes(route.original_router.routes)


def build_capability_registry(
    app: FastAPI,
    runtime: RuntimeFacade | None,
) -> tuple[CapabilitySpec, ...]:
    capabilities: list[CapabilitySpec] = []
    account_runtime_available = getattr(app.state, "trader_manager", None) is not None
    for route in _iter_routes(app.routes):
        if isinstance(route, APIRoute) and route.path.startswith("/api/"):
            methods = sorted(route.methods or ())
            surface: CapabilitySurface = "http"
        elif isinstance(route, APIWebSocketRoute) and route.path.startswith("/ws/"):
            methods = ["WEBSOCKET"]
            surface = "websocket"
        else:
            continue
        provider_method = _PROVIDER_METHODS.get(route.path)
        mode: CapabilityMode = "native"
        if route.path in _UNSUPPORTED_PATHS:
            mode = "unsupported"
        elif route.path in _DERIVED_PATHS:
            mode = "derived"
        status, reason_code = _probe_status(
            route.path,
            provider_method,
            runtime,
            account_runtime_available,
        )
        for method in methods:
            capabilities.append(
                CapabilitySpec(
                    name=route.name,
                    surface=surface,
                    domain=_domain(route.path),
                    path=route.path,
                    method=method,
                    provider_method=provider_method,
                    mode=mode,
                    write_effect=_write_effect(method, route.path),
                    thread_affinity=(
                        "qmt_main"
                        if provider_method or _domain(route.path) in _ACCOUNT_DOMAINS
                        else "none"
                    ),
                    status=status,
                    reason_code=reason_code,
                )
            )
    return tuple(sorted(capabilities, key=lambda item: (item.path, item.method)))


def capability_summary(registry: tuple[CapabilitySpec, ...]) -> dict[str, int]:
    return {
        status: sum(item.status == status for item in registry)
        for status in ("ok", "unavailable", "unsupported")
    }
