"""FastAPI application factory with lifespan management."""

import asyncio
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from bigqmt_signal_trader.rpc_client import RpcRequestRejected
from bigqmt_signal_trader import telemetry

from .._version import __version__
from .bigqmt import initialize_bigqmt_runtime
from .config import Settings, get_settings
from .history_readiness import ProbeState
from .helpers import NativePayloadError
from .runtime_controller import RuntimeController
from .observability import TraceMiddleware
from .security import require_api_key_for_writes
from .trading.manager import TradingWriteDisabled

logger = logging.getLogger("qmt_bridge")


# Every POST in these authenticated account domains can reach a broker-side
# write. Keep the recovery counter conservative so a newly added write route
# cannot be missed by the host's bridge-only restart guard.
_TRACKED_TRADING_WRITE_PREFIXES = (
    "/api/trading/",
    "/api/credit/",
    "/api/fund/",
    "/api/smt/",
)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    settings: Settings = app.state.settings
    telemetry.configure(role="bridge")
    telemetry.emit("bridge.startup", critical=True, version=__version__,
                   account_enabled=settings.account_enabled, notify_enabled=settings.notify_enabled)

    from .routers.download import start_download_jobs

    controller = RuntimeController(
        settings,
        runtime_factory=initialize_bigqmt_runtime,
        download_start=start_download_jobs,
        state=app.state,
    )
    app.state.runtime_controller = controller
    await controller.start()

    # Initialize notification module (independent of trading)
    if settings.notify_enabled:
        try:
            from .notify import NotifierManager

            notifier = NotifierManager(settings)
            await notifier.start()
            app.state.notifier_manager = notifier
            logger.info("Notification module initialized")

            # Inject notifier into trading callback if trading is active
            manager = getattr(app.state, "trader_manager", None)
            if manager is not None and hasattr(manager, "_callback"):
                manager._callback.set_notifier(notifier)
        except Exception:
            logger.exception("Failed to initialize notification module")
            app.state.notifier_manager = None
    else:
        app.state.notifier_manager = None

    yield

    telemetry.emit("bridge.shutdown.requested", critical=True)

    # Cleanup notifications
    notifier = getattr(app.state, "notifier_manager", None)
    if notifier is not None:
        try:
            await notifier.stop()
            logger.info("Notification module stopped")
        except Exception:
            logger.exception("Error stopping notification module")

    # Quote hubs own callback registrations against the current runtime.  Tear
    # them down before closing its transport so no late callback targets a
    # closed socket.
    for name in ("whole_quote_hub", "realtime_quote_hub"):
        hub = getattr(app.state, name, None)
        close = getattr(hub, "close", None)
        if callable(close):
            try:
                await close()
            except Exception:
                logger.exception("Error closing %s", name)

    # Download state is independent of market availability.  Its own close
    # contract retains an active native call rather than claiming cancellation.
    from .routers.download import close_download_jobs
    try:
        await asyncio.to_thread(close_download_jobs)
        app.state.download_jobs_error = None
    except Exception as exc:
        app.state.download_jobs_error = f"{exc.__class__.__name__}: {exc}"
        logger.warning("Download jobs were not closed: %s", exc)

    # Cleanup trading
    await controller.close()
    telemetry.emit("bridge.shutdown.completed", critical=True)
    # The process-level writer is also used by SDK consumers in this process;
    # lifespan flushes it, while the CLI owns its final shutdown.
    await asyncio.to_thread(telemetry.flush, 2.0)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if settings is None:
        settings = get_settings()

    app = FastAPI(
        title="QMT Bridge",
        description="Big QMT ZMQ market data and guarded account API bridge",
        version=__version__,
        lifespan=_lifespan,
    )
    app.state.settings = settings
    # Router dependencies normally resolve the module singleton.  Pin them to
    # this application instance so multiple app factories cannot leak settings.
    app.dependency_overrides[get_settings] = lambda: settings
    app.state.bigqmt_runtime = None
    app.state.bigqmt_runtime_error = None
    app.state.trader_manager = None
    app.state.download_jobs_error = None
    app.state.active_write_requests = 0
    app.state.active_write_requests_lock = threading.Lock()
    app.state.history_readiness_state = ProbeState()

    @app.middleware("http")
    async def track_trading_writes(request: Request, call_next):
        tracked = request.method == "POST" and request.url.path.startswith(
            _TRACKED_TRADING_WRITE_PREFIXES
        )
        lock = app.state.active_write_requests_lock
        if tracked:
            with lock:
                app.state.active_write_requests += 1
        try:
            return await call_next(request)
        finally:
            if tracked:
                with lock:
                    app.state.active_write_requests -= 1

    # Pure ASGI tracing wraps the existing HTTP middleware and observes final
    # send/stream errors rather than only response object construction.
    app.add_middleware(TraceMiddleware)

    @app.exception_handler(TradingWriteDisabled)
    async def trading_write_disabled_handler(_request, exc: TradingWriteDisabled):
        telemetry.emit("http.write_rejected", critical=True, outcome="rejected", blocker=exc.blocker,
                       reason_code="QMT_ORDER_WRITES_DISABLED", execution_started=False)
        return JSONResponse(
            status_code=403,
            content={
                "detail": {
                    "code": "QMT_ORDER_WRITES_DISABLED",
                    "blocker": exc.blocker,
                }
            },
        )

    @app.exception_handler(NativePayloadError)
    async def native_payload_error_handler(_request, exc: NativePayloadError):
        telemetry.emit("http.native_payload_error", outcome="error", error_type=type(exc).__name__)
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "reason_code": "invalid_native_payload",
                "message": str(exc),
            },
        )

    @app.exception_handler(RpcRequestRejected)
    async def rejected_native_request_handler(_request, exc: RpcRequestRejected):
        telemetry.emit("http.rpc_rejected", outcome="overloaded" if exc.code == "OVERLOADED" else "timeout",
                       reason_code=exc.code, rpc_request_id=exc.request_id, execution_started=False)
        return JSONResponse(
            status_code=503 if exc.code == "OVERLOADED" else 504,
            content={"status": "overloaded" if exc.code == "OVERLOADED" else "timeout",
                     "reason_code": exc.code, "request_id": exc.request_id,
                     "execution_started": False, "retryable": True, "message": str(exc)},
        )

    # ------------------------------------------------------------------
    # Register data routers (always available)
    # ------------------------------------------------------------------
    from .routers import (
        calendar,
        download,
        etf,
        financial,
        formula,
        futures,
        hk,
        instrument,
        legacy,
        market,
        meta,
        option,
        sector,
        tabular,
        tick,
        utility,
    )

    data_auth = [Depends(require_api_key_for_writes)]
    app.include_router(market.router, dependencies=data_auth)
    app.include_router(tick.router, dependencies=data_auth)
    app.include_router(sector.router, dependencies=data_auth)
    app.include_router(calendar.router, dependencies=data_auth)
    app.include_router(financial.router, dependencies=data_auth)
    app.include_router(instrument.router, dependencies=data_auth)
    app.include_router(option.router, dependencies=data_auth)
    app.include_router(etf.etf_router, dependencies=data_auth)
    app.include_router(etf.fund_data_router, dependencies=data_auth)
    app.include_router(etf.cb_router, dependencies=data_auth)
    app.include_router(futures.router, dependencies=data_auth)
    app.include_router(meta.router, dependencies=data_auth)
    app.include_router(download.router, dependencies=data_auth)
    app.include_router(formula.router, dependencies=data_auth)
    app.include_router(hk.router, dependencies=data_auth)
    app.include_router(tabular.router, dependencies=data_auth)
    app.include_router(utility.router, dependencies=data_auth)
    app.include_router(legacy.router, dependencies=data_auth)

    # ------------------------------------------------------------------
    # Register WebSocket endpoints
    # ------------------------------------------------------------------
    from .ws import formula as formula_ws, realtime, whole_quote

    app.include_router(realtime.router)
    app.include_router(whole_quote.router)
    app.include_router(formula_ws.router)

    # ------------------------------------------------------------------
    # Register notification router (conditional)
    # ------------------------------------------------------------------
    if settings.notify_enabled:
        from .notify.base import router as notify_router

        app.include_router(notify_router)

    # ------------------------------------------------------------------
    # Register trading routers (conditional)
    # ------------------------------------------------------------------
    if settings.account_enabled:
        from .routers import credit, fund, smt, trading

        app.include_router(trading.router)
        app.include_router(credit.router)
        app.include_router(fund.router)
        app.include_router(smt.router)

        from .ws import trade_callback

        app.include_router(trade_callback.router)

    return app
