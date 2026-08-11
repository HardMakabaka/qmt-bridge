"""FastAPI application factory with lifespan management."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .bigqmt import initialize_bigqmt_runtime, reset_bigqmt_runtime
from .config import Settings, get_settings
from .trading.manager import TradingWriteDisabled

logger = logging.getLogger("qmt_bridge")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    settings: Settings = app.state.settings

    try:
        runtime = initialize_bigqmt_runtime(settings)
        app.state.bigqmt_runtime = runtime
        app.state.bigqmt_runtime_error = None
        logger.info("Big QMT runtime initialized via %s", settings.zmq_endpoint)
    except Exception as exc:
        logger.exception("Failed to initialize Big QMT runtime")
        app.state.bigqmt_runtime = None
        app.state.bigqmt_runtime_error = f"{exc.__class__.__name__}: {exc}"

    if settings.account_enabled and app.state.bigqmt_runtime is not None:
        try:
            from .trading.manager import XtTraderManager

            manager = XtTraderManager(
                runtime=app.state.bigqmt_runtime,
                account_id=settings.trading_account_id,
                order_writes_enabled=settings.order_writes_enabled,
            )
            manager.connect()
            app.state.trader_manager = manager
            logger.info("Big QMT account query module initialized")
        except Exception:
            logger.exception("Failed to initialize Big QMT account query module")
            app.state.trader_manager = None
    else:
        app.state.trader_manager = None

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

    # Cleanup notifications
    notifier = getattr(app.state, "notifier_manager", None)
    if notifier is not None:
        try:
            await notifier.stop()
            logger.info("Notification module stopped")
        except Exception:
            logger.exception("Error stopping notification module")

    # Cleanup trading
    manager = getattr(app.state, "trader_manager", None)
    if manager is not None:
        try:
            manager.disconnect()
            logger.info("Big QMT account query module disconnected")
        except Exception:
            logger.exception("Error disconnecting Big QMT account query module")

    reset_bigqmt_runtime()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if settings is None:
        settings = get_settings()

    app = FastAPI(
        title="QMT Bridge",
        description="Big QMT ZMQ market data and guarded account API bridge",
        version="3.0.0",
        lifespan=_lifespan,
    )
    app.state.settings = settings

    @app.exception_handler(TradingWriteDisabled)
    async def trading_write_disabled_handler(_request, exc: TradingWriteDisabled):
        return JSONResponse(
            status_code=403,
            content={
                "detail": {
                    "code": "QMT_ORDER_WRITES_DISABLED",
                    "blocker": exc.blocker,
                }
            },
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

    app.include_router(market.router)
    app.include_router(tick.router)
    app.include_router(sector.router)
    app.include_router(calendar.router)
    app.include_router(financial.router)
    app.include_router(instrument.router)
    app.include_router(option.router)
    app.include_router(etf.etf_router)
    app.include_router(etf.fund_data_router)
    app.include_router(etf.cb_router)
    app.include_router(futures.router)
    app.include_router(meta.router)
    app.include_router(download.router)
    app.include_router(formula.router)
    app.include_router(hk.router)
    app.include_router(tabular.router)
    app.include_router(utility.router)
    app.include_router(legacy.router)

    # ------------------------------------------------------------------
    # Register WebSocket endpoints
    # ------------------------------------------------------------------
    from .ws import formula as formula_ws, l2_thousand, realtime, whole_quote

    app.include_router(realtime.router)
    app.include_router(whole_quote.router)
    app.include_router(formula_ws.router)
    app.include_router(l2_thousand.router)

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
        from .routers import bank, credit, fund, smt, trading

        app.include_router(trading.router)
        app.include_router(credit.router)
        app.include_router(fund.router)
        app.include_router(smt.router)
        app.include_router(bank.router)

        from .ws import trade_callback

        app.include_router(trade_callback.router)

    return app
