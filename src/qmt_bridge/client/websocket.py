"""WebSocketMixin — WebSocket subscription client methods."""

import json
import inspect
import time
import uuid
from typing import Callable
from bigqmt_signal_trader import telemetry


def _connection_options(connect, headers):
    name = "additional_headers" if "additional_headers" in inspect.signature(connect).parameters else "extra_headers"
    return {name: headers}


class WebSocketMixin:
    """Client methods for WebSocket endpoints."""

    async def _consume_subscription(self, connect, path, callback, payload=None):
        """Observe the existing single-connection/synchronous-callback contract."""
        with telemetry.bind_context(service_role="sdk", ws_session_id=uuid.uuid4().hex):
            with telemetry.span("sdk.ws", route=path) as observation:
                received = handled = 0
                callback_ms = 0.0
                report_at = time.monotonic()
                connection = None
                try:
                    async with connect(f"{self.ws_url}{path}", **_connection_options(connect, self._headers())) as ws:
                        connection = ws
                        telemetry.emit("sdk.ws.open")
                        if payload is not None:
                            await ws.send(json.dumps(payload))
                            telemetry.emit("sdk.ws.subscribe_sent")
                        async for message in ws:
                            received += 1
                            try:
                                data = json.loads(message)
                            except (ValueError, UnicodeError) as exc:
                                telemetry.emit("sdk.ws.decode_failed", outcome="error", error_type=type(exc).__name__)
                                raise
                            if isinstance(data, dict) and data.get("status") in {
                                "unsupported", "unavailable", "error", "partial", "stale", "timeout",
                            }:
                                telemetry.emit("sdk.ws.message_status", outcome=data["status"])
                            started = time.monotonic()
                            try:
                                callback(data)
                            except Exception as exc:
                                telemetry.emit("sdk.ws.callback_failed", critical=path == "/ws/trade",
                                               outcome="error", error_type=type(exc).__name__)
                                raise
                            finally:
                                callback_ms += (time.monotonic() - started) * 1000
                            handled += 1
                            if path == "/ws/trade" and isinstance(data, dict):
                                body = data.get("data") if isinstance(data.get("data"), dict) else {}
                                telemetry.emit("sdk.ws.trade_consumed", critical=True, event_type=data.get("type"),
                                               order_sys_id=body.get("order_sys_id") or body.get("order_sysid"),
                                               trade_id=body.get("trade_id") or body.get("traded_id"),
                                               evidence_source="sdk_callback_return")
                            if time.monotonic() - report_at >= 10:
                                telemetry.emit("sdk.ws.messages", received_count=received, handled_count=handled,
                                               callback_ms=round(callback_ms, 3))
                                report_at = time.monotonic()
                finally:
                    observation.update(received_count=received, handled_count=handled,
                                       callback_ms=round(callback_ms, 3),
                                       close_code=getattr(connection, "close_code", None))

    async def subscribe_realtime(
        self,
        stocks: list[str],
        callback: Callable[[dict], None],
        period: str = "tick",
    ):
        """Subscribe to realtime quote updates via WebSocket.

        Requires the ``websockets`` package::

            pip install websockets

        Usage::

            import asyncio
            from qmt_bridge import QMTClient

            client = QMTClient("192.168.1.100")

            def on_tick(data):
                print(data)

            asyncio.run(client.subscribe_realtime(
                stocks=["000001.SZ", "600519.SH"],
                callback=on_tick,
            ))
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package is required for realtime subscriptions. "
                "Install it with: pip install websockets"
            )

        await self._consume_subscription(websockets.connect, "/ws/realtime", callback,
                                         {"stocks": stocks, "period": period})

    async def subscribe_whole_quote(
        self,
        codes: list[str],
        callback: Callable[[dict], None],
    ):
        """Subscribe to whole-market quote updates via WebSocket.

        Requires the ``websockets`` package.
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package is required. Install with: pip install websockets"
            )

        await self._consume_subscription(websockets.connect, "/ws/whole_quote", callback, {"codes": codes})

    async def subscribe_trade_events(
        self,
        callback: Callable[[dict], None],
    ):
        """Subscribe to trade event callbacks via WebSocket.

        Requires the ``websockets`` package and an API key.
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package is required. Install with: pip install websockets"
            )

        await self._consume_subscription(websockets.connect, "/ws/trade", callback)

    async def subscribe_formula(
        self,
        formula_name: str,
        callback: Callable[[dict], None],
        *,
        stock_code: str = "",
        period: str = "1d",
    ):
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package is required. Install with: pip install websockets"
            )

        await self._consume_subscription(websockets.connect, "/ws/formula", callback, {
            "action": "subscribe", "formula_name": formula_name,
            "stock_code": stock_code, "period": period,
        })
