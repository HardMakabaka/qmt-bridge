"""Big QMT trading lifecycle and domain operations."""

from __future__ import annotations

import logging

from bigqmt_signal_trader.telemetry import emit, span
from bigqmt_signal_trader.trading_client import _batch_outcome, _receipt_outcome

logger = logging.getLogger("qmt_bridge.trading")


class TradingWriteDisabled(PermissionError):
    def __init__(self, blocker):
        self.blocker = blocker
        super().__init__(blocker)


class BigQmtTradingManager:
    """Own a direct Big QMT trading client; account identities are strings."""

    def __init__(self, runtime=None, account_id="", order_writes_enabled=True):
        self.runtime = runtime
        self.account_id = str(account_id or "")
        self.order_writes_enabled = bool(order_writes_enabled)
        self._trader = None
        self._callback = None

    def connect(self, event_loop=None):
        from .callbacks import BridgeTraderCallback

        if self.runtime is None:
            raise RuntimeError("Big QMT runtime is required")
        self._trader = self.runtime.new_trader()
        self._callback = BridgeTraderCallback()
        if event_loop is not None:
            self._callback.set_event_loop(event_loop)
        self._trader.register_callback(self._callback)
        self._trader.start()
        self._trader.connect()
        logger.info("Big QMT trading client connected, account=%s", self.account_id)

    def disconnect(self):
        if self._trader is not None:
            try:
                self._trader.stop()
            except Exception:
                logger.exception("Error stopping Big QMT trading client")
            self._trader = None

    def _account_id(self, account_id=""):
        target = str(account_id or self.account_id or "").strip()
        if not target:
            raise RuntimeError("Big QMT account_id is required")
        return target

    @property
    def write_blockers(self):
        blockers = []
        if self.runtime is not None:
            try:
                self.runtime.probe()
            except (OSError, RuntimeError, TimeoutError, ValueError):
                blockers.append("bigqmt_rpc_unavailable")
        if not self.order_writes_enabled:
            blockers.append("bridge_order_writes_disabled")
        if self.runtime is None or not bool(self.runtime.ping_payload.get("allow_order_methods", False)):
            blockers.append("bigqmt_order_methods_disabled")
        if self.runtime is None or self.runtime.ping_payload.get("terminal_real_mode") is not True:
            blockers.append("bigqmt_terminal_real_mode_required")
        return blockers

    @property
    def writes_enabled(self):
        return not self.write_blockers

    @property
    def event_status(self):
        from ..events import build_event_transport_status

        trader = self._trader
        thread = getattr(trader, "_event_thread", None)
        cursor = getattr(trader, "_event_cursor", None)
        return build_event_transport_status(
            cursor=cursor if isinstance(cursor, dict) else None,
            replay_gap=bool(getattr(trader, "_event_replay_gap", False)),
            listener_alive=bool(thread is not None and thread.is_alive()),
        )

    def _require_writes_enabled(self):
        blockers = self.write_blockers
        if blockers:
            emit("trading.write_gate", critical=True, account_id=self.account_id,
                 outcome="rejected", blocker=blockers[0])
            raise TradingWriteDisabled(blockers[0])
        emit("trading.write_gate", critical=True, account_id=self.account_id, outcome="success")

    def _client(self):
        if self._trader is None:
            raise RuntimeError("Big QMT trading client is not connected")
        return self._trader

    def order(self, stock_code, order_type, order_volume, price_type=5, price=0.0,
              strategy_name="", order_remark="", account_id=""):
        self._require_writes_enabled()
        target = self._account_id(account_id)
        fields = {"critical": True, "account_id": target, "client_submit_id": order_remark,
                  "stock_code": stock_code, "order_type": order_type,
                  "volume": order_volume, "price": price, "strategy_name": strategy_name}
        emit("trading.manager.submit.begin", outcome="started", **fields)
        try:
            with span("trading.manager.submit", **fields) as trace:
                receipt = self._client().submit_order(
                    stock_code=stock_code, order_type=order_type, order_volume=order_volume,
                    price_type=price_type, price=price, strategy_name=strategy_name,
                    order_remark=order_remark, account_id=target,
                )
                trace["outcome"] = _receipt_outcome(receipt)
        except Exception as exc:
            emit("trading.manager.submit.return", outcome="unknown", error_type=type(exc).__name__, **fields)
            raise
        emit("trading.manager.submit.return", outcome=_receipt_outcome(receipt), **fields)
        if isinstance(receipt, dict):
            return receipt.get("order_sys_id") or receipt.get("order_id") or receipt
        return receipt

    def submit_batch(self, orders, batch_id="", account_id=""):
        self._require_writes_enabled()
        target = self._account_id(account_id)
        emit("trading.manager.batch.begin", critical=True, account_id=target, batch_id=batch_id,
             item_count=len(orders or []), outcome="started")
        try:
            result = self._client().submit_batch(orders, batch_id=batch_id, account_id=target)
        except Exception as exc:
            emit("trading.manager.batch.return", critical=True, account_id=target, batch_id=batch_id,
                 item_count=len(orders or []), outcome="unknown", error_type=type(exc).__name__)
            raise
        emit("trading.manager.batch.return", critical=True, account_id=target, batch_id=batch_id,
             item_count=len(result or []), outcome=_batch_outcome(result))
        return result

    def cancel_order(self, order_id, account_id=""):
        self._require_writes_enabled()
        target = self._account_id(account_id)
        emit("trading.manager.cancel.begin", critical=True, account_id=target,
             order_sys_id=order_id, cancel_method="id", outcome="started")
        try:
            result = self._client().cancel_order(order_id, account_id=target)
        except Exception as exc:
            emit("trading.manager.cancel.return", critical=True, account_id=target,
                 order_sys_id=order_id, cancel_method="id", outcome="unknown", error_type=type(exc).__name__)
            raise
        emit("trading.manager.cancel.return", critical=True, account_id=target,
             order_sys_id=order_id, cancel_method="id",
             outcome="success" if bool(result) else "rejected")
        return result

    def cancel_order_sysid(self, order_sysid, market, account_id=""):
        self._require_writes_enabled()
        target = self._account_id(account_id)
        emit("trading.manager.cancel.begin", critical=True, account_id=target,
             order_sys_id=order_sysid, market=market, cancel_method="sysid", outcome="started")
        try:
            result = self._client().cancel_order_sysid(order_sysid, market=market, account_id=target)
        except Exception as exc:
            emit("trading.manager.cancel.return", critical=True, account_id=target,
                 order_sys_id=order_sysid, market=market, cancel_method="sysid",
                 outcome="unknown", error_type=type(exc).__name__)
            raise
        emit("trading.manager.cancel.return", critical=True, account_id=target,
             order_sys_id=order_sysid, market=market, cancel_method="sysid",
             outcome="success" if bool(result) else "rejected")
        return result

    def sync_transaction_from_external(self, operation, data_type, data, account_type="STOCK", account_id=""):
        self._require_writes_enabled()
        target = self._account_id(account_id)
        emit("trading.external_sync.begin", critical=True, account_id=target,
             operation=operation, data_type=data_type, item_count=len(data or []), outcome="started")
        try:
            result = self._client().sync_transaction(
                operation, data_type, data, account_type=account_type,
                account_id=target,
            )
        except Exception as exc:
            emit("trading.external_sync.return", critical=True, account_id=target,
                 operation=operation, data_type=data_type, outcome="unknown",
                 error_type=type(exc).__name__, filled=False)
            raise
        # The external synchronizer is not a broker fill acknowledgement.
        emit("trading.external_sync.return", critical=True, account_id=target,
             operation=operation, data_type=data_type, outcome=_receipt_outcome(result), filled=False)
        return result

    def query_orders(self, account_id="", cancelable_only=False, client_submit_id=""):
        return self._client().query_orders(
            account_id=self._account_id(account_id), cancelable_only=cancelable_only,
            client_submit_id=client_submit_id,
        )

    def query_order_detail(self, order_id=0, account_id=""):
        return self._client().query_order(order_id, account_id=self._account_id(account_id))

    def query_positions(self, account_id=""):
        return self._client().query_positions(account_id=self._account_id(account_id))

    def query_position(self, stock_code, account_id=""):
        return self._client().query_position(stock_code, account_id=self._account_id(account_id))

    def query_asset(self, account_id=""):
        return self._client().query_asset(account_id=self._account_id(account_id))

    def query_trades(self, account_id=""):
        return self._client().query_trades(account_id=self._account_id(account_id))

    def query_trade(self, trade_id, account_id=""):
        return self._client().query_trade(trade_id, account_id=self._account_id(account_id))

    def query_execution_snapshot(self, account_id="", order_strategy_name="bigqmt_signal_trader", trade_strategy_name=""):
        return self._client().query_execution_snapshot(
            account_id=self._account_id(account_id), order_strategy_name=order_strategy_name,
            trade_strategy_name=trade_strategy_name,
        )

    def query_extension(self, method, params=None, account_id=""):
        return self._client().query_extension(method, params, account_id=self._account_id(account_id))
