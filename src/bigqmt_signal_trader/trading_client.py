"""Direct Big QMT trading client with bounded execution-event replay."""

import json
import math
import threading
import time
import uuid
from typing import Any, Dict, Iterable, Optional

from .rpc_client import BigQmtRpcClient
from .telemetry import bind_context, emit, extract_trace, span


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, list):
        return value
    return [value]


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _order_type(item):
    value = item.get("order_type")
    if value is None:
        value = item.get("action")
    if str(value).upper() == "BUY":
        return 23
    if str(value).upper() == "SELL":
        return 24
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _receipt_outcome(value):
    """Conservative telemetry classification; never promote a false receipt."""
    if isinstance(value, dict):
        status = str(value.get("status") or "").upper()
        if status in ("SUBMITTED_UNCONFIRMED", "SUBMIT_UNKNOWN", "UNKNOWN"):
            return "unknown"
        if value.get("order_sys_id") or value.get("order_id"):
            return "success"
        if value.get("success") is False or value.get("accepted") is False:
            return "rejected"
        return "unknown"
    if value is False:
        return "rejected"
    if value in (None, "", 0, -1):
        return "unknown"
    return "success"


def _batch_outcome(items):
    outcomes = []
    for item in items or []:
        if isinstance(item, dict):
            if item.get("success") is False or item.get("accepted") is False:
                outcomes.append("rejected")
            elif item.get("idempotent") is True or item.get("confirmed") is True:
                outcomes.append("success")
            else:
                # The batch contract has historically not distinguished an
                # accepted but unconfirmed broker write from a receipt.
                outcomes.append("unknown")
        else:
            outcomes.append(_receipt_outcome(item))
    if not outcomes:
        return "empty"
    if all(item == "success" for item in outcomes):
        return "success"
    if all(item == "rejected" for item in outcomes):
        return "rejected"
    if all(item == "unknown" for item in outcomes):
        return "unknown"
    return "partial"


class BigQmtTradingClient:
    """Trading RPC facade using account-id strings and plain dictionaries.

    Big QMT owns order execution. This client only serializes requests and
    replays its execution-event stream.
    """

    def __init__(
        self,
        account_id="",
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
        client=None,
    ):
        self.account_id = str(account_id or "")
        self.client = client or BigQmtRpcClient(
            account_id=self.account_id,
            redis_client=redis_client,
            redis_config=redis_config,
            timeout_seconds=timeout_seconds,
        )
        if not self.account_id:
            self.account_id = str(getattr(self.client, "account_id", "") or "")
        self.callback = None
        self._event_thread = None
        self._event_running = False
        self._event_cursor = None
        self._event_replay_gap = False

    def select_account(self, account_id=""):
        target = str(account_id or self.account_id or "").strip()
        if not target:
            raise ValueError("Big QMT account_id is required")
        return target

    def register_callback(self, callback):
        self.callback = callback

    def start(self):
        self._start_event_listener()

    def connect(self):
        self.client.call("ping", account_id=self.select_account())

    def stop(self):
        self._event_running = False
        thread = self._event_thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        self._event_thread = None
        emit("trading.execution.listener.stop", account_id=self.account_id, outcome="success")

    def _start_event_listener(self):
        if self._event_thread is not None and self._event_thread.is_alive():
            emit("trading.execution.listener.start", account_id=self.account_id, outcome="rejected",
                 reason="already_running")
            return
        self._event_running = True
        self._event_thread = threading.Thread(
            target=self._event_loop, name="bigqmt-exec-events", daemon=True
        )
        self._event_thread.start()
        emit("trading.execution.listener.start", account_id=self.account_id, outcome="success")

    def _event_loop(self):
        event_config = dict(getattr(self.client, "execution_event_config", {}) or {})
        if str(event_config.get("transport") or "redis").lower() == "zmq":
            self._event_loop_zmq(event_config)
            return
        from .exec_events import order_channel, trade_channel

        while self._event_running:
            account_id = self.select_account()
            pubsub = None
            try:
                pubsub = self.client._redis().pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(order_channel(account_id), trade_channel(account_id))
                emit("trading.execution.listener.connect", account_id=account_id,
                     transport="redis", outcome="success")
                while self._event_running:
                    message = pubsub.get_message(timeout=1.0)
                    if message and message.get("type") == "message":
                        self._dispatch_event(message.get("data"))
            except Exception as exc:
                emit("trading.execution.listener.error", account_id=account_id,
                     transport="redis", outcome="unknown", error_type=type(exc).__name__)
                time.sleep(1.0)
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

    def _event_loop_zmq(self, event_config):
        zmq_config = dict(event_config.get("zmq") or {})
        endpoint = str(zmq_config.get("connect_address") or "tcp://127.0.0.1:15561")
        replay_interval = max(0.001, float(zmq_config.get("replay_interval_seconds") or 5.0))
        while self._event_running:
            socket = None
            try:
                import zmq

                socket = zmq.Context.instance().socket(zmq.SUB)
                socket.setsockopt(zmq.LINGER, 0)
                socket.setsockopt(zmq.RCVTIMEO, 1000)
                socket.setsockopt(zmq.SUBSCRIBE, b"")
                socket.connect(endpoint)
                emit("trading.execution.listener.connect", account_id=self.account_id,
                     transport="zmq", endpoint=endpoint, outcome="success")
                self._replay_execution_events()
                last_replay_at = time.time()
                while self._event_running:
                    try:
                        raw = socket.recv()
                    except zmq.Again:
                        if self._event_running and time.time() - last_replay_at >= replay_interval:
                            self._replay_execution_events()
                            last_replay_at = time.time()
                        continue
                    self._dispatch_event(raw)
            except Exception as exc:
                emit("trading.execution.listener.error", account_id=self.account_id,
                     transport="zmq", outcome="unknown", error_type=type(exc).__name__)
                if self._event_running:
                    time.sleep(1.0)
            finally:
                if socket is not None:
                    try:
                        socket.close(linger=0)
                    except Exception:
                        pass

    def _replay_execution_events(self):
        cursor = self._event_cursor or {"epoch": "", "sequence": 0}
        emit("trading.execution.replay", account_id=self.account_id, event_epoch=cursor.get("epoch"),
             event_sequence=cursor.get("sequence"), outcome="started")
        replay = self.client.call("get_events_since", {"cursor": dict(cursor)}) or {}
        self._event_replay_gap = bool(replay.get("gap"))
        emit("trading.execution.replay", critical=bool(self._event_replay_gap), account_id=self.account_id,
             outcome="gap" if self._event_replay_gap else "success",
             event_count=len(replay.get("events") or []))
        for event in replay.get("events") or []:
            self._dispatch_event(event, repair_gap=False)
        replay_cursor = replay.get("cursor")
        if isinstance(replay_cursor, dict):
            self._event_cursor = dict(replay_cursor)

    def _dispatch_event(self, raw, repair_gap=True):
        if isinstance(raw, dict):
            event = dict(raw)
        else:
            try:
                event = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw))
            except Exception:
                emit("trading.execution.event_drop", outcome="rejected", reason="malformed_payload")
                return
        if not isinstance(event, dict):
            emit("trading.execution.event_drop", outcome="rejected", reason="non_object_payload")
            return
        cursor = event.get("cursor")
        if isinstance(cursor, dict):
            current = self._event_cursor or {}
            same_epoch = str(current.get("epoch") or "") == str(cursor.get("epoch") or "")
            incoming = _safe_int(cursor.get("sequence"))
            current_sequence = _safe_int(current.get("sequence"))
            if same_epoch and incoming <= current_sequence:
                return
            if repair_gap and same_epoch and current.get("epoch") and incoming > current_sequence + 1:
                self._replay_execution_events()
                repaired = self._event_cursor or {}
                if str(repaired.get("epoch") or "") == str(cursor.get("epoch") or "") and _safe_int(repaired.get("sequence")) >= incoming:
                    return
            self._event_cursor = dict(cursor)
        if str(event.get("account_id") or "") not in ("", self.select_account()):
            emit("trading.execution.event", account_id=self.account_id, outcome="rejected",
                 reason="account_mismatch")
            return
        event_type = str(event.get("event_type") or "")
        trace_context = extract_trace(event)
        # A persistent listener thread must not leak one old event's context
        # into the next.  Legacy events without a wire trace receive a fresh
        # root trace and remain linked by epoch/sequence/order identifiers.
        if not trace_context:
            trace_context = {"trace_id": uuid.uuid4().hex}
        event_fields = {
            "account_id": self.account_id, "event_type": event_type,
            "order_sys_id": event.get("order_sys_id") or event.get("order_sysid"),
            "trade_id": event.get("trade_id"),
            "event_epoch": (event.get("cursor") or {}).get("epoch"),
            "event_sequence": (event.get("cursor") or {}).get("sequence"),
        }
        with bind_context(**trace_context):
            with span("trading.execution.dispatch", **event_fields) as trace:
                emit("trading.execution.event", critical=event_type in ("trade", "order"),
                     outcome="success", **event_fields)
                callback = self.callback
                if callback is None:
                    trace["outcome"] = "empty"
                    return
                try:
                    if event_type == "trade" and callable(getattr(callback, "on_trade", None)):
                        callback.on_trade(self._trade_from_dict(event))
                    elif event_type == "order" and callable(getattr(callback, "on_order", None)):
                        callback.on_order(self._order_from_dict(event))
                    else:
                        trace["outcome"] = "empty"
                except Exception as exc:
                    trace["outcome"] = "unknown"
                    emit("trading.execution.callback_error", critical=True,
                         outcome="unknown", error_type=type(exc).__name__, **event_fields)

    def _cached_snapshot(self, account_id):
        try:
            raw = self.client._redis().get("bigqmt:positions:%s" % account_id)
            return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)) if raw else {}
        except Exception:
            return {}

    def _redis_cache_enabled(self):
        return str(getattr(self.client, "transport_name", "") or "").lower() in ("redis", "", "default")

    def query_asset(self, account_id=""):
        target = self.select_account(account_id)
        try:
            data = self.client.call("get_asset", {"account_id": target}, account_id=target) or {}
        except Exception:
            data = self._cached_snapshot(target).get("asset") if self._redis_cache_enabled() else None
            if not isinstance(data, dict):
                raise
        cash = _optional_float(data.get("cash"))
        total_asset = _optional_float(data.get("total_asset"))
        frozen_cash = _optional_float(data.get("frozen_cash"))
        market_value = _optional_float(data.get("market_value"))
        if market_value is None and all(value is not None for value in (cash, total_asset, frozen_cash)):
            market_value = total_asset - cash - frozen_cash
        return {"account_id": target, "cash": cash, "available_cash": cash,
                "frozen_cash": frozen_cash, "total_asset": total_asset,
                "market_value": market_value}

    def query_positions(self, account_id=""):
        target = self.select_account(account_id)
        try:
            data = self.client.call("get_positions", {"account_id": target}, account_id=target) or []
        except Exception:
            snapshot = self._cached_snapshot(target)
            data = snapshot.get("positions") if self._redis_cache_enabled() else None
            if data is None:
                raise
        return [self._position_from_dict(target, item) for item in _as_list(data)]

    def query_position(self, stock_code, account_id=""):
        target = self.select_account(account_id)
        data = self.client.call("get_position", {"account_id": target, "stock_code": stock_code}, account_id=target)
        return self._position_from_dict(target, data) if data else None

    def query_orders(self, account_id="", cancelable_only=False, client_submit_id="", strategy_name="bigqmt_signal_trader"):
        target = self.select_account(account_id)
        data = self.client.call("query_orders", {"account_id": target, "cancelable_only": bool(cancelable_only), "strategy_name": strategy_name}, account_id=target) or []
        orders = [self._order_from_dict(item) for item in _as_list(data)]
        if client_submit_id:
            orders = [item for item in orders if str(item.get("order_remark") or "") == str(client_submit_id)]
        return orders

    def query_order(self, order_id, account_id=""):
        target = self.select_account(account_id)
        sought = str(order_id)
        for order in self.query_orders(target, strategy_name=""):
            if sought in (str(order.get("order_id") or ""), str(order.get("order_sysid") or "")):
                return order
        return None

    def query_trades(self, account_id="", strategy_name="bigqmt_signal_trader"):
        target = self.select_account(account_id)
        data = self.client.call("query_trades", {"account_id": target, "strategy_name": strategy_name}, account_id=target) or []
        return [self._trade_from_dict(item) for item in _as_list(data)]

    def query_trade(self, trade_id, account_id=""):
        sought = str(trade_id)
        for trade in self.query_trades(account_id, strategy_name=""):
            if str(trade.get("trade_id") or "") == sought:
                return trade
        return None

    def query_execution_snapshot(self, account_id="", order_strategy_name="bigqmt_signal_trader", trade_strategy_name=""):
        target = self.select_account(account_id)
        data = self.client.call("query_execution_snapshot", {"account_id": target, "order_strategy_name": order_strategy_name, "trade_strategy_name": trade_strategy_name}, account_id=target) or {}
        result = dict(data) if isinstance(data, dict) else {}
        result["orders"] = [self._order_from_dict(item) for item in _as_list(result.get("orders"))]
        result["trades"] = [self._trade_from_dict(item) for item in _as_list(result.get("trades"))]
        return result

    def submit_order(self, stock_code, order_type, order_volume, price_type=5, price=0.0, strategy_name="", order_remark="", account_id=""):
        target = self.select_account(account_id)
        fields = {"critical": True, "account_id": target, "client_submit_id": order_remark,
                  "stock_code": stock_code, "order_type": order_type, "volume": order_volume,
                  "price": price, "strategy_name": strategy_name}
        emit("trading.client.submit.begin", outcome="started", **fields)
        try:
            with span("trading.client.submit", **fields) as trace:
                result = self.client.call("submit_order", {"account_id": target, "stock_code": stock_code, "order_type": order_type, "order_volume": order_volume, "price_type": price_type, "price": price, "strategy_name": strategy_name, "order_remark": order_remark, "require_idempotency_check": True}, account_id=target) or {}
                trace["outcome"] = _receipt_outcome(result)
        except Exception as exc:
            emit("trading.client.submit.return", outcome="unknown", error_type=type(exc).__name__, **fields)
            raise
        emit("trading.client.submit.return", outcome=_receipt_outcome(result), **fields)
        return result

    def submit_batch(self, orders, batch_id="", account_id=""):
        target = self.select_account(account_id)
        payload = []
        for order in orders or []:
            item = dict(order or {})
            item.setdefault("account_id", target)
            payload.append(item)
        params = {"account_id": target, "orders": payload}
        if batch_id:
            params["batch_id"] = str(batch_id)
        emit("trading.client.batch.begin", critical=True, account_id=target, batch_id=batch_id,
             item_count=len(payload), outcome="started")
        try:
            result = self.client.call("submit_orders_batch", params, account_id=target) or []
        except Exception as exc:
            emit("trading.client.batch.return", critical=True, account_id=target, batch_id=batch_id,
                 item_count=len(payload), outcome="unknown", error_type=type(exc).__name__)
            raise
        emit("trading.client.batch.return", critical=True, account_id=target, batch_id=batch_id,
             item_count=len(result), outcome=_batch_outcome(result))
        return result

    def cancel_order_sysid(self, order_sysid, market="", account_id=""):
        target = self.select_account(account_id)
        emit("trading.client.cancel.begin", critical=True, account_id=target,
             order_sys_id=order_sysid, market=market, outcome="started")
        try:
            data = self.client.call("cancel_order", {"account_id": target, "market": market, "order_sysid": str(order_sysid)}, account_id=target) or {}
        except Exception as exc:
            emit("trading.client.cancel.return", critical=True, account_id=target,
                 order_sys_id=order_sysid, market=market, outcome="unknown", error_type=type(exc).__name__)
            raise
        accepted = bool(data.get("success", data)) if isinstance(data, dict) else bool(data)
        emit("trading.client.cancel.return", critical=True, account_id=target,
             order_sys_id=order_sysid, market=market, outcome="success" if accepted else "rejected")
        return accepted

    def cancel_order(self, order_id, account_id=""):
        return self.cancel_order_sysid(order_id, account_id=account_id)

    def sync_transaction(self, operation, data_type, data, account_type="STOCK", account_id=""):
        """Call the native QMT global external-transaction synchronizer."""
        target = self.select_account(account_id)
        emit("trading.client.external_sync.begin", critical=True, account_id=target,
             operation=operation, data_type=data_type, item_count=len(data or []), outcome="started")
        try:
            result = self.client.call(
                "sync_transaction_from_external",
                {
                    "operation": operation, "data_type": data_type,
                    "account_id": target, "account_type": account_type,
                    "data_list": list(data or []),
                }, account_id=target,
            )
        except Exception as exc:
            emit("trading.client.external_sync.return", critical=True, account_id=target,
                 operation=operation, data_type=data_type, outcome="unknown",
                 error_type=type(exc).__name__, filled=False)
            raise
        emit("trading.client.external_sync.return", critical=True, account_id=target,
             operation=operation, data_type=data_type, outcome=_receipt_outcome(result), filled=False)
        return result

    def query_extension(self, method, params=None, account_id=""):
        from .redis_rpc import READ_METHODS
        if method not in READ_METHODS:
            raise ValueError("query_extension accepts read-only native methods only")
        return self.client.call(str(method), dict(params or {}), account_id=self.select_account(account_id))

    def _position_from_dict(self, account_id, item):
        item = dict(item or {})
        available = _safe_int(item.get("available", item.get("can_use_volume")))
        cost = _safe_float(item.get("cost", item.get("avg_price")))
        return {"account_id": account_id, "stock_code": str(item.get("stock_code") or ""), "stock_name": str(item.get("stock_name") or ""), "volume": _safe_int(item.get("volume")), "available": available, "avg_price": cost, "cost_price": cost, "yesterday_volume": _safe_int(item.get("yesterday_volume"), _safe_int(item.get("volume")))}

    def _order_from_dict(self, item):
        item = dict(item or {})
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or item.get("order_id") or "")
        return {"account_id": str(item.get("account_id") or self.account_id), "stock_code": str(item.get("stock_code") or ""), "order_type": _order_type(item), "order_status": _safe_int(item.get("status", item.get("order_status")), 255), "order_volume": _safe_int(item.get("volume", item.get("order_volume"))), "traded_volume": _safe_int(item.get("traded_volume")), "price": _safe_float(item.get("price")), "order_sysid": order_sysid, "order_id": order_sysid or str(item.get("user_order_id") or ""), "strategy_name": str(item.get("strategy_name") or ""), "order_remark": str(item.get("remark") or item.get("user_order_id") or ""), "cursor": dict(item.get("cursor") or {}), "published_at": item.get("published_at")}

    def _trade_from_dict(self, item):
        item = dict(item or {})
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or "")
        return {"account_id": str(item.get("account_id") or self.account_id), "stock_code": str(item.get("stock_code") or ""), "order_type": _order_type(item), "order_sysid": order_sysid, "order_id": order_sysid, "trade_id": str(item.get("trade_id") or ""), "traded_volume": _safe_int(item.get("volume", item.get("traded_volume"))), "traded_price": _safe_float(item.get("price", item.get("traded_price"))), "traded_at": str(item.get("traded_at") or ""), "order_remark": str(item.get("user_order_id") or item.get("remark") or ""), "cursor": dict(item.get("cursor") or {}), "published_at": item.get("published_at")}
