"""Real-time order/trade execution events with bounded replay.

Big QMT fires ``order_callback(ContextInfo, orderInfo)`` and
``deal_callback(ContextInfo, dealInfo)`` inside the strategy process. We normalize
the QMT order/deal object (ThinkTrader ``m_*`` fields) into a plain dict. The
Big QMT runtime publishes an independent loopback ZMQ stream and retains a
bounded in-process replay buffer. Redis channels remain available for legacy
runtime configurations.

Channels (also used as capped streams for short replay, xadd + publish):
- ``bigqmt:order_events:{account_id}``
- ``bigqmt:trade_events:{account_id}``

The normalized fields are the bridge's native order and trade event payloads;
BigQmtTradingClient delivers these as ordinary dictionaries.
"""

import json
import threading
import time
import uuid

from collections import deque

from .telemetry import emit, inject_trace, span


ORDER_CHANNEL_TEMPLATE = "bigqmt:order_events:{account_id}"
TRADE_CHANNEL_TEMPLATE = "bigqmt:trade_events:{account_id}"

EVENT_ORDER = "order"
EVENT_TRADE = "trade"
DEFAULT_EVENT_ZMQ_BIND_ADDRESS = "tcp://127.0.0.1:15561"


class EventReplayBuffer(object):
    def __init__(self, maxlen=2000, epoch=None):
        self.maxlen = max(1, int(maxlen))
        self.epoch = str(epoch or uuid.uuid4().hex)
        self._events = deque(maxlen=self.maxlen)
        self._sequence = 0
        self._lock = threading.RLock()

    def cursor(self):
        with self._lock:
            return {"epoch": self.epoch, "sequence": self._sequence}

    def append(self, event):
        with self._lock:
            self._sequence += 1
            payload = dict(event or {})
            payload["cursor"] = {
                "epoch": self.epoch,
                "sequence": self._sequence,
            }
            payload.setdefault("published_at", time.time())
            self._events.append(payload)
            cursor = payload["cursor"]
            emit(
                "execution_event.replay_append",
                critical=True,
                event_type=payload.get("event_type"),
                event_epoch=cursor.get("epoch"),
                event_sequence=cursor.get("sequence"),
                order_sys_id=payload.get("order_sys_id"),
                trade_id=payload.get("trade_id"),
            )
            return dict(payload)

    def events_since(self, cursor=None):
        cursor = dict(cursor or {})
        with self._lock:
            requested_epoch = str(cursor.get("epoch") or self.epoch)
            try:
                requested_sequence = int(cursor.get("sequence") or 0)
            except (TypeError, ValueError):
                requested_sequence = 0
            first_sequence = (
                int(self._events[0]["cursor"]["sequence"])
                if self._events
                else self._sequence + 1
            )
            gap = (
                requested_epoch != self.epoch
                or requested_sequence < first_sequence - 1
                or requested_sequence > self._sequence
            )
            if gap:
                events = list(self._events)
            else:
                events = [
                    event
                    for event in self._events
                    if int(event["cursor"]["sequence"]) > requested_sequence
                ]
            result = {
                "cursor": {"epoch": self.epoch, "sequence": self._sequence},
                "events": [dict(event) for event in events],
                "gap": gap,
                "retained_from_sequence": first_sequence,
            }
            emit(
                "execution_event.replay_read",
                critical=gap,
                outcome="gap" if gap else "success",
                event_epoch=self.epoch,
                event_sequence=self._sequence,
                event_count=len(events),
                gap=gap,
                retained_from_sequence=first_sequence,
            )
            return result


class ZmqExecutionEventPublisher(object):
    def __init__(self, bind_address=None, maxlen=2000, replay_buffer=None):
        self.bind_address = str(
            bind_address or DEFAULT_EVENT_ZMQ_BIND_ADDRESS
        )
        if not _is_loopback_zmq_address(self.bind_address):
            raise ValueError("execution event endpoint must use tcp loopback")
        self.replay_buffer = replay_buffer or EventReplayBuffer(maxlen=maxlen)
        self._lock = threading.RLock()
        self._socket = None
        self._transient_count = 0
        self._transient_started_monotonic = None

    def start(self):
        with self._lock:
            if self._socket is not None:
                return self.bind_address
            import zmq

            socket = zmq.Context.instance().socket(zmq.PUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.SNDHWM, self.replay_buffer.maxlen)
            socket.bind(self.bind_address)
            self._socket = socket
            emit("execution_event.publisher_started", critical=True)
            return self.bind_address

    def publish(self, event):
        with self._lock:
            if self._socket is None:
                self.start()
            with span("execution_event.publish", event_type=(event or {}).get("event_type")) as result:
                # The trace travels in the durable event envelope, not merely
                # in this process-local span. Replay and the bridge event
                # listener can therefore restore its causal context.
                payload = self.replay_buffer.append(inject_trace(event))
                self._socket.send_string(
                    json.dumps(payload, ensure_ascii=False, default=str)
                )
                result["outcome"] = "success"
            return payload

    def publish_transient(self, event):
        with self._lock:
            if self._socket is None:
                self.start()
            payload = dict(event or {})
            payload.setdefault("published_at", time.time())
            try:
                import zmq

                self._socket.send_string(
                    json.dumps(payload, ensure_ascii=False, default=str),
                    flags=zmq.NOBLOCK,
                )
            except zmq.Again:
                emit("execution_event.transient_drop", critical=True, outcome="overloaded")
                return None
            if self._transient_started_monotonic is None:
                self._transient_started_monotonic = time.monotonic()
            self._transient_count += 1
            self._emit_transient_summary()
            return payload

    def _emit_transient_summary(self, force=False):
        """Report quote callback volume periodically, never once per tick."""
        if not self._transient_count:
            return
        started = self._transient_started_monotonic or time.monotonic()
        elapsed = time.monotonic() - started
        if not force and self._transient_count < 100 and elapsed < 60.0:
            return
        emit(
            "execution_event.transient_summary",
            outcome="success",
            event_count=self._transient_count,
            interval_ms=round(elapsed * 1000.0, 3),
        )
        self._transient_count = 0
        self._transient_started_monotonic = None

    def close(self):
        with self._lock:
            self._emit_transient_summary(force=True)
            socket = self._socket
            self._socket = None
            if socket is not None:
                socket.close(linger=0)
                emit("execution_event.publisher_closed", critical=True)


def _is_loopback_zmq_address(address):
    text = str(address or "").lower()
    return text.startswith("tcp://127.0.0.1:") or text.startswith(
        "tcp://localhost:"
    ) or text.startswith("tcp://[::1]:")


_ZMQ_EVENT_PUBLISHER = None
_ZMQ_EVENT_PUBLISHER_LOCK = threading.RLock()


def configure_zmq_event_publisher(config=None):
    global _ZMQ_EVENT_PUBLISHER
    config = dict(config or {})
    bind_address = str(
        config.get("bind_address") or DEFAULT_EVENT_ZMQ_BIND_ADDRESS
    )
    maxlen = int(config.get("maxlen") or 2000)
    with _ZMQ_EVENT_PUBLISHER_LOCK:
        current = _ZMQ_EVENT_PUBLISHER
        if current is not None and (
            current.bind_address != bind_address
            or current.replay_buffer.maxlen != maxlen
        ):
            current.close()
            current = None
        if current is None:
            current = ZmqExecutionEventPublisher(
                bind_address=bind_address,
                maxlen=maxlen,
            )
            _ZMQ_EVENT_PUBLISHER = current
        return current


def publish_zmq_event(event, config=None):
    return configure_zmq_event_publisher(config).publish(event)


def publish_transient_zmq_event(event, config=None):
    if config is not None:
        return configure_zmq_event_publisher(config).publish_transient(event)
    with _ZMQ_EVENT_PUBLISHER_LOCK:
        publisher = _ZMQ_EVENT_PUBLISHER
        if publisher is None:
            publisher = configure_zmq_event_publisher()
        return publisher.publish_transient(event)


def get_event_cursor():
    with _ZMQ_EVENT_PUBLISHER_LOCK:
        publisher = _ZMQ_EVENT_PUBLISHER
        if publisher is None:
            publisher = configure_zmq_event_publisher()
        return publisher.replay_buffer.cursor()


def get_events_since(cursor=None):
    with _ZMQ_EVENT_PUBLISHER_LOCK:
        publisher = _ZMQ_EVENT_PUBLISHER
        if publisher is None:
            publisher = configure_zmq_event_publisher()
        return publisher.replay_buffer.events_since(cursor)


def stop_zmq_event_publisher():
    global _ZMQ_EVENT_PUBLISHER
    with _ZMQ_EVENT_PUBLISHER_LOCK:
        publisher = _ZMQ_EVENT_PUBLISHER
        _ZMQ_EVENT_PUBLISHER = None
    if publisher is not None:
        publisher.close()

# ThinkTrader enum_EEntrustBS (买卖方向, the m_nDirection field), universal across
# 股票/期货/期权. Ref: https://dict.thinktrader.net/innerApi/enum_constants.html
ENTRUST_BUY = 48         # 买入 / 多
ENTRUST_SELL = 49        # 卖出 / 空
ENTRUST_PLEDGE_IN = 81   # 质押入库
ENTRUST_PLEDGE_OUT = 66  # 质押出库

# enum_EEntrustBS (买卖方向, the m_nDirection field), per QMT enum docs.
# 48=买, 49=卖.  Universal across 股票/期货/期权.
#
# Real-world findings from live COrderDetail/CDealDetail callbacks
# (diagnosed via exec_events_debug_raw_fields=True, 2026-07-29):
#   QMT returns m_nDirection=48 **unconditionally** — even for sell orders.
#   m_nOffsetFlag correctly reflects direction (48=买, 49=卖 for stocks).
#   m_nOpType correctly reflects direction (23=买, 24=卖) on orders.
#   query_orders uses m_nOffsetFlag and works correctly in production.
#
# Therefore _extract_direction uses an arbitration chain:
#   Preferred: m_nOffsetFlag (most reliable in live callbacks, matches query_orders)
#   Fallback:  m_nDirection (traditional EEntrustBS; can be stuck at 48 in calls)
#   Arbiter:   when direction≠offset (futures: sell+open=49+48),
#              consult m_nOpType (23/24) to resolve the conflict; for trades
#              (no m_nOpType) trust m_nOffsetFlag (QMT docs confirm stock
#              direction=offset).
#   Last:      order_type (MiniQMT STOCK_BUY=23 / STOCK_SELL=24) and plain text
# Unknown -> "" (the raw value is always preserved so callers can refine).
OFFSET_OPEN = 48
OFFSET_CLOSE = 49
OFFSET_CLOSE_TODAY = 51
OFFSET_CLOSE_YESTERDAY = 52

_BUY_DIRECTIONS = {ENTRUST_BUY, str(ENTRUST_BUY), OFFSET_OPEN, str(OFFSET_OPEN), 23, "23", "BUY", "buy", "B"}
_SELL_DIRECTIONS = {ENTRUST_SELL, str(ENTRUST_SELL), OFFSET_CLOSE, str(OFFSET_CLOSE), OFFSET_CLOSE_TODAY, str(OFFSET_CLOSE_TODAY), OFFSET_CLOSE_YESTERDAY, str(OFFSET_CLOSE_YESTERDAY), 24, "24", "SELL", "sell", "S"}


def order_channel(account_id):
    return ORDER_CHANNEL_TEMPLATE.format(account_id=str(account_id or ""))


def trade_channel(account_id):
    return TRADE_CHANNEL_TEMPLATE.format(account_id=str(account_id or ""))


def _attr(obj, names, default=None):
    for name in names:
        if isinstance(obj, dict):
            if name in obj and obj[name] is not None:
                return obj[name]
        else:
            value = getattr(obj, name, None)
            if value is not None:
                return value
    return default


def _action_from_direction(direction):
    if direction in _BUY_DIRECTIONS:
        return "BUY"
    if direction in _SELL_DIRECTIONS:
        return "SELL"
    return ""


def _is_buy(val):
    v = int(val)
    return v in _BUY_DIRECTIONS


def _is_sell(val):
    v = int(val)
    return v in _SELL_DIRECTIONS


def _conflict_resolve(d_val, o_val, obj):
    """When m_nDirection and m_nOffsetFlag disagree, arbitrate via m_nOpType.

    Live diagnosis confirms:
      - Stock sell: direction=48(buy), offset=49(sell), op_type=24(sell) → sell
      - Futures sell+open: direction=49(sell), offset=48(open), op_type=24(sell) → sell
      - Futures buy+close: direction=48(buy), offset=49(close), op_type=23(buy) → buy

    Returns a resolved value, or None if no arbiter can decide.
    """
    op = _attr(obj, ["m_nOpType", "op_type", "order_type"])
    if op is not None:
        try:
            op_int = int(op)
            if op_int in _BUY_DIRECTIONS:
                return d_val if _is_buy(d_val) else o_val if _is_buy(o_val) else op
            if op_int in _SELL_DIRECTIONS:
                return d_val if _is_sell(d_val) else o_val if _is_sell(o_val) else op
        except (TypeError, ValueError):
            if op in _BUY_DIRECTIONS:
                return d_val if _is_buy(d_val) else o_val if _is_buy(o_val) else op
            if op in _SELL_DIRECTIONS:
                return d_val if _is_sell(d_val) else o_val if _is_sell(o_val) else op
    # no arbiter — trust offset (QMT docs confirm stock direction=offset)
    return o_val


def _extract_direction(obj):
    """Extract buy/sell direction, matching query_orders' reliable logic.

    Priority chain (documented with live-diagnosis justification):
      1. m_nOffsetFlag         — most reliable in live callbacks (matches query_orders)
      2. m_nDirection           — traditional EEntrustBS (can be stuck at 48)
      3. Arbitration: when direction≠offset, consult m_nOpType (orders: 23/24)
         to resolve correctly for both stocks AND futures.
      4. m_nOpType / order_type — last resort fallback.

    The raw value is always returned (even pledge=81) so callers can inspect it;
    _action_from_direction maps only known buy/sell values, leaving others "".

    References
    ----------
    - Live diagnosis 2026-07-29 (COrderDetail/CDealDetail):
      m_nDirection=48 unconditionally, m_nOffsetFlag=48(buy)/49(sell) correct,
      m_nOpType=23(buy)/24(sell) correct (orders only).
    - QMT enum docs: enum_EEntrustBS (48=买,49=卖), enum_EOffset_Flag_Type
      (48=开仓,49=平仓). For stocks direction=offset; for futures they differ.
    - query_orders uses m_nOffsetFlag and works correctly in production.
    """
    offset = _attr(obj, ["m_nOffsetFlag", "offset_flag"])
    direction = _attr(obj, ["m_nDirection", "direction"])

    # 1. offset alone — use it directly (matches query_orders)
    if offset is not None and direction is None:
        try:
            o = int(offset)
            if o in _BUY_DIRECTIONS or o in _SELL_DIRECTIONS:
                return offset
        except (TypeError, ValueError):
            if offset in _BUY_DIRECTIONS or offset in _SELL_DIRECTIONS:
                return offset

    # 2. direction alone — use it
    if direction is not None and offset is None:
        try:
            d = int(direction)
            if d in _BUY_DIRECTIONS or d in _SELL_DIRECTIONS:
                return direction
            if d != 0:
                return direction
        except (TypeError, ValueError):
            if direction in _BUY_DIRECTIONS or direction in _SELL_DIRECTIONS:
                return direction
            return direction

    # 3. both present
    if direction is not None and offset is not None:
        try:
            d = int(direction)
            o = int(offset)
            d_valid = (d in _BUY_DIRECTIONS or d in _SELL_DIRECTIONS)
            o_valid = (o in _BUY_DIRECTIONS or o in _SELL_DIRECTIONS)

            if d_valid and o_valid:
                if d == o:
                    return direction  # agree → use either
                # disagree → arbitrate via m_nOpType
                return _conflict_resolve(d, o, obj)

            if d_valid and not o_valid:
                return direction
            if o_valid and not d_valid:
                return offset
            # neither valid — fall through
        except (TypeError, ValueError):
            pass

    # 4. last resort: m_nOpType / order_type
    return _attr(obj, ["m_nOpType", "op_type", "order_type"])


# Fields we care about when diagnosing a direction misread. Anything starting
# with "m_" is captured automatically; these are bridge field names that
# do not match that prefix.
_RAW_SNAPSHOT_EXTRA_FIELDS = (
    "stock_code",
    "order_type",
    "op_type",
    "direction",
    "offset_flag",
    "order_status",
    "order_volume",
    "traded_volume",
    "price",
    "order_id",
    "order_sysid",
    "order_sys_id",
    "trade_id",
    "traded_id",
    "strategy_name",
    "order_remark",
)


def raw_field_snapshot(obj, max_repr=120):
    """Capture every readable field of a live QMT callback object.

    Direction extraction relies on understanding what ``m_nDirection``,
    ``m_nOffsetFlag`` and ``m_nOpType`` carry in live callbacks. This dumps
    every readable field so one live order settles the question.

    Returns ``{name: "<type> <value>"}``. Never raises: a callback that dies
    while being diagnosed would be worse than no diagnosis.
    """
    snapshot = {}
    try:
        if isinstance(obj, dict):
            names = list(obj.keys())
        else:
            names = [name for name in dir(obj) if name.startswith("m_")]
            names.extend(_RAW_SNAPSHOT_EXTRA_FIELDS)
    except Exception:
        return {"__error__": "dir() failed"}
    seen = set()
    for name in names:
        key = str(name)
        if key in seen or key.startswith("__"):
            continue
        seen.add(key)
        try:
            if isinstance(obj, dict):
                if key not in obj:
                    continue
                value = obj[key]
            else:
                if not hasattr(obj, key):
                    continue
                value = getattr(obj, key)
            if callable(value):
                continue
            text = repr(value)
            if len(text) > max_repr:
                text = text[:max_repr] + "..."
            snapshot[key] = "%s %s" % (type(value).__name__, text)
        except Exception as exc:  # noqa: BLE001 - diagnostics must not break callbacks
            snapshot[key] = "<unreadable: %s>" % exc.__class__.__name__
    return snapshot


def format_raw_snapshot(kind, obj):
    """One-line, GBK-safe rendering of :func:`raw_field_snapshot` for the QMT panel."""
    snapshot = raw_field_snapshot(obj)
    parts = ["%s=%s" % (name, snapshot[name]) for name in sorted(snapshot)]
    return "[bigqmt_exec_raw] %s type=%s %s" % (
        kind,
        type(obj).__name__,
        " | ".join(parts) or "<no fields>",
    )


def normalize_order_event(order, account_id=""):
    """Build a JSON-able order event dict from a Big QMT orderInfo object."""
    direction = _extract_direction(order)
    return {
        "event_type": EVENT_ORDER,
        "account_id": str(_attr(order, ["m_strAccountID", "account_id"], account_id) or account_id or ""),
        "stock_code": str(_attr(order, ["m_strInstrumentID", "stock_code", "m_strInstrument"], "") or ""),
        "order_sys_id": str(_attr(order, ["m_strOrderSysID", "order_sys_id", "order_sysid", "order_id"], "") or ""),
        "order_volume": _attr(order, ["m_nVolumeTotal", "order_volume", "volume"]),
        "traded_volume": _attr(order, ["m_nVolumeTraded", "traded_volume"]),
        "price": _attr(order, ["m_dLimitPrice", "price", "limit_price"]),
        "status": _attr(order, ["m_nOrderStatus", "order_status", "status"]),
        "direction": direction,
        "action": _action_from_direction(direction),
        "offset_flag": _attr(order, ["m_nOffsetFlag", "offset_flag"]),
        "strategy_name": str(_attr(order, ["m_strOptName", "strategy_name", "order_remark", "remark"], "") or ""),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "created_at_ts": time.time(),
    }


def normalize_trade_event(trade, account_id=""):
    """Build a JSON-able trade (成交) event dict from a Big QMT dealInfo object."""
    direction = _extract_direction(trade)
    return {
        "event_type": EVENT_TRADE,
        "account_id": str(_attr(trade, ["m_strAccountID", "account_id"], account_id) or account_id or ""),
        "stock_code": str(_attr(trade, ["m_strInstrumentID", "stock_code"], "") or ""),
        "order_sys_id": str(_attr(trade, ["m_strOrderSysID", "order_sys_id", "order_sysid", "order_id"], "") or ""),
        "trade_id": str(_attr(trade, ["m_strTradeID", "trade_id"], "") or ""),
        "volume": _attr(trade, ["m_nVolume", "volume", "traded_volume"]),
        "price": _attr(trade, ["m_dPrice", "price", "traded_price"]),
        "amount": _attr(trade, ["m_dTradeAmount", "amount"]),
        "commission": _attr(trade, ["m_dComssion", "m_dCommission", "commission"]),
        "direction": direction,
        "action": _action_from_direction(direction),
        "offset_flag": _attr(trade, ["m_nOffsetFlag", "offset_flag"]),
        "traded_at": str(_attr(trade, ["m_strTradeTime", "traded_at", "trade_time"], "") or ""),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "created_at_ts": time.time(),
    }


def _publish(redis_client, channel, event, maxlen=2000):
    # Redis stream/pubsub is also a process boundary. Preserve any ambient
    # request/callback trace without changing the event's business fields.
    event = inject_trace(event)
    raw = json.dumps(event, ensure_ascii=False, default=str)
    try:
        redis_client.xadd(channel, {"payload": raw}, maxlen=maxlen, approximate=True)
    except Exception as exc:
        emit("execution_event.redis_replay_write", critical=True, outcome="error", error_type=type(exc).__name__)
    try:
        redis_client.publish(channel, raw)
        emit("execution_event.redis_publish", critical=True, outcome="success", event_type=(event or {}).get("event_type"))
    except Exception as exc:
        emit("execution_event.redis_publish", critical=True, outcome="error", error_type=type(exc).__name__)
        raise
    return event


def publish_order_event(redis_client, account_id, event):
    return _publish(redis_client, order_channel(account_id), event)


def publish_trade_event(redis_client, account_id, event):
    return _publish(redis_client, trade_channel(account_id), event)
