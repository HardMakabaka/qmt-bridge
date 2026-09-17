"""Redis Stream signal source.

Redis is only a transport here. It must never call passorder or inspect QMT.
"""

import datetime as _dt
import json

from ..models import TradeSignal
from ..telemetry import emit
from .redis_common import decode_text, redis_mapping_to_text


DEFAULT_STREAM_KEY_TEMPLATE = "bigqmt:signals:{account_id}"
DEFAULT_GROUP = "bigqmt-signal-trader"


def _json_default(value):
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _coerce_scalar(value):
    text = decode_text(value).strip()
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in ("none", "null"):
        return None
    return text


def parse_stream_payload(fields):
    text_fields = redis_mapping_to_text(fields)
    payload_text = text_fields.get("payload") or text_fields.get("data")
    if payload_text:
        payload = json.loads(payload_text)
        if not isinstance(payload, dict):
            raise ValueError("Redis stream payload must be a JSON object")
        return payload
    return {decode_text(key): _coerce_scalar(value) for key, value in fields.items()}


class RedisStreamSignalSource:
    def __init__(
        self,
        redis_client,
        stream_key_template=DEFAULT_STREAM_KEY_TEMPLATE,
        group_name=DEFAULT_GROUP,
        consumer_name="bigqmt-consumer",
        block_ms=0,
        reclaim_idle_ms=60000,
        reclaim_count=20,
    ):
        self.redis = redis_client
        self.stream_key_template = stream_key_template
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.block_ms = int(block_ms or 0)
        self.reclaim_idle_ms = max(0, int(reclaim_idle_ms or 0))
        self.reclaim_count = max(1, int(reclaim_count or 1))
        self._stream_ids_by_signal_id = {}
        self._created_groups = set()
        self._reclaim_cursor_by_stream = {}

    def _stream_key(self, account_id):
        return self.stream_key_template.format(account_id=account_id)

    def _ensure_group(self, stream_key):
        if stream_key in self._created_groups:
            return
        try:
            self.redis.xgroup_create(stream_key, self.group_name, id="0-0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._created_groups.add(stream_key)

    def fetch(self, account_id, limit):
        stream_key = self._stream_key(account_id)
        self._ensure_group(stream_key)
        signals = self._reclaim(stream_key, limit)
        remaining = max(0, int(limit) - len(signals))
        if remaining <= 0:
            return signals
        kwargs = {
            "groupname": self.group_name,
            "consumername": self.consumer_name,
            "streams": {stream_key: ">"},
            "count": remaining,
        }
        if self.block_ms > 0:
            kwargs["block"] = self.block_ms
        rows = self.redis.xreadgroup(**kwargs) or []
        for _, entries in rows:
            for stream_id, fields in entries:
                payload = parse_stream_payload(fields)
                signal = TradeSignal.from_dict(payload)
                self._stream_ids_by_signal_id[signal.signal_id] = (stream_key, stream_id)
                signals.append(signal)
        emit("signal.redis.fetch", account_id=account_id,
             outcome="empty" if not signals else "success", count=len(signals))
        return signals

    def _reclaim(self, stream_key, limit):
        """Boundedly claim abandoned pending entries for this consumer.

        Claiming only makes them visible again.  The application reconciles
        existing submitted state before acknowledging, and never blindly
        resubmits a reclaimed signal.
        """
        if self.reclaim_idle_ms <= 0 or not hasattr(self.redis, "xautoclaim"):
            return []
        count = min(max(1, int(limit)), self.reclaim_count)
        cursor = self._reclaim_cursor_by_stream.get(stream_key, "0-0")
        try:
            result = self.redis.xautoclaim(
                stream_key, self.group_name, self.consumer_name,
                self.reclaim_idle_ms, start_id=cursor, count=count,
            )
        except Exception:
            # Redis versions prior to XAUTOCLAIM continue to consume new
            # messages safely; do not turn an optional recovery mechanism
            # into a signal execution failure.
            return []
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            return []
        self._reclaim_cursor_by_stream[stream_key] = decode_text(result[0]) or "0-0"
        entries = result[1] or []
        signals = []
        for stream_id, fields in entries:
            payload = parse_stream_payload(fields)
            signal = TradeSignal.from_dict(payload)
            self._stream_ids_by_signal_id[signal.signal_id] = (stream_key, stream_id)
            signals.append(signal)
        emit("signal.redis.reclaim", critical=bool(signals),
             outcome="empty" if not signals else "success", count=len(signals))
        return signals

    def ack(self, signal):
        ref = self._stream_ids_by_signal_id.pop(signal.signal_id, None)
        if not ref:
            return None
        stream_key, stream_id = ref
        result = self.redis.xack(stream_key, self.group_name, stream_id)
        emit("signal.redis.ack", critical=True, signal_id=signal.signal_id,
             outcome="success" if result else "unknown")
        return result


def push_trade_signal(redis_client, payload, account_id=None, stream_key_template=DEFAULT_STREAM_KEY_TEMPLATE):
    if isinstance(payload, TradeSignal):
        account_id = account_id or payload.account_id
        raw_payload = dict(payload.raw_payload)
    else:
        raw_payload = dict(payload)
        account_id = account_id or raw_payload.get("account_id")
    if not account_id:
        raise ValueError("account_id is required")
    stream_key = stream_key_template.format(account_id=account_id)
    return redis_client.xadd(stream_key, {"payload": json.dumps(raw_payload, ensure_ascii=False, default=_json_default)})
