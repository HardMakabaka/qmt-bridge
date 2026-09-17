"""Redis signal state store."""

import datetime as _dt

from ..telemetry import emit


class RedisStateStore:
    def __init__(
        self,
        redis_client,
        account_id="default",
        claim_key_template="bigqmt:signal_claim:{account_id}:{signal_id}",
        status_key_template="bigqmt:signal_status:{account_id}:{signal_id}",
        claim_ttl_seconds=3600,
        status_ttl_seconds=86400,
    ):
        self.redis = redis_client
        self.account_id = account_id
        self.claim_key_template = claim_key_template
        self.status_key_template = status_key_template
        self.claim_ttl_seconds = int(claim_ttl_seconds)
        self.status_ttl_seconds = int(status_ttl_seconds)
        self._accounts_by_signal_id = {}

    def _account_for(self, signal_id):
        return self._accounts_by_signal_id.get(signal_id) or self.account_id

    def _claim_key(self, account_id, signal_id):
        return self.claim_key_template.format(account_id=account_id, signal_id=signal_id)

    def _status_key(self, account_id, signal_id):
        return self.status_key_template.format(account_id=account_id, signal_id=signal_id)

    @staticmethod
    def _now_text():
        return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write_status(self, account_id, signal_id, mapping):
        key = self._status_key(account_id, signal_id)
        fields = {
            "signal_id": signal_id,
            "account_id": account_id,
            "updated_at": self._now_text(),
        }
        fields.update({k: "" if v is None else str(v) for k, v in mapping.items()})
        self.redis.hset(key, mapping=fields)
        if self.status_ttl_seconds > 0:
            self.redis.expire(key, self.status_ttl_seconds)

    def claim(self, signal, consumer_id):
        account_id = signal.account_id or self.account_id
        self._accounts_by_signal_id[signal.signal_id] = account_id
        key = self._claim_key(account_id, signal.signal_id)
        ok = self.redis.set(key, consumer_id, nx=True, ex=self.claim_ttl_seconds)
        if ok:
            self._write_status(
                account_id,
                signal.signal_id,
                {
                    "status": "CLAIMED",
                    "consumer_id": consumer_id,
                    "stock_code": signal.stock_code,
                    "action": signal.action.value,
                    "message": "",
                },
            )
        emit("signal.state.claim", critical=True, signal_id=signal.signal_id,
             account_id=account_id, outcome="success" if ok else "rejected")
        return bool(ok)

    def mark_submitted(self, signal_id, result):
        account_id = self._account_for(signal_id)
        self._write_status(
            account_id,
            signal_id,
            {
                "status": result.status,
                "user_order_id": result.user_order_id,
                "order_sys_id": result.order_sys_id,
                "message": result.message,
            },
        )
        emit("signal.state.submitted", critical=True, signal_id=signal_id,
             account_id=account_id, client_submit_id=result.user_order_id,
             order_sys_id=result.order_sys_id, outcome="success", status=result.status)

    def mark_finished(self, signal_id, status, message=""):
        account_id = self._account_for(signal_id)
        self._write_status(
            account_id,
            signal_id,
            {
                "status": status,
                "message": message,
            },
        )
        emit("signal.state.finished", critical=True, signal_id=signal_id,
             account_id=account_id, outcome="success", status=status)

    def mark_submitting(self, signal_id, request):
        """Durably record the exact broker identity before ``passorder``.

        A Redis Stream acknowledgement is intentionally later than this write:
        after a process crash, recovery can reconcile this immutable intent
        against QMT instead of submitting the signal a second time.
        """
        account_id = str(request.account_id or self._account_for(signal_id))
        self._accounts_by_signal_id[signal_id] = account_id
        self._write_status(
            account_id,
            signal_id,
            {
                "status": "SUBMITTING",
                "user_order_id": request.remark,
                "stock_code": request.stock_code,
                "action": request.action,
                "volume": request.volume,
                "price": request.price,
                "price_type": request.price_type,
                "strategy_name": request.strategy_name,
                "message": "durable intent recorded before broker submission",
            },
        )
        emit("signal.state.intent", critical=True, signal_id=signal_id, account_id=account_id,
             client_submit_id=request.remark, stock_code=request.stock_code,
             action=request.action, volume=request.volume, price=request.price,
             strategy_name=request.strategy_name, outcome="success")

    def get_status(self, signal_id, account_id=None):
        """Read durable signal state for safe stream-pending reconciliation."""
        account_id = str(account_id or self._account_for(signal_id))
        raw = self.redis.hgetall(self._status_key(account_id, signal_id)) or {}
        decoded = {}
        for key, value in raw.items():
            if isinstance(key, bytes):
                key = key.decode("utf-8", "replace")
            if isinstance(value, bytes):
                value = value.decode("utf-8", "replace")
            decoded[str(key)] = str(value)
        return decoded
