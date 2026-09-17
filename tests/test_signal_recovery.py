import datetime as dt

from bigqmt_signal_trader.adapters.signal_redis import RedisStreamSignalSource
from bigqmt_signal_trader.app import SignalTradingApp
from bigqmt_signal_trader.models import SignalAction, TradeSignal


def _payload(signal_id="signal-1"):
    now = dt.datetime.now()
    return {
        "signal_id": signal_id, "account_id": "acct-1", "action": "BUY",
        "stock_code": "000001.SZ", "amount": 100, "schema_version": 1,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "expire_at": (now + dt.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
    }


class _StreamRedis:
    def __init__(self):
        self.claims = []

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def xautoclaim(self, *args, **kwargs):
        self.claims.append((args, kwargs))
        return "0-0", [("1-0", {"payload": __import__("json").dumps(_payload())})], []

    def xreadgroup(self, **_kwargs):
        return []

    def xack(self, *args):
        return args


def test_stream_source_reclaims_at_most_configured_pending_entries():
    redis = _StreamRedis()
    source = RedisStreamSignalSource(redis, reclaim_idle_ms=1000, reclaim_count=1)
    signals = source.fetch("acct-1", 20)
    assert [signal.signal_id for signal in signals] == ["signal-1"]
    assert redis.claims[0][1]["count"] == 1


class _Source:
    def __init__(self, signal):
        self.signal = signal
        self.acks = []

    def fetch(self, _account_id, _limit):
        return [self.signal]

    def ack(self, signal):
        self.acks.append(signal.signal_id)


class _State:
    def get_status(self, signal_id, _account_id):
        return {"signal_id": signal_id, "status": "SUBMITTED_UNCONFIRMED"}

    def claim(self, *_args):
        raise AssertionError("a reclaimed submitted signal must not be reclaimed for execution")


class _Positions:
    def get_positions(self, _account_id):
        return []

    def get_asset(self, _account_id):
        return {}


def test_reclaimed_submitted_signal_is_acked_without_resubmission():
    signal = TradeSignal.from_dict(_payload())
    source = _Source(signal)
    app = SignalTradingApp(
        "acct-1", source, object(), _Positions(),
        order_gateway=object(), position_sync_sink=type("Sink", (), {"publish": lambda *_a: None})(),
        state_store=_State(),
    )
    app.tick()
    assert source.acks == ["signal-1"]


class _CrashWindowSource:
    def __init__(self, signal):
        self.signal = signal
        self.acks = []

    def fetch(self, _account_id, _limit):
        return [self.signal]

    def ack(self, signal):
        self.acks.append(signal.signal_id)


class _CrashWindowState:
    def __init__(self):
        self.status = {}
        self.claims = 0
        self.finished = []

    def get_status(self, _signal_id, _account_id):
        return dict(self.status)

    def claim(self, _signal, _consumer):
        self.claims += 1
        return self.claims == 1

    def mark_submitting(self, _signal_id, request):
        self.status = {
            "status": "SUBMITTING", "user_order_id": request.remark,
            "stock_code": request.stock_code, "action": request.action,
            "volume": str(request.volume), "price": str(request.price),
            "strategy_name": request.strategy_name,
        }

    def mark_submitted(self, *_args):
        # Simulate process death / failed durable receipt after passorder.
        raise OSError("state write interrupted")

    def mark_finished(self, *_args):
        self.finished.append(_args)


class _SubmitThenUnknownGateway:
    def __init__(self):
        self.submit_calls = 0
        self.lookup_calls = 0

    def submit(self, request):
        self.submit_calls += 1
        return type("Result", (), {
            "status": "SUBMITTED", "user_order_id": request.remark,
            "order_sys_id": "broker-1", "message": "accepted",
        })()

    def query_submission_identities_strict(self, _account_id, _strategy_name):
        self.lookup_calls += 1
        return [], []


def test_crash_after_broker_submit_stays_pending_when_reconciliation_is_unknown(trace_events):
    payload = _payload()
    payload.update({"price_type": "FIX_PRICE", "price": 10.0, "remark": "stable-order-id"})
    signal = TradeSignal.from_dict(payload)
    source = _CrashWindowSource(signal)
    state = _CrashWindowState()
    gateway = _SubmitThenUnknownGateway()
    app = SignalTradingApp(
        "acct-1", source, object(), _Positions(), gateway,
        type("Sink", (), {"publish": lambda *_a: None})(), state,
    )

    app.tick()  # native submit ran, but durable final receipt failed
    app.tick()  # reclaimed pending signal must query, not submit again

    assert gateway.submit_calls == 1
    assert gateway.lookup_calls == 1
    assert source.acks == []
    assert state.finished == []
    names = [event["event_name"] for event in trace_events()]
    assert "signal.intent.persisted" in names
    assert "signal.submission.unknown" in names
    assert "signal.reconcile.pending" in names
