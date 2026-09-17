import json

from bigqmt_signal_trader.exec_events import EventReplayBuffer, ZmqExecutionEventPublisher, publish_order_event
from bigqmt_signal_trader.telemetry import bind_context


def test_replay_append_and_gap_are_traced(trace_events):
    replay = EventReplayBuffer(maxlen=1, epoch="epoch-test")
    replay.append({"event_type": "order", "order_sys_id": "sys-1"})
    replay.append({"event_type": "trade", "trade_id": "trade-1"})
    result = replay.events_since({"epoch": "epoch-test", "sequence": 0})

    assert result["gap"] is True
    names = [event["event_name"] for event in trace_events()]
    assert "execution_event.replay_append" in names
    assert "execution_event.replay_read" in names
from bigqmt_signal_trader.exec_events import ZmqExecutionEventPublisher


class _Socket:
    def send_string(self, _value, **_kwargs):
        return None

    def close(self, **_kwargs):
        return None


def test_transient_quotes_are_aggregated_not_emitted_per_tick(trace_events):
    publisher = ZmqExecutionEventPublisher("tcp://127.0.0.1:15561")
    publisher._socket = _Socket()
    for _ in range(3):
        publisher.publish_transient({"event_type": "quote"})
    publisher.close()

    names = [event["event_name"] for event in trace_events()]
    assert "execution_event.transient_publish" not in names
    assert names.count("execution_event.transient_summary") == 1


def test_durable_zmq_event_injects_trace_into_wire_and_replay(trace_events):
    class Socket:
        def __init__(self):
            self.messages = []

        def send_string(self, value, **_kwargs):
            self.messages.append(value)

        def close(self, **_kwargs):
            return None

    publisher = ZmqExecutionEventPublisher("tcp://127.0.0.1:15561")
    socket = Socket()
    publisher._socket = socket
    with bind_context(trace_id="trace-wire", span_id="span-wire"):
        payload = publisher.publish({"event_type": "order", "order_sys_id": "sys-1"})

    wire = json.loads(socket.messages[0])
    assert wire["trace"]["trace_id"] == "trace-wire"
    assert payload["trace"] == wire["trace"]
    replay = publisher.replay_buffer.events_since()
    assert replay["events"][0]["trace"] == wire["trace"]


def test_durable_redis_event_injects_trace_into_stream_and_pubsub(trace_events):
    class Redis:
        def __init__(self):
            self.stream = None
            self.pubsub = None

        def xadd(self, _channel, fields, **_kwargs):
            self.stream = fields["payload"]

        def publish(self, _channel, payload):
            self.pubsub = payload

    redis = Redis()
    with bind_context(trace_id="trace-redis", span_id="span-redis"):
        payload = publish_order_event(redis, "account", {"event_type": "order"})
    assert payload["trace"]["trace_id"] == "trace-redis"
    assert json.loads(redis.stream)["trace"] == json.loads(redis.pubsub)["trace"]
