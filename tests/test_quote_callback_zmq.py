import socket
import time

import zmq

from bigqmt_signal_trader.exec_events import (
    EventReplayBuffer,
    ZmqExecutionEventPublisher,
)


def _loopback_endpoint():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    return f"tcp://127.0.0.1:{port}"


def test_transient_quote_shares_zmq_without_advancing_trade_cursor():
    # Given: the execution publisher and a live subscriber on an isolated loopback port.
    endpoint = _loopback_endpoint()
    publisher = ZmqExecutionEventPublisher(
        bind_address=endpoint,
        replay_buffer=EventReplayBuffer(epoch="quote-test"),
    )
    publisher.start()
    subscriber = zmq.Context.instance().socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.RCVTIMEO, 2000)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.connect(endpoint)
    time.sleep(0.2)

    try:
        # When: a quote callback is published through the shared transient channel.
        publisher.publish_transient(
            {
                "event_type": "quote",
                "code": "000001.SZ",
                "data": {"lastPrice": 10.5},
            }
        )
        event = subscriber.recv_json()

        # Then: the quote crosses the real ZMQ socket and trade replay cursor stays unchanged.
        assert event["event_type"] == "quote"
        assert event["data"]["lastPrice"] == 10.5
        assert publisher.replay_buffer.cursor() == {
            "epoch": "quote-test",
            "sequence": 0,
        }
    finally:
        subscriber.close(linger=0)
        publisher.close()
