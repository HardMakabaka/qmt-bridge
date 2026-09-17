"""Deadline propagation between FormulaServer and the normal RPC fallback."""

import time

import pytest

from bigqmt_signal_trader.formula_server import Unroutable
from bigqmt_signal_trader.request_budget import request_budget
from bigqmt_signal_trader.rpc_client import BigQmtRpcClient


def test_expired_formula_fallback_does_not_start_rpc():
    class FormulaMiss:
        def supports(self, method):
            return True

        def call(self, method, params, deadline_monotonic=None):
            assert deadline_monotonic is not None
            time.sleep(0.02)
            raise Unroutable(method)

    class Transport:
        def __init__(self):
            self.sent = []

        def send_request(self, request, timeout_seconds):
            self.sent.append((request, timeout_seconds))
            return {"ok": True, "data": {}}

    transport = Transport()
    client = BigQmtRpcClient(
        account_id="acct-1", redis_config={"transport": "zmq"}, timeout_seconds=1
    )
    client._formula_router_instance = FormulaMiss()
    client._transport_instance = transport

    with request_budget(time.monotonic() + 0.005):
        with pytest.raises(TimeoutError, match="deadline exhausted"):
            client.call("get_instrument_detail", {"code": "000001.SZ"})

    assert transport.sent == []
