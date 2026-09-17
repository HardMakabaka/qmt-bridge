from unittest.mock import Mock

import pytest

from qmt_bridge.client.base import BaseClient


@pytest.mark.parametrize("method,args", [("_get", ("/test",)), ("_post", ("/test", {})), ("_delete", ("/test",))])
def test_all_http_methods_use_timeout_without_replaying_request(monkeypatch, method, args):
    opener = Mock(side_effect=TimeoutError("test timeout"))
    monkeypatch.setattr("qmt_bridge.client.base.urlopen_direct", opener)
    client = BaseClient("127.0.0.1", timeout=0.25)
    with pytest.raises(TimeoutError):
        getattr(client, method)(*args)
    assert opener.call_count == 1
    assert opener.call_args.kwargs == {"timeout": 0.25}


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_timeout_must_be_positive_and_finite(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        BaseClient("127.0.0.1", timeout=timeout)


@pytest.mark.parametrize("status", ["overloaded", "timeout", "partial", "stale"])
def test_client_does_not_discard_incomplete_or_failed_response(status):
    payload = {"status": status, "data": {"000001.SZ": []}, "reason": "fixture"}
    assert BaseClient("127.0.0.1")._response_value(payload) is payload
