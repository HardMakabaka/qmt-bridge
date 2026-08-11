import json
import os
import urllib.request

from qmt_bridge.client import base as client_base
from qmt_bridge.client.base import BaseClient
from qmt_bridge.no_proxy import disable_environment_proxies


def test_disable_environment_proxies_clears_python_proxy_env(monkeypatch):
    for name in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        monkeypatch.setenv(name, "http://127.0.0.1:10206")

    disable_environment_proxies()

    for name in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        assert name not in os.environ
    assert os.environ["NO_PROXY"] == "*"
    assert os.environ["no_proxy"] == "*"
    proxies = urllib.request.getproxies()
    assert "http" not in proxies
    assert "https" not in proxies
    assert "all" not in proxies


def test_base_client_uses_direct_urlopen(monkeypatch):
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps({"status": "ok"}).encode()

    def fake_urlopen_direct(request):
        calls.append(request.full_url)
        return FakeResponse()

    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:10206")
    monkeypatch.setattr(client_base, "urlopen_direct", fake_urlopen_direct)

    payload = BaseClient("127.0.0.1")._get("/api/meta/health")

    assert payload == {"status": "ok"}
    assert calls == ["http://127.0.0.1:13543/api/meta/health"]


def test_base_client_encodes_get_query_parameters(monkeypatch):
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen_direct(request):
        calls.append(request.full_url)
        return FakeResponse()

    monkeypatch.setattr(client_base, "urlopen_direct", fake_urlopen_direct)

    BaseClient("127.0.0.1")._get(
        "/api/smt/secu_rate",
        {"stock_code": "600000.SH", "label": "a b&c", "unused": None},
    )

    assert calls == [
        "http://127.0.0.1:13543/api/smt/secu_rate?"
        "stock_code=600000.SH&label=a+b%26c"
    ]


def test_client_response_value_preserves_provider_failure_envelope() -> None:
    client = BaseClient("127.0.0.1")
    failure = {
        "status": "unsupported",
        "data": None,
        "reason_code": "native_method_missing",
    }

    assert client._response_value(failure, default={}) is failure
    assert client._response_value({"status": "ok", "data": [1]}, default=[]) == [1]
