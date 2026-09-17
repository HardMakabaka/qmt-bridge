import ast
from pathlib import Path

from qmt_bridge.server.app import create_app
from qmt_bridge.server.config import Settings


def _client_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if not isinstance(node, ast.JoinedStr):
        return None
    parts = []
    for value in node.values:
        if isinstance(value, ast.Constant):
            parts.append(str(value.value))
        elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name):
            parts.append("{" + value.value.id + "}")
    return "".join(parts)


def test_python_client_covers_every_public_http_operation() -> None:
    client_operations = set()
    client_root = Path("src/qmt_bridge/client")
    for source_path in client_root.glob("*.py"):
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {"_get", "_post", "_delete"} or not node.args:
                continue
            path = _client_path(node.args[0])
            if path is not None:
                client_operations.add((node.func.attr[1:].upper(), path))

    app = create_app(Settings(account_enabled=True, trading_account_id="acct-1"))
    server_operations = {
        (method.upper(), path)
        for path, operations in app.openapi()["paths"].items()
        for method in operations
        if method in {"get", "post", "put", "patch", "delete"}
    }

    assert client_operations == server_operations


def test_minute_recovery_clients_preserve_status_and_request_contract(monkeypatch) -> None:
    from qmt_bridge import QMTClient

    client = QMTClient("127.0.0.1", port=1)
    calls = []
    payload = {"status": "partial", "missing_stocks": ["000300.SH"], "data": {}}

    def fake_get(path, params=None):
        calls.append((path, params))
        return payload

    monkeypatch.setattr(client, "_get", fake_get)
    assert client.get_minute_tail(["000300.SH"], "20260820145700", "20260820145900") is payload
    assert calls[-1] == ("/api/market/minute_tail", {
        "stocks": "000300.SH", "start_time": "20260820145700", "end_time": "20260820145900",
        "count": 3, "refresh_missing": True,
    })
    assert client.get_history_readiness("000300.SH", "20260820145700", "20260820145900") is payload
    assert calls[-1][0] == "/api/meta/history-readiness"
    assert calls[-1][1]["timeout_seconds"] == 8
    assert client.get_recovery_status() is payload
    assert calls[-1] == ("/api/meta/recovery-status", None)
