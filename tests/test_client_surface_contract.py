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
