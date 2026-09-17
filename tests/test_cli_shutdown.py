from pathlib import Path
from types import SimpleNamespace
import time

from qmt_bridge.server.cli import _start_local_shutdown_watcher


def test_local_shutdown_watcher_requires_matching_nonce(tmp_path: Path) -> None:
    server = SimpleNamespace(should_exit=False)
    request = tmp_path / "shutdown.request"
    stop, worker = _start_local_shutdown_watcher(server, str(request), "nonce-1")
    try:
        request.write_text("wrong", encoding="utf-8")
        time.sleep(0.15)
        assert server.should_exit is False

        request.write_text("nonce-1", encoding="utf-8")
        worker.join(timeout=1)
        assert server.should_exit is True
    finally:
        stop.set()
        worker.join(timeout=1)
