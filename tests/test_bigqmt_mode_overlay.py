import sys
from pathlib import Path
from types import SimpleNamespace


OVERLAY = Path(__file__).resolve().parents[1] / "src" / "mecostock_bigqmt_mode_overlay.py"


def test_mode_overlay_exposes_current_qmt_request_id() -> None:
    calls = []

    class FakeHandlers:
        def _handle_ping(self, params):
            return {"pong": True}

    module_name = "bigqmt_signal_trader.redis_rpc"
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = SimpleNamespace(BigQmtRpcHandlers=FakeHandlers)
    namespace = {
        "__name__": "mecostock_overlay_test",
        "init": lambda context: calls.append(("init", context)),
        "adjust": lambda context: calls.append(("adjust", context)),
        "handlebar": lambda context: calls.append(("handlebar", context)),
    }
    try:
        exec(compile(OVERLAY.read_bytes(), str(OVERLAY), "exec"), namespace, namespace)
        context = SimpleNamespace(context=SimpleNamespace(request_id="qmt-request-17"))

        namespace["init"](context)
        first = FakeHandlers()._handle_ping({})

        context.context.request_id = "qmt-request-18"
        namespace["adjust"](context)
        second = FakeHandlers()._handle_ping({})
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous

    assert first["qmt_request_id"] == "qmt-request-17"
    assert second["qmt_request_id"] == "qmt-request-18"
    assert [item[0] for item in calls] == ["init", "adjust"]
