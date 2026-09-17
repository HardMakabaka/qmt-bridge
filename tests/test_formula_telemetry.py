from bigqmt_signal_trader.formula_server import (
    FormulaServerRouter,
    FormulaServerUnavailable,
)
from bigqmt_signal_trader.telemetry import bind_context


class _UnavailableFormulaClient:
    timeout_seconds = 0.1
    host = "127.0.0.1"
    port = 58600

    def request(self, _func, _params, **_kwargs):
        raise FormulaServerUnavailable("fixture offline")

    def close(self):
        pass


def test_formula_fallback_records_trace_outcome(trace_events):
    router = FormulaServerRouter(
        client=_UnavailableFormulaClient(), methods=["get_instrument"],
        failure_cooldown_seconds=1,
    )
    with bind_context(trace_id="formula-trace", rpc_request_id="rpc-formula"):
        try:
            router.call("get_instrument", {"code": "000001.SZ"})
        except Exception:
            pass
    events = trace_events()
    fallback = [event for event in events if event["event_name"] == "formula.router.fallback"][-1]
    assert fallback["trace_id"] == "formula-trace"
    assert fallback["rpc_request_id"] == "rpc-formula"
    assert fallback["outcome"] == "timeout"
