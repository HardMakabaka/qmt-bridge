import importlib

import pytest

from bigqmt_signal_trader import telemetry


@pytest.fixture
def strategy(monkeypatch):
    module = importlib.import_module("bigqmt_signal_trader_strategy")
    module.reset_app()
    monkeypatch.setattr(module, "_rpc_service", None)
    monkeypatch.setattr(module, "_account_id", "")
    return module


def test_adjust_phase_error_emits_error_and_reraises(strategy, trace_events):
    def broken():
        raise ValueError("expected")

    with pytest.raises(ValueError, match="expected"):
        strategy._adjust_phase("broken", broken)

    events = [e for e in trace_events() if e["event_name"] == "adjust.phase"]
    assert events[-1]["outcome"] == "error"
    assert events[-1]["error_type"] == "ValueError"


def test_fast_adjust_phases_are_summarized_not_emitted_per_call(strategy, monkeypatch, trace_events):
    monkeypatch.setattr(strategy.time, "perf_counter", lambda: 1.0)
    monkeypatch.setattr(strategy.time, "time", lambda: 1.0)
    for _ in range(20):
        assert strategy._adjust_phase("noop", lambda: None) is None

    names = [e["event_name"] for e in trace_events()]
    assert "adjust.phase" not in names
    assert "adjust.phase.summary" not in names


@pytest.mark.parametrize("config, account, expected", [
    ({"exec_events": {"enabled": False}}, "", "not_enabled"),
    ({"exec_events": {"enabled": True}}, "", "no_account"),
])
def test_exec_publish_early_returns_keep_one_root_trace(
    strategy, monkeypatch, trace_events, config, account, expected,
):
    monkeypatch.setattr(strategy, "_build_config", lambda: config)
    monkeypatch.setattr(strategy, "_account_id", account)
    strategy._publish_exec_event("order", object())

    events = [e for e in trace_events() if e["event_name"].startswith("exec_event")]
    trace_ids = {e.get("trace_id") for e in events}
    terminal = [e for e in events if e["event_name"] == "exec_event_publish_terminal"]
    assert len(trace_ids) == 1 and None not in trace_ids
    assert terminal[-1]["outcome"] == expected


def test_init_configures_qmt_strategy_root_trace(strategy, monkeypatch, trace_events):
    class Context(object):
        pass

    monkeypatch.setattr(strategy, "_detect_account_id", lambda _ctx: "")
    monkeypatch.setattr(strategy, "_apply_gil_tuning", lambda: None)
    monkeypatch.setattr(strategy, "_start_latency_probe", lambda: None)
    monkeypatch.setattr(strategy, "_build_config", lambda: {})
    monkeypatch.setattr(strategy.BigQmtRuntimeAdapter, "__new__", lambda _cls, _ctx: object())
    sentinel = object()
    monkeypatch.setattr(strategy, "init_app", lambda _runtime, _build: sentinel)
    monkeypatch.setattr(strategy, "_start_rpc_service", lambda *_args: None)
    monkeypatch.setattr(strategy, "_schedule_adjust_if_needed", lambda *_args: None)
    monkeypatch.setattr(strategy, "_diag_startup", lambda *_args: None)
    # Keep the fixture's isolated writer while proving init invokes its config path.
    monkeypatch.setattr(telemetry, "configure", lambda **_kwargs: True)

    assert strategy.init(Context()) is sentinel
    events = [e for e in trace_events() if e["event_name"].startswith("strategy.init")]
    assert events[0]["event_name"] == "strategy.init.start"
    assert events[0]["trace_id"]
    assert events[0]["service_role"] == "qmt_strategy"
