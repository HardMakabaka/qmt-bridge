"""Shared test fixtures for QMT Bridge."""

import json
import os

import pytest


# Never write test diagnostics into the user's normal runtime trace directory.
os.environ.setdefault("QMT_BRIDGE_TRACE_ENABLED", "false")


@pytest.fixture
def trace_events(tmp_path):
    """Opt-in isolated JSONL reader for end-to-end telemetry assertions."""
    from bigqmt_signal_trader import telemetry

    telemetry.shutdown(2.0)
    telemetry.configure(enabled=True, directory=str(tmp_path / "traces"), role="test",
                        max_bytes=5 * 1024 * 1024, backup_count=3, queue_size=16384)

    def read_events():
        assert telemetry.flush(2.0), "telemetry did not flush within the test budget"
        records = []
        for path in (tmp_path / "traces").glob("*.jsonl*"):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    records.append(json.loads(line))
        return records

    yield read_events
    telemetry.shutdown(2.0)
    telemetry.configure(enabled=False)
