"""Bounded, optional correlation headers shared by the dependency-free SDK."""

import re

from bigqmt_signal_trader import telemetry


_TRACE_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_SPAN_ID = re.compile(r"^(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def outgoing_trace_headers() -> dict[str, str]:
    context = telemetry.current_context()
    headers = {}
    for field, header, pattern in (
        ("trace_id", "X-QMT-Trace-Id", _TRACE_ID),
        ("span_id", "X-QMT-Span-Id", _SPAN_ID),
        ("http_request_id", "X-Request-ID", _REQUEST_ID),
    ):
        value = context.get(field)
        if isinstance(value, str) and pattern.fullmatch(value):
            headers[header] = value
    return headers


def incoming_trace_context(headers) -> dict[str, str]:
    """Ignore malformed/unbounded IDs; never treat correlation as identity."""
    context = {}
    for header, field, pattern in (
        ("x-qmt-trace-id", "trace_id", _TRACE_ID),
        ("x-qmt-span-id", "span_id", _SPAN_ID),
        ("x-request-id", "http_request_id", _REQUEST_ID),
    ):
        value = headers.get(header, "")
        if isinstance(value, str) and len(value) <= 128 and pattern.fullmatch(value):
            context[field] = value
    # A remote parent is meaningful only when its trace is valid as well.
    if "trace_id" not in context:
        context.pop("span_id", None)
    return context
