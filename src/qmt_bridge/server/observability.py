"""ASGI-boundary tracing without buffering request or response streams."""

import json
import uuid

from starlette.datastructures import Headers

from bigqmt_signal_trader import telemetry
from qmt_bridge.trace_headers import incoming_trace_context


class TraceMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        kind = scope["type"]
        if kind not in {"http", "websocket"}:
            return await self.app(scope, receive, send)

        context = {"trace_id": None, "span_id": None, "parent_span_id": None,
                   **incoming_trace_context(Headers(scope=scope))}
        context.setdefault("http_request_id" if kind == "http" else "ws_session_id", uuid.uuid4().hex)
        context["service_role"] = "bridge"
        state = getattr(scope.get("app"), "state", None)
        controller = getattr(state, "runtime_controller", None)
        if controller is not None:
            context["runtime_generation"] = getattr(controller, "generation", 0)
        name = "http.request" if kind == "http" else "ws.session"
        with telemetry.bind_context(**context), telemetry.span(name, method=scope.get("method", "WS")) as result:
            trace = telemetry.current_context()
            scope.setdefault("state", {})["trace_context"] = trace
            response_complete = False
            disconnected = False
            sent_bytes = 0
            status_code = None
            close_code = None
            json_response = False
            response_prefix = bytearray()
            response_too_large = False

            async def observed_receive():
                nonlocal disconnected, close_code
                message = await receive()
                if message["type"] in {"http.disconnect", "websocket.disconnect"}:
                    disconnected = True
                    close_code = message.get("code", close_code)
                return message

            async def observed_send(message):
                nonlocal status_code, sent_bytes, response_complete, close_code, json_response, response_too_large
                if message["type"] == "http.response.start":
                    status_code = message["status"]
                    headers = list(message.get("headers", []))
                    json_response = any(
                        key.lower() == b"content-type" and b"application/json" in value.lower()
                        for key, value in headers
                    )
                    headers = [(key, value) for key, value in headers
                               if key.lower() not in {b"x-request-id", b"x-qmt-trace-id"}]
                    headers.extend([
                        (b"x-request-id", context["http_request_id"].encode("ascii")),
                        (b"x-qmt-trace-id", str(trace.get("trace_id", "")).encode("ascii")),
                    ])
                    message = {**message, "headers": headers}
                elif message["type"] == "http.response.body":
                    body = message.get("body", b"")
                    # At most 4KiB is inspected, including JSON split by the
                    # existing BaseHTTPMiddleware. Chunks are still forwarded
                    # immediately; large market responses are never accumulated.
                    if json_response and not response_too_large:
                        if len(response_prefix) + len(body) <= 4096:
                            response_prefix.extend(body)
                        else:
                            response_prefix.clear()
                            response_too_large = True
                    if json_response and not response_too_large and not message.get("more_body", False):
                        try:
                            payload = json.loads(response_prefix)
                            if isinstance(payload, dict) and isinstance(payload.get("status"), str):
                                result["business_status"] = payload["status"][:64]
                        except (ValueError, UnicodeError):
                            pass
                    sent_bytes += len(body)
                elif message["type"] == "websocket.accept":
                    result["accepted"] = True
                elif message["type"] == "websocket.close":
                    close_code = message.get("code", 1000)
                await send(message)
                if message["type"] == "http.response.body" and not message.get("more_body", False):
                    response_complete = True

            try:
                await self.app(scope, observed_receive, observed_send)
            except BaseException:
                # ServerErrorMiddleware may emit its 500 response outside this
                # boundary. Preserve the exception, never claim it was sent.
                result["outcome"] = "error"
                raise
            finally:
                route = scope.get("route")
                result.update(route=getattr(route, "path", "unmatched"),
                              response_bytes=sent_bytes, disconnected=disconnected)
                if kind == "http":
                    result.update(http_status=status_code, response_complete=response_complete,
                                  evidence_source="asgi_send")
                    result.setdefault("outcome", "client_disconnected" if disconnected and not response_complete else (
                        "rejected" if status_code is not None and 400 <= status_code < 500 else
                        "error" if status_code is not None and status_code >= 500 else
                        "success" if response_complete else "incomplete"
                    ))
                else:
                    result["close_code"] = close_code
                    result.setdefault("outcome", "closed")
