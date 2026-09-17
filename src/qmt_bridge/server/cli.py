"""CLI entry point — ``qmt-server`` command."""

import argparse
import hmac
import os
import threading
import uuid
from dataclasses import replace
from pathlib import Path

from bigqmt_signal_trader import telemetry

from .config import Settings, _load_env_file, reset_settings
from .singleton import ensure_qmt_server_port_available
from ..no_proxy import disable_environment_proxies


def _start_local_shutdown_watcher(
    server: object,
    shutdown_file: str,
    shutdown_token: str,
) -> tuple[threading.Event, threading.Thread]:
    """Accept a one-shot local shutdown request from the owning controller.

    This is deliberately a file/nonce handshake, not an HTTP administration
    endpoint.  The process only acts on its caller-provided private path and
    matching nonce; setting ``should_exit`` lets Uvicorn run FastAPI lifespan
    cleanup normally.
    """
    stop = threading.Event()
    request_path = Path(shutdown_file)

    def watch() -> None:
        while not stop.wait(0.1):
            try:
                candidate = request_path.read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                continue
            except OSError:
                continue
            if hmac.compare_digest(candidate, shutdown_token):
                setattr(server, "should_exit", True)
                return

    worker = threading.Thread(target=watch, name="qmt-bridge-local-shutdown", daemon=True)
    worker.start()
    return stop, worker


def main():
    """Parse CLI args, build settings, and start the server."""
    # Load .env before parsing so defaults come from env
    _load_env_file()
    disable_environment_proxies()

    parser = argparse.ArgumentParser(
        prog="qmt-server",
        description="QMT Bridge API Server",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("QMT_BRIDGE_HOST", "0.0.0.0"),
        help="Listen host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("QMT_BRIDGE_PORT", "13543")),
        help="Listen port (default: 13543)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("QMT_BRIDGE_LOG_LEVEL", "info"),
        choices=["critical", "error", "warning", "info", "debug"],
        help="Uvicorn log level (default: info)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("QMT_BRIDGE_WORKERS", "1")),
        help="Number of workers (default: 1, keep 1 on Windows)",
    )
    parser.add_argument(
        "--no-singleton",
        action="store_true",
        default=os.environ.get("QMT_BRIDGE_SINGLETON", "true").lower()
        in ("0", "false", "no", "off"),
        help="Disable the non-destructive bridge-port availability preflight",
    )
    parser.add_argument(
        "--account-enabled",
        action="store_true",
        default=os.environ.get("QMT_BRIDGE_ACCOUNT_ENABLED", "").lower()
        in ("1", "true", "yes"),
        help="Enable Big QMT account query routes",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("QMT_BRIDGE_API_KEY", ""),
        help="API key for authenticated endpoints",
    )
    parser.add_argument(
        "--qmt-root",
        default=os.environ.get("QMT_BRIDGE_QMT_ROOT", ""),
        help="Big QMT terminal root for diagnostics",
    )
    parser.add_argument(
        "--account-id",
        default=os.environ.get("QMT_BRIDGE_TRADING_ACCOUNT_ID", ""),
        help="Trading account ID",
    )
    parser.add_argument(
        "--zmq-endpoint",
        default=os.environ.get(
            "QMT_BRIDGE_ZMQ_ENDPOINT", "tcp://127.0.0.1:15560"
        ),
        help="Big QMT ZMQ RPC endpoint",
    )
    parser.add_argument(
        "--event-zmq-endpoint",
        default=os.environ.get(
            "QMT_BRIDGE_EVENT_ZMQ_ENDPOINT", "tcp://127.0.0.1:15561"
        ),
        help="Big QMT execution event ZMQ endpoint",
    )
    parser.add_argument(
        "--order-writes-enabled",
        action="store_true",
        default=os.environ.get(
            "QMT_BRIDGE_ORDER_WRITES_ENABLED", "true"
        ).lower()
        in ("1", "true", "yes", "on"),
        help="Enable bridge-side order and cancel writes",
    )
    parser.add_argument("--shutdown-file", default="", help=argparse.SUPPRESS)
    parser.add_argument("--shutdown-token", default="", help=argparse.SUPPRESS)

    args = parser.parse_args()
    if bool(args.shutdown_file) != bool(args.shutdown_token):
        parser.error("--shutdown-file and --shutdown-token must be supplied together")

    # Build settings from CLI args (override env)
    settings = replace(
        Settings.from_env(),
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        workers=1 if args.workers != 1 else args.workers,
        singleton=not args.no_singleton,
        api_key=args.api_key,
        qmt_root=args.qmt_root,
        zmq_endpoint=args.zmq_endpoint,
        event_zmq_endpoint=args.event_zmq_endpoint,
        account_enabled=args.account_enabled,
        trading_account_id=args.account_id,
        order_writes_enabled=args.order_writes_enabled,
    )
    reset_settings(settings)
    ensure_qmt_server_port_available(port=settings.port, enabled=settings.singleton)

    import uvicorn

    from .app import create_app

    app = create_app(settings)
    config = uvicorn.Config(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        workers=settings.workers,
    )
    server = uvicorn.Server(config)
    watcher_stop: threading.Event | None = None
    watcher: threading.Thread | None = None
    if args.shutdown_file:
        watcher_stop, watcher = _start_local_shutdown_watcher(
            server, args.shutdown_file, args.shutdown_token
        )
    telemetry.configure(
        enabled=os.environ.get("QMT_BRIDGE_TRACE_ENABLED", "true").strip().lower()
        not in {"0", "false", "no", "off"},
        directory=os.environ.get("QMT_BRIDGE_TRACE_DIR") or None,
        role="bridge_server",
    )
    trace_id = os.environ.get("QMT_BRIDGE_TRACE_ID", "") or uuid.uuid4().hex
    context = telemetry.bind_context(trace_id=trace_id)
    exit_outcome = "success"
    exit_error_type = ""
    try:
        context.__enter__()
        telemetry.emit("process.server_start", critical=True, outcome="started", port=settings.port)
        server.run()
    except Exception as exc:
        exit_outcome = "error"
        exit_error_type = type(exc).__name__
        raise
    finally:
        if watcher_stop is not None:
            watcher_stop.set()
        if watcher is not None:
            watcher.join(timeout=1)
        telemetry.emit("process.server_exit", critical=True, outcome=exit_outcome, error_type=exit_error_type)
        context.__exit__(None, None, None)
        telemetry.flush(0.5)
        telemetry.shutdown(0.5)


if __name__ == "__main__":
    main()
