"""CLI entry point — ``qmt-server`` command."""

import argparse
import os
from dataclasses import replace

from .config import Settings, _load_env_file, reset_settings
from .singleton import stop_existing_qmt_servers
from ..no_proxy import disable_environment_proxies


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
        help="Disable qmt-server singleton cleanup before binding the port",
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
        "--order-writes-enabled",
        action="store_true",
        default=os.environ.get(
            "QMT_BRIDGE_ORDER_WRITES_ENABLED", "true"
        ).lower()
        in ("1", "true", "yes", "on"),
        help="Enable bridge-side order and cancel writes",
    )

    args = parser.parse_args()

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
        account_enabled=args.account_enabled,
        trading_account_id=args.account_id,
        order_writes_enabled=args.order_writes_enabled,
    )
    reset_settings(settings)
    stop_existing_qmt_servers(port=settings.port, enabled=settings.singleton)

    import uvicorn

    from .app import create_app

    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        workers=settings.workers,
    )


if __name__ == "__main__":
    main()
