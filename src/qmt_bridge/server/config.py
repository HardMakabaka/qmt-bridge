"""Server configuration — Settings loaded from .env and environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path

from ..no_proxy import disable_environment_proxies


def _load_env_file(env_path: Path | None = None) -> None:
    """Load .env file into os.environ (stdlib only, no python-dotenv)."""
    if env_path is None:
        # Look for .env relative to CWD
        env_path = Path.cwd() / ".env"
    if not env_path.is_file():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip()
                # Real env vars take precedence
                if key and key not in os.environ:
                    os.environ[key] = val


@dataclass
class Settings:
    """Application settings, resolved from environment variables."""

    host: str = "0.0.0.0"
    port: int = 13543
    log_level: str = "info"
    workers: int = 1
    singleton: bool = True

    # Security
    api_key: str = ""
    require_auth_for_data: bool = False

    runtime: str = "bigqmt"
    qmt_root: str = ""
    rpc_transport: str = "zmq"
    zmq_endpoint: str = "tcp://127.0.0.1:15560"
    event_zmq_endpoint: str = "tcp://127.0.0.1:15561"
    rpc_timeout_seconds: float = 6.0
    formula_enabled: bool = True
    formula_host: str = "127.0.0.1"
    formula_port: int = 58600
    formula_history_read_workers: int = 2
    local_cache_enabled: bool = False
    local_cache_dir: str = ""
    local_cache_format: str = "auto"

    account_enabled: bool = False
    trading_account_id: str = ""
    order_writes_enabled: bool = True

    # Notification
    notify_enabled: bool = False
    notify_backends: str = ""  # "feishu", "webhook", "feishu,webhook"
    notify_event_types: str = ""  # allowed event types, comma-separated, empty=all
    notify_ignore_event_types: str = ""  # excluded event types

    # Feishu webhook
    feishu_webhook_url: str = ""
    feishu_webhook_secret: str = ""  # optional v2 signing key

    # Generic webhook
    webhook_url: str = ""
    webhook_secret: str = ""  # optional, sent as X-Webhook-Secret header

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> "Settings":
        """Create settings from environment variables (loads .env first)."""
        _load_env_file(env_path)
        disable_environment_proxies()
        return cls(
            host=os.environ.get("QMT_BRIDGE_HOST", "0.0.0.0"),
            port=int(os.environ.get("QMT_BRIDGE_PORT", "13543")),
            log_level=os.environ.get("QMT_BRIDGE_LOG_LEVEL", "info"),
            workers=int(os.environ.get("QMT_BRIDGE_WORKERS", "1")),
            singleton=os.environ.get("QMT_BRIDGE_SINGLETON", "true").lower()
            in ("1", "true", "yes", "on"),
            api_key=os.environ.get("QMT_BRIDGE_API_KEY", ""),
            require_auth_for_data=os.environ.get(
                "QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA", ""
            ).lower()
            in ("1", "true", "yes"),
            runtime=os.environ.get("QMT_BRIDGE_RUNTIME", "bigqmt").strip().lower(),
            qmt_root=os.environ.get("QMT_BRIDGE_QMT_ROOT", ""),
            rpc_transport=os.environ.get(
                "QMT_BRIDGE_RPC_TRANSPORT", "zmq"
            ).strip().lower(),
            zmq_endpoint=os.environ.get(
                "QMT_BRIDGE_ZMQ_ENDPOINT", "tcp://127.0.0.1:15560"
            ),
            event_zmq_endpoint=os.environ.get(
                "QMT_BRIDGE_EVENT_ZMQ_ENDPOINT", "tcp://127.0.0.1:15561"
            ),
            rpc_timeout_seconds=float(
                os.environ.get("QMT_BRIDGE_RPC_TIMEOUT_SECONDS", "6.0")
            ),
            formula_enabled=os.environ.get(
                "QMT_BRIDGE_FORMULA_ENABLED", "true"
            ).lower()
            in ("1", "true", "yes", "on"),
            formula_host=os.environ.get(
                "QMT_BRIDGE_FORMULA_HOST", "127.0.0.1"
            ),
            formula_port=int(
                os.environ.get("QMT_BRIDGE_FORMULA_PORT", "58600")
            ),
            formula_history_read_workers=int(
                os.environ.get("QMT_BRIDGE_FORMULA_HISTORY_READ_WORKERS", "2")
            ),
            local_cache_enabled=os.environ.get(
                "QMT_BRIDGE_LOCAL_CACHE_ENABLED", "false"
            ).lower()
            in ("1", "true", "yes", "on"),
            local_cache_dir=os.environ.get("QMT_BRIDGE_LOCAL_CACHE_DIR", ""),
            local_cache_format=os.environ.get(
                "QMT_BRIDGE_LOCAL_CACHE_FORMAT", "auto"
            ),
            account_enabled=os.environ.get(
                "QMT_BRIDGE_ACCOUNT_ENABLED", ""
            ).lower()
            in ("1", "true", "yes"),
            trading_account_id=os.environ.get(
                "QMT_BRIDGE_TRADING_ACCOUNT_ID", ""
            ),
            order_writes_enabled=os.environ.get(
                "QMT_BRIDGE_ORDER_WRITES_ENABLED", "true"
            ).lower()
            in ("1", "true", "yes", "on"),
            # Notification
            notify_enabled=os.environ.get(
                "QMT_BRIDGE_NOTIFY_ENABLED", ""
            ).lower()
            in ("1", "true", "yes"),
            notify_backends=os.environ.get("QMT_BRIDGE_NOTIFY_BACKENDS", ""),
            notify_event_types=os.environ.get(
                "QMT_BRIDGE_NOTIFY_EVENT_TYPES", ""
            ),
            notify_ignore_event_types=os.environ.get(
                "QMT_BRIDGE_NOTIFY_IGNORE_EVENT_TYPES", ""
            ),
            feishu_webhook_url=os.environ.get(
                "QMT_BRIDGE_FEISHU_WEBHOOK_URL", ""
            ),
            feishu_webhook_secret=os.environ.get(
                "QMT_BRIDGE_FEISHU_WEBHOOK_SECRET", ""
            ),
            webhook_url=os.environ.get("QMT_BRIDGE_WEBHOOK_URL", ""),
            webhook_secret=os.environ.get("QMT_BRIDGE_WEBHOOK_SECRET", ""),
        )


# Module-level singleton, lazily initialized
_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the global settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings


def reset_settings(settings: Settings | None = None) -> None:
    """Replace the global settings (used by CLI to apply overrides)."""
    global _settings
    _settings = settings
