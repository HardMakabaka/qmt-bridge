"""Non-destructive single-instance guard for the qmt-server listener."""

from __future__ import annotations

import logging
import socket

logger = logging.getLogger("qmt_bridge")


class QmtServerPortInUse(RuntimeError):
    """Raised before startup when the configured IPv4 listener port is occupied."""


def ensure_qmt_server_port_available(*, port: int, enabled: bool = True) -> None:
    """Fail before startup if the bridge port is occupied, without stopping anything.

    This preflight is advisory: Uvicorn's later bind remains the final arbiter,
    so a process that binds after this probe still causes a normal bind failure.
    """
    if not enabled:
        return
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("0.0.0.0", int(port)))
    except OSError as exc:
        raise QmtServerPortInUse(
            "QMT Bridge port %s is already in use; refusing to stop another process" % port
        ) from exc


def stop_existing_qmt_servers(*, port: int, enabled: bool | None = None) -> list[dict]:
    """Legacy no-op retained for callers of the former destructive helper.

    Bridge replacement belongs to the host recovery controller, which owns PID
    identity and in-flight-write safety checks. This package never stops a
    process based on its command line.
    """
    del port, enabled
    logger.warning("stop_existing_qmt_servers is non-destructive and does nothing")
    return []
