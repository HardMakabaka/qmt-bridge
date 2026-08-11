"""HTTP/WebSocket bridge for Big QMT market data and guarded account access."""

from qmt_bridge._version import __version__
from qmt_bridge.client import QMTClient

__all__ = ["QMTClient", "__version__"]
