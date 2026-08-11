"""Helpers for forcing qmt-bridge HTTP traffic to bypass environment proxies."""

from __future__ import annotations

import os
import urllib.request
from typing import Any

PROXY_ENV_VARS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)

NO_PROXY_ENV_VARS = ("NO_PROXY", "no_proxy")


def disable_environment_proxies() -> None:
    """Clear process proxy vars and make urllib default to a direct opener."""

    for name in PROXY_ENV_VARS:
        os.environ.pop(name, None)
    for name in NO_PROXY_ENV_VARS:
        os.environ[name] = "*"
    urllib.request.install_opener(build_direct_opener())


def build_direct_opener() -> urllib.request.OpenerDirector:
    """Return a urllib opener that ignores environment proxy configuration."""

    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


_DIRECT_OPENER = build_direct_opener()


def urlopen_direct(request: urllib.request.Request, **kwargs: Any):
    """Open a request without consulting environment proxy variables."""

    return _DIRECT_OPENER.open(request, **kwargs)
