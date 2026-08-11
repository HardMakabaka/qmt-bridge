# This ASCII-only overlay is appended to the SHA-pinned Big QMT entry by the
# installer. It reports QMT's current model request id without modifying the
# vendored upstream source; the host bridge matches that id to QMT's own log.
import sys as _mecostock_sys


_MECOSTOCK_BASE_INIT = init
_MECOSTOCK_BASE_ADJUST = adjust
_MECOSTOCK_BASE_HANDLEBAR = handlebar
_MECOSTOCK_REQUEST_ID = ""


def _mecostock_detect_request_id(context_info):
    candidates = (
        getattr(context_info, "request_id", None),
        getattr(getattr(context_info, "context", None), "request_id", None),
    )
    for candidate in candidates:
        request_id = str(candidate or "").strip()
        if request_id:
            return request_id
    return ""


def _mecostock_refresh_request_id(context_info):
    global _MECOSTOCK_REQUEST_ID
    _MECOSTOCK_REQUEST_ID = _mecostock_detect_request_id(context_info)


def _mecostock_install_ping_attestation():
    rpc_module = _mecostock_sys.modules.get("bigqmt_signal_trader.redis_rpc")
    handlers = getattr(rpc_module, "BigQmtRpcHandlers", None)
    if handlers is None:
        raise RuntimeError("BigQmtRpcHandlers is unavailable")
    if not hasattr(handlers, "_mecostock_base_handle_ping"):
        handlers._mecostock_base_handle_ping = handlers._handle_ping

        def _handle_ping(self, params):
            payload = handlers._mecostock_base_handle_ping(self, params)
            payload["qmt_request_id"] = _MECOSTOCK_REQUEST_ID
            return payload

        handlers._handle_ping = _handle_ping


def init(ContextInfo):
    _mecostock_refresh_request_id(ContextInfo)
    _mecostock_install_ping_attestation()
    result = _MECOSTOCK_BASE_INIT(ContextInfo)
    _mecostock_refresh_request_id(ContextInfo)
    return result


def adjust(ContextInfo):
    _mecostock_refresh_request_id(ContextInfo)
    return _MECOSTOCK_BASE_ADJUST(ContextInfo)


def handlebar(ContextInfo):
    _mecostock_refresh_request_id(ContextInfo)
    return _MECOSTOCK_BASE_HANDLEBAR(ContextInfo)
