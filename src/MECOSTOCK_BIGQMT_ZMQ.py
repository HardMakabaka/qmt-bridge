#coding:gbk

import os
import sys


def _find_bridge_entry():
    for path in sys.path:
        if not path or not os.path.isdir(path):
            continue
        candidate = os.path.join(path, "MECOSTOCK_BIGQMT_BRIDGE.py")
        if os.path.isfile(candidate):
            return candidate
    raise RuntimeError("MECOSTOCK_BIGQMT_BRIDGE.py was not found on QMT sys.path")


_BRIDGE_ENTRY = _find_bridge_entry()
with open(_BRIDGE_ENTRY, "rb") as _bridge_file:
    _BRIDGE_SOURCE = _bridge_file.read()
exec(compile(_BRIDGE_SOURCE, _BRIDGE_ENTRY, "exec"), globals(), globals())
