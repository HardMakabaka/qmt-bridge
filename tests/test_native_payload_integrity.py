from datetime import datetime, timedelta
import struct
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from qmt_bridge.server.helpers import NativePayloadError, _dataframe_dict_to_records, _financial_data_to_records, _numpy_to_python
from qmt_bridge.server.qmt_local_dat import _EXPECTED_TIMES, _read_stock


def test_tail_reader_expands_past_multiple_incomplete_days(tmp_path):
    path = tmp_path / "fixture.DAT"
    first = datetime(2024, 1, 2, tzinfo=ZoneInfo("Asia/Shanghai"))
    with path.open("wb") as file:
        file.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f")
        for index in range(4):
            day = first + timedelta(days=index)
            for bar in (_EXPECTED_TIMES if index == 0 else _EXPECTED_TIMES[:-1]):
                timestamp = int(day.replace(hour=bar // 10000, minute=bar // 100 % 100).timestamp())
                file.write(struct.pack("<16I", timestamp, 10000, 10100, 9900, 10000, 0, 10, 0, 10000, 0, 0, 0, 0, 0, 0, 0))
    complete = _read_stock(path, start=None, end_exclusive=None, count=-1)
    tail = _read_stock(path, start=None, end_exclusive=None, count=3)
    assert len(tail) == 3
    assert tail.equals(complete.tail(3))


def test_invalid_native_payload_is_not_a_successful_empty_array():
    with pytest.raises(NativePayloadError):
        _dataframe_dict_to_records({"000001.SZ": {"unexpected": "format"}})
    with pytest.raises(NativePayloadError):
        _financial_data_to_records({"000001.SZ": {"balance": "wrong format"}})


def test_numpy_array_nan_is_json_safe_recursively():
    assert _numpy_to_python({"values": np.array([1.0, np.nan, np.inf])}) == {"values": [1.0, None, None]}
