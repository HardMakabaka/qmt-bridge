"""QMT Bridge — data conversion helpers."""

import numpy as np
import pandas as pd


_OBJECT_FIELDS = (
    "account_id",
    "cash",
    "frozen_cash",
    "market_value",
    "total_asset",
    "stock_code",
    "volume",
    "can_use_volume",
    "frozen_volume",
    "open_price",
    "order_id",
    "order_sysid",
    "order_type",
    "order_volume",
    "price_type",
    "price",
    "traded_id",
    "traded_time",
    "traded_volume",
    "traded_price",
    "status",
    "status_msg",
    "strategy_name",
    "order_remark",
    "error_id",
    "error_msg",
    "m_strAccountID",
    "m_strInstrumentID",
    "m_nVolume",
    "m_nCanUseVolume",
    "m_dCash",
    "m_dAvailable",
    "m_dFrozenCash",
    "m_dMarketValue",
    "m_dTotalAsset",
    "m_dPrice",
    "m_dTradedPrice",
)


def _numpy_to_python(obj):
    """Recursively convert numpy types in a nested structure to Python types."""
    if isinstance(obj, dict):
        return {k: _numpy_to_python(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_numpy_to_python(i) for i in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if hasattr(obj, "_asdict"):
        return _numpy_to_python(obj._asdict())
    if hasattr(obj, "__dict__"):
        return {k: _numpy_to_python(v) for k, v in vars(obj).items() if not k.startswith("_")}
    object_fields = {field: _numpy_to_python(getattr(obj, field)) for field in _OBJECT_FIELDS if hasattr(obj, field)}
    if object_fields:
        return object_fields
    return obj


def _market_data_to_records(
    raw: dict, stock_list: list[str], field_list: list[str]
) -> dict[str, list[dict]]:
    """Convert xtdata.get_market_data() result to JSON-friendly records.

    raw is {field: DataFrame} where each DataFrame has stocks as rows and
    timestamps as columns.  We pivot into {stock: [{date, field1, field2, ...}]}.
    """
    result: dict[str, list[dict]] = {}
    for stock in stock_list:
        rows: dict[str, dict] = {}
        for field in field_list:
            df = raw.get(field)
            if df is None:
                continue
            if stock in df.index:
                for date, value in df.loc[stock].items():
                    entry = rows.setdefault(str(date), {"date": str(date)})
                    entry[field] = value.item() if hasattr(value, "item") else value
        result[stock] = list(rows.values())
    return result


def _dataframe_dict_to_records(data: dict) -> dict[str, list[dict]]:
    """Convert {stock: DataFrame} format (get_market_data_ex / get_local_data return value).

    Returns {stock: [row_dict, ...]} where each row_dict includes all columns.
    """
    result: dict[str, list[dict]] = {}
    for stock, df in data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            records = df.reset_index().to_dict(orient="records")
            result[stock] = [_numpy_to_python(r) for r in records]
        else:
            result[stock] = []
    return result


def _financial_data_to_records(data: dict) -> dict:
    """Convert {stock: {table: DataFrame}} format (get_financial_data return value).

    Returns {stock: {table: [row_dict, ...]}}.
    """
    result: dict = {}
    for stock, tables in data.items():
        stock_data: dict = {}
        if isinstance(tables, dict):
            for table_name, df in tables.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    records = df.reset_index().to_dict(orient="records")
                    stock_data[table_name] = [_numpy_to_python(r) for r in records]
                else:
                    stock_data[table_name] = []
        result[stock] = stock_data
    return result
