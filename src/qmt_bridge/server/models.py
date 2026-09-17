"""QMT Bridge — Pydantic request/response models."""

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Legacy / Download models
# ---------------------------------------------------------------------------

class DownloadRequest(BaseModel):
    stock: str
    period: str = "1d"
    start: str = ""
    end: str = ""


class HistoryDownloadJobRequest(BaseModel):
    stocks: list[str]
    period: str = "1d"
    start_time: str = ""
    end_time: str = ""
    batch_size: int = 10
    max_attempts: int = 2


class SectorDownloadRequest(BaseModel):
    timeout_seconds: float | None = None


class FinancialDownloadRequest(BaseModel):
    stocks: list[str]
    tables: list[str] = []
    start_time: str = ""
    end_time: str = ""


class CreateSectorRequest(BaseModel):
    sector_name: str
    parent_node: str = ""


class AddSectorStocksRequest(BaseModel):
    sector_name: str
    stocks: list[str]


# ---------------------------------------------------------------------------
# Trading models
# ---------------------------------------------------------------------------

class OrderRequest(BaseModel):
    account_id: str = ""
    stock_code: str
    order_type: int  # xtconstant order type
    order_volume: int
    price_type: int = 5  # LATEST_PRICE
    price: float = 0.0
    strategy_name: str = ""
    order_remark: str = ""
    client_submit_id: str = Field(min_length=1, max_length=24)


class CancelRequest(BaseModel):
    account_id: str = ""
    order_id: int = 0
    order_sysid: str = ""
    market: int | str = ""


class QueryOrderRequest(BaseModel):
    account_id: str = ""
    cancelable_only: bool = False
    client_submit_id: str = Field(default="", max_length=24)


class QueryPositionRequest(BaseModel):
    account_id: str = ""


class QueryAssetRequest(BaseModel):
    account_id: str = ""


class CreditQueryRequest(BaseModel):
    account_id: str = ""


class SMTQueryRequest(BaseModel):
    account_id: str = ""


# ---------------------------------------------------------------------------
# Formula / Model API models
# ---------------------------------------------------------------------------

class CallFormulaRequest(BaseModel):
    formula_name: str
    stock_code: str
    period: str = "1d"
    start_time: str = ""
    end_time: str = ""
    count: int = -1
    dividend_type: str = "none"
    params: dict = {}


class CallFormulaBatchRequest(BaseModel):
    formula_name: str
    stock_codes: list[str]
    period: str = "1d"
    start_time: str = ""
    end_time: str = ""
    count: int = -1
    dividend_type: str = "none"
    params: dict = {}


# ---------------------------------------------------------------------------
# Additional download models
# ---------------------------------------------------------------------------

class FinancialDownload2Request(BaseModel):
    stocks: list[str]
    tables: list[str] = []


class SyncTransactionRequest(BaseModel):
    account_id: str = ""
    account_type: str = "STOCK"
    operation: str
    data_type: str
    data: list[dict] = Field(default_factory=list)
