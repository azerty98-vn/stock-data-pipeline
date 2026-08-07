"""Data contract cho raw OHLCV layer.

Grain: 1 row = 1 mã chứng khoán (symbol) x 1 ngày giao dịch (date).
Đây là contract mà MỌI nguồn (vnstock, yfinance, ...) phải map về trước khi
ghi xuống raw layer — nếu API nguồn đổi format, việc validate ở đây sẽ fail
ngay tại điểm ingest thay vì âm thầm trôi lỗi xuống staging/mart.
"""

from datetime import date

from pydantic import BaseModel, ConfigDict, field_validator


class OhlcvRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @field_validator("open", "high", "low", "close")
    @classmethod
    def price_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"price must be > 0, got {v}")
        return v

    @field_validator("volume")
    @classmethod
    def volume_must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"volume must be >= 0, got {v}")
        return v

    @field_validator("symbol")
    @classmethod
    def symbol_must_be_upper(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("symbol must not be empty")
        return v.strip().upper()
