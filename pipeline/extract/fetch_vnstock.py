"""Fetch daily OHLCV cho mã VN qua vnstock, validate theo data contract.

Fail-fast: nếu vnstock đổi tên cột/format response, validate_ohlcv() raise
ngay tại đây (ValidationError liệt kê rõ symbol/date/field lỗi) thay vì để
dữ liệu sai âm thầm trôi xuống staging.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import pandas as pd
from pydantic import ValidationError

from pipeline.contracts.schema import OhlcvRecord

logger = logging.getLogger(__name__)

SOURCE = "vnstock"

# Tên cột vnstock trả về (source="VCI", quote.history) -> tên cột trong contract
COLUMN_MAP = {
    "time": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


def fetch_raw(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Gọi vnstock, trả về DataFrame thô (chưa validate)."""
    from vnstock import Vnstock

    stock = Vnstock().stock(symbol=symbol, source="VCI")
    df = stock.quote.history(start=start, end=end, interval="1D")
    if df is None or df.empty:
        raise ValueError(f"vnstock trả về rỗng cho symbol={symbol} [{start}..{end}]")
    return df


def validate_ohlcv(symbol: str, raw_df: pd.DataFrame) -> list[OhlcvRecord]:
    """Map DataFrame thô về contract, validate từng row. Fail-fast."""
    missing = set(COLUMN_MAP) - set(raw_df.columns)
    if missing:
        raise ValueError(
            f"vnstock schema đã đổi cho symbol={symbol}: thiếu cột {missing}. "
            f"Cột hiện có: {list(raw_df.columns)}"
        )

    df = raw_df.rename(columns=COLUMN_MAP)
    records: list[OhlcvRecord] = []
    errors: list[str] = []
    for _, row in df.iterrows():
        try:
            row_date = pd.to_datetime(row["date"]).date()
            records.append(
                OhlcvRecord(
                    source=SOURCE,
                    symbol=symbol,
                    date=row_date,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(row["volume"]),
                )
            )
        except (ValidationError, ValueError, TypeError) as exc:
            errors.append(f"{symbol} {row.get('date')}: {exc}")

    if errors:
        raise ValueError(
            f"{len(errors)} row(s) fail contract validation cho symbol={symbol}:\n"
            + "\n".join(errors[:10])
        )
    return records


def fetch_ohlcv(symbol: str, start: str, end: str) -> list[OhlcvRecord]:
    raw_df = fetch_raw(symbol, start, end)
    return validate_ohlcv(symbol, raw_df)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch VN OHLCV qua vnstock, validate contract")
    parser.add_argument("--symbols", nargs="+", required=True, help="vd: VNM FPT HPG")
    parser.add_argument("--start", default=str(date.today().replace(day=1)))
    parser.add_argument("--end", default=str(date.today()))
    args = parser.parse_args()

    exit_code = 0
    for symbol in args.symbols:
        try:
            records = fetch_ohlcv(symbol, args.start, args.end)
            logger.info("OK %s: %d row(s) validated", symbol, len(records))
        except Exception:
            logger.exception("FAIL %s", symbol)
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
