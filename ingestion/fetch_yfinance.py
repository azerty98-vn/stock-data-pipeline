"""Fetch daily OHLCV cho mã quốc tế qua yfinance, validate theo data contract.

Cùng contract (ingestion.utils.schema.OhlcvRecord) với fetch_vnstock.py —
2 nguồn khác nhau nhưng phải hội tụ về cùng 1 grain (symbol, date) trước khi
ghi raw layer, để staging layer union được 2 nguồn mà không cần biết chi
tiết API gốc.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import pandas as pd
from pydantic import ValidationError

from ingestion.utils.schema import OhlcvRecord

logger = logging.getLogger(__name__)

SOURCE = "yfinance"

COLUMN_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


def fetch_raw(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Gọi yfinance, trả về DataFrame thô (chưa validate)."""
    import yfinance as yf

    df = yf.download(symbol, start=start, end=end, interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        raise ValueError(f"yfinance trả về rỗng cho symbol={symbol} [{start}..{end}]")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def validate_ohlcv(symbol: str, raw_df: pd.DataFrame) -> list[OhlcvRecord]:
    """Map DataFrame thô về contract, validate từng row. Fail-fast."""
    missing = set(COLUMN_MAP) - set(raw_df.columns)
    if missing:
        raise ValueError(
            f"yfinance schema đã đổi cho symbol={symbol}: thiếu cột {missing}. "
            f"Cột hiện có: {list(raw_df.columns)}"
        )

    df = raw_df.rename(columns=COLUMN_MAP).reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]

    records: list[OhlcvRecord] = []
    errors: list[str] = []
    for _, row in df.iterrows():
        try:
            row_date = pd.to_datetime(row[date_col]).date()
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
            errors.append(f"{symbol} {row.get(date_col)}: {exc}")

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
    parser = argparse.ArgumentParser(description="Fetch international OHLCV qua yfinance, validate contract")
    parser.add_argument("--symbols", nargs="+", required=True, help="vd: AAPL MSFT")
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
