"""Backfill lịch sử OHLCV theo batch date-range (không phải theo ngày lẻ).

Vì sao cần backfill (ngoài daily_ingest_dag chỉ fetch 1 ngày/lần):
- fct_moving_averages/fct_volatility cần tối thiểu 50 ngày lịch sử liên tục
  mới ra kết quả đúng (xem transform/models/marts/fct_moving_averages.sql)
  — nếu chỉ chạy daily_ingest_dag mỗi ngày, phải đợi 50 phiên thật mới có
  dữ liệu, không thực tế cho demo/dashboard.
- Ngày 2 trong plan gốc: "phát hiện dữ liệu 2 tuần trước bị sai — thiết kế
  cách backfill mà không phải chạy lại từ đầu toàn bộ pipeline". Script này
  là câu trả lời: chạy lại đúng khoảng ngày cần sửa, không đụng phần còn lại.

Vì sao "theo batch" (date-range chunk) chứ không phải lặp fetch từng ngày:
- fetch_ohlcv(symbol, start, end) đã hỗ trợ sẵn 1 khoảng ngày trong 1 lần
  gọi API — gộp N ngày thành 1 call/symbol/batch giảm số round-trip tới
  vnstock/yfinance đáng kể so với N call riêng lẻ, ít khả năng bị rate-limit.
- batch_days mặc định 30: đủ nhỏ để dừng giữa chừng (lỗi mạng, rate-limit)
  và chạy tiếp từ batch sau mà không mất dữ liệu batch trước — idempotency
  đã có sẵn ở raw layer (overwrite theo key) và warehouse (WRITE_TRUNCATE
  theo partition, xem gcs_writer.py/bq_loader.py), nên resume an toàn.
- Trong 1 batch, vẫn validate + load vào warehouse THEO TỪNG NGÀY (không
  gộp) để giữ đúng partition grain nhất quán với daily_ingest_dag — batch
  chỉ gộp ở bước gọi API, không gộp ở bước load.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pipeline.config import INTL_SYMBOLS, VN_SYMBOLS
from pipeline.contracts.schema import OhlcvRecord

logger = logging.getLogger(__name__)


def _daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _backfill_source_batch(
    source: str,
    fetch_ohlcv,
    symbols: list[str],
    batch_start: date,
    batch_end: date,
) -> None:
    from pipeline.load.bq_loader import load_day_to_bigquery
    from pipeline.load.gcs_writer import write_records
    from quality.validate_raw import validate_raw_batch

    all_records: list[OhlcvRecord] = []
    for symbol in symbols:
        try:
            records = fetch_ohlcv(symbol, start=batch_start.isoformat(), end=batch_end.isoformat())
        except ValueError:
            # Cả symbol không có phiên giao dịch nào trong batch (delist,
            # chưa niêm yết ở đầu batch...) — không phải lỗi của backfill.
            logger.warning("%s %s [%s..%s]: no data returned", source, symbol, batch_start, batch_end)
            continue
        all_records.extend(records)

    if not all_records:
        logger.warning("%s [%s..%s]: batch rỗng hoàn toàn, bỏ qua", source, batch_start, batch_end)
        return

    # gcs_writer tự group theo (source, symbol, date) -> ghi đúng 1 file
    # parquet/ngày dù input là nhiều ngày gộp lại từ 1 batch.
    write_records(all_records)

    for day in _daterange(batch_start, batch_end):
        if day.weekday() >= 5:  # cuối tuần: cả VN lẫn quốc tế đều không giao dịch
            continue
        validate_raw_batch(source, symbols, day)
        load_day_to_bigquery(source, symbols, day)


def backfill(start: date, end: date, batch_days: int = 30) -> None:
    from pipeline.extract.fetch_vnstock import fetch_ohlcv as fetch_vn
    from pipeline.extract.fetch_yfinance import fetch_ohlcv as fetch_intl

    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=batch_days - 1), end)
        logger.info("Backfilling batch %s -> %s", batch_start, batch_end)

        _backfill_source_batch("vnstock", fetch_vn, VN_SYMBOLS, batch_start, batch_end)
        _backfill_source_batch("yfinance", fetch_intl, INTL_SYMBOLS, batch_start, batch_end)

        batch_start = batch_end + timedelta(days=1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill OHLCV lịch sử theo batch date-range")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--batch-days", type=int, default=30)
    args = parser.parse_args()

    backfill(date.fromisoformat(args.start), date.fromisoformat(args.end), args.batch_days)


if __name__ == "__main__":
    main()
