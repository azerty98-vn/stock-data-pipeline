"""Validate raw OHLCV batch (1 source x N symbol x 1 ngày) bằng Great
Expectations, TRƯỚC khi load vào BigQuery.

Vị trí trong pipeline (3 lớp phòng thủ, không thừa nhau):

  pipeline/contracts/schema.py (pydantic, per-row, lúc EXTRACT)
    -> bắt lỗi NGAY khi 1 row lệch contract, trước khi ghi xuống GCS. Rẻ và
       sớm nhất, nhưng chỉ nhìn được từng row độc lập.

  quality/validate_raw.py (Great Expectations, per-batch, TRƯỚC LOAD — đây)
    -> nhìn được CẢ BATCH (N symbol) cùng lúc, bắt được vấn đề chỉ lộ ra ở
       mức tổng hợp mà pydantic không thấy được (vd: thiếu hẳn quá nửa số
       symbol trong ngày dù từng symbol lẻ có data hợp lệ — nhiều khả năng
       API outage diện rộng, khác với 1-2 mã nghỉ giao dịch riêng lẻ).

  dbt test (transform/models/**, per-table, SAU khi đã ở warehouse)
    -> bắt lỗi business rule, kể cả lỗi phát sinh trong chính bước
       transform (không phải lỗi từ nguồn).

Freshness ("chưa tới") được tách riêng ở dbt `source freshness`
(transform/models/staging/_sources.yml), không thuộc phạm vi file này —
file này chỉ trả lời "data đã tới có đúng schema/hợp lý không", không trả
lời "data đã tới hay chưa".
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date

import pandas as pd

from pipeline.load.gcs_writer import blob_path

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = ["source", "symbol", "date", "open", "high", "low", "close", "volume"]

# > 50% symbol thiếu raw data trong 1 ngày: coi là API outage diện rộng
# (fail cứng), khác với 1-2 mã nghỉ giao dịch riêng lẻ (chỉ log warning).
MISSING_SYMBOL_OUTAGE_THRESHOLD = 0.5


def _read_batch(source: str, symbols: list[str], record_date: date) -> pd.DataFrame:
    from google.cloud import storage

    bucket_name = os.environ["GCS_BUCKET"]
    project = os.environ["BQ_PROJECT"]
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)

    frames = []
    for symbol in symbols:
        blob = bucket.blob(blob_path(source, symbol, record_date))
        if blob.exists():
            frames.append(pd.read_parquet(io.BytesIO(blob.download_as_bytes())))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=EXPECTED_COLUMNS)


def validate_raw_batch(source: str, symbols: list[str], record_date: date) -> None:
    """Raise ValueError nếu batch raw của (source, record_date) vi phạm
    expectation suite, hoặc nếu quá nhiều symbol thiếu data (outage).
    Không raise nếu chỉ 1 vài symbol thiếu (nghỉ lễ/delist) — chỉ log warning.
    """
    df = _read_batch(source, symbols, record_date)
    found_symbols = set(df["symbol"].unique()) if not df.empty else set()
    missing_ratio = 1 - (len(found_symbols) / len(symbols))

    if missing_ratio > MISSING_SYMBOL_OUTAGE_THRESHOLD:
        raise ValueError(
            f"{source} {record_date}: thiếu raw data cho {len(symbols) - len(found_symbols)}/{len(symbols)} "
            f"symbol ({missing_ratio:.0%}) — vượt ngưỡng {MISSING_SYMBOL_OUTAGE_THRESHOLD:.0%}, "
            "nhiều khả năng API outage diện rộng chứ không phải nghỉ lễ đơn lẻ."
        )
    if df.empty:
        logger.warning("%s %s: không symbol nào có raw data (nghỉ lễ toàn thị trường?)", source, record_date)
        return
    if missing_ratio > 0:
        logger.warning(
            "%s %s: thiếu raw data cho %d/%d symbol (nghỉ lễ/delist riêng lẻ, dưới ngưỡng outage)",
            source, record_date, len(symbols) - len(found_symbols), len(symbols),
        )

    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_pandas(f"{source}_pandas")
    data_asset = datasource.add_dataframe_asset(name="raw_ohlcv")
    batch_request = data_asset.build_batch_request(dataframe=df)

    suite_name = "raw_ohlcv_suite"
    context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

    validator.expect_table_columns_to_match_set(column_set=EXPECTED_COLUMNS)
    for col in ("source", "symbol", "date"):
        validator.expect_column_values_to_not_be_null(col)
    for col in ("open", "high", "low", "close"):
        validator.expect_column_values_to_be_between(col, min_value=0, strict_min=True)
    validator.expect_column_values_to_be_between("volume", min_value=0)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(name=f"{source}_raw_ohlcv_checkpoint", validator=validator)
    result = checkpoint.run()

    if not result["success"]:
        raise ValueError(f"Great Expectations validation FAILED for {source} {record_date}: {result}")

    logger.info("GE validation OK: %s %s (%d row, %d symbol)", source, record_date, len(df), len(found_symbols))
