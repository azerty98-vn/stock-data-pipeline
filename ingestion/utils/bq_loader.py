"""Load raw OHLCV từ S3 vào BigQuery.

BigQuery external table trực tiếp trên S3 cần BigQuery Omni (paid, phức
tạp cho side-project). Thay vào đó: đọc parquet từ S3 bằng boto3, load
thẳng vào BigQuery bằng load job — đơn giản hơn, đủ dùng cho volume nhỏ.

Idempotency: dùng partition decorator `table$YYYYMMDD` +
WRITE_TRUNCATE — load lại cùng 1 ngày N lần luôn ghi đè đúng partition đó,
không tạo duplicate và không đụng tới các ngày khác (partition-overwrite,
cùng nguyên tắc với S3 writer ở raw layer).

GCP project chưa setup ở thời điểm viết code này — BQ_PROJECT/
GOOGLE_APPLICATION_CREDENTIALS là placeholder trong .env.example, cần điền
giá trị thật trước khi task này chạy được.
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date

import pandas as pd

from ingestion.utils.s3_writer import object_key

logger = logging.getLogger(__name__)

RAW_TABLE = "raw_ohlcv"

BQ_SCHEMA = [
    {"name": "source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "open", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "high", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "low", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "close", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "volume", "type": "INT64", "mode": "REQUIRED"},
]


def _read_parquet_from_s3(bucket: str, key: str, s3_client) -> pd.DataFrame | None:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        return None
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def load_day_to_bigquery(source: str, symbols: list[str], record_date: date) -> int:
    """Đọc raw/{source}/{symbol}/{date}.parquet cho từng symbol, load vào
    BigQuery raw_ohlcv$YYYYMMDD (WRITE_TRUNCATE — overwrite đúng partition).

    Trả về số row đã load. Trả về 0 nếu không có symbol nào có data cho ngày đó
    (vd: ngày nghỉ lễ) — caller quyết định đây có phải lỗi hay không.
    """
    from google.cloud import bigquery

    from ingestion.utils.s3_writer import _s3_client

    bucket = os.environ["S3_BUCKET"]
    s3_client = _s3_client()

    frames = []
    for symbol in symbols:
        key = object_key(source, symbol, record_date)
        df = _read_parquet_from_s3(bucket, key, s3_client)
        if df is not None:
            frames.append(df)
        else:
            logger.warning("No raw object for %s/%s on %s (holiday/delist?)", source, symbol, record_date)

    if not frames:
        return 0

    combined = pd.concat(frames, ignore_index=True)

    project = os.environ["BQ_PROJECT"]
    dataset = os.environ.get("BQ_DATASET", "stock_pipeline")
    partition = record_date.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{RAW_TABLE}${partition}"

    bq_client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(**f) for f in BQ_SCHEMA],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="date"),
    )
    job = bq_client.load_table_from_dataframe(combined, table_id, job_config=job_config)
    job.result()
    logger.info("Loaded %d row(s) into %s", len(combined), table_id)
    return len(combined)
