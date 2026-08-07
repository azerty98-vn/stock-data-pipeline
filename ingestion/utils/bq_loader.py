"""Load raw OHLCV từ GCS vào BigQuery.

Raw storage và warehouse cùng nằm trên GCP nên BigQuery load job đọc thẳng
`gs://` URI native — không cần đọc file về Python rồi upload lại qua
pandas như khi raw layer nằm trên cloud khác (cross-cloud, không có
external/load native).

Idempotency: dùng partition decorator `table$YYYYMMDD` + WRITE_TRUNCATE —
load lại cùng 1 ngày N lần luôn ghi đè đúng partition đó, không tạo
duplicate và không đụng tới các ngày khác (partition-overwrite, cùng
nguyên tắc với GCS writer ở raw layer).

Schema truyền tường minh vào job_config (thay vì để BigQuery auto-detect
từ parquet) để load fail ngay nếu raw parquet có cột lệch contract — cùng
nguyên tắc fail-fast với validate_ohlcv() ở ingestion layer, chỉ khác điểm
kiểm tra (đây là lớp phòng thủ thứ 2, phòng trường hợp parquet trên GCS bị
ghi sai bởi 1 phiên bản code cũ).

GCP project chưa setup ở thời điểm viết code này — BQ_PROJECT/
GOOGLE_APPLICATION_CREDENTIALS là placeholder trong .env.example, cần điền
giá trị thật trước khi task này chạy được.
"""

from __future__ import annotations

import logging
import os
from datetime import date

from ingestion.utils.gcs_writer import blob_path

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


def load_day_to_bigquery(source: str, symbols: list[str], record_date: date) -> int:
    """Load raw/{source}/{symbol}/{date}.parquet (mỗi symbol 1 file trên GCS)
    vào BigQuery raw_ohlcv$YYYYMMDD (WRITE_TRUNCATE — overwrite đúng partition).

    Trả về số row trong partition sau khi load. Trả về 0 nếu không symbol nào
    có data cho ngày đó (vd: ngày nghỉ lễ) — caller quyết định đây có phải
    lỗi hay không.
    """
    from google.cloud import bigquery, storage

    bucket_name = os.environ["GCS_BUCKET"]
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ.get("BQ_DATASET", "stock_pipeline")

    gcs_client = storage.Client(project=project)
    bucket = gcs_client.bucket(bucket_name)

    uris = []
    for symbol in symbols:
        path = blob_path(source, symbol, record_date)
        if bucket.blob(path).exists():
            uris.append(f"gs://{bucket_name}/{path}")
        else:
            logger.warning("No raw object for %s/%s on %s (holiday/delist?)", source, symbol, record_date)

    if not uris:
        return 0

    partition = record_date.strftime("%Y%m%d")
    table_id = f"{project}.{dataset}.{RAW_TABLE}${partition}"

    bq_client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[bigquery.SchemaField(**f) for f in BQ_SCHEMA],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="date"),
    )
    job = bq_client.load_table_from_uri(uris, table_id, job_config=job_config)
    job.result()

    table = bq_client.get_table(table_id)
    logger.info("Loaded partition %s into %s (%d row total)", partition, table_id, table.num_rows)
    return table.num_rows
