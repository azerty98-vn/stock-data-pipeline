"""Ghi raw OHLCV records lên Google Cloud Storage với key idempotent.

Idempotency strategy: mỗi (source, symbol, date) map 1:1 sang 1 blob path
`raw/{source}/{symbol}/{date}.parquet`. Upload = overwrite theo path (GCS
vốn overwrite nếu path trùng, không versioning theo mặc định), nên chạy lại
DAG của cùng 1 ngày N lần luôn cho cùng 1 kết quả — không cần
transaction/dedupe ở raw layer. Merge/upsert theo grain (symbol, date) được
để lại cho dbt xử lý ở staging, vì raw layer chỉ là bản chụp thô, không
phải bảng có PK cần merge.

Dùng chung 1 project GCP (và cùng GOOGLE_APPLICATION_CREDENTIALS) với
BigQuery ở bq_loader.py — không cần bộ credentials thứ 2 như khi raw layer
nằm trên AWS S3 (cross-cloud), và load job đọc thẳng gs:// URI native.
"""

from __future__ import annotations

import io
import logging
import os
from collections import defaultdict
from datetime import date

import pandas as pd

from ingestion.utils.schema import OhlcvRecord

logger = logging.getLogger(__name__)


def _gcs_client():
    from google.cloud import storage

    return storage.Client(project=os.environ.get("BQ_PROJECT"))


def blob_path(source: str, symbol: str, record_date: date) -> str:
    return f"raw/{source}/{symbol}/{record_date.isoformat()}.parquet"


def ensure_bucket(bucket_name: str, client=None):
    client = client or _gcs_client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        client.create_bucket(bucket_name, location=os.environ.get("GCS_LOCATION", "asia-southeast1"))
    return bucket


def write_records(records: list[OhlcvRecord], bucket_name: str | None = None, client=None) -> list[str]:
    """Ghi mỗi (source, symbol, date) thành 1 blob parquet riêng. Trả về list path đã ghi."""
    bucket_name = bucket_name or os.environ["GCS_BUCKET"]
    client = client or _gcs_client()
    bucket = ensure_bucket(bucket_name, client)

    groups: dict[tuple[str, str, date], list[OhlcvRecord]] = defaultdict(list)
    for r in records:
        groups[(r.source, r.symbol, r.date)].append(r)

    written_paths: list[str] = []
    for (source, symbol, record_date), group in groups.items():
        path = blob_path(source, symbol, record_date)
        df = pd.DataFrame([r.model_dump() for r in group])
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")
        written_paths.append(path)
        logger.info("Wrote gs://%s/%s (%d row)", bucket_name, path, len(group))

    return written_paths
