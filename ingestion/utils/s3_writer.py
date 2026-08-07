"""Ghi raw OHLCV records lên S3 (hoặc MinIO local) với key idempotent.

Idempotency strategy: mỗi (source, symbol, date) map 1:1 sang 1 object key
`raw/{source}/{symbol}/{date}.parquet`. Ghi = overwrite theo key (S3
PutObject vốn đã overwrite nếu key trùng), nên chạy lại DAG của cùng 1 ngày
N lần luôn cho cùng 1 kết quả — không cần transaction/dedupe ở raw layer.
Merge/upsert theo grain (symbol, date) được để lại cho dbt xử lý ở staging,
vì raw layer chỉ là bản chụp thô, không phải bảng có PK cần merge.

Local dev trỏ S3_ENDPOINT_URL vào MinIO; khi có AWS account thật, chỉ cần
đổi biến môi trường (bỏ S3_ENDPOINT_URL hoặc trỏ endpoint AWS), code không đổi.
"""

from __future__ import annotations

import io
import logging
import os
from collections import defaultdict
from datetime import date

import boto3
import pandas as pd

from ingestion.utils.schema import OhlcvRecord

logger = logging.getLogger(__name__)


def _s3_client():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def object_key(source: str, symbol: str, record_date: date) -> str:
    return f"raw/{source}/{symbol}/{record_date.isoformat()}.parquet"


def ensure_bucket(bucket: str, client=None) -> None:
    client = client or _s3_client()
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def write_records(records: list[OhlcvRecord], bucket: str | None = None, client=None) -> list[str]:
    """Ghi mỗi (source, symbol, date) thành 1 object parquet riêng. Trả về list key đã ghi."""
    bucket = bucket or os.environ["S3_BUCKET"]
    client = client or _s3_client()
    ensure_bucket(bucket, client)

    groups: dict[tuple[str, str, date], list[OhlcvRecord]] = defaultdict(list)
    for r in records:
        groups[(r.source, r.symbol, r.date)].append(r)

    written_keys: list[str] = []
    for (source, symbol, record_date), group in groups.items():
        key = object_key(source, symbol, record_date)
        df = pd.DataFrame([r.model_dump() for r in group])
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        written_keys.append(key)
        logger.info("Wrote s3://%s/%s (%d row)", bucket, key, len(group))

    return written_keys
