"""Daily ingest DAG.

Dependency design (Ngày 4 trong plan gốc):
  fetch_vn (N symbol, độc lập nhau)   \
                                        > cả 2 nhóm phải xong -> dbt_run_placeholder
  fetch_intl (N symbol, độc lập nhau) /

- fetch_vn và fetch_intl chạy song song: 2 nguồn dữ liệu độc lập, không có
  quan hệ phụ thuộc nghiệp vụ nào giữa chúng.
- Trong mỗi nhóm, symbol cũng chạy song song với nhau (map task) vì mỗi
  symbol là 1 đơn vị công việc độc lập, lỗi 1 symbol không nên chặn symbol khác.
- dbt_run là downstream của CẢ HAI nhóm (không phải từng cái riêng), vì
  staging models union cả 2 nguồn — chạy dbt trước khi 1 trong 2 nguồn xong
  sẽ transform trên dữ liệu thiếu.
- retry=2 + exponential backoff + alert_on_failure: lỗi fetch là lỗi "fail
  cứng" (xem ingestion/utils/alerts.py) vì downstream (moving average,
  volatility) cần đủ dữ liệu mới tính đúng.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from ingestion.config import INTL_SYMBOLS, VN_SYMBOLS
from ingestion.utils.alerts import alert_on_failure

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="daily_ingest_dag",
    schedule="0 16 * * 1-5",  # sau giờ đóng cửa HOSE (16:00 UTC+7), thứ 2-6
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stock-pipeline", "ingest"],
)
def daily_ingest_dag():
    @task
    def fetch_and_write_vn(symbol: str, ds: str | None = None) -> str:
        from ingestion.fetch_vnstock import fetch_ohlcv
        from ingestion.utils.s3_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    @task
    def fetch_and_write_intl(symbol: str, ds: str | None = None) -> str:
        from ingestion.fetch_yfinance import fetch_ohlcv
        from ingestion.utils.s3_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    vn_results = fetch_and_write_vn.expand(symbol=VN_SYMBOLS)
    intl_results = fetch_and_write_intl.expand(symbol=INTL_SYMBOLS)

    # Placeholder: dbt project chưa tồn tại (sẽ thêm ở Bước 6-7). Giữ node
    # này để dependency graph phản ánh đúng thiết kế cuối cùng ngay từ bây giờ.
    dbt_run_placeholder = EmptyOperator(task_id="dbt_run_placeholder")

    [vn_results, intl_results] >> dbt_run_placeholder


daily_ingest_dag()
