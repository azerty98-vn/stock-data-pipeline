"""Daily ingest DAG.

Dependency design (Ngày 4 trong plan gốc):
  fetch_vn (N symbol, độc lập nhau)   \
                                        > load_to_warehouse -> dbt_run -> dbt_test
  fetch_intl (N symbol, độc lập nhau) /

- fetch_vn và fetch_intl chạy song song: 2 nguồn dữ liệu độc lập, không có
  quan hệ phụ thuộc nghiệp vụ nào giữa chúng.
- Trong mỗi nhóm, symbol cũng chạy song song với nhau (map task) vì mỗi
  symbol là 1 đơn vị công việc độc lập, lỗi 1 symbol không nên chặn symbol khác.
- load_to_warehouse là downstream của CẢ HAI nhóm (không phải từng cái
  riêng), vì nó load raw của cả 2 nguồn cùng lúc; dbt_run lại downstream của
  load_to_warehouse vì staging query trực tiếp trên bảng warehouse, không
  phải trên GCS.
- dbt_test tách riêng khỏi dbt_run (không gộp `dbt build`) để phân biệt rõ
  2 loại thất bại trong Airflow UI: dbt_run fail = lỗi transform (SQL/schema
  drift), dbt_test fail = business rule vi phạm dù transform chạy được —
  hữu ích khi debug vì 2 nguyên nhân cần hướng điều tra khác nhau.
- retry=2 + exponential backoff + alert_on_failure: lỗi fetch là lỗi "fail
  cứng" (xem ingestion/utils/alerts.py) vì downstream (moving average,
  volatility) cần đủ dữ liệu mới tính đúng.

load_to_warehouse, dbt_run, dbt_test cần BQ_PROJECT/GOOGLE_APPLICATION_CREDENTIALS
thật (GCP project đang được setup — xem .env.example) nên chưa chạy được ở
thời điểm viết code này.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from ingestion.config import INTL_SYMBOLS, VN_SYMBOLS
from ingestion.utils.alerts import alert_on_failure

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

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
        from ingestion.utils.gcs_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    @task
    def fetch_and_write_intl(symbol: str, ds: str | None = None) -> str:
        from ingestion.fetch_yfinance import fetch_ohlcv
        from ingestion.utils.gcs_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    @task
    def load_to_warehouse(ds: str | None = None) -> dict[str, int]:
        from datetime import date as date_cls

        from ingestion.utils.bq_loader import load_day_to_bigquery

        record_date = date_cls.fromisoformat(ds)
        return {
            "vnstock": load_day_to_bigquery("vnstock", VN_SYMBOLS, record_date),
            "yfinance": load_day_to_bigquery("yfinance", INTL_SYMBOLS, record_date),
        }

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target dev",
    )

    vn_results = fetch_and_write_vn.expand(symbol=VN_SYMBOLS)
    intl_results = fetch_and_write_intl.expand(symbol=INTL_SYMBOLS)
    warehouse_result = load_to_warehouse()

    [vn_results, intl_results] >> warehouse_result >> dbt_run >> dbt_test


daily_ingest_dag()
