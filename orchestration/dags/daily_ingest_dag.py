"""Daily ingest DAG — điều phối toàn bộ pipeline ELT.

Pipeline stage -> code:
  Extract   (pipeline/extract/)   : gọi API nguồn, validate contract (pydantic)
  Validate  (quality/)            : Great Expectations, validate cả batch trước khi load
  Load      (pipeline/load/)      : ghi raw -> GCS, raw -> BigQuery
  Transform (transform/, dbt)     : freshness -> staging -> intermediate -> marts

Dependency design (Ngày 4 trong plan gốc):
  fetch_vn -> validate_raw_vn     \
                                    > load_to_warehouse -> dbt_source_freshness -> dbt_run -> dbt_test
  fetch_intl -> validate_raw_intl /

- fetch_vn và fetch_intl (và các nhánh validate theo sau) chạy song song:
  2 nguồn dữ liệu độc lập, không có quan hệ phụ thuộc nghiệp vụ nào giữa chúng.
- Trong mỗi nhóm fetch, symbol cũng chạy song song với nhau (map task) vì
  mỗi symbol là 1 đơn vị công việc độc lập, lỗi 1 symbol không nên chặn
  symbol khác.
- validate_raw_* đứng SAU fetch (đọc lại batch đã ghi trên GCS) và TRƯỚC
  load_to_warehouse — đúng vị trí "trước khi load vào warehouse" theo thiết
  kế (xem quality/validate_raw.py để biết vì sao tách riêng khỏi pydantic
  ở extract và dbt test ở transform, không phải trùng lặp).
- load_to_warehouse là downstream của CẢ HAI nhánh validate (không phải
  từng cái riêng), vì nó load raw của cả 2 nguồn cùng lúc.
- dbt_source_freshness đứng SAU load_to_warehouse (freshness kiểm tra bảng
  trong warehouse, không phải file trên GCS) và TRƯỚC dbt_run — tách biệt
  "chưa tới" (freshness fail = warn, không chặn) khỏi "tới nhưng sai" (dbt
  test fail = chặn).
- dbt_test tách riêng khỏi dbt_run (không gộp `dbt build`) để phân biệt rõ
  2 loại thất bại trong Airflow UI: dbt_run fail = lỗi transform (SQL/schema
  drift), dbt_test fail = business rule vi phạm dù transform chạy được —
  hữu ích khi debug vì 2 nguyên nhân cần hướng điều tra khác nhau.
- retry=2 + exponential backoff + alert_on_failure: lỗi fetch/validate là
  lỗi "fail cứng" (xem pipeline/alerts.py) vì downstream (moving average,
  volatility) cần đủ dữ liệu mới tính đúng.

load_to_warehouse, dbt_source_freshness, dbt_run, dbt_test, validate_raw_*
cần BQ_PROJECT/GCS_BUCKET/GOOGLE_APPLICATION_CREDENTIALS thật (GCP project
đang được setup — xem .env.example) nên chưa chạy được ở thời điểm viết
code này.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from pipeline.alerts import alert_on_failure
from pipeline.config import INTL_SYMBOLS, VN_SYMBOLS

TRANSFORM_PROJECT_DIR = "/opt/airflow/transform"

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
        from pipeline.extract.fetch_vnstock import fetch_ohlcv
        from pipeline.load.gcs_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    @task
    def fetch_and_write_intl(symbol: str, ds: str | None = None) -> str:
        from pipeline.extract.fetch_yfinance import fetch_ohlcv
        from pipeline.load.gcs_writer import write_records

        records = fetch_ohlcv(symbol, start=ds, end=ds)
        keys = write_records(records)
        return f"{symbol}: {len(keys)} key(s) written"

    @task
    def validate_raw_vn(ds: str | None = None) -> str:
        from datetime import date as date_cls

        from quality.validate_raw import validate_raw_batch

        validate_raw_batch("vnstock", VN_SYMBOLS, date_cls.fromisoformat(ds))
        return "vnstock: batch validated OK"

    @task
    def validate_raw_intl(ds: str | None = None) -> str:
        from datetime import date as date_cls

        from quality.validate_raw import validate_raw_batch

        validate_raw_batch("yfinance", INTL_SYMBOLS, date_cls.fromisoformat(ds))
        return "yfinance: batch validated OK"

    @task
    def load_to_warehouse(ds: str | None = None) -> dict[str, int]:
        from datetime import date as date_cls

        from pipeline.load.bq_loader import load_day_to_bigquery

        record_date = date_cls.fromisoformat(ds)
        return {
            "vnstock": load_day_to_bigquery("vnstock", VN_SYMBOLS, record_date),
            "yfinance": load_day_to_bigquery("yfinance", INTL_SYMBOLS, record_date),
        }

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"dbt source freshness --project-dir {TRANSFORM_PROJECT_DIR} "
            f"--profiles-dir {TRANSFORM_PROJECT_DIR} --target dev"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {TRANSFORM_PROJECT_DIR} --profiles-dir {TRANSFORM_PROJECT_DIR} --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {TRANSFORM_PROJECT_DIR} --profiles-dir {TRANSFORM_PROJECT_DIR} --target dev",
    )

    vn_results = fetch_and_write_vn.expand(symbol=VN_SYMBOLS)
    intl_results = fetch_and_write_intl.expand(symbol=INTL_SYMBOLS)
    vn_validated = validate_raw_vn()
    intl_validated = validate_raw_intl()
    warehouse_result = load_to_warehouse()

    vn_results >> vn_validated
    intl_results >> intl_validated
    [vn_validated, intl_validated] >> warehouse_result >> dbt_source_freshness >> dbt_run >> dbt_test


daily_ingest_dag()
