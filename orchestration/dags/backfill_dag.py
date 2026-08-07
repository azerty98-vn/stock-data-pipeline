"""Backfill DAG — trigger thủ công (schedule=None), không chạy tự động.

Dùng khi cần seed lịch sử ban đầu (marts cần 50+ ngày mới ra kết quả) hoặc
sửa dữ liệu sai của 1 khoảng ngày trong quá khứ (xem pipeline/backfill.py
để biết đầy đủ lý do thiết kế). Trigger qua Airflow UI với Dag Run
config dạng JSON, vd:

    {"start": "2024-01-01", "end": "2024-03-31", "batch_days": 30}

Không đặt trong daily_ingest_dag vì đây là thao tác vận hành đặc biệt
(chạy khi cần, không phải theo lịch), gộp chung sẽ làm daily DAG khó đọc
và dễ backfill nhầm khi chỉ định sai tham số ngày.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from pipeline.alerts import alert_on_failure

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="backfill_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stock-pipeline", "backfill", "manual-trigger"],
    params={"start": "", "end": "", "batch_days": 30},
)
def backfill_dag():
    @task
    def run_backfill(**context) -> str:
        from datetime import date as date_cls

        from pipeline.backfill import backfill

        params = context["params"]
        if not params.get("start") or not params.get("end"):
            raise AirflowSkipException(
                "Thiếu 'start'/'end' trong Dag Run config — vd: "
                '{"start": "2024-01-01", "end": "2024-03-31", "batch_days": 30}'
            )

        start = date_cls.fromisoformat(params["start"])
        end = date_cls.fromisoformat(params["end"])
        batch_days = int(params.get("batch_days") or 30)

        backfill(start, end, batch_days)
        return f"Backfilled {start} -> {end} (batch_days={batch_days})"

    run_backfill()


backfill_dag()
