from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="daily_ingest_dag",
    schedule="0 16 * * 1-5",  # sau giờ đóng cửa HOSE (16:00 UTC+7), thứ 2-6
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stock-pipeline", "ingest"],
)
def daily_ingest_dag():
    @task
    def hello():
        print("Airflow stack is up.")

    hello()


daily_ingest_dag()
