# Stock Data Pipeline

End-to-end ELT pipeline cho dữ liệu OHLCV (VN qua vnstock, quốc tế qua yfinance): Airflow điều phối extract → load raw → validate (Great Expectations) → load warehouse → transform (dbt).

## Luồng dữ liệu

```
                    ┌────────────────────── EXTRACT ──────────────────────┐
                    │  pipeline/extract/fetch_vnstock.py                   │
                    │  pipeline/extract/fetch_yfinance.py                  │
                    │  -> validate từng row theo pipeline/contracts/       │
                    │     schema.py (fail-fast nếu API đổi format)         │
                    └──────────────────────┬────────────────────────────-─┘
                                            v
                    ┌──────────────────── LOAD (raw) ──────────────────────┐
                    │  pipeline/load/gcs_writer.py                         │
                    │  -> raw/{source}/{symbol}/{date}.parquet trên GCS    │
                    │     (idempotent: overwrite theo key)                 │
                    └──────────────────────┬────────────────────────────-─┘
                                            v
                    ┌───────────────────── VALIDATE ───────────────────────┐
                    │  quality/validate_raw.py (Great Expectations)        │
                    │  -> validate CẢ BATCH (N symbol/ngày) vừa ghi trên   │
                    │     GCS, TRƯỚC khi load vào warehouse                │
                    └──────────────────────┬────────────────────────────-─┘
                                            v
                    ┌────────────────── LOAD (warehouse) ──────────────────┐
                    │  pipeline/load/bq_loader.py                          │
                    │  -> BigQuery raw_ohlcv$YYYYMMDD                      │
                    │     (idempotent: WRITE_TRUNCATE theo partition)      │
                    └──────────────────────┬───────────────────────────-──┘
                                            v
                    ┌───────────────────TRANSFORM (dbt) ───────────────────┐
                    │  dbt source freshness (data đã "tới" chưa)           │
                    │  transform/models/staging/    (view, full refresh)   │
                    │  transform/models/intermediate/ (union VN + intl)    │
                    │  transform/models/marts/      (incremental, merge)   │
                    │  -> fct_daily_returns, fct_moving_averages,          │
                    │     fct_volatility, fct_volume_anomaly               │
                    └──────────────────────────────────────────────────────┘

Toàn bộ pipeline trên được orchestration/dags/daily_ingest_dag.py (Airflow)
điều phối theo lịch hàng ngày.
```

## Cấu trúc repo

```
stock-data-pipeline/
├── orchestration/          # Airflow: điều phối toàn bộ pipeline
│   ├── Dockerfile
│   └── dags/daily_ingest_dag.py
├── pipeline/
│   ├── extract/            # E: gọi API nguồn, trả về data đã validate
│   ├── load/                # L: ghi raw -> GCS, raw -> BigQuery
│   ├── contracts/           # Data contract dùng chung cho extract layer
│   ├── alerts.py
│   └── config.py            # Danh sách symbol theo dõi
├── transform/                # T: dbt project (staging -> intermediate -> marts)
├── quality/                  # Great Expectations (raw layer validation)
├── dashboard/
├── docs/                     # Quyết định thiết kế (grain, idempotency, error handling...)
└── docker-compose.yml
```
