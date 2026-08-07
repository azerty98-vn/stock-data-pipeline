# Stock Data Pipeline

End-to-end ELT pipeline cho dữ liệu OHLCV (VN qua vnstock, quốc tế qua yfinance): Airflow điều phối extract → load raw → load warehouse → transform (dbt).

## Luồng dữ liệu

```
                    ┌────────────────────── EXTRACT ──────────────────────┐
                    │  pipeline/extract/fetch_vnstock.py                   │
                    │  pipeline/extract/fetch_yfinance.py                  │
                    │  -> validate theo pipeline/contracts/schema.py       │
                    │     (fail-fast nếu API đổi format)                   │
                    └──────────────────────┬────────────────────────────-─┘
                                            v
                    ┌─────────────────────── LOAD ─────────────────────────┐
                    │  pipeline/load/gcs_writer.py                         │
                    │  -> raw/{source}/{symbol}/{date}.parquet trên GCS    │
                    │     (idempotent: overwrite theo key)                 │
                    │                                                      │
                    │  pipeline/load/bq_loader.py                          │
                    │  -> BigQuery raw_ohlcv$YYYYMMDD                      │
                    │     (idempotent: WRITE_TRUNCATE theo partition)      │
                    └──────────────────────┬───────────────────────────-──┘
                                            v
                    ┌───────────────────TRANSFORM (dbt) ───────────────────┐
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

## Chạy local

```bash
cp .env.example .env   # điền GCS_BUCKET, BQ_PROJECT; đặt service account json tại keys/bq-service-account.json
docker compose up --build
```

Airflow UI: http://localhost:8080 (admin/admin — đổi mật khẩu sau khi chạy).

## Trạng thái

Xem lịch sử commit — mỗi commit tương ứng 1 bước trong pipeline (extract → load → transform → orchestration), kèm lý do trade-off trong message.
