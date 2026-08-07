# Data Model & Design Decisions

Tài liệu này tổng hợp các quyết định thiết kế đã đưa ra khi xây dựng pipeline — grain/contract, idempotency/backfill, phân loại lỗi, và dependency của DAG. Mỗi quyết định gắn với code thật, không phải mô tả lý thuyết.

## 1. Grain & data contract

| Layer | Bảng/model | Grain (1 row =) | Contract |
|---|---|---|---|
| Raw | `raw_ohlcv` (BigQuery, partitioned theo `date`) | 1 symbol × 1 ngày giao dịch × 1 source | `pipeline/contracts/schema.py` (`OhlcvRecord`, pydantic) — validate tại **extract time**, trước khi ghi xuống GCS |
| Staging | `stg_ohlcv_vn`, `stg_ohlcv_intl` | 1 symbol × 1 ngày giao dịch (đã tách theo source) | dbt tests: `unique(symbol, date)`, `not_null`, `accepted_range` cho giá — xem `transform/models/staging/_stg_ohlcv__models.yml` |
| Intermediate | `int_ohlcv_unioned` | 1 symbol × 1 ngày giao dịch (union VN + intl, thêm cột `market`) | Kế thừa contract của 2 staging model |
| Marts | `fct_daily_returns`, `fct_moving_averages`, `fct_volatility`, `fct_volume_anomaly` | 1 symbol × 1 ngày giao dịch | dbt tests: `unique(symbol, date)` — xem `transform/models/marts/_marts.yml` |

**Tự vấn (từ plan gốc): nếu vnstock/yfinance đổi format response, pipeline có báo lỗi ngay không?**
Có — `pipeline/extract/fetch_vnstock.py` và `fetch_yfinance.py` check `missing = set(COLUMN_MAP) - set(raw_df.columns)` ngay sau khi gọi API; nếu API đổi tên/bớt cột, `ValueError` raise ngay tại extract, trước khi bất kỳ dòng nào chạm tới GCS. Đây là lớp phòng thủ đầu tiên trong 3 lớp (xem mục 3).

## 2. Idempotency & backfill

| Layer | Chiến lược | Vì sao |
|---|---|---|
| Raw (GCS) | Overwrite theo key `raw/{source}/{symbol}/{date}.parquet` | Raw là bản chụp thô, không có PK cần merge — ghi lại cùng ngày N lần luôn ra cùng kết quả. Xem `pipeline/load/gcs_writer.py`. |
| Raw (BigQuery) | Partition decorator `raw_ohlcv$YYYYMMDD` + `WRITE_TRUNCATE` | Load lại 1 ngày chỉ ghi đè đúng partition đó, không đụng ngày khác. Xem `pipeline/load/bq_loader.py`. |
| Staging | Full refresh (view) | Volume nhỏ, staging phải luôn phản ánh đúng raw mới nhất — không có lý do để incremental ở layer này. |
| Marts | Incremental + `merge` theo `(symbol, date)`, lookback window > window tính toán dài nhất (vd. 60 ngày cho MA50) | Bảng lớn dần theo thời gian, full refresh mỗi ngày tốn compute không cần thiết cho rolling-window math. Lookback phải dư ra để dòng đầu tiên của mỗi batch incremental vẫn có đủ lịch sử — xem comment trong từng file `transform/models/marts/*.sql`. |

**Tình huống (từ plan gốc): phát hiện dữ liệu 2 tuần trước bị sai — backfill mà không chạy lại từ đầu.**
`pipeline/backfill.py` (trigger qua `orchestration/dags/backfill_dag.py`, `schedule=None`) nhận `--start`/`--end`/`--batch-days`, fetch lại đúng khoảng ngày cần sửa theo batch date-range, rồi validate + load lại đúng các partition đó. Vì raw và warehouse đều idempotent theo key/partition, backfill không ảnh hưởng tới dữ liệu ngoài khoảng ngày chỉ định, và có thể dừng/resume giữa chừng an toàn.

**Trade-off đã biết:** marts chỉ tự sửa dữ liệu trong `lookback_days` gần nhất khi chạy `dbt run` bình thường. Sửa dữ liệu cũ hơn window đó cần `dbt run --full-refresh` (hoặc backfill lại đúng khoảng ngày rồi full-refresh marts liên quan).

## 3. Xử lý lỗi — 3 lớp phòng thủ, không trùng nhau

| Lớp | Vị trí | Bắt loại lỗi nào | Hành động khi fail |
|---|---|---|---|
| 1. Contract (pydantic) | `pipeline/contracts/schema.py`, chạy tại **extract** | Lỗi ở **từng row**: giá ≤ 0, thiếu field, sai kiểu, API đổi tên cột | Fail cứng — task Airflow fail, retry 2 lần (exponential backoff), alert qua `pipeline/alerts.py` |
| 2. Batch validation (Great Expectations) | `quality/validate_raw.py`, chạy **sau khi ghi GCS, trước khi load BigQuery** | Lỗi chỉ lộ ra ở mức **cả batch**: thiếu >50% symbol trong ngày (outage diện rộng) → fail cứng; thiếu 1-vài symbol (nghỉ lễ/delist) → warning, tiếp tục | Fail cứng nếu vượt ngưỡng outage; warning + tiếp tục nếu dưới ngưỡng |
| 3. Freshness (dbt source freshness) | `transform/models/staging/_sources.yml`, chạy **sau khi load warehouse, trước dbt run** | Dữ liệu "chưa tới" (không phải "tới nhưng sai") — `warn_after: 30h`, `error_after: 54h` (30h vì cuối tuần thị trường không giao dịch) | `warn`: log, không chặn. `error`: chặn `dbt_run` |
| 4. Business rule (dbt test) | `transform/models/**/*.yml`, chạy **sau transform** | Lỗi business rule dù transform chạy thành công: duplicate `(symbol, date)`, giá âm sau transform | Fail cứng — task `dbt_test` riêng biệt với `dbt_run` để phân biệt "lỗi SQL" vs "lỗi business rule" trong Airflow UI |

**Nguyên tắc phân loại fail cứng vs degrade gracefully:** nếu lỗi ảnh hưởng tới **tính đúng của rolling-window calculation** ở mart layer (moving average, volatility cần dữ liệu liên tục, không được thiếu ngày giữa chừng) → fail cứng + alert. Nếu lỗi chỉ là **thiếu 1-vài đơn vị nhỏ** trong phạm vi chấp nhận được (1 symbol nghỉ lễ, không phải cả thị trường) → log warning, tiếp tục, không chặn pipeline.

## 4. DAG dependency

### `daily_ingest_dag` (chạy theo lịch, `0 16 * * 1-5`)

```
fetch_vn (N symbol, song song)   -> validate_raw_vn    \
                                                          > load_to_warehouse -> dbt_source_freshness -> dbt_run -> dbt_test
fetch_intl (N symbol, song song) -> validate_raw_intl  /
```

- `fetch_vn`/`fetch_intl` song song: 2 nguồn độc lập, không có quan hệ nghiệp vụ.
- Symbol trong cùng 1 nhóm cũng song song (Airflow mapped task): mỗi symbol là 1 đơn vị việc độc lập, lỗi 1 symbol không chặn symbol khác.
- `validate_raw_*` đứng sau fetch tương ứng (đọc lại batch vừa ghi trên GCS), trước `load_to_warehouse`.
- `load_to_warehouse` downstream của **cả 2** nhánh validate (không phải từng cái riêng) vì nó load raw của cả 2 nguồn cùng lúc.
- `dbt_source_freshness` đứng sau `load_to_warehouse` (freshness kiểm tra bảng trong warehouse, không phải file trên GCS).
- `dbt_test` tách khỏi `dbt_run` để phân biệt 2 loại thất bại trong Airflow UI (xem mục 3).

### `backfill_dag` (trigger thủ công, `schedule=None`)

Độc lập hoàn toàn với `daily_ingest_dag` — không share task, chỉ share code (`pipeline/backfill.py` gọi lại đúng các hàm extract/load/validate mà DAG hàng ngày dùng). Tách DAG riêng vì đây là thao tác vận hành đặc biệt (chạy khi cần, có tham số ngày do người vận hành nhập), gộp chung sẽ làm DAG hàng ngày khó đọc và dễ backfill nhầm nếu Trigger DAG w/ Config bị dùng sai chỗ.

## 5. Nếu dataset lớn gấp 100 lần, chỗ nào vỡ trước tiên?

- **`quality/validate_raw.py`**: đọc toàn bộ batch vào pandas DataFrame trong memory trước khi validate — với vài trăm symbol × nhiều năm lịch sử, cách này không scale, cần chuyển sang validate trực tiếp trên BigQuery (GE có SQL execution engine) thay vì pandas.
- **`pipeline/load/gcs_writer.py`**: mỗi record group ghi 1 object riêng qua `put_object`/`upload_from_file` tuần tự — với hàng nghìn symbol, cần ghi song song (thread pool) hoặc gộp batch write.
- **Marts incremental `lookback_days`**: cố định (10-60 ngày) không phụ thuộc volume — vẫn ổn khi số symbol tăng, nhưng thời gian chạy `dbt run` sẽ tăng tuyến tính theo số symbol vì mỗi partition BigQuery giờ chứa nhiều row hơn.
- **`pipeline/extract/fetch_*.py`**: gọi API tuần tự từng symbol (không song song trong 1 task) — Airflow mapped task đã song song hoá ở mức DAG, nhưng nếu 1 batch backfill cần fetch nhiều symbol trong `pipeline/backfill.py`, vòng lặp `for symbol in symbols` hiện tại là tuần tự, sẽ chậm tuyến tính theo số symbol.
