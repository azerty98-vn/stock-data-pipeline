-- Grain: 1 row = 1 symbol x 1 trading date (VN market, nguồn vnstock).
--
-- Raw layer đã idempotent theo partition (bq_loader.py: WRITE_TRUNCATE theo
-- ngày), nên về lý thuyết không nên có duplicate (symbol, date) ở đây.
-- row_number() dedupe là lớp phòng thủ thứ 2: nếu vẫn có duplicate (bug ở
-- raw layer), model này chọn 1 row "arbitrary" (order by close) thay vì
-- fail cứng — đánh đổi correctness lấy availability, chấp nhận được cho
-- side-project. Test `unique(symbol, date)` ở _stg_ohlcv__models.yml chỉ
-- xác nhận layer NÀY sạch, không phát hiện được bug ở raw — nếu cần bắt bug
-- tận gốc, thêm test unique trên source thay vì trên staging.

with source as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume
    from {{ source('raw', 'raw_ohlcv') }}
    where source = 'vnstock'
),

deduped as (
    select
        *,
        row_number() over (partition by symbol, date order by close) as rn
    from source
)

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
from deduped
where rn = 1
