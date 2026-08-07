-- Grain: 1 row = 1 symbol x 1 trading date (quốc tế, nguồn yfinance).
-- Cùng logic dedupe với stg_ohlcv_vn.sql — xem comment ở đó.

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
    where source = 'yfinance'
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
