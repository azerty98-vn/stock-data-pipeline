{{ config(
    materialized='incremental',
    unique_key=['symbol', 'date'],
    on_schema_change='sync_all_columns'
) }}

-- Incremental (khác staging, vốn full-refresh): bảng này lớn dần theo thời
-- gian và daily_return chỉ cần close hôm nay + hôm qua, nên không cần quét
-- lại toàn bộ lịch sử mỗi lần chạy.
--
-- Trade-off đã biết: lookback_days=10 đủ bù late-arriving data trong vài
-- ngày gần nhất. Nếu phát hiện giá SAI của > 10 ngày trước (vd: API trả
-- nhầm giá 2 tuần trước, xem Ngày 2 trong plan gốc), phải chạy
-- `dbt run --full-refresh` để backfill lại toàn bộ, incremental thường
-- ngày sẽ không tự sửa các ngày cũ hơn lookback window.
{% set lookback_days = 10 %}

with base as (
    select * from {{ ref('int_ohlcv_unioned') }}
    {% if is_incremental() %}
    where date >= date_sub(
        (select max(date) from {{ this }}), interval {{ lookback_days }} day
    )
    {% endif %}
),

lagged as (
    select
        market,
        symbol,
        date,
        close,
        lag(close) over (partition by symbol order by date) as prev_close
    from base
)

select
    market,
    symbol,
    date,
    close,
    prev_close,
    safe_divide(close - prev_close, prev_close) as daily_return
from lagged
-- Ngày đầu tiên của mỗi batch incremental thiếu prev_close (nằm ngoài
-- window) bị loại ở đây — không phải bug: ngày đó đã được tính đúng ở lần
-- chạy trước đó và MERGE sẽ giữ nguyên row cũ vì không nằm trong output lần này.
where prev_close is not null
