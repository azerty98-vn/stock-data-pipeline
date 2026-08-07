{{ config(
    materialized='incremental',
    unique_key=['symbol', 'date'],
    on_schema_change='sync_all_columns'
) }}

-- MA50 cần đúng 50 ngày lịch sử liên tục trước mỗi ngày; lookback_days=60
-- (>50 + margin) đảm bảo NGAY CẢ ngày đầu tiên trong output cũng có đủ 50
-- ngày lịch sử phía trước để tính đúng — nếu lookback = 50 thì ngày đầu
-- tiên của mỗi batch sẽ bị tính MA50 trên window cụt (< 50 điểm, sai).
{% set lookback_days = 60 %}

with base as (
    select * from {{ ref('int_ohlcv_unioned') }}
    {% if is_incremental() %}
    where date >= date_sub(
        (select max(date) from {{ this }}), interval {{ lookback_days }} day
    )
    {% endif %}
),

windowed as (
    select
        market,
        symbol,
        date,
        close,
        row_number() over (partition by symbol order by date) as rn,
        avg(close) over (
            partition by symbol order by date
            rows between 19 preceding and current row
        ) as ma_20,
        avg(close) over (
            partition by symbol order by date
            rows between 49 preceding and current row
        ) as ma_50
    from base
)

select
    market,
    symbol,
    date,
    close,
    ma_20,
    ma_50
from windowed
-- rn > 50: loại các ngày ở đầu window chưa đủ 50 điểm lịch sử (MA50 sẽ
-- partial/sai nếu giữ lại) — cùng nguyên tắc "đủ context trước khi tính"
-- như fct_daily_returns, nhưng ngưỡng khác vì window dài hơn (50 vs 1 ngày).
where rn > 50
