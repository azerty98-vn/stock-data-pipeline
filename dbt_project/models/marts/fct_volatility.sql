{{ config(
    materialized='incremental',
    unique_key=['symbol', 'date'],
    on_schema_change='sync_all_columns'
) }}

-- Volatility 20 ngày = stddev của daily_return, nên phụ thuộc fct_daily_returns
-- (mart khác) chứ không phải int_ohlcv_unioned trực tiếp — dbt tự xếp đúng
-- thứ tự chạy nhờ ref(). Cùng nguyên tắc "lookback > window size" như
-- fct_moving_averages: cần 20 ngày return liên tục trước mỗi điểm output.
{% set window_days = 20 %}
{% set lookback_days = 30 %}

with returns as (
    select * from {{ ref('fct_daily_returns') }}
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
        daily_return,
        row_number() over (partition by symbol order by date) as rn,
        stddev_samp(daily_return) over (
            partition by symbol order by date
            rows between {{ window_days - 1 }} preceding and current row
        ) as volatility_20d
    from returns
)

select
    market,
    symbol,
    date,
    volatility_20d,
    volatility_20d * sqrt(252) as volatility_20d_annualized
from windowed
where rn > {{ window_days }}
