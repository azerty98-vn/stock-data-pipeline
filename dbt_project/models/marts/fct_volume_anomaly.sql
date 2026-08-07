{{ config(
    materialized='incremental',
    unique_key=['symbol', 'date'],
    on_schema_change='sync_all_columns'
) }}

-- Anomaly = volume hôm nay > 2x volume trung bình 20 ngày TRƯỚC đó (không
-- gồm hôm nay — "1 preceding" chứ không phải "current row") — nếu tính cả
-- hôm nay vào baseline thì spike sẽ tự làm loãng baseline của chính nó,
-- khiến ngưỡng 2x khó bị vượt hơn thực tế.
{% set window_days = 20 %}
{% set lookback_days = 30 %}

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
        volume,
        row_number() over (partition by symbol order by date) as rn,
        avg(volume) over (
            partition by symbol order by date
            rows between {{ window_days }} preceding and 1 preceding
        ) as avg_volume_20d_prior
    from base
)

select
    market,
    symbol,
    date,
    volume,
    avg_volume_20d_prior,
    volume > 2 * avg_volume_20d_prior as is_volume_anomaly
from windowed
where rn > {{ window_days }}
