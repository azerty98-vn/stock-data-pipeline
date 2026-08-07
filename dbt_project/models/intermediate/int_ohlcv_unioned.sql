-- Union 2 nguồn đã cùng grain (symbol, date) và cùng contract nhờ
-- OhlcvRecord ở ingestion layer — marts không cần biết VN hay quốc tế,
-- chỉ cần cột `market` để filter/group khi cần.

select 'VN' as market, * from {{ ref('stg_ohlcv_vn') }}
union all
select 'INTL' as market, * from {{ ref('stg_ohlcv_intl') }}
