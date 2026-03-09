with latest as (

    select CAST(MAX(snapshot_date) AS DATE) as latest_date
    from {{ ref('stg_coin_markets') }}

)

select
    s.coin_id,
    s.coin_name,
    s.coin_symbol,
    s.price_usd,
    s.price_change_pct_24h,
    s.price_change_pct_7d,
    s.market_rank,
    ABS(s.price_change_pct_24h) as abs_change_24h

from {{ ref('stg_coin_markets') }} AS s
inner join latest ON s.snapshot_date = latest.latest_date

order by abs_change_24h DESC
