with latest as (

    select CAST(MAX(snapshot_date) AS DATE) as latest_date
    from {{ ref('stg_coin_markets') }}

)

select
    s.coin_id,
    s.coin_name,
    s.coin_symbol,
    s.market_rank,
    s.price_usd,
    s.market_cap_usd,
    s.volume_24h_usd,
    s.price_change_pct_24h,
    s.price_change_pct_7d,
    s.snapshot_date

from {{ ref('stg_coin_markets') }} AS s
inner join latest ON s.snapshot_date = latest.latest_date

order by s.market_rank ASC
