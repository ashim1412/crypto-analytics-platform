with latest as (

    select CAST(MAX(snapshot_date) AS DATE) as latest_date
    from {{ ref('stg_coin_markets') }}

),

filtered as (

    select s.*
    from {{ ref('stg_coin_markets') }} AS s
    inner join latest ON s.snapshot_date = latest.latest_date

),

with_totals as (

    select
        *,
        SUM(market_cap_usd) OVER () as total_market_cap

    from filtered

)

select
    coin_id,
    coin_name,
    coin_symbol,
    market_cap_usd,
    ROUND((market_cap_usd / total_market_cap) * 100, 4) as dominance_pct,
    market_rank,
    snapshot_date

from with_totals

order by dominance_pct DESC
