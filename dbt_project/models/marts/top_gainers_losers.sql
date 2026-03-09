with latest as (

    select CAST(MAX(snapshot_date) AS DATE) as latest_date
    from {{ ref('stg_coin_markets') }}

),

filtered as (

    select s.*
    from {{ ref('stg_coin_markets') }} AS s
    inner join latest ON s.snapshot_date = latest.latest_date

),

gainers as (

    select
        coin_id,
        coin_name,
        coin_symbol,
        price_usd,
        price_change_pct_24h,
        'gainer' as category,
        ROW_NUMBER() over (order by price_change_pct_24h DESC) as rank_position,
        snapshot_date

    from filtered

),

losers as (

    select
        coin_id,
        coin_name,
        coin_symbol,
        price_usd,
        price_change_pct_24h,
        'loser' as category,
        ROW_NUMBER() over (order by price_change_pct_24h ASC) as rank_position,
        snapshot_date

    from filtered

)

select
    coin_id,
    coin_name,
    coin_symbol,
    price_usd,
    price_change_pct_24h,
    category,
    rank_position,
    snapshot_date

from gainers where rank_position <= 10

union all

select
    coin_id,
    coin_name,
    coin_symbol,
    price_usd,
    price_change_pct_24h,
    category,
    rank_position,
    snapshot_date

from losers where rank_position <= 10
