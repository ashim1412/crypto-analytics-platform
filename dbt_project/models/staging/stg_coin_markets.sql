with source as (

    select * from {{ source('bronze', 'raw_coin_markets') }}

),

renamed as (

    select
        CAST(id as varchar) as coin_id,
        CAST(symbol as varchar) as coin_symbol,
        CAST(name as varchar) as coin_name,
        CAST(current_price as double) as price_usd,
        CAST(market_cap as double) as market_cap_usd,
        CAST(total_volume as double) as volume_24h_usd,
        CAST(price_change_percentage_24h as double) as price_change_pct_24h,
        CAST(price_change_percentage_7d_in_currency as double) as price_change_pct_7d,
        CAST(market_cap_rank as integer) as market_rank,
        CAST(circulating_supply as double) as circulating_supply,
        TRY_CAST(last_updated as timestamp) as last_updated_at,
        CAST(ingested_at as timestamp) as ingested_at,
        CAST(ingested_at as date) as snapshot_date

    from source

),

deduped as (

    select
        *,
        ROW_NUMBER() over (
            partition by coin_id, snapshot_date
            order by ingested_at desc
        ) as _row_num

    from renamed

)

select
    coin_id,
    coin_symbol,
    coin_name,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    price_change_pct_24h,
    price_change_pct_7d,
    market_rank,
    circulating_supply,
    last_updated_at,
    ingested_at,
    snapshot_date

from deduped
where _row_num = 1
