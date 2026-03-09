select
    snapshot_date,
    coin_id,
    coin_name,
    coin_symbol,
    AVG(volume_24h_usd) as avg_volume,
    MAX(volume_24h_usd) as max_volume,
    MIN(volume_24h_usd) as min_volume

from {{ ref('stg_coin_markets') }}

group by snapshot_date, coin_id, coin_name, coin_symbol

order by snapshot_date DESC, avg_volume DESC
