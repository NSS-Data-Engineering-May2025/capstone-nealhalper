{{ config(
    materialized='view',
    description='Cleaned and validated Bitcoin daily price data'
) }}

WITH cleaned_bitcoin AS (
    SELECT 
        record_date,
        coin_id,
        TRY_CAST(price_usd AS DECIMAL(15,2)) AS price_usd,
        TRY_CAST(market_cap_usd AS DECIMAL(20,2)) AS market_cap_usd,
        TRY_CAST(total_volume_usd AS DECIMAL(20,2)) AS total_volume_usd,
        TRY_CAST(max_supply AS DECIMAL(20,8)) AS max_supply,
        is_valid_json,
        record_collection_timestamp,
        batch_collection_timestamp,
        dbt_created_at
    FROM {{ ref('stg_coingecko_raw') }}
    WHERE coin_id = 'bitcoin'
        AND record_date IS NOT NULL
        AND price_usd IS NOT NULL
        AND is_valid_json = TRUE  
),

validated_bitcoin AS (
    SELECT *,
        CASE 
            WHEN price_usd <= 0 THEN FALSE
            WHEN price_usd IS NULL THEN FALSE 
            ELSE TRUE
        END AS is_valid_price,

        LAG(price_usd) OVER (ORDER BY record_date) AS prev_day_price,
        price_usd - LAG(price_usd) OVER (ORDER BY record_date) AS price_change,
        ROUND(
            ((price_usd - LAG(price_usd) OVER (ORDER BY record_date)) / 
             LAG(price_usd) OVER (ORDER BY record_date)) * 100, 2
        ) AS price_change_pct
        
    FROM cleaned_bitcoin
)

SELECT 
    DATE(record_date) AS date,
    coin_id,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    max_supply,
    prev_day_price,
    price_change,
    price_change_pct,
    is_valid_price,
    record_collection_timestamp,
    batch_collection_timestamp,
    dbt_created_at
FROM validated_bitcoin
WHERE is_valid_price = TRUE
ORDER BY date