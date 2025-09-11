{{ config(
    materialized='table',
    description='Daily Bitcoin price facts with economic context'
) }}

WITH bitcoin_daily AS (
    SELECT * FROM {{ ref('int_bitcoin_daily') }}
),

economic_daily AS (
    SELECT 
        date,
        MAX(CASE WHEN series_name = 'federal_funds_rate' THEN observation_value END) AS fed_funds_rate,
        MAX(CASE WHEN series_name = 'treasury_10y' THEN observation_value END) AS treasury_10y,
        MAX(CASE WHEN series_name = 'treasury_2y' THEN observation_value END) AS treasury_2y,
        MAX(CASE WHEN series_name = 'sofr_rate' THEN observation_value END) AS sofr_rate,
        MAX(CASE WHEN series_name = 'sp500_index' THEN observation_value END) AS sp500_index,
        MAX(CASE WHEN series_name = 'initial_claims' THEN observation_value END) AS initial_claims,
        MAX(CASE WHEN series_name = 'unemployment_rate' THEN observation_value END) AS unemployment_rate
    FROM {{ ref('int_economic_indicators') }}
    GROUP BY date
)

SELECT 
    btc.date,
    btc.coin_id,
    btc.price_usd,
    btc.market_cap_usd,
    btc.total_volume_usd,
    btc.high_24h_usd,
    btc.low_24h_usd,
    btc.price_change_24h,
    btc.price_change_pct_24h,
    btc.circulating_supply,
    btc.total_supply,
    btc.max_supply,
    btc.prev_day_price,
    btc.price_change,
    btc.price_change_pct,
    
    COALESCE(
        econ.fed_funds_rate,
        LAST_VALUE(econ.fed_funds_rate IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS fed_funds_rate,
    
    COALESCE(
        econ.treasury_10y,
        LAST_VALUE(econ.treasury_10y IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS treasury_10y,
    
    COALESCE(
        econ.treasury_2y,
        LAST_VALUE(econ.treasury_2y IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS treasury_2y,
    
    COALESCE(
        econ.sofr_rate,
        LAST_VALUE(econ.sofr_rate IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS sofr_rate,
    
    COALESCE(
        econ.sp500_index,
        LAST_VALUE(econ.sp500_index IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS sp500_index,
    
    econ.initial_claims,
    econ.unemployment_rate,
    
    ROUND(btc.price_usd / LAG(btc.price_usd, 1) OVER (ORDER BY btc.date) - 1, 6) AS daily_return,
    ROUND(btc.price_usd / LAG(btc.price_usd, 7) OVER (ORDER BY btc.date) - 1, 4) AS weekly_return,
    ROUND(btc.price_usd / LAG(btc.price_usd, 30) OVER (ORDER BY btc.date) - 1, 4) AS monthly_return,
    
    STDDEV(btc.price_change_pct) OVER (
        ORDER BY btc.date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS price_30d_volatility,
    
    CASE 
        WHEN econ.fed_funds_rate IS NOT NULL 
        THEN 'Economic Data Available'
        ELSE 'Economic Data Missing'
    END AS economic_data_status,
    
    btc.is_valid_price,
    btc.record_collection_timestamp,
    btc.batch_collection_timestamp,
    btc.dbt_created_at
    
FROM bitcoin_daily btc
LEFT JOIN economic_daily econ ON btc.date = econ.date
ORDER BY btc.date