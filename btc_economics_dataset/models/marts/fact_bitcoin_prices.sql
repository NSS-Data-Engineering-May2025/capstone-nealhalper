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
        MAX(CASE WHEN series_name = 'sofr_rate' THEN observation_value END) AS sofr_rate,
        MAX(CASE WHEN series_name = 'sp500_index' THEN observation_value END) AS sp500_index,
        MAX(CASE WHEN series_name = 'unemployment_rate' THEN observation_value END) AS unemployment_rate
    FROM {{ ref('int_economic_indicators') }}
    GROUP BY date
)

SELECT 
    btc.date,
    btc.price_usd,
    btc.market_cap_usd,
    btc.total_volume_usd,
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
        econ.sp500_index,
        LAST_VALUE(econ.sp500_index IGNORE NULLS) OVER (
            ORDER BY btc.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS sp500_index,
    
    ROUND(btc.price_usd / LAG(btc.price_usd, 7) OVER (ORDER BY btc.date) - 1, 4) AS weekly_return,
    ROUND(btc.price_usd / LAG(btc.price_usd, 30) OVER (ORDER BY btc.date) - 1, 4) AS monthly_return,
    
    btc.dbt_created_at
    
FROM bitcoin_daily btc
LEFT JOIN economic_daily econ ON btc.date = econ.date
ORDER BY btc.date