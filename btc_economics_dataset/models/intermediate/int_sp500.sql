{{ config(
    materialized='table',
    description='Processed FRED S&P 500 data with market analysis'
) }}

WITH base_sp500 AS (
    SELECT * 
    FROM {{ ref('stg_fred_raw') }}
    WHERE data_category = 'fred'
      AND series_key = 'sp500_index'
      AND is_valid_observation = TRUE
),

numeric_sp500 AS (
    SELECT 
        *,
        TRY_CAST(observation_value AS DECIMAL(12,4)) AS sp500_value,
        DATE(observation_date) AS market_date
    FROM base_sp500
    WHERE TRY_CAST(observation_value AS DECIMAL(12,4)) IS NOT NULL
),

sp500_changes AS (
    SELECT 
        *,
        LAG(sp500_value) OVER (
            PARTITION BY series_key 
            ORDER BY market_date
        ) AS previous_value,
        
        LAG(market_date) OVER (
            PARTITION BY series_key 
            ORDER BY market_date
        ) AS previous_date
    FROM numeric_sp500
),

sp500_metrics AS (
    SELECT 
        *,
        sp500_value - previous_value AS daily_change,
        
        CASE 
            WHEN previous_value IS NOT NULL AND previous_value != 0 
            THEN (sp500_value - previous_value) / previous_value * 100
            ELSE NULL
        END AS daily_return_pct,
        
        -- Moving averages for technical analysis
        AVG(sp500_value) OVER (
            PARTITION BY series_key 
            ORDER BY market_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20d,
        
        AVG(sp500_value) OVER (
            PARTITION BY series_key 
            ORDER BY market_date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50d,
        
        AVG(sp500_value) OVER (
            PARTITION BY series_key 
            ORDER BY market_date 
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS ma_200d,
        
        -- Volatility (20-day rolling standard deviation of returns)
        STDDEV(
            CASE 
                WHEN previous_value IS NOT NULL AND previous_value != 0 
                THEN (sp500_value - previous_value) / previous_value * 100
                ELSE NULL
            END
        ) OVER (
            PARTITION BY series_key 
            ORDER BY market_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volatility_20d
        
    FROM sp500_changes
),

final_sp500 AS (
    SELECT 
        *,
        -- Market regime classification
        CASE 
            WHEN volatility_20d < 10 THEN 'Low Vol'
            WHEN volatility_20d BETWEEN 10 AND 20 THEN 'Normal Vol'
            WHEN volatility_20d > 20 THEN 'High Vol'
            ELSE 'Unknown'
        END AS volatility_regime,
        
        -- Bull/Bear market indicator (simplified)
        CASE 
            WHEN ma_20d > ma_50d AND ma_50d > ma_200d THEN 'Bullish'
            WHEN ma_20d < ma_50d AND ma_50d < ma_200d THEN 'Bearish'
            ELSE 'Mixed'
        END AS market_trend,
        
        -- Trend vs moving average
        CASE 
            WHEN sp500_value > ma_20d THEN 'Above MA20'
            ELSE 'Below MA20'
        END AS trend_vs_ma20
        
    FROM sp500_metrics
)

SELECT 
    filename,
    data_category,
    batch_collection_timestamp,
    batch_start_date,
    batch_end_date,
    series_key,
    series_id,
    series_name,
    market_date AS observation_date,
    observation_value,
    sp500_value,
    daily_change,
    daily_return_pct,
    ma_20d,
    ma_50d,
    ma_200d,
    volatility_20d,
    trend_vs_ma20,
    volatility_regime,
    market_trend,
    realtime_start,
    realtime_end,
    dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM final_sp500
ORDER BY series_key, market_date