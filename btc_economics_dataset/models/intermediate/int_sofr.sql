{{ config(
    materialized='table',
    description='Processed FRED SOFR data with money market analysis'
) }}

WITH base_sofr AS (
    SELECT * 
    FROM {{ ref('stg_fred_raw') }}
    WHERE data_category = 'fred'
      AND series_key IN ('sofr_rate', 'sofr_index', 'sofr_30d_avg', 'sofr_90d_avg', 'sofr_180d_avg')
      AND is_valid_observation = TRUE
),

numeric_sofr AS (
    SELECT 
        *,
        TRY_CAST(observation_value AS DECIMAL(10,6)) AS sofr_rate,
        DATE(observation_date) AS sofr_date
    FROM base_sofr
    WHERE TRY_CAST(observation_value AS DECIMAL(10,6)) IS NOT NULL
),

sofr_categories AS (
    SELECT 
        *,
        CASE 
            WHEN series_key = 'sofr_rate' THEN 'Overnight Rate'
            WHEN series_key = 'sofr_index' THEN 'Index'
            WHEN series_key = 'sofr_30d_avg' THEN '30-Day Average'
            WHEN series_key = 'sofr_90d_avg' THEN '90-Day Average'
            WHEN series_key = 'sofr_180d_avg' THEN '180-Day Average'
            ELSE 'Other'
        END AS sofr_category,
        
        CASE 
            WHEN series_key = 'sofr_rate' THEN 'Daily'
            WHEN series_key = 'sofr_index' THEN 'Daily Index'
            ELSE 'Average'
        END AS rate_type
    FROM numeric_sofr
),

sofr_changes AS (
    SELECT 
        *,
        LAG(sofr_rate) OVER (
            PARTITION BY series_key 
            ORDER BY sofr_date
        ) AS previous_rate,
        
        LAG(sofr_date) OVER (
            PARTITION BY series_key 
            ORDER BY sofr_date
        ) AS previous_date
    FROM sofr_categories
),

sofr_metrics AS (
    SELECT 
        *,
        sofr_rate - previous_rate AS rate_change_bps,
        (sofr_rate - previous_rate) * 100 AS rate_change_basis_points,
        
        -- 30-day moving average
        AVG(sofr_rate) OVER (
            PARTITION BY series_key 
            ORDER BY sofr_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30d,
        
        -- Volatility in basis points
        STDDEV(sofr_rate) OVER (
            PARTITION BY series_key 
            ORDER BY sofr_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * 100 AS volatility_30d_bps,
        
        -- Money market stress indicator
        CASE 
            WHEN ABS(sofr_rate - LAG(sofr_rate, 5) OVER (
                PARTITION BY series_key ORDER BY sofr_date
            )) > 0.25 THEN 'High Stress'
            WHEN ABS(sofr_rate - LAG(sofr_rate, 5) OVER (
                PARTITION BY series_key ORDER BY sofr_date
            )) > 0.10 THEN 'Moderate Stress'
            ELSE 'Normal'
        END AS money_market_stress,
        
        -- Rate environment classification
        CASE 
            WHEN sofr_rate < 1.0 THEN 'Ultra Low'
            WHEN sofr_rate BETWEEN 1.0 AND 3.0 THEN 'Low'
            WHEN sofr_rate BETWEEN 3.0 AND 5.0 THEN 'Moderate'
            WHEN sofr_rate > 5.0 THEN 'High'
            ELSE 'Unknown'
        END AS rate_environment
        
    FROM sofr_changes
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
    sofr_date AS observation_date,
    observation_value,
    sofr_rate,
    sofr_category,
    rate_type,
    rate_change_bps,
    rate_change_basis_points,
    ma_30d,
    volatility_30d_bps,
    money_market_stress,
    rate_environment,
    realtime_start,
    realtime_end,
    dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM sofr_metrics
ORDER BY series_key, sofr_date