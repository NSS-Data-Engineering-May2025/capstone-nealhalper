{{ config(
    materialized='table',
    description='Processed FRED interest rates with yield curve analysis'
) }}

WITH base_rates AS (
    SELECT * 
    FROM {{ ref('stg_fred_raw') }}
    WHERE data_category = 'fred'
      AND series_key IN ('federal_funds_rate', 'treasury_10y', 'treasury_2y', 'treasury_3m', 'prime_rate')
      AND is_valid_observation = TRUE
),

numeric_rates AS (
    SELECT 
        *,
        TRY_CAST(observation_value AS DECIMAL(10,4)) AS rate_value,
        DATE(observation_date) AS rate_date
    FROM base_rates
    WHERE TRY_CAST(observation_value AS DECIMAL(10,4)) IS NOT NULL
),

rate_categories AS (
    SELECT 
        *,
        CASE 
            WHEN series_key = 'federal_funds_rate' THEN 'Monetary Policy'
            WHEN series_key IN ('treasury_10y', 'treasury_2y', 'treasury_3m') THEN 'Treasury Yield'
            WHEN series_key = 'prime_rate' THEN 'Bank Lending'
            ELSE 'Other'
        END AS rate_category,
        
        CASE 
            WHEN series_key = 'treasury_3m' THEN 0.25
            WHEN series_key = 'treasury_2y' THEN 2.0
            WHEN series_key = 'treasury_10y' THEN 10.0
            ELSE NULL
        END AS maturity_years
    FROM numeric_rates
),

rate_changes AS (
    SELECT 
        *,
        LAG(rate_value) OVER (
            PARTITION BY series_key 
            ORDER BY rate_date
        ) AS previous_rate,
        
        LAG(rate_date) OVER (
            PARTITION BY series_key 
            ORDER BY rate_date
        ) AS previous_date
    FROM rate_categories
),

final_metrics AS (
    SELECT 
        *,
        rate_value - previous_rate AS rate_change,
        
        CASE 
            WHEN previous_rate IS NOT NULL AND previous_rate != 0 
            THEN (rate_value - previous_rate) / previous_rate * 100
            ELSE NULL
        END AS rate_change_pct,
        
        -- 30-day moving average
        AVG(rate_value) OVER (
            PARTITION BY series_key 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30d,
        
        -- Volatility (30-day rolling standard deviation)
        STDDEV(rate_value) OVER (
            PARTITION BY series_key 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d
        
    FROM rate_changes
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
    rate_date AS observation_date,
    observation_value,
    rate_value,
    rate_category,
    maturity_years,
    rate_change,
    rate_change_pct,
    ma_30d,
    volatility_30d,
    realtime_start,
    realtime_end,
    dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM final_metrics
ORDER BY series_key, rate_date