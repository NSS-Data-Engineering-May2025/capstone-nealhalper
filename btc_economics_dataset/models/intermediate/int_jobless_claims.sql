{{ config(
    materialized='table',
    description='Processed FRED jobless claims with labor market analysis'
) }}

WITH base_claims AS (
    SELECT * 
    FROM {{ ref('stg_fred_raw') }}
    WHERE data_category = 'fred'
      AND series_key IN ('initial_claims', 'continued_claims', 'unemployment_rate')
      AND is_valid_observation = TRUE
),

numeric_claims AS (
    SELECT 
        *,
        CASE 
            WHEN series_key = 'unemployment_rate' THEN TRY_CAST(observation_value AS DECIMAL(5,2))
            ELSE TRY_CAST(observation_value AS INTEGER) 
        END AS claims_value,
        DATE(observation_date) AS claims_date
    FROM base_claims
    WHERE (
        (series_key = 'unemployment_rate' AND TRY_CAST(observation_value AS DECIMAL(5,2)) IS NOT NULL)
        OR 
        (series_key != 'unemployment_rate' AND TRY_CAST(observation_value AS INTEGER) IS NOT NULL)
    )
),

claims_categories AS (
    SELECT 
        *,
        CASE 
            WHEN series_key = 'initial_claims' THEN 'Weekly Initial Claims'
            WHEN series_key = 'continued_claims' THEN 'Weekly Continued Claims'
            WHEN series_key = 'unemployment_rate' THEN 'Monthly Unemployment Rate'
            ELSE 'Other'
        END AS claims_category,
        
        CASE 
            WHEN series_key IN ('initial_claims', 'continued_claims') THEN 'Weekly'
            WHEN series_key = 'unemployment_rate' THEN 'Monthly'
            ELSE 'Unknown'
        END AS frequency
    FROM numeric_claims
),

claims_changes AS (
    SELECT 
        *,
        LAG(claims_value) OVER (
            PARTITION BY series_key 
            ORDER BY claims_date
        ) AS previous_claims,
        
        LAG(claims_date) OVER (
            PARTITION BY series_key 
            ORDER BY claims_date
        ) AS previous_date
    FROM claims_categories
),

claims_metrics AS (
    SELECT 
        *,
        claims_value - previous_claims AS claims_change,
        
        CASE 
            WHEN previous_claims IS NOT NULL AND previous_claims != 0 
            THEN (claims_value - previous_claims) / previous_claims * 100
            ELSE NULL
        END AS claims_change_pct,
        
        -- 4-week moving average for weekly data
        CASE 
            WHEN frequency = 'Weekly' THEN
                AVG(claims_value) OVER (
                    PARTITION BY series_key 
                    ORDER BY claims_date 
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                )
            ELSE NULL
        END AS ma_4w,
        
        -- 12-week moving average for trend analysis
        CASE 
            WHEN frequency = 'Weekly' THEN
                AVG(claims_value) OVER (
                    PARTITION BY series_key 
                    ORDER BY claims_date 
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                )
            ELSE NULL
        END AS ma_12w,
        
        -- Volatility
        STDDEV(claims_value) OVER (
            PARTITION BY series_key 
            ORDER BY claims_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS volatility_12w,
        
        -- Labor market health indicator
        CASE 
            WHEN series_key = 'initial_claims' AND claims_value < 300000 THEN 'Healthy'
            WHEN series_key = 'initial_claims' AND claims_value BETWEEN 300000 AND 400000 THEN 'Moderate'
            WHEN series_key = 'initial_claims' AND claims_value > 400000 THEN 'Stressed'
            WHEN series_key = 'unemployment_rate' AND claims_value < 4.0 THEN 'Low'
            WHEN series_key = 'unemployment_rate' AND claims_value BETWEEN 4.0 AND 6.0 THEN 'Normal'
            WHEN series_key = 'unemployment_rate' AND claims_value > 6.0 THEN 'High'
            ELSE NULL
        END AS labor_market_status
        
    FROM claims_changes
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
    claims_date AS observation_date,
    observation_value,
    claims_value,
    claims_category,
    frequency,
    claims_change,
    claims_change_pct,
    ma_4w,
    ma_12w,
    volatility_12w,
    labor_market_status,
    realtime_start,
    realtime_end,
    dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM claims_metrics
ORDER BY series_key, claims_date