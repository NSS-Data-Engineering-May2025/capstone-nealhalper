{{ config(
    materialized='table',
    description='Consolidated economic indicators from all FRED data categories'
) }}

WITH interest_rates AS (
    SELECT 
        series_key,
        series_id,
        series_name,
        observation_date AS date,  -- Fixed: use observation_date not rate_date
        observation_value,
        rate_value AS numeric_value,
        rate_category AS indicator_category,
        'Interest Rates' AS data_category,
        
        -- Standardize indicator names
        CASE 
            WHEN series_key = 'federal_funds_rate' THEN 'Federal Funds Rate'
            WHEN series_key = 'treasury_10y' THEN '10-Year Treasury'
            WHEN series_key = 'treasury_2y' THEN '2-Year Treasury'
            WHEN series_key = 'treasury_3m' THEN '3-Month Treasury'
            WHEN series_key = 'prime_rate' THEN 'Prime Rate'
            ELSE UPPER(SUBSTRING(series_key, 1, 1)) || SUBSTRING(series_key, 2)
        END AS indicator_name,
        
        batch_collection_timestamp,
        dbt_created_at
        
    FROM {{ ref('int_interest_rates') }}
),

sofr_rates AS (
    SELECT 
        series_key,
        series_id,
        series_name,
        observation_date AS date,  -- Fixed: use observation_date not sofr_date
        observation_value,
        sofr_rate AS numeric_value,
        sofr_category AS indicator_category,
        'SOFR' AS data_category,
        
        -- Standardize indicator names
        CASE 
            WHEN series_key = 'sofr_rate' THEN 'SOFR Overnight Rate'
            WHEN series_key = 'sofr_index' THEN 'SOFR Index'
            WHEN series_key = 'sofr_30d_avg' THEN 'SOFR 30-Day Average'
            WHEN series_key = 'sofr_90d_avg' THEN 'SOFR 90-Day Average'
            WHEN series_key = 'sofr_180d_avg' THEN 'SOFR 180-Day Average'
            ELSE UPPER(SUBSTRING(series_key, 1, 1)) || SUBSTRING(series_key, 2)
        END AS indicator_name,
        
        batch_collection_timestamp,
        dbt_created_at
        
    FROM {{ ref('int_sofr') }}
),

jobless_claims AS (
    SELECT 
        series_key,
        series_id,
        series_name,
        observation_date AS date,  -- Fixed: use observation_date not claims_date
        observation_value,
        claims_value AS numeric_value,
        claims_category AS indicator_category,
        'Employment' AS data_category,
        
        -- Standardize indicator names
        CASE 
            WHEN series_key = 'initial_claims' THEN 'Initial Unemployment Claims'
            WHEN series_key = 'continued_claims' THEN 'Continued Unemployment Claims'
            WHEN series_key = 'unemployment_rate' THEN 'Unemployment Rate'
            ELSE UPPER(SUBSTRING(series_key, 1, 1)) || SUBSTRING(series_key, 2)
        END AS indicator_name,
        
        batch_collection_timestamp,
        dbt_created_at
        
    FROM {{ ref('int_jobless_claims') }}
),

sp500_data AS (
    SELECT 
        series_key,
        series_id,
        series_name,
        observation_date AS date,  -- Fixed: use observation_date not market_date
        observation_value,
        sp500_value AS numeric_value,
        'Stock Market' AS indicator_category,
        'Stock Market' AS data_category,
        
        -- Standardize indicator names
        'S&P 500 Index' AS indicator_name,
        
        batch_collection_timestamp,
        dbt_created_at
        
    FROM {{ ref('int_sp500') }}
),

-- Union all indicators
all_indicators AS (
    SELECT * FROM interest_rates
    UNION ALL
    SELECT * FROM sofr_rates
    UNION ALL
    SELECT * FROM jobless_claims
    UNION ALL
    SELECT * FROM sp500_data
)

SELECT 
    series_key,
    series_id,
    series_name,
    date,
    observation_value,
    numeric_value,
    indicator_category,
    data_category,
    indicator_name,
    batch_collection_timestamp,
    dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM all_indicators
WHERE numeric_value IS NOT NULL
ORDER BY data_category, series_key, date