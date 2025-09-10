{{ config(
    materialized='view',
    description='Cleaned and standardized economic indicators from FRED'
) }}

WITH cleaned_economic AS (
    SELECT 
        data_category,
        series_name,
        series_id,
        observation_date,
        {{ convert_fred_value('observation_value') }} AS observation_value,
        batch_collection_timestamp,
        dbt_created_at
    FROM {{ ref('stg_fred_raw') }}
    WHERE observation_value IS NOT NULL
        AND observation_value != '.' 
        AND observation_date IS NOT NULL
),

standardized_economic AS (
    SELECT 
        data_category,
        series_name,
        series_id,
        DATE(observation_date) AS date,
        observation_value,

        CASE 
            WHEN series_name = 'federal_funds_rate' THEN 'Federal Funds Rate'
            WHEN series_name = 'treasury_10y' THEN '10-Year Treasury'
            WHEN series_name = 'treasury_2y' THEN '2-Year Treasury'
            WHEN series_name = 'sofr_rate' THEN 'SOFR Rate'
            WHEN series_name = 'sp500_index' THEN 'S&P 500 Index'
            WHEN series_name = 'initial_claims' THEN 'Initial Jobless Claims'
            WHEN series_name = 'unemployment_rate' THEN 'Unemployment Rate'
            ELSE CONCAT(
                UPPER(LEFT(REPLACE(series_name, '_', ' '), 1)),
                LOWER(SUBSTRING(REPLACE(series_name, '_', ' '), 2))
            )
        END AS indicator_name,
        
        CASE 
            WHEN data_category = 'interest_rates' THEN 'Interest Rates'
            WHEN data_category = 'sofr' THEN 'SOFR'
            WHEN data_category = 'sp500' THEN 'Stock Market'
            WHEN data_category = 'jobless_claims' THEN 'Employment'
            ELSE CONCAT(
                UPPER(LEFT(REPLACE(data_category, '_', ' '), 1)),
                LOWER(SUBSTRING(REPLACE(data_category, '_', ' '), 2))
            )
        END AS indicator_category,
        
        batch_collection_timestamp,
        dbt_created_at
        
    FROM cleaned_economic
)

SELECT *
FROM standardized_economic
ORDER BY date, indicator_category, series_name