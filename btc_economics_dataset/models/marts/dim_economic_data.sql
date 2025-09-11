{{ config(
    materialized='table',
    description='Dimension table for economic indicators with metadata and historical context'
) }}

WITH economic_indicators AS (
    SELECT * FROM {{ ref('int_economic_indicators') }}
),

indicator_metadata AS (
    SELECT DISTINCT
        series_id,
        series_name,
        indicator_name,
        indicator_category,
        data_category,
        
        CASE 
            WHEN series_id = 'FEDFUNDS' THEN 'The federal funds rate is the interest rate at which depository institutions trade federal funds with each other overnight.'
            WHEN series_id = 'GS10' THEN 'Market yield on U.S. Treasury securities at 10-year constant maturity.'
            WHEN series_id = 'GS2' THEN 'Market yield on U.S. Treasury securities at 2-year constant maturity.'
            WHEN series_id = 'GS3M' THEN 'Market yield on U.S. Treasury securities at 3-month constant maturity.'
            WHEN series_id = 'DPRIME' THEN 'Bank prime loan rate is the rate charged by banks to their most creditworthy customers.'
            WHEN series_id = 'SOFR' THEN 'Secured Overnight Financing Rate based on transactions in the Treasury repurchase market.'
            WHEN series_id = 'SOFR30DAYAVG' THEN '30-day average of the Secured Overnight Financing Rate.'
            WHEN series_id = 'SOFR90DAYAVG' THEN '90-day average of the Secured Overnight Financing Rate.'
            WHEN series_id = 'SOFR180DAYAVG' THEN '180-day average of the Secured Overnight Financing Rate.'
            WHEN series_id = 'SOFRINDEX' THEN 'SOFR Index provides a compounded measure of SOFR over time.'
            WHEN series_id = 'SP500' THEN 'S&P 500 stock market index measuring the stock performance of 500 large companies.'
            WHEN series_id = 'ICSA' THEN 'Initial claims for unemployment insurance benefits.'
            WHEN series_id = 'CCSA' THEN 'Continued claims for unemployment insurance benefits.'
            WHEN series_id = 'ICSA4W' THEN '4-week moving average of initial claims for unemployment insurance.'
            WHEN series_id = 'UNRATE' THEN 'Unemployment rate as a percentage of the civilian labor force.'
            ELSE 'Economic indicator from FRED database.'
        END AS indicator_description,
        
        CASE 
            WHEN series_id IN ('FEDFUNDS', 'GS10', 'GS2', 'GS3M', 'DPRIME') THEN 'Percent'
            WHEN series_id LIKE 'SOFR%' THEN 'Percent'
            WHEN series_id = 'SP500' THEN 'Index'
            WHEN series_id IN ('ICSA', 'CCSA', 'ICSA4W') THEN 'Number of Claims'
            WHEN series_id = 'UNRATE' THEN 'Percent'
            ELSE 'Unknown'
        END AS unit_of_measure,
        
        CASE 
            WHEN series_id IN ('FEDFUNDS', 'GS10', 'GS2', 'GS3M', 'DPRIME') THEN 'Daily'
            WHEN series_id LIKE 'SOFR%' THEN 'Daily'
            WHEN series_id = 'SP500' THEN 'Daily'
            WHEN series_id IN ('ICSA', 'CCSA', 'ICSA4W') THEN 'Weekly'
            WHEN series_id = 'UNRATE' THEN 'Monthly'
            ELSE 'Unknown'
        END AS frequency,
        
        CASE 
            WHEN indicator_category = 'Interest Rates' THEN 1
            WHEN indicator_category = 'SOFR' THEN 2
            WHEN indicator_category = 'Stock Market' THEN 3
            WHEN indicator_category = 'Employment' THEN 4
            ELSE 99
        END AS category_sort_order
        
    FROM economic_indicators
),

economic_data_with_stats AS (
    SELECT 
        ei.*,
        im.indicator_description,
        im.unit_of_measure,
        im.frequency,
        im.category_sort_order,
        
        AVG(ei.observation_value) OVER (
            PARTITION BY ei.series_id 
            ORDER BY ei.date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS value_30d_avg,
        
        STDDEV(ei.observation_value) OVER (
            PARTITION BY ei.series_id 
            ORDER BY ei.date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS value_30d_stddev,
        
        ei.observation_value - LAG(ei.observation_value, 1) OVER (
            PARTITION BY ei.series_id ORDER BY ei.date
        ) AS daily_change,
        
        ei.observation_value - LAG(ei.observation_value, 7) OVER (
            PARTITION BY ei.series_id ORDER BY ei.date
        ) AS weekly_change,
        
        ei.observation_value - LAG(ei.observation_value, 30) OVER (
            PARTITION BY ei.series_id ORDER BY ei.date
        ) AS monthly_change,
        
        PERCENT_RANK() OVER (
            PARTITION BY ei.series_id ORDER BY ei.observation_value
        ) AS value_percentile_rank,
        
        CASE 
            WHEN ABS(
                ei.observation_value - AVG(ei.observation_value) OVER (PARTITION BY ei.series_id)
            ) > 2 * STDDEV(ei.observation_value) OVER (PARTITION BY ei.series_id)
            THEN TRUE
            ELSE FALSE
        END AS is_outlier
        
    FROM economic_indicators ei
    JOIN indicator_metadata im ON ei.series_id = im.series_id
)

SELECT 
    date,
    series_id,
    series_name,
    indicator_name,
    indicator_category,
    data_category,
    observation_value,
    value_30d_avg,
    value_30d_stddev,
    daily_change,
    weekly_change,
    monthly_change,
    value_percentile_rank,
    indicator_description,
    unit_of_measure,
    frequency,
    category_sort_order,
    is_outlier,
    
    CASE 
        WHEN daily_change > 0 THEN 'Up'
        WHEN daily_change < 0 THEN 'Down'
        ELSE 'Flat'
    END AS daily_trend,
    
    CASE 
        WHEN weekly_change > 0 THEN 'Up'
        WHEN weekly_change < 0 THEN 'Down'
        ELSE 'Flat'
    END AS weekly_trend,
    
    CASE 
        WHEN monthly_change > 0 THEN 'Up'
        WHEN monthly_change < 0 THEN 'Down'
        ELSE 'Flat'
    END AS monthly_trend,
    
    CASE 
        WHEN observation_value IS NULL THEN 'Missing'
        WHEN is_outlier THEN 'Outlier'
        ELSE 'Valid'
    END AS data_quality_flag,
    
    batch_collection_timestamp,
    dbt_created_at
    
FROM economic_data_with_stats
ORDER BY date, category_sort_order, series_name