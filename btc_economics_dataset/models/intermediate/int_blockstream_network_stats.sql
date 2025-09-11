{{ config(
    materialized='table',
    description='Bitcoin network statistics including difficulty and fee rates from Blockstream API'
) }}

WITH fee_data AS (
    SELECT * FROM {{ ref('stg_blockstream_raw') }}
    WHERE blockstream_category = 'fee_data'
),

difficulty_data AS (
    SELECT * FROM {{ ref('stg_blockstream_raw') }}
    WHERE blockstream_category = 'difficulty_data'
),

processed_fees AS (
    SELECT
        DATE(TRY_CAST(collection_timestamp AS TIMESTAMP)) AS observation_date,
        TRY_CAST(collection_timestamp AS TIMESTAMP) AS observation_timestamp,

        {{ safe_json_extract_number('bitcoin_data', '$.fast_fee') }} AS fast_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.standard_fee') }} AS standard_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.safe_fee') }} AS safe_fee_rate,
        
        api_source,
        dbt_created_at
        
    FROM fee_data
),

processed_difficulty AS (
    SELECT
        DATE(TRY_CAST(collection_timestamp AS TIMESTAMP)) AS observation_date,
        TRY_CAST(collection_timestamp AS TIMESTAMP) AS observation_timestamp,
    
        {{ safe_json_extract_number('bitcoin_data', '$.difficulty') }} AS current_difficulty,
        {{ safe_json_extract_number('bitcoin_data', '$.difficulty_adjustment') }} AS difficulty_adjustment_pct,
        {{ safe_json_extract_number('bitcoin_data', '$.estimated_next_difficulty') }} AS estimated_next_difficulty,
        {{ safe_json_extract_number('bitcoin_data', '$.blocks_until_adjustment') }} AS blocks_until_adjustment,
        {{ safe_json_extract_number('bitcoin_data', '$.time_until_adjustment') }} AS time_until_adjustment_hours,
        
        {{ safe_json_extract_number('bitcoin_data', '$.hashrate_7d') }} AS hashrate_7d_avg,
        {{ safe_json_extract_number('bitcoin_data', '$.hashrate_30d') }} AS hashrate_30d_avg,
        
        api_source,
        dbt_created_at
        
    FROM difficulty_data
),

daily_aggregated AS (
    SELECT
        COALESCE(pf.observation_date, pd.observation_date) AS observation_date,
        
        AVG(pf.fast_fee_rate) AS daily_avg_fast_fee_rate,
        MAX(pf.fast_fee_rate) AS daily_max_fast_fee_rate,
        MIN(pf.fast_fee_rate) AS daily_min_fast_fee_rate,
        
        AVG(pf.standard_fee_rate) AS daily_avg_standard_fee_rate,
        MAX(pf.standard_fee_rate) AS daily_max_standard_fee_rate,
        MIN(pf.standard_fee_rate) AS daily_min_standard_fee_rate,
        
        AVG(pf.safe_fee_rate) AS daily_avg_safe_fee_rate,
        MAX(pf.safe_fee_rate) AS daily_max_safe_fee_rate,
        MIN(pf.safe_fee_rate) AS daily_min_safe_fee_rate,
        
        AVG(pd.current_difficulty) AS daily_avg_difficulty,
        MAX(pd.current_difficulty) AS daily_max_difficulty,
        MIN(pd.current_difficulty) AS daily_min_difficulty,
        
        AVG(pd.difficulty_adjustment_pct) AS daily_avg_difficulty_adjustment_pct,
        MAX(pd.difficulty_adjustment_pct) AS daily_max_difficulty_adjustment_pct,
        MIN(pd.difficulty_adjustment_pct) AS daily_min_difficulty_adjustment_pct,
        
        AVG(pd.estimated_next_difficulty) AS daily_avg_estimated_next_difficulty,
        MAX(pd.estimated_next_difficulty) AS daily_max_estimated_next_difficulty,
        MIN(pd.estimated_next_difficulty) AS daily_min_estimated_next_difficulty,
    
        MAX(pd.blocks_until_adjustment) AS daily_blocks_until_adjustment,
        MAX(pd.time_until_adjustment_hours) AS daily_time_until_adjustment_hours,

        AVG(pd.hashrate_7d_avg) AS daily_avg_hashrate_7d,
        MAX(pd.hashrate_7d_avg) AS daily_max_hashrate_7d,
        MIN(pd.hashrate_7d_avg) AS daily_min_hashrate_7d,
        
        AVG(pd.hashrate_30d_avg) AS daily_avg_hashrate_30d,
        MAX(pd.hashrate_30d_avg) AS daily_max_hashrate_30d,
        MIN(pd.hashrate_30d_avg) AS daily_min_hashrate_30d,
        
        COUNT(pf.fast_fee_rate) AS daily_fee_reading_count,
        COUNT(pd.current_difficulty) AS daily_difficulty_reading_count,
        
        GREATEST(
            COALESCE(MAX(pf.observation_timestamp), '1900-01-01'::timestamp),
            COALESCE(MAX(pd.observation_timestamp), '1900-01-01'::timestamp)
        ) AS latest_observation_timestamp,
        
        GREATEST(
            COALESCE(MAX(pf.dbt_created_at), '1900-01-01'::timestamp),
            COALESCE(MAX(pd.dbt_created_at), '1900-01-01'::timestamp)
        ) AS dbt_created_at
        
    FROM processed_fees pf
    FULL OUTER JOIN processed_difficulty pd 
        ON pf.observation_date = pd.observation_date
    GROUP BY COALESCE(pf.observation_date, pd.observation_date)
)

SELECT 
    observation_date,
    
    daily_avg_fast_fee_rate,
    daily_max_fast_fee_rate,
    daily_min_fast_fee_rate,
    daily_avg_standard_fee_rate,
    daily_max_standard_fee_rate,
    daily_min_standard_fee_rate,
    daily_avg_safe_fee_rate,
    daily_max_safe_fee_rate,
    daily_min_safe_fee_rate,
    
    daily_avg_difficulty,
    daily_max_difficulty,
    daily_min_difficulty,
    daily_avg_difficulty_adjustment_pct,
    daily_max_difficulty_adjustment_pct,
    daily_min_difficulty_adjustment_pct,

    daily_avg_hashrate_7d,
    daily_max_hashrate_7d,
    daily_min_hashrate_7d,
    daily_avg_hashrate_30d,
    daily_max_hashrate_30d,
    daily_min_hashrate_30d,
    
    daily_blocks_until_adjustment,
    daily_time_until_adjustment_hours,
    daily_avg_estimated_next_difficulty,
    
    daily_fee_reading_count,
    daily_difficulty_reading_count,
    
    CASE 
        WHEN daily_avg_fast_fee_rate > 0 AND daily_avg_safe_fee_rate > 0 
        THEN ROUND(daily_avg_fast_fee_rate::FLOAT / daily_avg_safe_fee_rate, 2)
        ELSE NULL
    END AS avg_fee_urgency_multiplier,
    
    CASE 
        WHEN daily_max_fast_fee_rate > 0 AND daily_min_safe_fee_rate > 0 
        THEN ROUND(daily_max_fast_fee_rate::FLOAT / daily_min_safe_fee_rate, 2)
        ELSE NULL
    END AS peak_fee_urgency_multiplier,
    
    CASE 
        WHEN daily_avg_fast_fee_rate > 0 AND daily_max_fast_fee_rate > daily_min_fast_fee_rate
        THEN ROUND((daily_max_fast_fee_rate - daily_min_fast_fee_rate)::FLOAT / daily_avg_fast_fee_rate * 100, 2)
        ELSE NULL
    END AS daily_fee_volatility_pct,
    
    latest_observation_timestamp,
    dbt_created_at
    
FROM daily_aggregated
ORDER BY observation_date DESC