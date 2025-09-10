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
        
        'fee_rates' AS data_type,
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
        
        'difficulty_stats' AS data_type,
        api_source,
        dbt_created_at
        
    FROM difficulty_data
),

combined_network_stats AS (
    SELECT
        observation_date,
        observation_timestamp,
        fast_fee_rate,
        standard_fee_rate,
        safe_fee_rate,
        NULL AS current_difficulty,
        NULL AS difficulty_adjustment_pct,
        NULL AS estimated_next_difficulty,
        NULL AS blocks_until_adjustment,
        NULL AS time_until_adjustment_hours,
        NULL AS hashrate_7d_avg,
        NULL AS hashrate_30d_avg,
        data_type,
        api_source,
        dbt_created_at
    FROM processed_fees
    
    UNION ALL
    
    SELECT
        observation_date,
        observation_timestamp,
        NULL AS fast_fee_rate,
        NULL AS standard_fee_rate,
        NULL AS safe_fee_rate,
        current_difficulty,
        difficulty_adjustment_pct,
        estimated_next_difficulty,
        blocks_until_adjustment,
        time_until_adjustment_hours,
        hashrate_7d_avg,
        hashrate_30d_avg,
        data_type,
        api_source,
        dbt_created_at
    FROM processed_difficulty
),

daily_aggregated AS (
    SELECT
        observation_date,
        
        MAX(CASE WHEN data_type = 'fee_rates' THEN fast_fee_rate END) AS daily_fast_fee_rate,
        MAX(CASE WHEN data_type = 'fee_rates' THEN standard_fee_rate END) AS daily_standard_fee_rate,
        MAX(CASE WHEN data_type = 'fee_rates' THEN safe_fee_rate END) AS daily_safe_fee_rate,
        
        MAX(CASE WHEN data_type = 'difficulty_stats' THEN current_difficulty END) AS daily_difficulty,
        MAX(CASE WHEN data_type = 'difficulty_stats' THEN difficulty_adjustment_pct END) AS daily_difficulty_adjustment_pct,
        MAX(CASE WHEN data_type = 'difficulty_stats' THEN hashrate_7d_avg END) AS daily_hashrate_7d_avg,
        MAX(CASE WHEN data_type = 'difficulty_stats' THEN hashrate_30d_avg END) AS daily_hashrate_30d_avg,
        
        MAX(observation_timestamp) AS latest_observation_timestamp,
        MAX(dbt_created_at) AS dbt_created_at
        
    FROM combined_network_stats
    GROUP BY observation_date
)

SELECT 
    observation_date,
    daily_fast_fee_rate,
    daily_standard_fee_rate,
    daily_safe_fee_rate,
    daily_difficulty,
    daily_difficulty_adjustment_pct,
    daily_hashrate_7d_avg,
    daily_hashrate_30d_avg,
    
    CASE 
        WHEN daily_fast_fee_rate > 0 AND daily_safe_fee_rate > 0 
        THEN ROUND(daily_fast_fee_rate::FLOAT / daily_safe_fee_rate, 2)
        ELSE NULL
    END AS fee_urgency_multiplier,
    
    latest_observation_timestamp,
    dbt_created_at
    
FROM daily_aggregated
ORDER BY observation_date DESC