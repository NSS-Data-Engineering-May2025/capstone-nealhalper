{{ config(
    materialized='table',
    description='Bitcoin fee estimates from Blockstream API - current and historical combined'
) }}

WITH current_fees AS (
    SELECT 
        filename,
        collection_timestamp,
        NULL as height,
        NULL as block_hash,
        'current' as fee_data_type,
        
        -- Fee estimates for all confirmation targets
        fee_1_block,
        fee_2_blocks,
        fee_3_blocks,
        fee_4_blocks,
        fee_5_blocks,
        fee_6_blocks,
        fee_7_blocks,
        fee_8_blocks,
        fee_9_blocks,
        fee_10_blocks,
        fee_11_blocks,
        fee_12_blocks,
        fee_13_blocks,
        fee_14_blocks,
        fee_15_blocks,
        fee_16_blocks,
        fee_17_blocks,
        fee_18_blocks,
        fee_19_blocks,
        fee_20_blocks,
        fee_21_blocks,
        fee_22_blocks,
        fee_23_blocks,
        fee_24_blocks,
        fee_25_blocks,
        fee_144_blocks,
        fee_504_blocks,
        fee_1008_blocks,
        
        full_raw_fee_estimates,
        NULL as block_timestamp,
        NULL as block_tx_count,
        NULL as block_size,
        NULL as block_weight,
        api_source,
        dbt_created_at
        
    FROM {{ ref('stg_fees_raw') }}
),

historical_fees AS (
    SELECT 
        filename,
        collection_timestamp,
        height,
        block_hash,
        'historical' as fee_data_type,
        
        -- Fee estimates for all confirmation targets
        fee_1_block,
        fee_2_blocks,
        fee_3_blocks,
        fee_4_blocks,
        fee_5_blocks,
        fee_6_blocks,
        fee_7_blocks,
        fee_8_blocks,
        fee_9_blocks,
        fee_10_blocks,
        fee_11_blocks,
        fee_12_blocks,
        fee_13_blocks,
        fee_14_blocks,
        fee_15_blocks,
        fee_16_blocks,
        fee_17_blocks,
        fee_18_blocks,
        fee_19_blocks,
        fee_20_blocks,
        fee_21_blocks,
        fee_22_blocks,
        fee_23_blocks,
        fee_24_blocks,
        fee_25_blocks,
        fee_144_blocks,
        fee_504_blocks,
        fee_1008_blocks,
        
        full_raw_fee_estimates,
        block_timestamp,
        block_tx_count,
        block_size,
        block_weight,
        api_source,
        dbt_created_at
        
    FROM {{ ref('stg_fees_historical_raw') }}
),

combined_fees AS (
    SELECT * FROM current_fees
    UNION ALL
    SELECT * FROM historical_fees
),

processed_fees AS (
    SELECT
        filename,
        DATE(TRY_CAST(collection_timestamp AS TIMESTAMP)) AS observation_date,
        TRY_CAST(collection_timestamp AS TIMESTAMP) AS observation_timestamp,
        height,
        block_hash,
        fee_data_type,
        
        -- Key fee rates
        fee_1_block as immediate_fee_rate,
        fee_6_blocks as fast_fee_rate,
        fee_144_blocks as standard_fee_rate,
        fee_1008_blocks as economy_fee_rate,
        
        -- All fee estimates preserved
        fee_1_block, fee_2_blocks, fee_3_blocks, fee_4_blocks, fee_5_blocks,
        fee_6_blocks, fee_7_blocks, fee_8_blocks, fee_9_blocks, fee_10_blocks,
        fee_11_blocks, fee_12_blocks, fee_13_blocks, fee_14_blocks, fee_15_blocks,
        fee_16_blocks, fee_17_blocks, fee_18_blocks, fee_19_blocks, fee_20_blocks,
        fee_21_blocks, fee_22_blocks, fee_23_blocks, fee_24_blocks, fee_25_blocks,
        fee_144_blocks, fee_504_blocks, fee_1008_blocks,
        
        -- Fee analysis metrics
        CASE 
            WHEN fee_1_block > 0 AND fee_1008_blocks > 0 
            THEN ROUND(fee_1_block::FLOAT / fee_1008_blocks, 2)
            ELSE NULL
        END AS urgency_multiplier,
        
        CASE 
            WHEN fee_144_blocks > 0 AND fee_1008_blocks > 0 
            THEN ROUND(fee_144_blocks::FLOAT / fee_1008_blocks, 2)
            ELSE NULL
        END AS standard_vs_economy_ratio,
        
        -- Block context (for historical data)
        CASE 
            WHEN block_timestamp IS NOT NULL 
            THEN TO_TIMESTAMP(block_timestamp)
            ELSE NULL
        END AS block_datetime,
        block_tx_count,
        block_size,
        block_weight,
        
        full_raw_fee_estimates,
        api_source,
        dbt_created_at
        
    FROM combined_fees
)

SELECT *
FROM processed_fees
ORDER BY observation_timestamp DESC, height DESC NULLS LAST