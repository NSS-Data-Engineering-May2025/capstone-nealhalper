{{ config(
    materialized='table',
    description='Processed Bitcoin block information from Blockstream API'
) }}

WITH blocks_raw AS (
    SELECT * FROM {{ ref('stg_blockstream_raw') }}  -- This is your blocks staging table
),

processed_blocks AS (
    SELECT
        filename,
        height,
        block_hash,
        collection_timestamp,
        
        -- Block identification
        raw_block_id,
        raw_height,
        raw_previousblockhash as previous_block_hash,
        raw_merkle_root as merkle_root,
        raw_version as version,
        
        -- Timestamps
        TO_TIMESTAMP(raw_timestamp) AS block_datetime,
        DATE(TO_TIMESTAMP(raw_timestamp)) AS block_date,
        TO_TIMESTAMP(raw_mediantime) AS median_time,
        
        -- Block size and transaction data
        raw_size as block_size,
        raw_weight as block_weight,
        raw_tx_count as transaction_count,
        
        -- Mining data
        raw_nonce as nonce,
        raw_difficulty as difficulty,
        raw_bits as bits,
        
        -- Calculated metrics
        CASE 
            WHEN raw_size > 0 THEN ROUND(raw_tx_count::FLOAT / raw_size * 1000000, 2)
            ELSE NULL
        END AS transaction_density_per_mb,
        
        CASE 
            WHEN raw_weight > 0 THEN ROUND(raw_weight::FLOAT / 4000000 * 100, 2)
            ELSE NULL
        END AS block_fullness_pct,
        
        CASE 
            WHEN raw_size > 0 THEN ROUND(raw_size::FLOAT / 1024.0, 2)
            ELSE NULL
        END AS block_size_kb,
        
        CASE 
            WHEN raw_size > 0 THEN ROUND(raw_size::FLOAT / (1024.0 * 1024.0), 2)
            ELSE NULL
        END AS block_size_mb,
        
        -- Raw data preservation
        full_raw_block_data,
        api_source,
        dbt_created_at
        
    FROM blocks_raw
    WHERE raw_block_id IS NOT NULL
        AND raw_height IS NOT NULL
        AND raw_timestamp IS NOT NULL
)

SELECT *
FROM processed_blocks
ORDER BY raw_height DESC