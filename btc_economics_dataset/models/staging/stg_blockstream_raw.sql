{{ config(
    materialized='table',
    unique_key='height',
    description='Raw Bitcoin block data from Blockstream API'
) }}

WITH filtered_files AS (
    SELECT
        filename,
        height,
        block_hash,
        collection_timestamp,
        
        -- All raw_block_data fields with proper type casting and null checks
        raw_block_data.id as raw_block_id,
        TRY_CAST(raw_block_data.height AS INTEGER) as raw_height,
        TRY_CAST(raw_block_data.version AS INTEGER) as raw_version,
        TRY_CAST(raw_block_data.timestamp AS BIGINT) as raw_timestamp,
        TRY_CAST(raw_block_data.tx_count AS INTEGER) as raw_tx_count,
        TRY_CAST(raw_block_data.size AS BIGINT) as raw_size,
        TRY_CAST(raw_block_data.weight AS BIGINT) as raw_weight,
        raw_block_data.merkle_root as raw_merkle_root,
        raw_block_data.previousblockhash as raw_previousblockhash,
        TRY_CAST(raw_block_data.mediantime AS BIGINT) as raw_mediantime,
        TRY_CAST(raw_block_data.nonce AS BIGINT) as raw_nonce,
        raw_block_data.bits as raw_bits,
        TRY_CAST(raw_block_data.difficulty AS DOUBLE) as raw_difficulty,
        
        -- Keep entire raw JSON object
        raw_block_data as full_raw_block_data,
        
        'Blockstream API' AS api_source,
        CURRENT_TIMESTAMP AS dbt_created_at

    FROM read_json_auto(
        's3://bronze/blocks/height_91*.json',  -- Reverted to specific pattern
        union_by_name=true,
        ignore_errors=true,
        maximum_depth=2,                 -- Reduced depth
        sample_size=5,                   -- Conservative sample size
        maximum_object_size=102400       -- Back to 100KB limit
    )
    
    WHERE raw_block_data IS NOT NULL
        AND raw_block_data.height IS NOT NULL
        AND raw_block_data.timestamp IS NOT NULL
        AND TRY_CAST(raw_block_data.height AS INTEGER) > 0
        AND TRY_CAST(raw_block_data.tx_count AS INTEGER) > 0
        AND TRY_CAST(raw_block_data.size AS BIGINT) > 0
        
        -- Back to 30 day window
        AND TRY_CAST(raw_block_data.timestamp AS BIGINT) > (EXTRACT(EPOCH FROM CURRENT_DATE - INTERVAL 30 DAY))
    
    ORDER BY TRY_CAST(raw_block_data.height AS INTEGER) DESC
    LIMIT 1000  -- Conservative limit
)

SELECT 
    filename,
    raw_height as height,
    block_hash,
    collection_timestamp,
    raw_block_id,
    raw_height,
    raw_version,
    raw_timestamp,
    raw_tx_count,
    raw_size,
    raw_weight,
    raw_merkle_root,
    raw_previousblockhash,
    raw_mediantime,
    raw_nonce,
    raw_bits,
    raw_difficulty,
    full_raw_block_data,
    api_source,
    dbt_created_at

FROM filtered_files
WHERE raw_height IS NOT NULL
    AND raw_timestamp IS NOT NULL
    AND raw_tx_count IS NOT NULL
    AND raw_size IS NOT NULL
    AND raw_difficulty IS NOT NULL
ORDER BY height DESC