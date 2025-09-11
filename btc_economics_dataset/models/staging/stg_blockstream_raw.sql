{{ config(
    materialized='view',
    description='Raw Blockstream API data from MinIO bronze bucket with expanded data types'
) }}

WITH blocks_data AS (
    SELECT
        filename,
        'block_data' AS blockstream_category,
        header,
        block_id,
        in_best_chain,
        height,
        next_best,
        NULL AS mempool_size,
        NULL AS fee_rate,
        'Blockstream API' AS api_source,
        CURRENT_TIMESTAMP AS collection_timestamp,
        CURRENT_TIMESTAMP AS dbt_created_at
    FROM read_json_auto('s3://bronze/blocks/*.json')
),

fees_data AS (
    SELECT
        filename,
        'fee_data' AS blockstream_category,
        NULL AS header,
        NULL AS block_id,
        NULL AS in_best_chain,
        NULL AS height,
        NULL AS next_best,
        NULL AS mempool_size,
        -- We'll need to see the actual fee structure, but placeholder for now
        * EXCLUDE (filename),
        'Blockstream API' AS api_source,
        CURRENT_TIMESTAMP AS collection_timestamp,
        CURRENT_TIMESTAMP AS dbt_created_at
    FROM read_json_auto('s3://bronze/fees/*.json')
),

mempool_data AS (
    SELECT
        filename,
        'mempool_data' AS blockstream_category,
        NULL AS header,
        NULL AS block_id,
        NULL AS in_best_chain,
        NULL AS height,
        NULL AS next_best,
        -- We'll need to see the actual mempool structure, but placeholder for now
        * EXCLUDE (filename),
        'Blockstream API' AS api_source,
        CURRENT_TIMESTAMP AS collection_timestamp,
        CURRENT_TIMESTAMP AS dbt_created_at
    FROM read_json_auto('s3://bronze/mempool/*.json')
)

-- For now, let's just work with blocks data since we know the structure
SELECT 
    filename,
    blockstream_category,
    header,
    block_id,
    in_best_chain,
    height,
    next_best,
    api_source,
    collection_timestamp,
    dbt_created_at
FROM blocks_data