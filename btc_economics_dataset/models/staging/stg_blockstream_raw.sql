{{ config(
    materialized='view',
    description='Raw Blockstream API data from MinIO bronze bucket with expanded data types'
) }}

WITH raw_blockstream_data AS (
    SELECT
        filename,
        batch_info,
        bitcoin_data
    FROM read_json_auto('s3://blockstream-data/**/*.json')
),

parsed_data AS (
    SELECT
        filename,
        {{ safe_json_extract_string('batch_info', '$.collection_timestamp') }} AS collection_timestamp,
        {{ safe_json_extract_string('batch_info', '$.api_source') }} AS api_source,
        {{ safe_json_extract_string('batch_info', '$.data_type') }} AS data_type,
        {{ safe_json_extract_number('batch_info', '$.record_count') }} AS record_count,
        bitcoin_data,
        CURRENT_TIMESTAMP AS dbt_created_at,
        
        -- Classify data types for routing
        CASE 
            WHEN {{ safe_json_extract_string('batch_info', '$.data_type') }} LIKE '%block%' THEN 'block_data'
            WHEN {{ safe_json_extract_string('batch_info', '$.data_type') }} LIKE '%mempool%' THEN 'mempool_data'
            WHEN {{ safe_json_extract_string('batch_info', '$.data_type') }} LIKE '%fee%' THEN 'fee_data'
            WHEN {{ safe_json_extract_string('batch_info', '$.data_type') }} LIKE '%difficulty%' THEN 'difficulty_data'
            ELSE 'other_data'
        END AS blockstream_category
        
    FROM raw_blockstream_data
    WHERE bitcoin_data IS NOT NULL
)

SELECT *
FROM parsed_data