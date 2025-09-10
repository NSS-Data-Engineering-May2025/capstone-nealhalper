{{ config(
    materialized='table',
    description='Processed Bitcoin block information from Blockstream API'
) }}

WITH blockstream_raw AS (
    SELECT * FROM {{ ref('stg_blockstream_raw') }}
    WHERE blockstream_category = 'block_data'
),

block_data AS (
    SELECT
        filename,
        collection_timestamp,
        api_source,
        
        {{ safe_json_extract_string('bitcoin_data', '$.id') }} AS block_hash,
        {{ safe_json_extract_number('bitcoin_data', '$.height') }} AS block_height,
        {{ safe_json_extract_string('bitcoin_data', '$.previousblockhash') }} AS previous_block_hash,
        {{ safe_json_extract_string('bitcoin_data', '$.merkleroot') }} AS merkle_root,

        {{ safe_json_extract_number('bitcoin_data', '$.timestamp') }} AS block_timestamp,
        {{ safe_json_extract_number('bitcoin_data', '$.size') }} AS block_size,
        {{ safe_json_extract_number('bitcoin_data', '$.weight') }} AS block_weight,
        {{ safe_json_extract_number('bitcoin_data', '$.tx_count') }} AS transaction_count,

        {{ safe_json_extract_number('bitcoin_data', '$.nonce') }} AS nonce,
        {{ safe_json_extract_number('bitcoin_data', '$.difficulty') }} AS difficulty,
        {{ safe_json_extract_number('bitcoin_data', '$.bits') }} AS bits,
        {{ safe_json_extract_string('bitcoin_data', '$.version') }} AS version,
        
        {{ safe_json_extract_number('bitcoin_data', '$.fee_total') }} AS total_fees,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_per_kb') }} AS avg_fee_per_kb,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_per_vbyte') }} AS avg_fee_per_vbyte,
        
        dbt_created_at
    FROM blockstream_raw
),

processed_blocks AS (
    SELECT
        block_hash,
        block_height,
        previous_block_hash,
        merkle_root,

        TO_TIMESTAMP(block_timestamp) AS block_datetime,
        DATE(TO_TIMESTAMP(block_timestamp)) AS block_date,
        
        block_size,
        block_weight,
        transaction_count,
        nonce,
        difficulty,
        bits,
        version,
        total_fees,
        avg_fee_per_kb,
        avg_fee_per_vbyte,
        
        CASE 
            WHEN block_size > 0 THEN ROUND(transaction_count::FLOAT / block_size * 1000000, 2)
            ELSE NULL
        END AS tx_density_per_mb,
        
        CASE 
            WHEN transaction_count > 0 THEN ROUND(total_fees::FLOAT / transaction_count, 0)
            ELSE NULL
        END AS avg_fee_per_tx,
        
        CASE 
            WHEN block_weight > 0 THEN ROUND(block_weight::FLOAT / 4000000 * 100, 2)
            ELSE NULL
        END AS block_fullness_pct,
        
        collection_timestamp,
        api_source,
        dbt_created_at
        
    FROM block_data
    WHERE block_hash IS NOT NULL
        AND block_height IS NOT NULL
        AND block_timestamp IS NOT NULL
)

SELECT *
FROM processed_blocks
ORDER BY block_height DESC