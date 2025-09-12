-- models/staging/stg_mempool_raw.sql
{{ config(
    materialized='view',
    description='Raw mempool data from Blockstream API'
) }}

SELECT
    filename,
    collection_timestamp,
    
    -- Extract mempool summary data
    raw_mempool_data.count as mempool_tx_count,
    raw_mempool_data.vsize as mempool_vsize,
    raw_mempool_data.total_fee as mempool_total_fee,
    raw_mempool_data.fee_histogram as mempool_fee_histogram,
    
    -- Keep entire raw JSON object
    raw_mempool_data as full_raw_mempool_data,
    
    'Blockstream API' AS api_source,
    CURRENT_TIMESTAMP AS dbt_created_at

FROM read_json_auto('s3://bronze/mempool/*/*/*.json')