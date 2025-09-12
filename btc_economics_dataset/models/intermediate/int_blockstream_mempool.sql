{{ config(
    materialized='table',
    description='Bitcoin mempool and unconfirmed transaction data from Blockstream API'
) }}

WITH mempool_raw AS (
    SELECT * FROM {{ ref('stg_mempool_raw') }}
),

processed_mempool AS (
    SELECT
        filename,
        collection_timestamp,
        DATE(TRY_CAST(collection_timestamp AS TIMESTAMP)) AS observation_date,
        TRY_CAST(collection_timestamp AS TIMESTAMP) AS observation_timestamp,
        
        -- Direct mempool metrics
        mempool_tx_count as unconfirmed_transaction_count,
        mempool_vsize as total_vsize,
        mempool_total_fee as total_fees_pending,
        mempool_fee_histogram as fee_histogram,
        
        -- Calculated metrics
        CASE 
            WHEN mempool_tx_count > 0 AND mempool_total_fee > 0 
            THEN ROUND(mempool_total_fee::FLOAT / mempool_tx_count, 0)
            ELSE NULL
        END AS avg_fee_per_unconfirmed_transaction,
        
        CASE 
            WHEN mempool_vsize > 0 AND mempool_total_fee > 0 
            THEN ROUND(mempool_total_fee::FLOAT / mempool_vsize, 2)
            ELSE NULL
        END AS avg_fee_per_vbyte_mempool,
        
        CASE 
            WHEN mempool_vsize > 0 
            THEN ROUND(mempool_vsize::FLOAT / 1024.0, 2)
            ELSE NULL
        END AS mempool_vsize_kb,
        
        CASE 
            WHEN mempool_vsize > 0 
            THEN ROUND(mempool_vsize::FLOAT / (1024.0 * 1024.0), 2)
            ELSE NULL
        END AS mempool_vsize_mb,
        
        -- Congestion level classification
        CASE 
            WHEN mempool_tx_count > 50000 THEN 'High Congestion'
            WHEN mempool_tx_count > 20000 THEN 'Moderate Congestion'
            WHEN mempool_tx_count > 5000 THEN 'Low Congestion'
            ELSE 'Clear'
        END AS congestion_level,
        
        -- Raw data preservation
        full_raw_mempool_data,
        api_source,
        dbt_created_at
        
    FROM mempool_raw
    WHERE mempool_tx_count IS NOT NULL
)

SELECT *
FROM processed_mempool
ORDER BY observation_timestamp DESC