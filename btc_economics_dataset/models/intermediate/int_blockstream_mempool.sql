{{ config(
    materialized='table',
    description='Bitcoin mempool and unconfirmed transaction data from Blockstream API'
) }}

WITH blockstream_raw AS (
    SELECT * FROM {{ ref('stg_blockstream_raw') }}
    WHERE blockstream_category = 'mempool_data'
),

mempool_data AS (
    SELECT
        filename,
        collection_timestamp,
        api_source,
        
        {{ safe_json_extract_number('bitcoin_data', '$.count') }} AS unconfirmed_transaction_count,
        {{ safe_json_extract_number('bitcoin_data', '$.vsize') }} AS total_vsize,
        {{ safe_json_extract_number('bitcoin_data', '$.total_fee') }} AS total_fees_pending,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_histogram') }} AS fee_histogram,
        
        {{ safe_json_extract_number('bitcoin_data', '$.fee_rates.fastest') }} AS fastest_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_rates.halfhour') }} AS halfhour_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_rates.hour') }} AS hour_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_rates.economy') }} AS economy_fee_rate,
        {{ safe_json_extract_number('bitcoin_data', '$.fee_rates.minimum') }} AS minimum_fee_rate,
        
        dbt_created_at
    FROM blockstream_raw
),

processed_mempool AS (
    SELECT
        DATE(TRY_CAST(collection_timestamp AS TIMESTAMP)) AS observation_date,
        TRY_CAST(collection_timestamp AS TIMESTAMP) AS observation_timestamp,
        
        unconfirmed_transaction_count,
        total_vsize,
        total_fees_pending,

        fastest_fee_rate,
        halfhour_fee_rate,
        hour_fee_rate,
        economy_fee_rate,
        minimum_fee_rate,
        
        CASE 
            WHEN unconfirmed_transaction_count > 0 AND total_fees_pending > 0 
            THEN ROUND(total_fees_pending::FLOAT / unconfirmed_transaction_count, 0)
            ELSE NULL
        END AS avg_fee_per_unconfirmed_transaction,
        
        CASE 
            WHEN total_vsize > 0 AND total_fees_pending > 0 
            THEN ROUND(total_fees_pending::FLOAT / total_vsize, 2)
            ELSE NULL
        END AS avg_fee_per_vbyte_mempool,
        
        CASE 
            WHEN fastest_fee_rate > 0 AND economy_fee_rate > 0 
            THEN ROUND(fastest_fee_rate::FLOAT / economy_fee_rate, 2)
            ELSE NULL
        END AS fee_urgency_ratio,
        
        CASE 
            WHEN unconfirmed_transaction_count > 50000 THEN 'High Congestion'
            WHEN unconfirmed_transaction_count > 20000 THEN 'Moderate Congestion'
            WHEN unconfirmed_transaction_count > 5000 THEN 'Low Congestion'
            ELSE 'Clear'
        END AS congestion_level,
        
        api_source,
        dbt_created_at
        
    FROM mempool_data
    WHERE unconfirmed_tx_count IS NOT NULL
)

SELECT *
FROM processed_mempool
ORDER BY observation_timestamp DESC