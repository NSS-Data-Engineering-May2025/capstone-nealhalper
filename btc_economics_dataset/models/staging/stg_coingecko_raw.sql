{{ config(
    materialized='view',
    description='Raw CoinGecko Bitcoin historical data from MinIO'
) }}

WITH raw_files AS (
    SELECT 
        filename,
        batch_info,  
        records      
    FROM read_json_auto('s3://{{ var("minio_bucket_coingecko") }}/*.json')
),

exploded_records AS (
    SELECT 
        filename,
        {{ safe_json_extract_string('batch_info', '$.collection_timestamp') }} AS batch_collection_timestamp,
        {{ safe_json_extract_string('batch_info', '$.start_date') }} AS batch_start_date,
        {{ safe_json_extract_string('batch_info', '$.end_date') }} AS batch_end_date,
        {{ safe_json_extract_number('batch_info', '$.record_count', 0) }} AS batch_record_count,
        
        unnest(records) AS record_data
    FROM raw_files
    WHERE records IS NOT NULL
)

SELECT 
    filename,
    batch_collection_timestamp,
    batch_start_date AS batch_first_date,
    batch_end_date AS batch_last_date,
    batch_record_count,

    {{ safe_json_extract_string('record_data', '$.date') }} AS record_date,
    {{ safe_json_extract_string('record_data', '$.coin_id') }} AS coin_id,
    {{ safe_json_extract_string('record_data', '$.collection_timestamp') }} AS record_collection_timestamp,
    record_data AS raw_api_response,

    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.current_price.usd') }} AS price_usd,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.market_cap.usd') }} AS market_cap_usd,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.total_volume.usd') }} AS total_volume_usd,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.high_24h.usd') }} AS high_24h_usd,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.low_24h.usd') }} AS low_24h_usd,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.price_change_24h') }} AS price_change_24h,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.price_change_percentage_24h') }} AS price_change_pct_24h,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.circulating_supply') }} AS circulating_supply,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.total_supply') }} AS total_supply,
    {{ safe_json_extract_number('record_data', '$.raw_data.market_data.max_supply') }} AS max_supply,
    
    CASE 
        WHEN {{ safe_json_extract_string('record_data', '$.date') }} IS NOT NULL 
         AND {{ safe_json_extract_string('record_data', '$.coin_id') }} IS NOT NULL 
         AND {{ safe_json_extract_number('record_data', '$.raw_data.market_data.current_price.usd') }} IS NOT NULL 
        THEN TRUE
        ELSE FALSE
    END AS is_valid_json,
    
    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM exploded_records