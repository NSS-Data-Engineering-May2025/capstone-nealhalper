{{ config(
    materialized='view',
    description='Staged CoinGecko raw data with improved JSON parsing'
) }}

WITH raw_files AS (
    SELECT 
        batch_info,
        records AS raw_records_array
    FROM {{ source('coingecko', 'coingecko_raw') }}
    WHERE records IS NOT NULL
        AND LENGTH(records) > 10
),

-- Parse the records as JSON and extract array elements using JSON_EXTRACT_ARRAY
unnested_records AS (
    SELECT 
        batch_info,
        raw_records_array,
        -- Parse the records array as JSON
        TRY_CAST(raw_records_array AS JSON) AS records_json
    FROM raw_files
    WHERE TRY_CAST(raw_records_array AS JSON) IS NOT NULL
),

-- Use a different approach to unnest JSON arrays in DuckDB
individual_records AS (
    SELECT 
        u.batch_info,
        u.raw_records_array,
        -- Use array indexing to access individual records
        json_extract(u.records_json, '$[' || i || ']') AS record_json,
        i AS record_index
    FROM unnested_records u,
    -- Generate series based on the array length with explicit type casting
    GENERATE_SERIES(0::BIGINT, (json_array_length(u.records_json) - 1)::BIGINT) AS t(i)
    WHERE json_array_length(u.records_json) > 0
),

-- Parse each individual record
parsed_records AS (
    SELECT 
        batch_info,
        record_json,
        record_index,
        
        -- Extract fields from each individual record using json_extract_string
        json_extract_string(record_json, '$.date') AS record_date_raw,
        json_extract_string(record_json, '$.coin_id') AS coin_id,
        json_extract_string(record_json, '$.collection_timestamp') AS collection_timestamp,
        json_extract(record_json, '$.raw_data') AS raw_data_json,
        
        -- Validate JSON parsing
        CASE 
            WHEN json_extract(record_json, '$.raw_data') IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_valid_json
        
    FROM individual_records
    WHERE record_json IS NOT NULL
),

-- Extract Bitcoin data from the raw_data nested structure
bitcoin_extracted AS (
    SELECT 
        p.batch_info,
        p.record_date_raw,
        p.coin_id,
        p.collection_timestamp,
        p.is_valid_json,
        p.record_index,
        
        -- Extract basic coin info from raw_data using json_extract_string
        json_extract_string(p.raw_data_json, '$.id') AS api_coin_id,
        json_extract_string(p.raw_data_json, '$.name') AS coin_name,
        json_extract_string(p.raw_data_json, '$.symbol') AS coin_symbol,
        
        -- Extract market data object
        json_extract(p.raw_data_json, '$.market_data') AS market_data,
        
        -- For debugging: show the raw_data structure
        p.raw_data_json AS debug_raw_data_json
        
    FROM parsed_records p
    WHERE p.is_valid_json = TRUE
        AND p.coin_id = 'bitcoin'  -- Filter for Bitcoin only
        AND json_extract_string(p.raw_data_json, '$.id') = 'bitcoin'  -- Double-check the API data
),

-- Extract all price and market data
final_extraction AS (
    SELECT 
        b.batch_info,
        b.record_date_raw,
        b.coin_id,
        b.collection_timestamp,
        b.api_coin_id,
        b.coin_name,
        b.coin_symbol,
        b.is_valid_json,
        b.record_index,
        
        -- Extract USD prices from current_price object using json_extract_string
        json_extract_string(b.market_data, '$.current_price.usd') AS price_usd,
        
        -- Extract market cap in USD
        json_extract_string(b.market_data, '$.market_cap.usd') AS market_cap_usd,
        
        -- Extract volume in USD  
        json_extract_string(b.market_data, '$.total_volume.usd') AS total_volume_usd,
        
        -- Supply data - only max_supply since it's known for Bitcoin
        21000000.0 AS max_supply,  -- Bitcoin's known max supply
        
        -- For debugging: show the market_data structure
        b.market_data AS debug_market_data
        
    FROM bitcoin_extracted b
    WHERE b.market_data IS NOT NULL
)

SELECT 
    -- Date handling - handle multiple date formats
    CASE 
        WHEN f.record_date_raw IS NULL THEN NULL
        -- Handle YYYY-MM-DD format (like "2024-10-30")
        WHEN f.record_date_raw ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN TRY_CAST(f.record_date_raw AS DATE)
        -- Handle DD-MM-YYYY format (like "10-09-2024")
        WHEN f.record_date_raw ~ '^\d{1,2}-\d{1,2}-\d{4}$' 
        THEN TRY_CAST(STRPTIME(f.record_date_raw, '%d-%m-%Y') AS DATE)
        -- Fallback: try direct casting first, then try the DD-MM-YYYY format
        ELSE COALESCE(
            TRY_CAST(f.record_date_raw AS DATE),
            TRY_CAST(STRPTIME(f.record_date_raw, '%d-%m-%Y') AS DATE),
            TRY_CAST(SUBSTRING(f.batch_info::text, 1, 10) AS DATE)
        )
    END AS record_date,
    
    f.coin_id,
    f.api_coin_id,
    f.coin_name,
    f.coin_symbol,
    
    -- Clean price data with validation
    CASE 
        WHEN f.price_usd IS NULL OR f.price_usd = '' OR f.price_usd = 'null' THEN NULL
        WHEN TRY_CAST(f.price_usd AS DECIMAL(15,2)) <= 0 THEN NULL
        ELSE TRY_CAST(f.price_usd AS DECIMAL(15,2))
    END AS price_usd,
    
    CASE 
        WHEN f.market_cap_usd IS NULL OR f.market_cap_usd = '' OR f.market_cap_usd = 'null' THEN NULL
        WHEN TRY_CAST(f.market_cap_usd AS DECIMAL(20,2)) <= 0 THEN NULL
        ELSE TRY_CAST(f.market_cap_usd AS DECIMAL(20,2))
    END AS market_cap_usd,
    
    CASE 
        WHEN f.total_volume_usd IS NULL OR f.total_volume_usd = '' OR f.total_volume_usd = 'null' THEN NULL
        WHEN TRY_CAST(f.total_volume_usd AS DECIMAL(20,2)) <= 0 THEN NULL
        ELSE TRY_CAST(f.total_volume_usd AS DECIMAL(20,2))
    END AS total_volume_usd,
    
    -- Only keep max_supply since it's a known constant for Bitcoin
    f.max_supply,
    
    -- Metadata
    f.is_valid_json,
    TRY_CAST(f.collection_timestamp AS TIMESTAMP) AS record_collection_timestamp,
    DATE_PART('hour', TRY_CAST(f.collection_timestamp AS TIMESTAMP)) AS collection_hour,
    
    -- For debugging (remove in production)
    f.batch_info AS debug_batch_info,
    f.record_date_raw AS debug_record_date_raw,
    f.record_index AS debug_record_index,
    LEFT(f.debug_market_data::text, 200) AS debug_market_data_sample,
    
    -- Data completeness scoring for available fields only
    CASE 
        WHEN f.price_usd IS NOT NULL THEN 1 ELSE 0 
    END +
    CASE 
        WHEN f.market_cap_usd IS NOT NULL THEN 1 ELSE 0 
    END +
    CASE 
        WHEN f.total_volume_usd IS NOT NULL THEN 1 ELSE 0 
    END AS data_completeness_score,
    
    TRY_CAST(f.collection_timestamp AS TIMESTAMP) AS batch_collection_timestamp,
    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM final_extraction f
WHERE f.coin_id = 'bitcoin'
ORDER BY record_date DESC