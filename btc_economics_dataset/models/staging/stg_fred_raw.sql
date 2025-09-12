{{ config(
    materialized='view',
    description='Raw FRED economic data from MinIO - all categories'
) }}

WITH raw_files AS (
    SELECT 
        filename,
        batch_info,    
        series_data   
    FROM read_json_auto('s3://{{ var("minio_bucket_fred") }}/**/*.json') 
),

parsed_batches AS (
    SELECT 
        filename,
        regexp_extract(filename, 'fred-data/([^/]+)/', 1) AS data_category,
        batch_info,
        series_data
    FROM raw_files
    WHERE series_data IS NOT NULL
),

series_with_batch AS (
    SELECT 
        filename,
        data_category,
        {{ safe_json_extract_string('batch_info', '$.collection_timestamp') }} AS batch_collection_timestamp,
        {{ safe_json_extract_string('batch_info', '$.start_date') }} AS batch_start_date,
        {{ safe_json_extract_string('batch_info', '$.end_date') }} AS batch_end_date,
        series_data
    FROM parsed_batches
),

-- Dynamic extraction using DuckDB's json_keys and json_extract
series_keys AS (
    SELECT 
        filename,
        data_category,
        batch_collection_timestamp,
        batch_start_date,
        batch_end_date,
        unnest(json_keys(series_data)) as series_key,
        series_data
    FROM series_with_batch
),

series_with_info AS (
    SELECT 
        filename,
        data_category,
        batch_collection_timestamp,
        batch_start_date,
        batch_end_date,
        series_key,
        json_extract(series_data, '$.' || series_key) as series_info
    FROM series_keys
    WHERE json_extract(series_data, '$.' || series_key) IS NOT NULL
),

-- Extract data points from each series
exploded_observations AS (
    SELECT 
        filename,
        data_category,
        batch_collection_timestamp,
        batch_start_date,
        batch_end_date,
        series_key,
        
        {{ safe_json_extract_string('series_info', '$.series_id') }} AS series_id,
        series_key AS series_name,
        
        {{ safe_json_extract_string('observation', '$.date') }} AS observation_date,
        {{ safe_json_extract_string('observation', '$.value') }} AS observation_value,
        {{ safe_json_extract_string('observation', '$.realtime_start') }} AS realtime_start,
        {{ safe_json_extract_string('observation', '$.realtime_end') }} AS realtime_end
        
    FROM series_with_info,
    LATERAL (
        SELECT unnest(json_extract(series_info, '$.data_points[*]')) AS observation
    )
    WHERE json_extract(series_info, '$.data_points') IS NOT NULL
)

SELECT 
    filename,
    data_category,
    batch_collection_timestamp,
    batch_start_date,
    batch_end_date,
    series_key,
    series_id,
    series_name,
    observation_date,
    observation_value,
    realtime_start,
    realtime_end,

    CASE 
        WHEN observation_date IS NOT NULL 
         AND observation_value IS NOT NULL 
         AND observation_value != '.' 
        THEN TRUE
        ELSE FALSE
    END AS is_valid_observation,

    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM exploded_observations
WHERE observation_date IS NOT NULL
ORDER BY data_category, series_key, observation_date