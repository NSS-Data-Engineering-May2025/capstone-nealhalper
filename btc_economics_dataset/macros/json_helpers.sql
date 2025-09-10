{#
    Macros for handling JSON data in DuckDB
    These macros provide convenient functions for parsing and extracting data from JSON objects
#}

{% macro safe_json_extract(json_column, json_path, default_value="NULL") %}
    {#
        Safely extract a value from JSON with a default fallback
        
        Args:
            json_column: The column containing JSON data
            json_path: The JSON path to extract (e.g., '$.field.subfield')
            default_value: Value to return if extraction fails (default: NULL)
    #}
    COALESCE(
        TRY_CAST(json_extract({{ json_column }}, '{{ json_path }}') AS VARCHAR),
        {{ default_value }}
    )
{% endmacro %}

{% macro safe_json_extract_string(json_column, json_path, default_value="''") %}
    {#
        Safely extract a string value from JSON with string default
        
        Args:
            json_column: The column containing JSON data
            json_path: The JSON path to extract
            default_value: String value to return if extraction fails (default: empty string)
    #}
    COALESCE(
        json_extract_string({{ json_column }}, '{{ json_path }}'),
        {{ default_value }}
    )
{% endmacro %}

{% macro safe_json_extract_number(json_column, json_path, default_value=0) %}
    {#
        Safely extract a numeric value from JSON with numeric default
        
        Args:
            json_column: The column containing JSON data
            json_path: The JSON path to extract
            default_value: Numeric value to return if extraction fails (default: 0)
    #}
    COALESCE(
        TRY_CAST(json_extract({{ json_column }}, '{{ json_path }}') AS DECIMAL(20,8)),
        {{ default_value }}
    )
{% endmacro %}

{% macro extract_coingecko_market_data(raw_data_column) %}
    {#
        Extract common market data fields from CoinGecko API responses
        
        Args:
            raw_data_column: Column containing the raw CoinGecko API response JSON
    #}
    {{ safe_json_extract_number(raw_data_column, '$.market_data.current_price.usd') }} AS price_usd,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.market_cap.usd') }} AS market_cap_usd,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.total_volume.usd') }} AS total_volume_usd,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.high_24h.usd') }} AS high_24h_usd,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.low_24h.usd') }} AS low_24h_usd,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.price_change_24h') }} AS price_change_24h,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.price_change_percentage_24h') }} AS price_change_pct_24h,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.circulating_supply') }} AS circulating_supply,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.total_supply') }} AS total_supply,
    {{ safe_json_extract_number(raw_data_column, '$.market_data.max_supply') }} AS max_supply
{% endmacro %}

{% macro extract_fred_observation_data(observation_column) %}
    {#
        Extract observation data from FRED API responses
        
        Args:
            observation_column: Column containing FRED observation data
    #}
    {{ safe_json_extract_string(observation_column, '$.date') }} AS observation_date,
    {{ safe_json_extract_string(observation_column, '$.value') }} AS observation_value,
    {{ safe_json_extract_string(observation_column, '$.realtime_start') }} AS realtime_start,
    {{ safe_json_extract_string(observation_column, '$.realtime_end') }} AS realtime_end
{% endmacro %}

{% macro parse_batch_info(batch_info_column) %}
    {#
        Extract batch information metadata
        
        Args:
            batch_info_column: Column containing batch metadata JSON
    #}
    {{ safe_json_extract_string(batch_info_column, '$.collection_timestamp') }} AS batch_collection_timestamp,
    {{ safe_json_extract_string(batch_info_column, '$.start_date') }} AS batch_start_date,
    {{ safe_json_extract_string(batch_info_column, '$.end_date') }} AS batch_end_date,
    {{ safe_json_extract_number(batch_info_column, '$.record_count', 0) }} AS batch_record_count,
    {{ safe_json_extract_string(batch_info_column, '$.api_source') }} AS api_source
{% endmacro %}

{% macro unnest_json_array(json_column, array_path) %}
    {#
        Unnest a JSON array into individual rows
        
        Args:
            json_column: The column containing JSON data
            array_path: The path to the array to unnest (e.g., '$.records')
    #}
    unnest(json_extract({{ json_column }}, '{{ array_path }}[*]'))
{% endmacro %}

{% macro validate_json_structure(json_column, required_fields) %}
    {#
        Validate that JSON contains required fields
        
        Args:
            json_column: The column containing JSON data
            required_fields: List of required field paths
    #}
    {% set validation_checks = [] %}
    {% for field in required_fields %}
        {% set check = "json_extract(" ~ json_column ~ ", '" ~ field ~ "') IS NOT NULL" %}
        {% do validation_checks.append(check) %}
    {% endfor %}
    
    CASE 
        WHEN {{ validation_checks | join(' AND ') }} THEN TRUE
        ELSE FALSE
    END AS is_valid_json
{% endmacro %}

{% macro extract_s3_path_components(filename_column) %}
    {#
        Extract components from S3/MinIO file paths
        
        Args:
            filename_column: Column containing the file path
    #}
    regexp_extract({{ filename_column }}, '([^/]+)$', 1) AS filename_only,
    regexp_extract({{ filename_column }}, '^s3://([^/]+)/', 1) AS bucket_name,
    regexp_extract({{ filename_column }}, '^s3://[^/]+/(.+)/[^/]+$', 1) AS folder_path,
    regexp_extract({{ filename_column }}, '(\d{8})', 1) AS date_from_filename
{% endmacro %}

{% macro convert_fred_value(value_column) %}
    {#
        Convert FRED string values to numeric, handling special cases
        
        Args:
            value_column: Column containing FRED observation values as strings
    #}
    CASE 
        WHEN {{ value_column }} = '.' THEN NULL  
        WHEN {{ value_column }} = '' THEN NULL   
        WHEN {{ value_column }} IS NULL THEN NULL
        ELSE TRY_CAST({{ value_column }} AS DECIMAL(15,4))
    END
{% endmacro %}

{% macro generate_surrogate_key(columns) %}
    {#
        Generate a surrogate key by hashing multiple columns
        
        Args:
            columns: List of column names to include in the hash
    #}
    hash({{ columns | join(" || '|' || ") }})
{% endmacro %}

{% macro date_spine(start_date, end_date, date_part='day') %}
    {#
        Generate a date spine between two dates
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            date_part: Date part to increment by ('day', 'week', 'month')
    #}
    WITH RECURSIVE date_spine AS (
        SELECT DATE('{{ start_date }}') AS date_day
        UNION ALL
        SELECT date_day + INTERVAL 1 {{ date_part }}
        FROM date_spine
        WHERE date_day < DATE('{{ end_date }}')
    )
    SELECT date_day FROM date_spine
{% endmacro %}

{% macro pivot_fred_series(source_table, series_list, date_column='date', value_column='observation_value', series_column='series_name') %}
    {#
        Pivot FRED series data from long to wide format
        
        Args:
            source_table: Name of the source table or model
            series_list: List of series names to pivot
            date_column: Name of the date column
            value_column: Name of the value column
            series_column: Name of the series identifier column
    #}
    SELECT 
        {{ date_column }},
        {% for series in series_list %}
        MAX(CASE WHEN {{ series_column }} = '{{ series }}' THEN {{ value_column }} END) AS {{ series }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source_table }}
    WHERE {{ series_column }} IN ({% for series in series_list %}'{{ series }}'{% if not loop.last %}, {% endif %}{% endfor %})
    GROUP BY {{ date_column }}
{% endmacro %}

{#
    Example usage in models:
    
    SELECT {{ safe_json_extract_number('raw_data', '$.market_data.current_price.usd', 0) }} AS price
    
    SELECT {{ extract_coingecko_market_data('raw_api_response') }}

    SELECT *, {{ validate_json_structure('raw_data', ['$.date', '$.value']) }}
    
    {{ pivot_fred_series(ref('stg_fred_raw'), ['FEDFUNDS', 'GS10', 'SOFR']) }}
#}