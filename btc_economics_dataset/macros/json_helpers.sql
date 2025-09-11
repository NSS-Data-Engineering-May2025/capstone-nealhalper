{% macro safe_json_extract_string(json_column, json_path, default_value="NULL") %}
    {# Safely extract string from JSON with fallback #}
    COALESCE(
        json_extract_string({{ json_column }}, '{{ json_path }}'),
        {{ default_value }}
    )
{% endmacro %}

{% macro safe_json_extract_number(json_column, json_path, default_value=0) %}
    {# Safely extract number from JSON with type casting and fallback #}
    COALESCE(
        TRY_CAST(json_extract({{ json_column }}, '{{ json_path }}') AS DECIMAL(20,8)),
        {{ default_value }}
    )
{% endmacro %}

{% macro convert_fred_observation_value(value_column) %}
    {# Convert FRED API observation values, handling missing data markers #}
    CASE 
        WHEN {{ value_column }} = '.' THEN NULL    
        WHEN {{ value_column }} = '' THEN NULL   
        WHEN {{ value_column }} IS NULL THEN NULL  
        ELSE TRY_CAST({{ value_column }} AS DECIMAL(15,4))
    END
{% endmacro %}
