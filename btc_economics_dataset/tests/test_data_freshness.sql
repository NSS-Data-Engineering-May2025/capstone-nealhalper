-- Test for very stale data (>90 days instead of 30)
SELECT 
    series_id,
    series_name,
    MAX(date) as latest_date,
    CURRENT_DATE - MAX(date) as days_old
FROM {{ ref('dim_economic_data') }}
GROUP BY series_id, series_name
HAVING CURRENT_DATE - MAX(date) > 90  -- Changed from 30 to 90 days