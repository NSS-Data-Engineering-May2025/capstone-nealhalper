-- Test for data gaps longer than 7 days in daily series
WITH date_gaps AS (
    SELECT 
        series_id,
        series_name,
        date,
        LAG(date) OVER (PARTITION BY series_id ORDER BY date) as prev_date,
        date - LAG(date) OVER (PARTITION BY series_id ORDER BY date) as gap_days
    FROM {{ ref('dim_economic_data') }}
    WHERE frequency = 'Daily'  -- Use the frequency column to identify daily series
)
SELECT 
    series_id,
    series_name,
    prev_date,
    date,
    gap_days
FROM date_gaps
WHERE gap_days > 7
  AND prev_date IS NOT NULL