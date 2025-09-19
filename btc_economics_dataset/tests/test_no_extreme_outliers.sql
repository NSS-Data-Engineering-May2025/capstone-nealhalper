-- Test for extreme outliers (beyond 3 standard deviations)
WITH stats AS (
    SELECT 
        series_id,
        AVG(numeric_value) as mean_value,
        STDDEV(numeric_value) as std_value
    FROM {{ ref('dim_economic_data') }}
    WHERE numeric_value IS NOT NULL
    GROUP BY series_id
),
outliers AS (
    SELECT 
        d.series_id,
        d.series_name,
        d.date,
        d.numeric_value,
        s.mean_value,
        s.std_value,
        ABS(d.numeric_value - s.mean_value) / NULLIF(s.std_value, 0) as z_score
    FROM {{ ref('dim_economic_data') }} d
    JOIN stats s ON d.series_id = s.series_id
    WHERE s.std_value > 0 
      AND ABS(d.numeric_value - s.mean_value) / s.std_value > 3
)
SELECT * FROM outliers