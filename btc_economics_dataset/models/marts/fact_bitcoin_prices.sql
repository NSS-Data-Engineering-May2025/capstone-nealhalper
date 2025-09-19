{{ config(
    materialized='table',
    description='Daily Bitcoin price facts with economic context'
) }}

WITH bitcoin_daily AS (
    SELECT * FROM {{ ref('int_bitcoin_daily') }}
),

-- Debug: Let's first check what economic data we have
economic_data_available AS (
    SELECT 
        date,
        series_key,
        series_id,
        indicator_name,
        numeric_value,
        data_category
    FROM {{ ref('int_economic_indicators') }}
    WHERE numeric_value IS NOT NULL
),

-- Pivot economic indicators using the correct column names
economic_daily AS (
    SELECT 
        date,
        -- Use series_key (not series_name) and the actual values from your data
        MAX(CASE WHEN series_key = 'federal_funds_rate' THEN numeric_value END) AS fed_funds_rate,
        MAX(CASE WHEN series_key = 'treasury_10y' THEN numeric_value END) AS treasury_10y,
        MAX(CASE WHEN series_key = 'treasury_2y' THEN numeric_value END) AS treasury_2y,
        MAX(CASE WHEN series_key = 'treasury_3m' THEN numeric_value END) AS treasury_3m,
        MAX(CASE WHEN series_key = 'sofr_rate' THEN numeric_value END) AS sofr_rate,
        MAX(CASE WHEN series_key = 'sp500_index' THEN numeric_value END) AS sp500_index,
        MAX(CASE WHEN series_key = 'initial_claims' THEN numeric_value END) AS initial_claims,
        MAX(CASE WHEN series_key = 'continued_claims' THEN numeric_value END) AS continued_claims,
        MAX(CASE WHEN series_key = 'unemployment_rate' THEN numeric_value END) AS unemployment_rate,
        
        -- Count available indicators per date for debugging
        COUNT(DISTINCT series_key) AS available_indicators_count
        
    FROM {{ ref('int_economic_indicators') }}
    WHERE numeric_value IS NOT NULL
    GROUP BY date
),

-- Create a complete date range for proper joining
date_spine AS (
    SELECT DISTINCT date 
    FROM bitcoin_daily
    UNION
    SELECT DISTINCT date 
    FROM economic_daily
),

-- Forward-fill economic data for missing dates (weekends/holidays)
economic_filled AS (
    SELECT 
        ds.date,
        
        -- Use LAST_VALUE with IGNORE NULLS to forward-fill
        LAST_VALUE(ed.fed_funds_rate IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS fed_funds_rate,
        
        LAST_VALUE(ed.treasury_10y IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS treasury_10y,
        
        LAST_VALUE(ed.treasury_2y IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS treasury_2y,
        
        LAST_VALUE(ed.treasury_3m IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS treasury_3m,
        
        LAST_VALUE(ed.sofr_rate IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sofr_rate,
        
        LAST_VALUE(ed.sp500_index IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sp500_index,
        
        LAST_VALUE(ed.initial_claims IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS initial_claims,
        
        LAST_VALUE(ed.continued_claims IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS continued_claims,
        
        LAST_VALUE(ed.unemployment_rate IGNORE NULLS) OVER (
            ORDER BY ds.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS unemployment_rate,
        
        ed.available_indicators_count,
        
        -- Flag if this is original data or forward-filled
        CASE 
            WHEN ed.date IS NOT NULL THEN 'Original'
            ELSE 'Forward-Filled'
        END AS economic_data_source
        
    FROM date_spine ds
    LEFT JOIN economic_daily ed ON ds.date = ed.date
)

SELECT 
    btc.date,
    btc.coin_id,
    btc.price_usd,
    btc.market_cap_usd,
    btc.total_volume_usd,
    btc.max_supply,
    btc.prev_day_price,
    btc.price_change,
    btc.price_change_pct,
    
    -- Economic indicators (forward-filled)
    ef.fed_funds_rate,
    ef.treasury_10y,
    ef.treasury_2y,
    ef.treasury_3m,
    ef.sofr_rate,
    ef.sp500_index,
    ef.initial_claims,
    ef.continued_claims,
    ef.unemployment_rate,
    
    -- Bitcoin returns (CORRECTED)
    CASE 
        WHEN LAG(btc.price_usd, 1) OVER (ORDER BY btc.date) IS NOT NULL 
            AND LAG(btc.price_usd, 1) OVER (ORDER BY btc.date) > 0
        THEN ROUND((btc.price_usd / LAG(btc.price_usd, 1) OVER (ORDER BY btc.date)) - 1, 6)
        ELSE NULL
    END AS daily_return,
    
    CASE 
        WHEN LAG(btc.price_usd, 7) OVER (ORDER BY btc.date) IS NOT NULL 
            AND LAG(btc.price_usd, 7) OVER (ORDER BY btc.date) > 0
        THEN ROUND((btc.price_usd / LAG(btc.price_usd, 7) OVER (ORDER BY btc.date)) - 1, 4)
        ELSE NULL
    END AS weekly_return,
    
    CASE 
        WHEN LAG(btc.price_usd, 30) OVER (ORDER BY btc.date) IS NOT NULL 
            AND LAG(btc.price_usd, 30) OVER (ORDER BY btc.date) > 0
        THEN ROUND((btc.price_usd / LAG(btc.price_usd, 30) OVER (ORDER BY btc.date)) - 1, 4)
        ELSE NULL
    END AS monthly_return,
    
    -- Volatility (CORRECTED - using calculated daily returns)
    STDDEV(
        CASE 
            WHEN LAG(btc.price_usd, 1) OVER (ORDER BY btc.date) IS NOT NULL 
                AND LAG(btc.price_usd, 1) OVER (ORDER BY btc.date) > 0
            THEN (btc.price_usd / LAG(btc.price_usd, 1) OVER (ORDER BY btc.date)) - 1
            ELSE NULL
        END
    ) OVER (
        ORDER BY btc.date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS price_30d_volatility,
    
    -- Data quality indicators
    ef.available_indicators_count,
    ef.economic_data_source,
    
    CASE 
        WHEN ef.fed_funds_rate IS NOT NULL THEN 'Available'
        ELSE 'Missing'
    END AS economic_data_status,
    
    -- Calculate yield curve spread (10Y - 2Y)
    CASE 
        WHEN ef.treasury_10y IS NOT NULL AND ef.treasury_2y IS NOT NULL 
        THEN ef.treasury_10y - ef.treasury_2y
    END AS yield_curve_spread,
    
    btc.is_valid_price,
    btc.record_collection_timestamp,
    btc.batch_collection_timestamp,
    btc.dbt_created_at,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM bitcoin_daily btc
LEFT JOIN economic_filled ef ON btc.date = ef.date
ORDER BY btc.date