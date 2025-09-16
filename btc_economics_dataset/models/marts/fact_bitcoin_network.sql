{{ config(
    materialized='table',
    description='Daily Bitcoin network facts combining price, block, mempool, and network statistics'
) }}

WITH bitcoin_prices AS (
    SELECT 
        date,
        price_usd,
        market_cap_usd,
        total_volume_usd,
        daily_return,
        weekly_return,
        monthly_return,
        price_30d_volatility
    FROM {{ ref('fact_bitcoin_prices') }}
    WHERE date IS NOT NULL
),

network_stats AS (
    SELECT 
        observation_date AS date,
        -- Block metrics with null handling
        blocks_mined as daily_block_count,
        avg_block_size,
        avg_block_weight,
        avg_transactions_per_block,
        total_transactions as daily_transaction_count,
        avg_block_fullness_pct,
        block_size_volatility,
        latest_block_height,
        avg_difficulty,
        
        -- Mempool metrics with null handling
        avg_mempool_tx_count as avg_mempool_size,
        max_mempool_tx_count as max_mempool_size,
        min_mempool_tx_count as min_mempool_size,
        avg_pending_fees,
        avg_mempool_fee_rate,
        daily_congestion_level,
        mempool_readings_count,  -- FIXED: Added 's' to match the intermediate model
        
        -- Fee metrics with COALESCE for current vs historical
        COALESCE(avg_current_immediate_fee, avg_historical_immediate_fee) as daily_avg_immediate_fee,
        COALESCE(avg_current_fast_fee, avg_historical_fast_fee) as daily_avg_fast_fee,
        COALESCE(avg_current_standard_fee, avg_historical_standard_fee) as daily_avg_standard_fee,
        COALESCE(avg_current_economy_fee, avg_historical_economy_fee) as daily_avg_economy_fee,
        COALESCE(avg_current_urgency_multiplier, avg_historical_urgency_multiplier) as avg_fee_urgency_multiplier,
        
        -- Data quality metrics
        COALESCE(current_fee_readings, 0) as current_fee_reading_count,
        COALESCE(historical_fee_readings, 0) as historical_fee_reading_count
        
    FROM {{ ref('int_blockstream_network_stats') }}
    WHERE observation_date IS NOT NULL
),

-- Use INNER JOIN to only include dates where we have both price and network data
combined_network_data AS (
    SELECT 
        bp.date,
        
        -- Price data (always available due to INNER JOIN)
        bp.price_usd,
        bp.market_cap_usd,
        bp.total_volume_usd,
        bp.daily_return,
        bp.weekly_return,
        bp.monthly_return,
        bp.price_30d_volatility,
        
        -- Network data (may have nulls for specific metrics)
        ns.daily_block_count,
        ns.avg_block_size,
        ns.avg_block_weight,
        ns.avg_transactions_per_block,
        ns.daily_transaction_count,
        ns.avg_block_fullness_pct,
        ns.block_size_volatility,
        ns.latest_block_height,
        ns.avg_difficulty,
        
        -- Mempool data
        ns.avg_mempool_size,
        ns.max_mempool_size,
        ns.min_mempool_size,
        ns.avg_pending_fees,
        ns.avg_mempool_fee_rate,
        ns.daily_congestion_level,
        ns.mempool_readings_count,  -- FIXED: Added 's' to match the intermediate model
        
        -- Fee data
        ns.daily_avg_immediate_fee,
        ns.daily_avg_fast_fee,
        ns.daily_avg_standard_fee,
        ns.daily_avg_economy_fee,
        ns.avg_fee_urgency_multiplier,
        ns.current_fee_reading_count,
        ns.historical_fee_reading_count
        
    FROM bitcoin_prices bp
    INNER JOIN network_stats ns ON bp.date = ns.date  -- Changed to INNER JOIN
)

SELECT 
    date,
    
    -- Price metrics
    price_usd,
    market_cap_usd,
    total_volume_usd,
    daily_return,
    weekly_return,
    monthly_return,
    price_30d_volatility,
    
    -- Block metrics
    daily_block_count,
    avg_block_size,
    avg_block_weight,
    avg_transactions_per_block,
    daily_transaction_count,
    avg_block_fullness_pct,
    block_size_volatility,
    latest_block_height,
    avg_difficulty,
    
    -- Mempool metrics
    avg_mempool_size,
    max_mempool_size,
    min_mempool_size,
    avg_pending_fees,
    avg_mempool_fee_rate,
    daily_congestion_level,
    
    -- Fee metrics
    daily_avg_immediate_fee,
    daily_avg_fast_fee,
    daily_avg_standard_fee,
    daily_avg_economy_fee,
    avg_fee_urgency_multiplier,
    
    -- Data quality metrics
    current_fee_reading_count,
    historical_fee_reading_count,
    mempool_readings_count,  -- FIXED: Added 's' to match the intermediate model
    
    -- Calculated network metrics with better null handling
    CASE 
        WHEN daily_transaction_count > 0 AND avg_pending_fees > 0
        THEN ROUND(CAST(avg_pending_fees AS DOUBLE) / CAST(daily_transaction_count AS DOUBLE), 2)
        ELSE NULL
    END AS network_avg_fee_per_tx,
    
    CASE 
        WHEN price_usd > 0 AND avg_difficulty > 0
        THEN ROUND(CAST(avg_difficulty AS DOUBLE) / CAST(price_usd AS DOUBLE), 6)
        ELSE NULL
    END AS difficulty_to_price_ratio,
    
    CASE 
        WHEN avg_block_fullness_pct > 90 THEN 'High Usage'
        WHEN avg_block_fullness_pct > 70 THEN 'Moderate Usage'
        WHEN avg_block_fullness_pct > 50 THEN 'Low Usage'
        WHEN avg_block_fullness_pct IS NOT NULL THEN 'Minimal Usage'
        ELSE NULL
    END AS network_usage_level,
    
    CASE 
        WHEN current_fee_reading_count > 0 AND historical_fee_reading_count > 0 AND mempool_readings_count > 0
        THEN 'Complete'
        WHEN current_fee_reading_count > 0 OR historical_fee_reading_count > 0 OR mempool_readings_count > 0
        THEN 'Partial'
        ELSE 'Missing'
    END AS blockstream_data_completeness,
    
    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM combined_network_data
WHERE date IS NOT NULL
ORDER BY date DESC