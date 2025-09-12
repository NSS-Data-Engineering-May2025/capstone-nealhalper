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
),

block_stats AS (
    SELECT 
        block_date AS date,
        COUNT(*) AS daily_block_count,
        AVG(block_size) AS avg_block_size,
        AVG(block_weight) AS avg_block_weight,
        AVG(transaction_count) AS avg_transactions_per_block,
        SUM(transaction_count) AS daily_transaction_count,
        AVG(transaction_density_per_mb) AS avg_transaction_density,
        AVG(block_fullness_pct) AS avg_block_fullness_pct,
        STDDEV(block_size) AS block_size_volatility,
        MAX(height) AS latest_block_height,
        AVG(difficulty) AS avg_difficulty
        
    FROM {{ ref('int_blockstream_blocks') }}
    GROUP BY block_date
),

mempool_stats AS (
    SELECT 
        observation_date AS date,
        AVG(unconfirmed_transaction_count) AS avg_mempool_size,
        MAX(unconfirmed_transaction_count) AS max_mempool_size,
        MIN(unconfirmed_transaction_count) AS min_mempool_size,
        AVG(total_fees_pending) AS avg_pending_fees,
        AVG(avg_fee_per_vbyte_mempool) AS avg_mempool_fee_rate,
        COUNT(*) AS mempool_reading_count,
        
        -- Get the most common congestion level for the day
        MODE() WITHIN GROUP (ORDER BY congestion_level) AS daily_congestion_level
        
    FROM {{ ref('int_blockstream_mempool') }}
    GROUP BY observation_date
),

fee_stats_pivot AS (
    SELECT 
        observation_date AS date,
        -- Pivot current vs historical fees into separate columns
        MAX(CASE WHEN fee_data_type = 'current' THEN immediate_fee_rate END) as current_immediate_fee,
        MAX(CASE WHEN fee_data_type = 'historical' THEN immediate_fee_rate END) as historical_immediate_fee,
        MAX(CASE WHEN fee_data_type = 'current' THEN fast_fee_rate END) as current_fast_fee,
        MAX(CASE WHEN fee_data_type = 'historical' THEN fast_fee_rate END) as historical_fast_fee,
        MAX(CASE WHEN fee_data_type = 'current' THEN standard_fee_rate END) as current_standard_fee,
        MAX(CASE WHEN fee_data_type = 'historical' THEN standard_fee_rate END) as historical_standard_fee,
        MAX(CASE WHEN fee_data_type = 'current' THEN economy_fee_rate END) as current_economy_fee,
        MAX(CASE WHEN fee_data_type = 'historical' THEN economy_fee_rate END) as historical_economy_fee,
        MAX(CASE WHEN fee_data_type = 'current' THEN urgency_multiplier END) as current_urgency_multiplier,
        MAX(CASE WHEN fee_data_type = 'historical' THEN urgency_multiplier END) as historical_urgency_multiplier,
        
        -- Count readings by type
        COUNT(CASE WHEN fee_data_type = 'current' THEN 1 END) as current_fee_reading_count,
        COUNT(CASE WHEN fee_data_type = 'historical' THEN 1 END) as historical_fee_reading_count
        
    FROM {{ ref('int_blockstream_fees') }}
    GROUP BY observation_date  -- This ensures one row per date
),

network_stats AS (
    SELECT 
        observation_date AS date,
        avg_difficulty,
        avg_block_fullness_pct,
        avg_current_immediate_fee,
        avg_current_fast_fee,
        avg_current_standard_fee,
        avg_current_economy_fee,
        avg_current_urgency_multiplier,
        avg_historical_immediate_fee,
        avg_historical_fast_fee,
        avg_historical_standard_fee,
        avg_historical_economy_fee,
        avg_historical_urgency_multiplier,
        avg_mempool_tx_count,
        max_mempool_tx_count,
        min_mempool_tx_count,
        avg_pending_fees,
        avg_mempool_fee_rate,
        current_fee_readings,
        historical_fee_readings,
        mempool_readings_count
    FROM {{ ref('int_blockstream_network_stats') }}
),

combined_network_data AS (
    SELECT 
        COALESCE(bp.date, bs.date, ms.date, fps.date) AS date,
        
        -- Price data
        bp.price_usd,
        bp.market_cap_usd,
        bp.total_volume_usd,
        bp.daily_return,
        bp.weekly_return,
        bp.monthly_return,
        bp.price_30d_volatility,
        
        -- Block data
        bs.daily_block_count,
        bs.avg_block_size,
        bs.avg_block_weight,
        bs.avg_transactions_per_block,
        bs.daily_transaction_count,
        bs.avg_transaction_density,
        bs.avg_block_fullness_pct,
        bs.block_size_volatility,
        bs.latest_block_height,
        bs.avg_difficulty,
        
        -- Mempool data
        ms.avg_mempool_size,
        ms.max_mempool_size,
        ms.min_mempool_size,
        ms.avg_pending_fees,
        ms.avg_mempool_fee_rate,
        ms.daily_congestion_level,
        ms.mempool_reading_count,
        
        -- Fee data (now properly aggregated into single row per date)
        fps.current_immediate_fee,
        fps.historical_immediate_fee,
        fps.current_fast_fee,
        fps.historical_fast_fee,
        fps.current_standard_fee,
        fps.historical_standard_fee,
        fps.current_economy_fee,
        fps.historical_economy_fee,
        fps.current_urgency_multiplier,
        fps.historical_urgency_multiplier,
        fps.current_fee_reading_count,
        fps.historical_fee_reading_count
        
    FROM bitcoin_prices bp
    FULL OUTER JOIN block_stats bs ON bp.date = bs.date
    FULL OUTER JOIN mempool_stats ms ON bp.date = ms.date
    FULL OUTER JOIN fee_stats_pivot fps ON bp.date = fps.date  -- Single JOIN instead of two
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
    avg_transaction_density,
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
    
    -- Fee metrics (prioritize current fees, fallback to historical)
    COALESCE(current_immediate_fee, historical_immediate_fee) AS daily_avg_immediate_fee,
    COALESCE(current_fast_fee, historical_fast_fee) AS daily_avg_fast_fee,
    COALESCE(current_standard_fee, historical_standard_fee) AS daily_avg_standard_fee,
    COALESCE(current_economy_fee, historical_economy_fee) AS daily_avg_economy_fee,
    COALESCE(current_urgency_multiplier, historical_urgency_multiplier) AS avg_fee_urgency_multiplier,
    
    -- Data quality metrics
    current_fee_reading_count,
    historical_fee_reading_count,
    mempool_reading_count,
    
    -- Calculated network metrics
    CASE 
        WHEN daily_transaction_count > 0 AND avg_pending_fees > 0
        THEN ROUND(avg_pending_fees::FLOAT / daily_transaction_count, 2)
        ELSE NULL
    END AS network_avg_fee_per_tx,
    
    CASE 
        WHEN price_usd > 0 AND avg_difficulty > 0
        THEN ROUND(avg_difficulty::FLOAT / price_usd, 6)
        ELSE NULL
    END AS difficulty_to_price_ratio,
    
    CASE 
        WHEN avg_block_fullness_pct > 90 THEN 'High Usage'
        WHEN avg_block_fullness_pct > 70 THEN 'Moderate Usage'
        WHEN avg_block_fullness_pct > 50 THEN 'Low Usage'
        ELSE 'Minimal Usage'
    END AS network_usage_level,
    
    CASE 
        WHEN current_fee_reading_count > 0 AND historical_fee_reading_count > 0 AND mempool_reading_count > 0
        THEN 'Complete'
        WHEN current_fee_reading_count > 0 OR historical_fee_reading_count > 0 OR mempool_reading_count > 0
        THEN 'Partial'
        ELSE 'Missing'
    END AS blockstream_data_completeness,
    
    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM combined_network_data
WHERE date IS NOT NULL
ORDER BY date DESC