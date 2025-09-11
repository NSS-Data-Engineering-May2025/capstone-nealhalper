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
        price_change_pct,
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
        AVG(total_fees) AS avg_block_fees,
        AVG(avg_fee_per_kb) AS avg_fee_per_kb,
        AVG(avg_fee_per_vbyte) AS avg_fee_per_vbyte,
        AVG(avg_fee_per_transaction) AS avg_fee_per_transaction,
        AVG(block_fullness_pct) AS avg_block_fullness_pct,
        AVG(transaction_density_per_mb) AS avg_transaction_density,
        
        SUM(transaction_count) AS daily_transaction_count,
        SUM(total_fees) AS daily_total_fees,
        
        STDDEV(block_size) AS block_size_volatility,
        MAX(block_height) AS latest_block_height
        
    FROM {{ ref('int_blockstream_blocks') }}
    GROUP BY block_date
),

mempool_stats AS (
    SELECT 
        observation_date AS date,
        AVG(unconfirmed_transaction_count) AS avg_mempool_size,
        AVG(total_fees_pending) AS avg_pending_fees,
        AVG(fastest_fee_rate) AS avg_fastest_fee_rate,
        AVG(economy_fee_rate) AS avg_economy_fee_rate,
        AVG(fee_urgency_ratio) AS avg_fee_urgency_ratio,
        
        MAX(unconfirmed_transaction_count) AS max_mempool_size,
        MIN(unconfirmed_transaction_count) AS min_mempool_size,
        
        MODE() WITHIN GROUP (ORDER BY congestion_level) AS daily_congestion_level
        
    FROM {{ ref('int_blockstream_mempool') }}
    GROUP BY observation_date
),

network_stats AS (
    SELECT 
        observation_date AS date,
        daily_avg_fast_fee_rate,
        daily_avg_standard_fee_rate,
        daily_avg_safe_fee_rate,
        daily_avg_difficulty,
        daily_avg_hashrate_7d,
        daily_avg_hashrate_30d,
        daily_blocks_until_adjustment,
        daily_time_until_adjustment_hours,
        avg_fee_urgency_multiplier,
        peak_fee_urgency_multiplier,
        daily_fee_volatility_pct,
        daily_fee_reading_count,
        daily_difficulty_reading_count
    FROM {{ ref('int_blockstream_network_stats') }}
),

combined_network_data AS (
    SELECT 
        COALESCE(bp.date, bs.date, ms.date, ns.date) AS date,
        
        bp.price_usd,
        bp.market_cap_usd,
        bp.total_volume_usd,
        bp.price_change_pct,
        bp.daily_return,
        bp.weekly_return,
        bp.monthly_return,
        bp.price_30d_volatility,
        
        bs.daily_block_count,
        bs.avg_block_size,
        bs.avg_block_weight,
        bs.avg_transactions_per_block,
        bs.daily_transaction_count,
        bs.daily_total_fees,
        bs.avg_fee_per_kb,
        bs.avg_fee_per_vbyte,
        bs.avg_fee_per_transaction,
        bs.avg_block_fullness_pct,
        bs.avg_transaction_density,
        bs.block_size_volatility,
        bs.latest_block_height,
        
        ms.avg_mempool_size,
        ms.max_mempool_size,
        ms.min_mempool_size,
        ms.avg_pending_fees,
        ms.avg_fastest_fee_rate AS mempool_fastest_fee_rate,
        ms.avg_economy_fee_rate AS mempool_economy_fee_rate,
        ms.avg_fee_urgency_ratio AS mempool_urgency_ratio,
        ms.daily_congestion_level,
        
        ns.daily_avg_fast_fee_rate,
        ns.daily_avg_standard_fee_rate,
        ns.daily_avg_safe_fee_rate,
        ns.daily_avg_difficulty,
        ns.daily_avg_hashrate_7d,
        ns.daily_avg_hashrate_30d,
        ns.daily_blocks_until_adjustment,
        ns.daily_time_until_adjustment_hours,
        ns.avg_fee_urgency_multiplier,
        ns.peak_fee_urgency_multiplier,
        ns.daily_fee_volatility_pct,
        ns.daily_fee_reading_count,
        ns.daily_difficulty_reading_count
        
    FROM bitcoin_prices bp
    FULL OUTER JOIN block_stats bs ON bp.date = bs.date
    FULL OUTER JOIN mempool_stats ms ON bp.date = ms.date
    FULL OUTER JOIN network_stats ns ON bp.date = ns.date
)

SELECT 
    date,
    price_usd,
    market_cap_usd,
    total_volume_usd,
    price_change_pct,
    daily_return,
    weekly_return,
    monthly_return,
    price_30d_volatility,
    
    daily_block_count,
    avg_block_size,
    avg_block_weight,
    avg_transactions_per_block,
    daily_transaction_count,
    daily_total_fees,
    avg_fee_per_transaction,
    avg_block_fullness_pct,
    latest_block_height,
    
    avg_mempool_size,
    max_mempool_size,
    daily_congestion_level,
    
    daily_avg_difficulty,
    daily_avg_hashrate_7d,
    daily_avg_hashrate_30d,
    daily_avg_fast_fee_rate,
    daily_avg_safe_fee_rate,
    avg_fee_urgency_multiplier,
    daily_fee_volatility_pct,
    
    CASE 
        WHEN daily_total_fees > 0 AND daily_transaction_count > 0
        THEN ROUND(daily_total_fees::FLOAT / daily_transaction_count, 2)
        ELSE NULL
    END AS network_avg_fee_per_tx,
    
    CASE 
        WHEN price_usd > 0 AND daily_avg_hashrate_7d > 0
        THEN ROUND(daily_avg_hashrate_7d::FLOAT / price_usd, 6)
        ELSE NULL
    END AS hashrate_to_price_ratio,
    
    CASE 
        WHEN avg_block_fullness_pct > 90 THEN 'High Usage'
        WHEN avg_block_fullness_pct > 70 THEN 'Moderate Usage'
        WHEN avg_block_fullness_pct > 50 THEN 'Low Usage'
        ELSE 'Minimal Usage'
    END AS network_usage_level,
    
    CASE 
        WHEN daily_fee_reading_count > 0 AND daily_difficulty_reading_count > 0 
        THEN 'Complete'
        WHEN daily_fee_reading_count > 0 OR daily_difficulty_reading_count > 0 
        THEN 'Partial'
        ELSE 'Missing'
    END AS blockstream_data_completeness,
    
    CURRENT_TIMESTAMP AS dbt_created_at
    
FROM combined_network_data
ORDER BY date DESC