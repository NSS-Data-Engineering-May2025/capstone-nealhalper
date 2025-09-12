{{ config(
    materialized='table',
    description='Combined Bitcoin network statistics from blocks, fees, and mempool data'
) }}

WITH daily_blocks AS (
    SELECT 
        block_date,
        COUNT(*) as blocks_mined,
        AVG(difficulty) as avg_difficulty,
        MAX(difficulty) as max_difficulty,
        MIN(difficulty) as min_difficulty,
        AVG(transaction_count) as avg_transactions_per_block,
        SUM(transaction_count) as total_transactions,
        AVG(block_size) as avg_block_size,
        AVG(block_fullness_pct) as avg_block_fullness_pct
    FROM {{ ref('int_blockstream_blocks') }}
    GROUP BY block_date
),

daily_fees AS (
    SELECT 
        observation_date,
        fee_data_type,
        AVG(immediate_fee_rate) as avg_immediate_fee,
        MAX(immediate_fee_rate) as max_immediate_fee,
        MIN(immediate_fee_rate) as min_immediate_fee,
        AVG(fast_fee_rate) as avg_fast_fee,
        AVG(standard_fee_rate) as avg_standard_fee,
        AVG(economy_fee_rate) as avg_economy_fee,
        AVG(urgency_multiplier) as avg_urgency_multiplier,
        COUNT(*) as fee_readings_count
    FROM {{ ref('int_blockstream_fees') }}
    GROUP BY observation_date, fee_data_type
),

daily_mempool AS (
    SELECT 
        observation_date,
        AVG(unconfirmed_transaction_count) as avg_mempool_tx_count,
        MAX(unconfirmed_transaction_count) as max_mempool_tx_count,
        MIN(unconfirmed_transaction_count) as min_mempool_tx_count,
        AVG(total_fees_pending) as avg_pending_fees,
        AVG(avg_fee_per_vbyte_mempool) as avg_mempool_fee_rate,
        COUNT(*) as mempool_readings_count
    FROM {{ ref('int_blockstream_mempool') }}
    GROUP BY observation_date
),

-- Separate current and historical fees into individual CTEs
current_fees AS (
    SELECT * FROM daily_fees WHERE fee_data_type = 'current'
),

historical_fees AS (
    SELECT * FROM daily_fees WHERE fee_data_type = 'historical'
),

combined_daily_stats AS (
    SELECT 
        COALESCE(db.block_date, cf.observation_date, hf.observation_date, dm.observation_date) as observation_date,
        
        -- Block metrics
        db.blocks_mined,
        db.avg_difficulty,
        db.max_difficulty,
        db.min_difficulty,
        db.avg_transactions_per_block,
        db.total_transactions,
        db.avg_block_size,
        db.avg_block_fullness_pct,
        
        -- Fee metrics (current)
        cf.avg_immediate_fee as avg_current_immediate_fee,
        cf.avg_fast_fee as avg_current_fast_fee,
        cf.avg_standard_fee as avg_current_standard_fee,
        cf.avg_economy_fee as avg_current_economy_fee,
        cf.avg_urgency_multiplier as avg_current_urgency_multiplier,
        
        -- Fee metrics (historical)
        hf.avg_immediate_fee as avg_historical_immediate_fee,
        hf.avg_fast_fee as avg_historical_fast_fee,
        hf.avg_standard_fee as avg_historical_standard_fee,
        hf.avg_economy_fee as avg_historical_economy_fee,
        hf.avg_urgency_multiplier as avg_historical_urgency_multiplier,
        
        -- Mempool metrics
        dm.avg_mempool_tx_count,
        dm.max_mempool_tx_count,
        dm.min_mempool_tx_count,
        dm.avg_pending_fees,
        dm.avg_mempool_fee_rate,
        
        -- Data quality metrics
        cf.fee_readings_count as current_fee_readings,
        hf.fee_readings_count as historical_fee_readings,
        dm.mempool_readings_count,
        
        CURRENT_TIMESTAMP as dbt_created_at
        
    FROM daily_blocks db
    FULL OUTER JOIN current_fees cf 
        ON db.block_date = cf.observation_date 
    FULL OUTER JOIN historical_fees hf 
        ON db.block_date = hf.observation_date 
    FULL OUTER JOIN daily_mempool dm 
        ON db.block_date = dm.observation_date
)

SELECT *
FROM combined_daily_stats
ORDER BY observation_date DESC