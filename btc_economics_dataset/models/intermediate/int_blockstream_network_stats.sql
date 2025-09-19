{{ config(
    materialized='table',
    description='Combined Bitcoin network statistics from blocks, fees, and mempool data'
) }}

WITH daily_blocks AS (
    SELECT 
        block_date,
        COUNT(*) as blocks_mined,
        AVG(CAST(difficulty AS DOUBLE)) as avg_difficulty,
        MAX(CAST(difficulty AS DOUBLE)) as max_difficulty,
        MIN(CAST(difficulty AS DOUBLE)) as min_difficulty,
        AVG(CAST(transaction_count AS DOUBLE)) as avg_transactions_per_block,
        SUM(CAST(transaction_count AS BIGINT)) as total_transactions,
        AVG(CAST(block_size AS DOUBLE)) as avg_block_size,
        AVG(CAST(block_fullness_pct AS DOUBLE)) as avg_block_fullness_pct,
        AVG(CAST(block_weight AS DOUBLE)) as avg_block_weight,
        STDDEV(CAST(block_size AS DOUBLE)) as block_size_volatility,
        MAX(CAST(height AS INTEGER)) as latest_block_height
    FROM {{ ref('int_blockstream_blocks') }}
    WHERE block_date IS NOT NULL
        AND difficulty IS NOT NULL
        AND transaction_count IS NOT NULL
        AND block_size IS NOT NULL
    GROUP BY block_date
),

daily_fees AS (
    SELECT 
        observation_date,
        fee_data_type,
        AVG(CAST(immediate_fee_rate AS DOUBLE)) as avg_immediate_fee,
        MAX(CAST(immediate_fee_rate AS DOUBLE)) as max_immediate_fee,
        MIN(CAST(immediate_fee_rate AS DOUBLE)) as min_immediate_fee,
        AVG(CAST(fast_fee_rate AS DOUBLE)) as avg_fast_fee,
        AVG(CAST(standard_fee_rate AS DOUBLE)) as avg_standard_fee,
        AVG(CAST(economy_fee_rate AS DOUBLE)) as avg_economy_fee,
        AVG(CAST(urgency_multiplier AS DOUBLE)) as avg_urgency_multiplier,
        COUNT(*) as fee_readings_count
    FROM {{ ref('int_blockstream_fees') }}
    WHERE observation_date IS NOT NULL
        AND immediate_fee_rate IS NOT NULL
        AND fee_data_type IS NOT NULL
    GROUP BY observation_date, fee_data_type
),

daily_mempool AS (
    SELECT 
        observation_date,
        AVG(CAST(unconfirmed_transaction_count AS DOUBLE)) as avg_mempool_tx_count,
        MAX(CAST(unconfirmed_transaction_count AS BIGINT)) as max_mempool_tx_count,
        MIN(CAST(unconfirmed_transaction_count AS BIGINT)) as min_mempool_tx_count,
        AVG(CAST(total_fees_pending AS DOUBLE)) as avg_pending_fees,
        AVG(CAST(avg_fee_per_vbyte_mempool AS DOUBLE)) as avg_mempool_fee_rate,
        COUNT(*) as mempool_readings_count,
        MODE() WITHIN GROUP (ORDER BY congestion_level) as daily_congestion_level
    FROM {{ ref('int_blockstream_mempool') }}
    WHERE observation_date IS NOT NULL
        AND unconfirmed_transaction_count IS NOT NULL
    GROUP BY observation_date
),

-- Separate current and historical fees into individual CTEs
current_fees AS (
    SELECT * FROM daily_fees WHERE fee_data_type = 'current'
),

historical_fees AS (
    SELECT * FROM daily_fees WHERE fee_data_type = 'historical'
),

-- Get all available dates from any source
all_dates AS (
    SELECT DISTINCT block_date as observation_date FROM daily_blocks
    UNION
    SELECT DISTINCT observation_date FROM current_fees
    UNION 
    SELECT DISTINCT observation_date FROM historical_fees
    UNION
    SELECT DISTINCT observation_date FROM daily_mempool
),

combined_daily_stats AS (
    SELECT 
        ad.observation_date,
        
        -- Block metrics (LEFT JOIN to preserve all dates)
        db.blocks_mined,
        db.avg_difficulty,
        db.max_difficulty,
        db.min_difficulty,
        db.avg_transactions_per_block,
        db.total_transactions,
        db.avg_block_size,
        db.avg_block_fullness_pct,
        db.avg_block_weight,
        db.block_size_volatility,
        db.latest_block_height,
        
        -- Fee metrics (current) - LEFT JOIN
        cf.avg_immediate_fee as avg_current_immediate_fee,
        cf.avg_fast_fee as avg_current_fast_fee,
        cf.avg_standard_fee as avg_current_standard_fee,
        cf.avg_economy_fee as avg_current_economy_fee,
        cf.avg_urgency_multiplier as avg_current_urgency_multiplier,
        cf.fee_readings_count as current_fee_readings,
        
        -- Fee metrics (historical) - LEFT JOIN
        hf.avg_immediate_fee as avg_historical_immediate_fee,
        hf.avg_fast_fee as avg_historical_fast_fee,
        hf.avg_standard_fee as avg_historical_standard_fee,
        hf.avg_economy_fee as avg_historical_economy_fee,
        hf.avg_urgency_multiplier as avg_historical_urgency_multiplier,
        hf.fee_readings_count as historical_fee_readings,
        
        -- Mempool metrics - LEFT JOIN
        dm.avg_mempool_tx_count,
        dm.max_mempool_tx_count,
        dm.min_mempool_tx_count,
        dm.avg_pending_fees,
        dm.avg_mempool_fee_rate,
        dm.mempool_readings_count,
        dm.daily_congestion_level,
        
        CURRENT_TIMESTAMP as dbt_created_at
        
    FROM all_dates ad
    LEFT JOIN daily_blocks db ON ad.observation_date = db.block_date
    LEFT JOIN current_fees cf ON ad.observation_date = cf.observation_date 
    LEFT JOIN historical_fees hf ON ad.observation_date = hf.observation_date 
    LEFT JOIN daily_mempool dm ON ad.observation_date = dm.observation_date
)

SELECT *
FROM combined_daily_stats
WHERE observation_date IS NOT NULL
ORDER BY observation_date DESC