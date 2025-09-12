{{ config(
    materialized='view',
    description='Raw Bitcoin block data from Blockstream API'
) }}

SELECT
    filename,
    height,
    block_hash,
    collection_timestamp,
    
    -- All raw_block_data fields
    raw_block_data.id as raw_block_id,
    raw_block_data.height as raw_height,
    raw_block_data.version as raw_version,
    raw_block_data.timestamp as raw_timestamp,
    raw_block_data.tx_count as raw_tx_count,
    raw_block_data.size as raw_size,
    raw_block_data.weight as raw_weight,
    raw_block_data.merkle_root as raw_merkle_root,
    raw_block_data.previousblockhash as raw_previousblockhash,
    raw_block_data.mediantime as raw_mediantime,
    raw_block_data.nonce as raw_nonce,
    raw_block_data.bits as raw_bits,
    raw_block_data.difficulty as raw_difficulty,
    
    -- Keep entire raw JSON object
    raw_block_data as full_raw_block_data,
    
    'Blockstream API' AS api_source,
    CURRENT_TIMESTAMP AS dbt_created_at

FROM read_json_auto('s3://bronze/blocks/*.json')