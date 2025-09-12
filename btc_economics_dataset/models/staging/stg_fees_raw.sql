-- models/staging/stg_fees_current_raw.sql
{{ config(
    materialized='view',
    description='Raw current fee estimates from Blockstream API'
) }}

SELECT
    filename,
    collection_timestamp,
    
    -- Extract individual fee estimates
    raw_fee_estimates."1" as fee_1_block,
    raw_fee_estimates."2" as fee_2_blocks,
    raw_fee_estimates."3" as fee_3_blocks,
    raw_fee_estimates."4" as fee_4_blocks,
    raw_fee_estimates."5" as fee_5_blocks,
    raw_fee_estimates."6" as fee_6_blocks,
    raw_fee_estimates."7" as fee_7_blocks,
    raw_fee_estimates."8" as fee_8_blocks,
    raw_fee_estimates."9" as fee_9_blocks,
    raw_fee_estimates."10" as fee_10_blocks,
    raw_fee_estimates."11" as fee_11_blocks,
    raw_fee_estimates."12" as fee_12_blocks,
    raw_fee_estimates."13" as fee_13_blocks,
    raw_fee_estimates."14" as fee_14_blocks,
    raw_fee_estimates."15" as fee_15_blocks,
    raw_fee_estimates."16" as fee_16_blocks,
    raw_fee_estimates."17" as fee_17_blocks,
    raw_fee_estimates."18" as fee_18_blocks,
    raw_fee_estimates."19" as fee_19_blocks,
    raw_fee_estimates."20" as fee_20_blocks,
    raw_fee_estimates."21" as fee_21_blocks,
    raw_fee_estimates."22" as fee_22_blocks,
    raw_fee_estimates."23" as fee_23_blocks,
    raw_fee_estimates."24" as fee_24_blocks,
    raw_fee_estimates."25" as fee_25_blocks,
    raw_fee_estimates."144" as fee_144_blocks,
    raw_fee_estimates."504" as fee_504_blocks,
    raw_fee_estimates."1008" as fee_1008_blocks,
    
    -- Keep entire raw JSON object
    raw_fee_estimates as full_raw_fee_estimates,
    
    'Blockstream API' AS api_source,
    CURRENT_TIMESTAMP AS dbt_created_at

FROM read_json_auto('s3://bronze/fees/current/*/*/*.json')