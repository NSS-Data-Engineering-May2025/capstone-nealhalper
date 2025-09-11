{{ config(materialized='table') }}
SELECT column_name, column_type 
FROM (DESCRIBE SELECT * FROM read_json_auto('s3://bronze/mempool/*.json'))