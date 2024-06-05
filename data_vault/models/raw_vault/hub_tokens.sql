{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['symbol']) }} AS token_key,
    symbol,
    load_date,
    record_source
FROM (
    SELECT 
        symbol,
        load_date,
        record_source
    FROM {{ ref('staging_pumpfun_king_of_the_hill') }}
    UNION ALL
    SELECT 
        symbol,
        load_date,
        record_source
    FROM {{ ref('staging_dexscreener') }}
    UNION ALL
    SELECT 
        symbol,
        load_date,
        record_source
    FROM {{ ref('staging_coinbase') }}
)

