{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        symbol,
        exchange,
        load_date,
        record_source
    FROM {{ ref('staging_coinbase') }}
    UNION ALL
    SELECT DISTINCT
        symbol,
        exchange,
        load_date,
        record_source
    FROM {{ ref('staging_dexscreener') }}
    UNION ALL
    SELECT DISTINCT
        symbol,
        exchange,
        load_date,
        record_source
    FROM {{ ref('staging_pumpfun_king_of_the_hill') }}
)

SELECT
    {{ generate_surrogate_key(['symbol', 'exchange']) }} AS crypto_exchange_key,
    {{ generate_surrogate_key(['symbol']) }} AS crypto_key,
    load_date,
    record_source
FROM source
