{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['symbol']) }} AS token_key,
    symbol,
    record_source
FROM (
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        record_source
    FROM {{ ref('staging_pumpfun_king_of_the_hill') }}
    UNION
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        record_source
    FROM {{ ref('staging_dexscreener') }}
    UNION
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        record_source
    FROM {{ ref('staging_coinbase') }}
) AS combined_data


