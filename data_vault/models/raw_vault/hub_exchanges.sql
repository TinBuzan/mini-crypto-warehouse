{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['exchange']) }} AS exchange_key,
    exchange
FROM (
    SELECT DISTINCT
        exchange
    FROM {{ ref('staging_pumpfun_king_of_the_hill') }}
    UNION
    SELECT DISTINCT
        exchange
    FROM {{ ref('staging_dexscreener') }}
    UNION
    SELECT DISTINCT
        exchange
    FROM {{ ref('staging_coinbase') }}
) AS combined_data


