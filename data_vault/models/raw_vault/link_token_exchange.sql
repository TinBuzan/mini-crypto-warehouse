{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        UPPER(TRIM(exchange)) AS exchange
    FROM {{ ref('staging_coinbase') }}
    UNION
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        UPPER(TRIM(exchange)) AS exchange
    FROM {{ ref('staging_dexscreener') }}
    UNION
    SELECT DISTINCT
        UPPER(TRIM(symbol)) AS symbol,
        UPPER(TRIM(exchange)) AS exchange
    FROM {{ ref('staging_pumpfun_king_of_the_hill') }}
)

, tokens AS (
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        token_key AS crypto_key
    FROM {{ ref('hub_tokens') }}
)

, exchanges AS (
    SELECT
        UPPER(TRIM(exchange)) AS exchange,
        exchange_key AS crypto_exchange_key
    FROM {{ ref('hub_exchanges') }}
)

-- Generate the final link table
SELECT
    {{ generate_surrogate_key(['t.crypto_key', 'e.crypto_exchange_key']) }} AS crypto_exchange_link_key,
    t.crypto_key,
    e.crypto_exchange_key,
    s.symbol,
    s.exchange
FROM source s
JOIN tokens t ON s.symbol = t.symbol
JOIN exchanges e ON s.exchange = e.exchange
