{{ config(materialized='table') }}
WITH source AS (
    SELECT DISTINCT
        symbol,
        market_cap,
        usd_market_cap,
        bonding_curve,
        reply_count,
        load_date,
        record_source
    FROM {{ ref('default.staging_pumpfun_king_of_the_hill') }}
)
SELECT
    {{ generate_surrogate_key(['symbol', 'load_date']) }} AS crypto_price_key,
    {{ generate_surrogate_key(['symbol']) }} AS crypto_key,
    symbol,
    market_cap,
    usd_market_cap,
    bonding_curve,
    reply_count,
    load_date,
    record_source
FROM source
