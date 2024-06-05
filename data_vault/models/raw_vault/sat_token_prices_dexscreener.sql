{{ config(
    materialized='incremental',
    unique_key='crypto_price_key'
) }}

WITH source AS (
    SELECT DISTINCT
        symbol,
        priceNative,
        priceUsd,
        txns_m5_buys,
        txns_m5_sells,
        txns_h1_buys,
        txns_h1_sells,
        txns_h6_buys,
        txns_h6_sells,
        txns_h24_buys,
        txns_h24_sells,
        volume_m5,
        volume_h1,
        volume_h6,
        volume_h24,
        load_date,
        record_source
    FROM {{ ref('default.staging_dexscreener') }}
    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ generate_surrogate_key(['symbol', 'load_date']) }} AS crypto_price_key,
    {{ generate_surrogate_key(['symbol']) }} AS crypto_key,
    symbol,
    priceNative,
    priceUsd,
    txns_m5_buys,
    txns_m5_sells,
    txns_h1_buys,
    txns_h1_sells,
    txns_h6_buys,
    txns_h6_sells,
    txns_h24_buys,
    txns_h24_sells,
    volume_m5,
    volume_h1,
    volume_h6,
    volume_h24,
    load_date,
    record_source
FROM source
