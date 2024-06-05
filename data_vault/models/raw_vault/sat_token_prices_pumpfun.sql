{{ config(
    materialized='incremental',
    unique_key='crypto_price_key'
) }}

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
    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
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
