{{ config(
    materialized='incremental',
    unique_key='crypto_price_key'
) }}

WITH source AS (
    SELECT DISTINCT
        product_id,
        open,
        high,
        low,
        last,
        volume,
        volume_30day,
        rfq_volume_24hour,
        rfq_volume_30day,
        load_date,
        record_source
    FROM {{ ref('default.staging_coinbase') }}
    {% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ generate_surrogate_key(['product_id', 'load_date']) }} AS crypto_price_key,
    {{ generate_surrogate_key(['product_id']) }} AS crypto_key,
    open,
    high,
    low,
    last,
    volume,
    volume_30day,
    rfq_volume_24hour,
    rfq_volume_30day,
    load_date,
    record_source
FROM source
