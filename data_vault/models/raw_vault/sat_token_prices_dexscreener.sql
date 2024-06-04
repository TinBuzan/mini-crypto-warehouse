{{ config(materialized='table') }}
WITH source AS (
    SELECT DISTINCT
        symbol,
        priceChange,
        priceChangePercent,
        weightedAvgPrice,
        prevClosePrice,
        lastPrice,
        lastQty,
        bidPrice,
        bidQty,
        askPrice,
        askQty,
        openPrice,
        highPrice,
        lowPrice,
        volume,
        quoteVolume,
        closeTime::timestamp AS price_date,
        load_date,
        record_source
    FROM {{ ref('staging_binance') }}
)
SELECT
    {{ generate_surrogate_key(['symbol', 'load_date']) }} AS crypto_price_key,
    {{ generate_surrogate_key(['symbol']) }} AS crypto_key,
    priceChange,
    priceChangePercent,
    weightedAvgPrice,
    prevClosePrice,
    lastPrice,
    lastQty,
    bidPrice,
    bidQty,
    askPrice,
    askQty,
    openPrice,
    highPrice,
    lowPrice,
    volume,
    quoteVolume,
    price_date,
    load_date,
    record_source
FROM source
