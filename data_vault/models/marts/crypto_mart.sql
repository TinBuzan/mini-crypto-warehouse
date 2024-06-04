{{ config(materialized='view') }}
SELECT
    h.crypto_symbol,
    s.priceChange,
    s.priceChangePercent,
    s.weightedAvgPrice,
    s.prevClosePrice,
    s.lastPrice,
    s.lastQty,
    s.bidPrice,
    s.bidQty,
    s.askPrice,
    s.askQty,
    s.openPrice,
    s.highPrice,
    s.lowPrice,
    s.volume,
    s.quoteVolume,
    s.price_date
FROM {{ ref('hub_tokens') }} h
JOIN {{ ref('sat_token_prices_coinbase') }} s
ON h.crypto_key = s.crypto_key
