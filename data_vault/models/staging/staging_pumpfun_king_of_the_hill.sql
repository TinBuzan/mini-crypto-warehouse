-- models/raw_vault/staging_dexscreener.sql

{{ config(
    materialized='table'
) }}

SELECT
    *
FROM
    mini_crypto_catalog.default.staging_pumpfun_king_of_the_hill
