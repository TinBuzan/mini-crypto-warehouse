{{ config(materialized='table') }}

select
    {{ generate_surrogate_key(['symbol']) }} as token_key,
    symbol,
    load_date,
    record_source
from (
    select 
        symbol,
        load_date,
        record_source
    from {{ ref('default.staging_pumpfun_king_of_the_hill') }}
    union all
    select 
        symbol,
        load_date,
        record_source
    from {{ ref('default.staging_dexscreener') }}
)
