{% snapshot crypto_prices_snapshot %}

{{
    config(
        target_schema='PARROT_SNAP',
        unique_key='symbol || to_char(price_date)',
        strategy='timestamp',
        updated_at='INGESTED_AT'
    )
}}

select
    SYMBOL,
    PRICE_DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    ADJ_CLOSE,
    VOLUME,
    INGESTED_AT
from {{ source('raw', 'CRYPTO_PRICES') }}

{% endsnapshot %}
