with source_data as (
SELECT
    ticker,
    title,
    type,
    cik,
    locale,
    primary_exchange,
    currency_symbol,
    currency_name,
    base_currency_symbol,
    base_currency_name,
    active,
    delisted_utc,
    composite_figi,
    share_class_figi,
    source_feed
FROM raw.all_tickers
)
SELECT * FROM source_data