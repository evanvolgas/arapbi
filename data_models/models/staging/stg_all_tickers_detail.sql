with source_data as (
SELECT
    cik,
    ticker,
    ticker_root,
    title,
    type,
    description,
    locale,
    active,
    address1,
    address2,
    city,
    state,
    country,
    postal_code,
    phone_number,
    homepage_url,
    list_date,
    sic_code,
    sic_description,
    total_employees,
    market,
    market_cap,
    primary_exchange,
    currency_name,
    share_class_shares_outstanding,
    weighted_shares_outstanding,
    composite_figi,
    share_class_figi
FROM raw.all_tickers_detail
)
SELECT * FROM source_data
