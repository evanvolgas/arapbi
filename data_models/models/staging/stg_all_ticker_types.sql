with source_data as (
SELECT
    asset_class,
    code,
    description,
    locale
FROM raw.all_ticker_types
)
SELECT * FROM source_data


