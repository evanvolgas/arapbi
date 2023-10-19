with source_data as (
SELECT
    ticker,
    open,
    close,
    volume_weighted_average_price,
    volume,
    transactions,
    date,
    EXTRACT(YEAR from date) as year,
    EXTRACT(MONTH from date) as month,
    EXTRACT(quarter from date) as quarter
FROM raw.all_tickers_history
)
SELECT * FROM source_data
