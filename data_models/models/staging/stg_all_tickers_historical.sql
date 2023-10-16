with source_data as (
SELECT
    ticker,
    open,
    close,
    volume_weighted_average_price,
    volume,
    transactions,
    date,
    EXTRACT(YEAR from date) as year

FROM raw.all_tickers_history
)
SELECT * FROM source_data