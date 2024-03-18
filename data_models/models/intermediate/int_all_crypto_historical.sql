with all_tickers AS (
SELECT
    ticker,
    title,
    type,
    cik
FROM {{ ref('stg_all_tickers') }}
), history as
(
SELECT
    ticker,
    open,
    close,
    volume_weighted_average_price,
    transactions,
    volume,
    date,
    year,
    month,
    quarter
FROM {{ ref('stg_all_crypto_historical') }}
)
SELECT
    history.ticker,
    all_tickers.title,
    history.open,
    history.close,
    history.volume_weighted_average_price,
    history.transactions,
    history.volume,
    history.date,
    history.year,
    history.month,
    history.quarter,
FROM history
LEFT JOIN all_tickers
    ON all_tickers.ticker = history.ticker
