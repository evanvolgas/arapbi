SELECT
    ticker,
    year,
    extract(quarter from date) as quarter,
    avg(volume_weighted_average_price) as volume_weighted_average_price,
    avg(cash_amount) AS avg_cash_amount,
    avg(weighted_shares_outstanding) as avg_weighted_shares_outstanding
FROM {{ ref('int_all_tickers_and_dividends_historical') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3