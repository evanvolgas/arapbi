SELECT
    type,
    year,
    avg(volume_weighted_average_price) as volume_weighted_average_price,
    avg(cash_amount) AS avg_cash_amount,
    avg(weighted_shares_outstanding) as avg_weighted_shares_outstanding,
    count(distinct ticker) as distinct_tickers
FROM {{ ref('int_all_tickers_and_dividends_historical') }}
WHERE year > 2018
GROUP BY 1, 2
