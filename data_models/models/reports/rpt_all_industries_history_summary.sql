
SELECT
    sic_description,
    avg(volume_weighted_average_price) as volume_weighted_average_price,
    avg(cash_amount) AS avg_cash_amount,
    avg(weighted_shares_outstanding) as avg_weighted_shares_outstanding,
    count(distinct ticker) as num_tickers,
    avg(transactions) AS avg_transactions,
    avg(volume) AS avg_volume
FROM {{ ref('int_all_tickers_and_dividends_historical') }}
GROUP BY 1
ORDER BY 1