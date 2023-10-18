SELECT *
FROM {{ ref('rpt_all_tickers_by_quarter') }} AS a
WHERE ticker in ('META', 'AAPL', "AMZN", "NFLX", "GOOGL", "MSFT")
ORDER BY ticker, year, quarter
