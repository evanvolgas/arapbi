{{
  config(
    cluster_by = ["ticker", "year", "quarter"],
  )
}}

SELECT *
FROM {{ ref('rpt_all_tickers_by_quarter') }} AS a
WHERE ticker in ('META', 'FB', 'AAPL', "AMZN", "NFLX", "GOOGL", "MSFT", "KO", "SFIX", "SNAP")
