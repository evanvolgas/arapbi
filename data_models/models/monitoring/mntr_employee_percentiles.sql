with p as (
  SELECT approx_quantiles(total_employees, 100) percentiles
  FROM {{ ref('int_all_tickers_and_dividends_historical') }}
  WHERE type = "Common Stock"
)
SELECT
  percentiles[offset(10)] as p10,
  percentiles[offset(25)] as p25,
  percentiles[offset(50)] as p50,
  percentiles[offset(75)] as p75,
  percentiles[offset(90)] as p90
FROM p