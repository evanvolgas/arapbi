{{
  config(
    cluster_by = ["ticker", "date"],
  )
}}

with source_data as (

    SELECT
        case when ticker = "FB" THEN "META" else ticker END AS ticker,
        year,
        month,
        min(date) as date,
        avg(volume_weighted_average_price) as volume_weighted_average_price,
        avg(cash_amount) AS cash_amount,
        avg(weighted_shares_outstanding) as weighted_shares_outstanding,
        avg(transactions) AS transactions,
        avg(volume) AS volume
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    WHERE ticker in ( "AAPL",
                      "ADBE",
                      "AMD",
                      "AMZN",
                      "ASML",
                      "AVGO",
                      "CRM",
                      "FB",
                      "GOOGL",
                      "INTC",
                      "INTU",
                      "MSFT",
                      "NFLX",
                      "NVDA",
                      "ORCL",
                      "TSLA",
                      "TSM")
      OR (ticker = 'META' and date >='2022-06-09')
    GROUP BY 1,2,3
)
SELECT * FROM source_data

