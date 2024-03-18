{{
  config(
    cluster_by = ["ticker", "date"],
  )
}}

with source_data as (

    SELECT
        case when ticker = "FB" THEN "META" ELSE ticker END AS ticker,
        title,
        type,
        cik,
        sic_description,
        date,
        month,
        quarter,
        year,
        volume_weighted_average_price as volume_weighted_average_price,
        cash_amount AS cash_amount,
        weighted_shares_outstanding as weighted_shares_outstanding,
        transactions AS transactions,
        volume AS volume
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
)
SELECT * FROM source_data
