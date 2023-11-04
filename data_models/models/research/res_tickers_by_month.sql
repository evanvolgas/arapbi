{{
  config(
    cluster_by = ["ticker", "date"],
  )
}}

with source_data as (

    SELECT
        ticker,
        title,
        type,
        cik,
        sic_description,
        year,
        month,
        min(date) as date,
        avg(volume_weighted_average_price) as volume_weighted_average_price,
        avg(cash_amount) AS cash_amount,
        avg(weighted_shares_outstanding) as weighted_shares_outstanding,
        avg(transactions) AS transactions,
        avg(volume) AS volume
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    WHERE ticker in ('META', 'FB', 'AAPL', "AMZN", "NFLX", "GOOGL", "MSFT", "KO", "SFIX", "SNAP")
    GROUP BY 1,2,3,4,5,6,7
)

SELECT * FROM source_data

