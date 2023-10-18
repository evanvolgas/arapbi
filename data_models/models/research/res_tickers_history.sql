with source_data as (

    SELECT
        ticker,
        title,
        type,
        cik,
        sic_description,
        date,
        volume_weighted_average_price as volume_weighted_average_price,
        cash_amount AS cash_amount,
        weighted_shares_outstanding as weighted_shares_outstanding,
        ticker as num_tickers,
        transactions AS transactions,
        volume AS volume
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    WHERE ticker in ('META', 'FB', 'AAPL', "AMZN", "NFLX", "GOOGL", "MSFT")
)

SELECT * FROM source_data
ORDER BY ticker, date
