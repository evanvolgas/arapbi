with source_data as (

    SELECT
        ticker,
        title,
        type,
        cik,
        sic_description,
        date,
        volume_weighted_average_price as volume_weighted_average_price,
        cash_amount AS avg_cash_amount,
        weighted_shares_outstanding as avg_weighted_shares_outstanding,
        ticker as num_tickers,
        transactions AS avg_transactions,
        volume AS avg_volume
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    WHERE ticker in ('META', 'AAPL', "AMZN", "NFLX", "GOOGL", "MSFT")
)

SELECT * FROM source_data
ORDER BY ticker, date
