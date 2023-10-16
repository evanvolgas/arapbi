with source_data as (

    SELECT
        ticker,
        title,
        type,
        cik,
        sic_description,
        avg(volume_weighted_average_price) as volume_weighted_average_price,
        avg(cash_amount) AS avg_cash_amount,
        avg(weighted_shares_outstanding) as avg_weighted_shares_outstanding,
        count(distinct ticker) as num_tickers,
        avg(transactions) AS avg_transactions,
        avg(volume) AS avg_volume,
        min(date) as min_date,
        max(date) as max_date,
        count(distinct date) as number_days
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    GROUP BY 1, 2, 3, 4, 5
)

select *
from source_data