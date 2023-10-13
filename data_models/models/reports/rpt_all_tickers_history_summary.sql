with source_data as (

    SELECT
        ticker,
        title,
        avg(volume_weighted_average_price) as avg_volume_weighted_average_price,
        avg(volume) as avg_volume,
        avg(transactions) as avg_transactions,
        avg(total_employees) as total_employees,
        min(date) as min_dt,
        max(date) as max_dt,
        count(distinct date) as number_of_days_per_quarter
    FROM {{ ref('int_all_tickers_and_dividends_historical') }}
    GROUP BY 1, 2
)

select *
from source_data