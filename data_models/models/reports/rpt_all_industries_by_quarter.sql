SELECT
    sic_description,
    year,
    extract(quarter from date) as quarter,
    avg(volume_weighted_average_price) as volume_weighted_average_price,
    avg(case when cash_amount is null then 0 else cash_amount end) AS avg_cash_amount,
    avg(weighted_shares_outstanding) as avg_weighted_shares_outstanding,
    count(distinct ticker) as num_tickers,
    avg(transactions) AS avg_transactions,
    avg(volume) AS avg_volume,
    min(date) as min_date,
    max(date) as max_date,
    count(distinct date) as number_days_in_quarter
FROM {{ ref('int_all_tickers_and_dividends_historical') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3