SELECT
    a.ticker,
    a.title,
    a.type,
    a.cik,
    a.sic_description,
    a.year,
    a.quarter,
    a.volume_weighted_average_price,
    case when a.avg_cash_amount is null then 0 else a.avg_cash_amount end as avg_cash_amount,
    a.avg_weighted_shares_outstanding,
    a.avg_transactions,
    a.avg_volume,
    a.number_days_in_quarter,
    b.start_date as quarter_start_date,
    b.end_date as quarter_end_date,
    b.cash_flow_unit,
    b.cash_flow_value,
    b.cash_flow_label
FROM {{ ref('int_all_tickers_by_quarter') }} AS a
LEFT JOIN  {{ ref('stg_all_financials_history') }} as b
    on a.min_dt >= b.start_date - 5
    and a.max_dt <= b.end_date + 5
    and a.ticker = b.ticker
    and a.cik = b.cik
    and concat("Q",a.quarter) = b.fiscal_period
ORDER BY a.ticker, a.title, a.type, b.start_date
