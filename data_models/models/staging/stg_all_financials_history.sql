with source_data as (
SELECT
    ticker,
    start_date,
    end_date,
    fiscal_period,
    fiscal_year,
    cik,
    company_name,
    cash_flow_label,
    cash_flow_value,
    cash_flow_unit
FROM raw.all_financials_history
WHERE fiscal_period IN ("Q1", "Q2", "Q3", "Q4")
)
SELECT * FROM source_data





