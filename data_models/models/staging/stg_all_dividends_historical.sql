with source_data as (
SELECT
    ticker,
    cash_amount,
    currency,
    declaration_date,
    EXTRACT(YEAR from declaration_date) as declaration_yr,
    dividend_type,
    ex_dividend_date,
    EXTRACT(YEAR from ex_dividend_date) as ex_dividend_yr,
    frequency,
    pay_date,
    EXTRACT(YEAR from pay_date) as pay_yr
FROM raw.all_dividends_history
)
SELECT * FROM source_data
