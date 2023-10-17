with num as (
    SELECT year, quarter, tag, adsh, sum(value) as value
    FROM `new-life-400922.raw.sec_financials_num`
    WHERE tag in (
        'EarningsPerShareBasic',
        'EarningsPerShareDiluted',
        'ProfitLoss'
    )
    GROUP BY 1, 2, 3, 4
),
sub as (
    SELECT DISTINCT adsh, year, quarter, cik
    FROM `new-life-400922.raw.sec_financials_sub`
)
SELECT
    a.year,
    a.quarter,
    b.cik,
    sum(case when tag = "EarningsPerShareBasic" then value else 0 end) as earnings_per_share_basic,
    sum(case when tag = "EarningsPerShareDiluted" then value else 0 end) as earnings_per_share_diluted,
    sum(case when tag = "ProfitLoss" then value else 0 end) as profit_loss,

FROM num as a
INNER JOIN sub AS b
ON a.adsh = b.adsh
AND a.year = b.year
AND a.quarter = b.quarter
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
