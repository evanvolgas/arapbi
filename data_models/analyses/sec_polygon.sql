with foo as (SELECT distinct a.year, a.quarter, cik
FROM `new-life-400922.raw.sec_financials_sub` as a
inner join `new-life-400922.raw.sec_financials_num` as b
on a.year = b.year
and a.quarter = b.quarter
and a.adsh = b.adsh)
select * from foo as c
inner join `new-life-400922.etl.rpt_all_tickers_by_quarter` as d
on c.cik = d.cik
and c.year = d.year
and c.quarter = d.quarter
order by c.cik,c.year, c.quarter
limit 2000