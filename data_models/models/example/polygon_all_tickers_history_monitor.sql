
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT
        count(distinct ticker) as distinct_tickers,
        count(*) as n_rows,
        min(date) as min_dt,
        max(date) as max_dt
    FROM raw.all_tickers_history
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
