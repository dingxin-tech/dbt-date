-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
with exp_tbl as (
select 2025 as fiscal_year_number, date'2025-02-02' as fiscal_year_start_date, date'2026-01-31' as fiscal_year_end_date
union all
select 2026 as fiscal_year_number, date'2026-02-01' as fiscal_year_start_date, date'2027-01-30' as fiscal_year_end_date
    )
    select 'boundary' as check_name, e.fiscal_year_number,
           cast(a.fiscal_year_start_date as string) as actual_start,
           cast(e.fiscal_year_start_date as string) as expected_start,
           cast(a.fiscal_year_end_date as string) as actual_end,
           cast(e.fiscal_year_end_date as string) as expected_end
    from exp_tbl e
    left join (
        select fiscal_year_number, min(fiscal_year_start_date) as fiscal_year_start_date,
               max(fiscal_year_end_date) as fiscal_year_end_date
        from {{ ref('mc_fiscal_years') }} group by fiscal_year_number
    ) a on a.fiscal_year_number = e.fiscal_year_number
    where a.fiscal_year_number is null
       or a.fiscal_year_start_date <> e.fiscal_year_start_date
       or a.fiscal_year_end_date <> e.fiscal_year_end_date
