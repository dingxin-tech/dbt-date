-- Coverage assertion: the column-by-column comparisons join model to expectation on
-- date_day, so a spine that quietly drops or adds a period would pass them. This test
-- compares the two row sets directly: same count, same first day, same last day.
with checks as (
    select 'cross_year_rows' as check_name,
           cast(m.n as string) as actual, cast(e.n as string) as expected
    from (select count(*) as n from {{ ref('mc_date_dimension_cross_year') }}) m,
         (select count(*) as n from {{ ref('mc_expected_date_dimension_cross_year') }}) e
    where m.n <> e.n
    union all
    select 'cross_year_span',
           cast(concat(cast(m.mn as string), ' .. ', cast(m.mx as string)) as string),
           cast(concat(cast(e.mn as string), ' .. ', cast(e.mx as string)) as string)
    from (select min(date_day) as mn, max(date_day) as mx from {{ ref('mc_date_dimension_cross_year') }}) m,
         (select min(date_day) as mn, max(date_day) as mx from {{ ref('mc_expected_date_dimension_cross_year') }}) e
    where m.mn <> e.mn or m.mx <> e.mx
    union all
    select 'leap_day_rows',
           cast(m.n as string), cast(e.n as string)
    from (select count(*) as n from {{ ref('mc_date_dimension_leap_day') }}) m,
         (select count(*) as n from {{ ref('mc_expected_date_dimension_leap_day') }}) e
    where m.n <> e.n
    union all
    select 'leap_day_span',
           cast(concat(cast(m.mn as string), ' .. ', cast(m.mx as string)) as string),
           cast(concat(cast(e.mn as string), ' .. ', cast(e.mx as string)) as string)
    from (select min(date_day) as mn, max(date_day) as mx from {{ ref('mc_date_dimension_leap_day') }}) m,
         (select min(date_day) as mn, max(date_day) as mx from {{ ref('mc_expected_date_dimension_leap_day') }}) e
    where m.mn <> e.mn or m.mx <> e.mx
)
select * from checks
