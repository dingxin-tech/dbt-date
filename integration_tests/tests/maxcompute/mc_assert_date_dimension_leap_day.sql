-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
select * from (
    select 'date_day' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.date_day as string), '<null>') <> coalesce(cast(e.date_day as string), '<null>')
    union all
    select 'prior_date_day' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_date_day as string), '<null>') <> coalesce(cast(e.prior_date_day as string), '<null>')
    union all
    select 'next_date_day' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.next_date_day as string), '<null>') <> coalesce(cast(e.next_date_day as string), '<null>')
    union all
    select 'prior_year_date_day' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_date_day as string), '<null>') <> coalesce(cast(e.prior_year_date_day as string), '<null>')
    union all
    select 'prior_year_over_year_date_day' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_over_year_date_day as string), '<null>') <> coalesce(cast(e.prior_year_over_year_date_day as string), '<null>')
    union all
    select 'day_of_week' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_week as string), '<null>') <> coalesce(cast(e.day_of_week as string), '<null>')
    union all
    select 'day_of_week_iso' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_week_iso as string), '<null>') <> coalesce(cast(e.day_of_week_iso as string), '<null>')
    union all
    select 'day_of_week_name' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_week_name as string), '<null>') <> coalesce(cast(e.day_of_week_name as string), '<null>')
    union all
    select 'day_of_week_name_short' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_week_name_short as string), '<null>') <> coalesce(cast(e.day_of_week_name_short as string), '<null>')
    union all
    select 'day_of_month' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_month as string), '<null>') <> coalesce(cast(e.day_of_month as string), '<null>')
    union all
    select 'day_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.day_of_year as string), '<null>') <> coalesce(cast(e.day_of_year as string), '<null>')
    union all
    select 'week_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.week_start_date as string), '<null>') <> coalesce(cast(e.week_start_date as string), '<null>')
    union all
    select 'week_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.week_end_date as string), '<null>') <> coalesce(cast(e.week_end_date as string), '<null>')
    union all
    select 'prior_year_week_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_week_start_date as string), '<null>') <> coalesce(cast(e.prior_year_week_start_date as string), '<null>')
    union all
    select 'prior_year_week_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_week_end_date as string), '<null>') <> coalesce(cast(e.prior_year_week_end_date as string), '<null>')
    union all
    select 'week_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.week_of_year as string), '<null>') <> coalesce(cast(e.week_of_year as string), '<null>')
    union all
    select 'iso_week_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.iso_week_start_date as string), '<null>') <> coalesce(cast(e.iso_week_start_date as string), '<null>')
    union all
    select 'iso_week_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.iso_week_end_date as string), '<null>') <> coalesce(cast(e.iso_week_end_date as string), '<null>')
    union all
    select 'prior_year_iso_week_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_iso_week_start_date as string), '<null>') <> coalesce(cast(e.prior_year_iso_week_start_date as string), '<null>')
    union all
    select 'prior_year_iso_week_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_iso_week_end_date as string), '<null>') <> coalesce(cast(e.prior_year_iso_week_end_date as string), '<null>')
    union all
    select 'iso_week_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.iso_week_of_year as string), '<null>') <> coalesce(cast(e.iso_week_of_year as string), '<null>')
    union all
    select 'prior_year_week_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_week_of_year as string), '<null>') <> coalesce(cast(e.prior_year_week_of_year as string), '<null>')
    union all
    select 'prior_year_iso_week_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_iso_week_of_year as string), '<null>') <> coalesce(cast(e.prior_year_iso_week_of_year as string), '<null>')
    union all
    select 'month_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.month_of_year as string), '<null>') <> coalesce(cast(e.month_of_year as string), '<null>')
    union all
    select 'month_name' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.month_name as string), '<null>') <> coalesce(cast(e.month_name as string), '<null>')
    union all
    select 'month_name_short' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.month_name_short as string), '<null>') <> coalesce(cast(e.month_name_short as string), '<null>')
    union all
    select 'month_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.month_start_date as string), '<null>') <> coalesce(cast(e.month_start_date as string), '<null>')
    union all
    select 'month_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.month_end_date as string), '<null>') <> coalesce(cast(e.month_end_date as string), '<null>')
    union all
    select 'prior_year_month_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_month_start_date as string), '<null>') <> coalesce(cast(e.prior_year_month_start_date as string), '<null>')
    union all
    select 'prior_year_month_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.prior_year_month_end_date as string), '<null>') <> coalesce(cast(e.prior_year_month_end_date as string), '<null>')
    union all
    select 'quarter_of_year' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.quarter_of_year as string), '<null>') <> coalesce(cast(e.quarter_of_year as string), '<null>')
    union all
    select 'quarter_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.quarter_start_date as string), '<null>') <> coalesce(cast(e.quarter_start_date as string), '<null>')
    union all
    select 'quarter_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.quarter_end_date as string), '<null>') <> coalesce(cast(e.quarter_end_date as string), '<null>')
    union all
    select 'year_number' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.year_number as string), '<null>') <> coalesce(cast(e.year_number as string), '<null>')
    union all
    select 'year_start_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.year_start_date as string), '<null>') <> coalesce(cast(e.year_start_date as string), '<null>')
    union all
    select 'year_end_date' as column_name, count(*) as mismatching_rows
    from {{ ref('mc_date_dimension_leap_day') }} a
    join {{ ref('mc_expected_date_dimension_leap_day') }} e on a.date_day = e.date_day
    where coalesce(cast(a.year_end_date as string), '<null>') <> coalesce(cast(e.year_end_date as string), '<null>')
) u
where mismatching_rows > 0
