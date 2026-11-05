-- Independently re-derived 4-5-4 period map (quarter, slot, boundaries, weeks).
{{ config(materialized='table', tags=['maxcompute']) }}
-- Independently derived 4-5-4 period map (24 periods, covering 2025-02-02..2027-01-30)
select * from (
    select 2025 as fiscal_year_number, 1 as fiscal_period_number, 1 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2025-02-02' as period_start_date, date'2025-03-01' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 2 as fiscal_period_number, 1 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2025-03-02' as period_start_date, date'2025-04-05' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 3 as fiscal_period_number, 1 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2025-04-06' as period_start_date, date'2025-05-03' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 4 as fiscal_period_number, 2 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2025-05-04' as period_start_date, date'2025-05-31' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 5 as fiscal_period_number, 2 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2025-06-01' as period_start_date, date'2025-07-05' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 6 as fiscal_period_number, 2 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2025-07-06' as period_start_date, date'2025-08-02' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 7 as fiscal_period_number, 3 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2025-08-03' as period_start_date, date'2025-08-30' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 8 as fiscal_period_number, 3 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2025-08-31' as period_start_date, date'2025-10-04' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 9 as fiscal_period_number, 3 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2025-10-05' as period_start_date, date'2025-11-01' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 10 as fiscal_period_number, 4 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2025-11-02' as period_start_date, date'2025-11-29' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 11 as fiscal_period_number, 4 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2025-11-30' as period_start_date, date'2026-01-03' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2025 as fiscal_year_number, 12 as fiscal_period_number, 4 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2026-01-04' as period_start_date, date'2026-01-31' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 1 as fiscal_period_number, 1 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2026-02-01' as period_start_date, date'2026-02-28' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 2 as fiscal_period_number, 1 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2026-03-01' as period_start_date, date'2026-04-04' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 3 as fiscal_period_number, 1 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2026-04-05' as period_start_date, date'2026-05-02' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 4 as fiscal_period_number, 2 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2026-05-03' as period_start_date, date'2026-05-30' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 5 as fiscal_period_number, 2 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2026-05-31' as period_start_date, date'2026-07-04' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 6 as fiscal_period_number, 2 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2026-07-05' as period_start_date, date'2026-08-01' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 7 as fiscal_period_number, 3 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2026-08-02' as period_start_date, date'2026-08-29' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 8 as fiscal_period_number, 3 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2026-08-30' as period_start_date, date'2026-10-03' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 9 as fiscal_period_number, 3 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2026-10-04' as period_start_date, date'2026-10-31' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 10 as fiscal_period_number, 4 as fiscal_quarter_number, 1 as fiscal_period_of_quarter, date'2026-11-01' as period_start_date, date'2026-11-28' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 11 as fiscal_period_number, 4 as fiscal_quarter_number, 2 as fiscal_period_of_quarter, date'2026-11-29' as period_start_date, date'2027-01-02' as period_end_date, 5 as weeks_in_period, 5 as max_week_of_period
    union all
    select 2026 as fiscal_year_number, 12 as fiscal_period_number, 4 as fiscal_quarter_number, 3 as fiscal_period_of_quarter, date'2027-01-03' as period_start_date, date'2027-01-30' as period_end_date, 4 as weeks_in_period, 4 as max_week_of_period
) fps
