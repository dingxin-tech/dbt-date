-- Aggregates the per-day fiscal result into one row per fiscal period so the
-- expectation can assert period boundaries and week counts instead of
-- restating the macro's own arithmetic.
{{ config(materialized='table', tags=['maxcompute']) }}

select
    fiscal_year_number,
    fiscal_period_number,
    max(fiscal_quarter_number) as fiscal_quarter_number,
    max(fiscal_period_of_quarter) as fiscal_period_of_quarter,
    min(date_day) as period_start_date,
    max(date_day) as period_end_date,
    count(distinct week_start_date) as weeks_in_period,
    max(fiscal_week_of_period) as max_week_of_period
from {{ ref('mc_fiscal_periods') }}
group by fiscal_year_number, fiscal_period_number
