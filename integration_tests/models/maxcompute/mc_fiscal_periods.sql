-- 4-5-4 fiscal periods over the same window.
{{ config(materialized='table', tags=['maxcompute']) }}
{{ dbt_date.get_fiscal_periods(ref('mc_date_dimension_fiscal'), year_end_month=1, week_start_day=1, shift_year=1) }}
