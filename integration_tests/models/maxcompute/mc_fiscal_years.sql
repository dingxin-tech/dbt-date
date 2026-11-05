-- Fiscal year boundaries: 'Saturday nearest the end of year_end_month', with the year start
-- taken from lag(week_end) + 1 day. The window deliberately contains a complete preceding
-- fiscal year, otherwise the first year would silently disappear (see the README).
{{ config(materialized='table', tags=['maxcompute']) }}
{{ dbt_date.get_fiscal_year_dates(ref('mc_date_dimension_fiscal'), year_end_month=1, week_start_day=1, shift_year=1) }}
