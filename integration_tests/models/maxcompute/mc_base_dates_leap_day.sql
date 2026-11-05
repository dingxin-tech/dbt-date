-- Fixed-range date spine built from literal start/end dates.
-- Expectation: the span is end-exclusive -- offsets 0..intervals-1, i.e. the same 4
-- rows every other adapter produces, so the last period is 2028-03-01, not 2028-02-27.
-- Month/year arithmetic still clamps to the last valid day the way MaxCompute dateadd
-- does (2024-01-31 +1 month = 2024-02-29).
{{ config(materialized='table', tags=['maxcompute']) }}
{{ dbt_date.get_base_dates(start_date='2028-02-27', end_date='2028-03-02', datepart='day') }}
