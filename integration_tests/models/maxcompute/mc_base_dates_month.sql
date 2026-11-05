-- Fixed-range date spine built from literal start/end dates.
-- Expectation: the span is end-exclusive -- offsets 0..intervals-1, i.e. the same 3
-- rows every other adapter produces, so the last period is 2024-03-31, not 2024-01-31.
-- Month/year arithmetic still clamps to the last valid day the way MaxCompute dateadd
-- does (2024-01-31 +1 month = 2024-02-29).
{{ config(materialized='table', tags=['maxcompute']) }}
{{ dbt_date.get_base_dates(start_date='2024-01-31', end_date='2024-04-30', datepart='month') }}
