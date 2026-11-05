-- convert_timezone() with no explicit target uses the `dbt_date:time_zone` variable.
-- Expected = -offset(that zone at the input instant), measured with datediff().
{{ config(materialized='table', tags=['maxcompute']) }}
    select 'tvar_jul' as case_id, 25200 as expected_shift_seconds,
           datediff(timestamp'2026-07-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-07-15 12:00:00'") }}, 'ss') as actual_shift_seconds
    union all
    select 'tvar_jan' as case_id, 28800 as expected_shift_seconds,
           datediff(timestamp'2026-01-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-01-15 12:00:00'") }}, 'ss') as actual_shift_seconds
