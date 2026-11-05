-- Timezone conversion measured in whole seconds with datediff(), so the assertion does
-- not depend on which timezone renders a timestamp. Expected = offset(source) -
-- offset(target) from the IANA tz database, covering UTC / Asia/Shanghai /
-- America/Los_Angeles including a spring-forward gap and a fall-back ambiguity.
{{ config(materialized='table', tags=['maxcompute']) }}
select 'tz_sh_to_la_jan' as case_id,
       57600 as expected_shift_seconds,
       datediff(timestamp'2026-01-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-01-15 12:00:00'", 'America/Los_Angeles', 'Asia/Shanghai')}}, 'ss') as actual_shift_seconds
union all
select 'tz_sh_to_la_jul' as case_id,
       54000 as expected_shift_seconds,
       datediff(timestamp'2026-07-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-07-15 12:00:00'", 'America/Los_Angeles', 'Asia/Shanghai')}}, 'ss') as actual_shift_seconds
union all
select 'tz_la_to_utc_jul' as case_id,
       -25200 as expected_shift_seconds,
       datediff(timestamp'2026-07-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-07-15 12:00:00'", 'UTC', 'America/Los_Angeles')}}, 'ss') as actual_shift_seconds
union all
select 'tz_utc_to_sh_jul' as case_id,
       -28800 as expected_shift_seconds,
       datediff(timestamp'2026-07-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-07-15 12:00:00'", 'Asia/Shanghai', 'UTC')}}, 'ss') as actual_shift_seconds
union all
select 'tz_la_to_la_jan' as case_id,
       0 as expected_shift_seconds,
       datediff(timestamp'2026-01-15 12:00:00', {{ dbt_date.convert_timezone("timestamp'2026-01-15 12:00:00'", 'America/Los_Angeles', 'America/Los_Angeles')}}, 'ss') as actual_shift_seconds
union all
select 'tz_gap_la_spring' as case_id,
       -28800 as expected_shift_seconds,
       datediff(timestamp'2026-03-08 02:30:00', {{ dbt_date.convert_timezone("timestamp'2026-03-08 02:30:00'", 'UTC', 'America/Los_Angeles')}}, 'ss') as actual_shift_seconds
union all
select 'tz_ambig_la_fall' as case_id,
       -25200 as expected_shift_seconds,
       datediff(timestamp'2026-11-01 01:30:00', {{ dbt_date.convert_timezone("timestamp'2026-11-01 01:30:00'", 'UTC', 'America/Los_Angeles')}}, 'ss') as actual_shift_seconds
union all
select 'tz_xyear_sh_to_la' as case_id,
       57600 as expected_shift_seconds,
       datediff(timestamp'2025-12-31 23:00:00', {{ dbt_date.convert_timezone("timestamp'2025-12-31 23:00:00'", 'America/Los_Angeles', 'Asia/Shanghai')}}, 'ss') as actual_shift_seconds
