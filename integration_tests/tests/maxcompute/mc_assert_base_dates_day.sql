-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
with agg as (
    select count(*) as n, min(date_day) as mn, max(date_day) as mx
    from {{ ref('mc_base_dates_day') }}
)
select 'rows' as failed_check, cast(n as string) as actual, '59' as expected
from agg where n <> 59
union all
select 'first_period', cast(mn as string), '2026-01-01 00:00:00'
from agg where mn <> timestamp'2026-01-01 00:00:00'
union all
select 'last_period', cast(mx as string), '2026-02-28 00:00:00'
from agg where mx <> timestamp'2026-02-28 00:00:00'
