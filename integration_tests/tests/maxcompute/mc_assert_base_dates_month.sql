-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
with agg as (
    select count(*) as n, min(date_month) as mn, max(date_month) as mx
    from {{ ref('mc_base_dates_month') }}
)
select 'rows' as failed_check, cast(n as string) as actual, '3' as expected
from agg where n <> 3
union all
select 'first_period', cast(mn as string), '2024-01-31 00:00:00'
from agg where mn <> timestamp'2024-01-31 00:00:00'
union all
select 'last_period', cast(mx as string), '2024-03-31 00:00:00'
from agg where mx <> timestamp'2024-03-31 00:00:00'
