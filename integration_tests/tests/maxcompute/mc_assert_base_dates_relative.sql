-- Contract assertion for the relative (n_dateparts) path: two rows, yesterday and today,
-- and explicitly not tomorrow. Rows/first/last are compared against dbt_date.today()
-- rendered in the same run, so this is a shape check rather than a fixed calendar value.
with agg as (
    select count(*) as n, min(date_day) as mn, max(date_day) as mx
    from {{ ref('mc_base_dates_relative') }}
),
bounds as (
    select cast({{ dbt.dateadd('day', -1, dbt_date.today()) }} as timestamp) as yesterday_ts,
           cast({{ dbt_date.today() }} as timestamp) as today_ts
)
select 'rows' as failed_check, cast(agg.n as string) as actual, '2' as expected
from agg where agg.n <> 2
union all
select 'first_period', cast(agg.mn as string), cast(bounds.yesterday_ts as string)
from agg, bounds where agg.mn <> bounds.yesterday_ts
union all
select 'last_period', cast(agg.mx as string), cast(bounds.today_ts as string)
from agg, bounds where agg.mx <> bounds.today_ts
union all
select 'tomorrow_excluded', 'present', 'absent'
from agg, bounds
where agg.mx > bounds.today_ts
