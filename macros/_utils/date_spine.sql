{% macro get_intervals_between(start_date, end_date, datepart) -%}
    {{ return(adapter.dispatch('get_intervals_between', 'dbt_date')(start_date, end_date, datepart)) }}
{%- endmacro %}

{% macro default__get_intervals_between(start_date, end_date, datepart) -%}
    {%- call statement('get_intervals_between', fetch_result=True) %}

        select {{ dbt.datediff(start_date, end_date, datepart) }}

    {%- endcall -%}

    {%- set value_list = load_result('get_intervals_between') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values[0]) }}
    {%- else -%}
        {{ return(1) }}
    {%- endif -%}

{%- endmacro %}




{% macro date_spine(datepart, start_date, end_date) %}
    {{ return(adapter.dispatch('date_spine', 'dbt_date')(datepart, start_date, end_date)) }}
{%- endmacro %}

{% macro default__date_spine(datepart, start_date, end_date) %}


{# call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dbt.dateadd(week, 1, current_date)"
) #}


with rawdata as (

    {{
        dbt_date.generate_series(
            dbt_date.get_intervals_between(start_date, end_date, datepart)
        )
    }}

),

all_periods as (

    select (
        {{
            dbt.dateadd(
                datepart,
                "(row_number() over (order by 1) - 1)",
                start_date
            )
        }}
    ) as date_{{datepart}}
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_{{datepart}} <= {{ end_date }}

)

select * from filtered

{% endmacro %}

{% macro maxcompute__date_spine(datepart, start_date, end_date) %}
{#
  Two MaxCompute-specific departures from the default implementation:
    1. the row source: default__date_spine builds it with generate_series(), whose
       cross-join body MaxCompute rejects (ODPS-0130252); maxcompute__generate_series
       keeps that row count and its "upper bound must be positive" guard, using the
       native sequence() + lateral view explode().
    2. the shape: the default emits a WITH block, and get_base_dates already wraps this
       macro inside `with date_spine as ( ... )`, so a nested WITH/lateral view would sit
       inside a CTE. This version stays a single flat SELECT so it can be embedded anywhere.

  The span itself is deliberately NOT redefined here: generate_series(intervals) yields
  `intervals` rows and `generated_number - 1` turns them into offsets 0..intervals-1, exactly
  like the default's `row_number() - 1`. So the end date is excluded -- 2026-01-01..2026-03-01
  daily is 59 rows ending 2026-02-28, and get_base_dates(n_dateparts=1) ends today rather than
  emitting tomorrow(). The datediff filter is kept only as a guard for datepart rounding; it
  must not be load-bearing, and an inclusive endpoint would be a behaviour change to propose
  separately rather than to fold into this compatibility fix.
#}
select * from (

    select (
        {{
            dbt.dateadd(
                datepart,
                'mc_offsets.offset_n',
                start_date
            )
        }}
    ) as date_{{datepart}}
    from (
        select generated_number - 1 as offset_n
        from (
            {{ dbt_date.generate_series(dbt_date.get_intervals_between(start_date, end_date, datepart)) }}
        ) mc_series
    ) mc_offsets

) mc_periods
where datediff(date_{{datepart}}, {{ end_date }}) <= 0
{% endmacro %}
