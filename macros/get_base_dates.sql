{% macro get_base_dates(start_date=None, end_date=None, n_dateparts=None, datepart="day") %}
    {{ adapter.dispatch('get_base_dates', 'dbt_date') (start_date, end_date, n_dateparts, datepart) }}
{% endmacro %}

{% macro default__get_base_dates(start_date, end_date, n_dateparts, datepart) %}

{%- if start_date and end_date -%}
{%- set start_date="cast('" ~ start_date ~ "' as " ~ dbt.type_timestamp() ~ ")" -%}
{%- set end_date="cast('" ~ end_date ~ "' as " ~ dbt.type_timestamp() ~ ")"  -%}

{%- elif n_dateparts and datepart -%}

{%- set start_date = dbt.dateadd(datepart, -1 * n_dateparts, dbt_date.today()) -%}
{%- set end_date = dbt_date.tomorrow() -%}
{%- endif -%}

with date_spine as
(

    {{ dbt_date.date_spine(
        datepart=datepart,
        start_date=start_date,
        end_date=end_date,
       )
    }}

)
select
    cast(d.date_{{ datepart }} as {{ dbt.type_timestamp() }}) as date_{{ datepart }}
from
    date_spine d
{% endmacro %}

{% macro bigquery__get_base_dates(start_date, end_date, n_dateparts, datepart) %}

{%- if start_date and end_date -%}
{%- set start_date="cast('" ~ start_date ~ "' as datetime )" -%}
{%- set end_date="cast('" ~ end_date ~ "' as datetime )" -%}

{%- elif n_dateparts and datepart -%}

{%- set start_date = dbt.dateadd(datepart, -1 * n_dateparts, dbt_date.today()) -%}
{%- set end_date = dbt_date.tomorrow() -%}
{%- endif -%}

with date_spine as
(

    {{ dbt_date.date_spine(
        datepart=datepart,
        start_date=start_date,
        end_date=end_date,
       )
    }}

)
select
    cast(d.date_{{ datepart }} as {{ dbt.type_timestamp() }}) as date_{{ datepart }}
from
    date_spine d
{% endmacro %}


{% macro trino__get_base_dates(start_date, end_date, n_dateparts, datepart) %}

{%- if start_date and end_date -%}
{%- set start_date="cast('" ~ start_date ~ "' as " ~ dbt.type_timestamp() ~ ")" -%}
{%- set end_date="cast('" ~ end_date ~ "' as " ~ dbt.type_timestamp() ~ ")"  -%}

{%- elif n_dateparts and datepart -%}

{%- set start_date = dbt.dateadd(datepart, -1 * n_dateparts, dbt_date.now()) -%}
{%- set end_date = dbt_date.tomorrow() -%}
{%- endif -%}

with date_spine as
(

    {{ dbt_date.date_spine(
        datepart=datepart,
        start_date=start_date,
        end_date=end_date,
       )
    }}

)
select
    cast(d.date_{{ datepart }} as {{ dbt.type_timestamp() }}) as date_{{ datepart }}
from
    date_spine d
{% endmacro %}

{% macro maxcompute__get_base_dates(start_date, end_date, n_dateparts, datepart) %}

{%- if start_date and end_date -%}
    {#- Before this fix the string branch emitted timestamp'2026-01-01' (no time part),
        which MaxCompute rejects, while the non-string branch happened to format correctly:
        two asymmetric branches, so every get_base_dates(start_date=..., end_date=...) call
        with literal dates failed and took the whole date spine / fiscal family with it. -#}
    {%- set start_date=dbt.type_timestamp() ~ "'" ~ dbt_date.maxcompute_timestamp_literal(start_date) ~ "'" -%}
    {%- set end_date=dbt.type_timestamp() ~ "'" ~ dbt_date.maxcompute_timestamp_literal(end_date) ~ "'" -%}
{%- elif n_dateparts and datepart -%}

{%- set start_date = dbt.dateadd(datepart, -1 * n_dateparts, dbt_date.today()) -%}
{%- set end_date = dbt_date.tomorrow() -%}
{%- endif -%}

with date_spine as
(

    {{ dbt_date.date_spine(
        datepart=datepart,
        start_date=start_date,
        end_date=end_date,
       )
    }}

)
select
    cast(d.date_{{ datepart }} as {{ dbt.type_timestamp() }}) as date_{{ datepart }}
from
    date_spine d
{% endmacro %}
