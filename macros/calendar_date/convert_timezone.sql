{%- macro convert_timezone(column, target_tz=None, source_tz=None) -%}
{%- set source_tz = "UTC" if not source_tz else source_tz -%}
{%- set target_tz = var("dbt_date:time_zone") if not target_tz else target_tz -%}
{{ adapter.dispatch('convert_timezone', 'dbt_date') (column, target_tz, source_tz) }}
{%- endmacro -%}

{% macro default__convert_timezone(column, target_tz, source_tz) -%}
convert_timezone('{{ source_tz }}', '{{ target_tz }}',
    cast({{ column }} as {{ dbt.type_timestamp() }})
)
{%- endmacro -%}

{%- macro bigquery__convert_timezone(column, target_tz, source_tz=None) -%}
timestamp(datetime({{ column }}, '{{ target_tz}}'))
{%- endmacro -%}

{% macro postgres__convert_timezone(column, target_tz, source_tz) -%}
cast(
    cast({{ column }} as {{ dbt.type_timestamp() }})
        at time zone '{{ source_tz }}' at time zone '{{ target_tz }}' as {{ dbt.type_timestamp() }}
)
{%- endmacro -%}

{%- macro redshift__convert_timezone(column, target_tz, source_tz) -%}
{{ return(dbt_date.default__convert_timezone(column, target_tz, source_tz)) }}
{%- endmacro -%}

{% macro duckdb__convert_timezone(column, target_tz, source_tz) -%}
{{ return(dbt_date.postgres__convert_timezone(column, target_tz, source_tz)) }}
{%- endmacro -%}

{%- macro spark__convert_timezone(column, target_tz, source_tz) -%}
from_utc_timestamp(
        to_utc_timestamp({{ column }}, '{{ source_tz }}'),
        '{{ target_tz }}'
        )
{%- endmacro -%}

{%- macro trino__convert_timezone(column, target_tz, source_tz) -%}
    cast((at_timezone(with_timezone(cast({{ column }} as {{ dbt.type_timestamp() }}), '{{ source_tz }}'), '{{ target_tz }}')) as {{ dbt.type_timestamp() }})
{%- endmacro -%}

{%- macro maxcompute__convert_timezone(column, target_tz, source_tz=None) -%}
{#-
  source_tz used to be dropped: the emitted SQL only shifted by the target offset,
  so convert_timezone(col, 'America/Los_Angeles', source_tz='Asia/Shanghai') moved the
  value by -7h instead of -15h. Measured on the server: from_utc_timestamp adds the
  target offset, to_utc_timestamp subtracts it, so composing the two yields the
  target-minus-source shift regardless of the project timezone. With source_tz='UTC'
  to_utc_timestamp() is the identity, so the default path keeps the previous SQL.
-#}
{%- set source_tz = 'UTC' if not source_tz else source_tz -%}
{%- if source_tz == 'UTC' -%}
cast(from_utc_timestamp({{ column }}, '{{ target_tz }}') as {{ dbt.type_timestamp() }})
{%- else -%}
cast(from_utc_timestamp(to_utc_timestamp({{ column }}, '{{ source_tz }}'), '{{ target_tz }}') as {{ dbt.type_timestamp() }})
{%- endif -%}
{%- endmacro -%}
