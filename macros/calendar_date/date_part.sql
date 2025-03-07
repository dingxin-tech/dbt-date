{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch('date_part', 'dbt_date') (datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{  date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro trino__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro maxcompute__date_part(datepart, date) -%}
    {% set datepart = datepart.lower() %}
    {%- if datepart == 'week' -%}
        weekofyear({{ date }})
    {%- elif datepart in['year', 'month', 'day', 'hour', 'minute'] -%}
        extract({{ datepart }} from {{ date }})
    {%- elif datepart == 'quarter' -%}
        quarter({{ date }})
    {%- elif datepart == 'dayofweek' -%}
        dayofweek({{ date }})
    {%- else -%}
    {{ exceptions.raise_compiler_error(
        "value " ~ datepart ~ " for date_part is not supported."
        )
    }}
    {% endif -%}
{%- endmacro %}
