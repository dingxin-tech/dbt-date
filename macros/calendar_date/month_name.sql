{%- macro month_name(date, short=True) -%}
    {{ adapter.dispatch('month_name', 'dbt_date') (date, short) }}
{%- endmacro %}

{%- macro default__month_name(date, short) -%}
{%- set f = 'MON' if short else 'MONTH' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro bigquery__month_name(date, short) -%}
{%- set f = '%b' if short else '%B' -%}
    format_date('{{ f }}', cast({{ date }} as date))
{%- endmacro %}

{%- macro snowflake__month_name(date, short) -%}
{%- set f = 'MON' if short else 'MMMM' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro postgres__month_name(date, short) -%}
{# FM = Fill mode, which suppresses padding blanks #}
{%- set f = 'FMMon' if short else 'FMMonth' -%}
    to_char({{ date }}, '{{ f }}')
{%- endmacro %}


{%- macro duckdb__month_name(date, short) -%}
    {%- if short -%}
    substr(monthname({{ date }}), 1, 3)
    {%- else -%}
    monthname({{ date }})
    {%- endif -%}
{%- endmacro %}

{%- macro spark__month_name(date, short) -%}
{%- set f = 'MMM' if short else 'MMMM' -%}
    date_format({{ date }}, '{{ f }}')
{%- endmacro %}

{%- macro trino__month_name(date, short) -%}
{%- set f = 'b' if short else 'M' -%}
    date_format({{ date }}, '%{{ f }}')
{%- endmacro %}

{%- macro maxcompute__month_name(date, short) -%}
{%- if short -%}
case month({{ date }})
    when 1 then 'Jan'
    when 2 then 'Feb'
    when 3 then 'Mar'
    when 4 then 'Apr'
    when 5 then 'May'
    when 6 then 'Jun'
    when 7 then 'Jul'
    when 8 then 'Aug'
    when 9 then 'Sep'
    when 10 then 'Oct'
    when 11 then 'Nov'
    when 12 then 'Dec'
end
{%- else -%}
case month({{ date }})
    when 1 then 'January'
    when 2 then 'February'
    when 3 then 'March'
    when 4 then 'April'
    when 5 then 'May'
    when 6 then 'June'
    when 7 then 'July'
    when 8 then 'August'
    when 9 then 'September'
    when 10 then 'October'
    when 11 then 'November'
    when 12 then 'December'
end
{%- endif -%}
{%- endmacro %}