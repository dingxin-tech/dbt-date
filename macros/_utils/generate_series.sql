{% macro get_powers_of_two(upper_bound) %}
    {{ return(adapter.dispatch('get_powers_of_two', 'dbt_date')(upper_bound)) }}
{% endmacro %}

{% macro default__get_powers_of_two(upper_bound) %}

    {% if upper_bound <= 0 %}
    {{ exceptions.raise_compiler_error("upper bound must be positive") }}
    {% endif %}

    {% for _ in range(1, 100) %}
       {% if upper_bound <= 2 ** loop.index %}{{ return(loop.index) }}{% endif %}
    {% endfor %}

{% endmacro %}


{% macro generate_series(upper_bound) %}
    {{ return(adapter.dispatch('generate_series', 'dbt_date')(upper_bound)) }}
{% endmacro %}

{% macro default__generate_series(upper_bound) %}

    {% set n = dbt_date.get_powers_of_two(upper_bound) %}

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    {% for i in range(n) %}
    p{{i}}.generated_number * power(2, {{i}})
    {% if not loop.last %} + {% endif %}
    {% endfor %}
    + 1
    as generated_number

    from

    {% for i in range(n) %}
    p as p{{i}}
    {% if not loop.last %} cross join {% endif %}
    {% endfor %}

    )

    select *
    from unioned
    where generated_number <= {{upper_bound}}
    order by generated_number

{% endmacro %}

{% macro maxcompute__generate_series(upper_bound) %}
    {#
      The default implementation builds 2^n rows by cross joining a 2-row CTE, which
      MaxCompute rejects: ODPS-0130252 "cartesian product is not allowed without mapjoin".
      sequence() + lateral view explode() is native and needs no extra session flag.
    #}
    {% if upper_bound <= 0 %}
    {{ exceptions.raise_compiler_error("upper bound must be positive") }}
    {% endif %}

    select mc_seq.val as generated_number
    from (select 1 as mc_seed) mc_row
    lateral view explode(sequence(1, {{ upper_bound }})) mc_seq as val
{% endmacro %}
