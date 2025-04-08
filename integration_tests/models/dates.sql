{{
    config(
        materialized = "table"
    )
}}
{{ dbt_date.get_date_dimension('DATE \'2015-01-01\'', 'DATE \'2022-12-31\'') }}
