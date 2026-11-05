-- Relative-range spine: get_base_dates(n_dateparts=1) derives
-- start = today - 1 day and end = tomorrow. The end-exclusive contract means the
-- generated span is yesterday and today -- it must not emit the future day.
-- Row counts only are asserted here, so the expectation moves with the run date.
{{ config(materialized='table', tags=['maxcompute']) }}
{{ dbt_date.get_base_dates(n_dateparts=1, datepart='day') }}
