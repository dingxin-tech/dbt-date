-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
with exp_tbl as (
select 'tvar_jul' as case_id, 25200 as expected_shift
union all
select 'tvar_jan' as case_id, 28800 as expected_shift
    )
    select t.case_id, t.actual_shift_seconds as actual, e.expected_shift
    from {{ ref('mc_timezone_default_zone') }} t
    join exp_tbl e on t.case_id = e.case_id
    where t.actual_shift_seconds <> e.expected_shift
