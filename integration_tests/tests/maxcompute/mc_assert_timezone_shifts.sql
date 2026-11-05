-- Contract assertion for the MaxCompute macros; it returns one row per violated expectation,
-- so a clean run means every compared column matched.
with exp_tbl as (
            select 'tz_sh_to_la_jan' as case_id, 57600 as expected_shift
union all
            select 'tz_sh_to_la_jul' as case_id, 54000 as expected_shift
union all
            select 'tz_la_to_utc_jul' as case_id, -25200 as expected_shift
union all
            select 'tz_utc_to_sh_jul' as case_id, -28800 as expected_shift
union all
            select 'tz_la_to_la_jan' as case_id, 0 as expected_shift
union all
            select 'tz_gap_la_spring' as case_id, -28800 as expected_shift
union all
            select 'tz_ambig_la_fall' as case_id, -25200 as expected_shift
union all
            select 'tz_xyear_sh_to_la' as case_id, 57600 as expected_shift
        )
        select t.case_id, t.actual_shift_seconds as actual, e.expected_shift
        from {{ ref('mc_timezone_shifts') }} t
        join exp_tbl e on t.case_id = e.case_id
        where t.actual_shift_seconds <> e.expected_shift
