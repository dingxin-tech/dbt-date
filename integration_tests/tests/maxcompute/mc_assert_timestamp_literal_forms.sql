-- Returns one row per violated expectation, so a clean run means every accepted input rendered
-- to exactly the expected text AND the server reads that text as the expected instant.
--
-- `rendered <> expected_rendered` is the macro's own contract. `cast(rendered as timestamp)` is
-- the engine's: a normalisation that MaxCompute interprets differently (the reason the ISO `T`
-- must be replaced by a space) fails here instead of producing NULL rows downstream. Both
-- comparisons treat a NULL read-back as a violation - a silent NULL is the defect this package
-- is supposed to make impossible.
with text_shape as (
    select case_id, rendered, expected_rendered, 'rendered text differs from the canonical form' as failure_kind
    from {{ ref('mc_timestamp_literal_forms') }}
    where rendered <> expected_rendered
),
engine_reads_it as (
    select case_id, rendered, expected_rendered,
           'the server did not read the rendered literal as the expected instant' as failure_kind
    from {{ ref('mc_timestamp_literal_forms') }}
    where cast(rendered as timestamp) is null
       or cast(rendered as timestamp) <> expected_value
),
-- Why the ISO form is normalised rather than passed through: measured, and pinned here so the
-- day MaxCompute changes this is the day this assertion tells us to re-read the helper.
raw_T_is_silent_NULL as (
    select 'raw_T_cast' as case_id, '' as rendered, '' as expected_rendered,
           'cast(<ISO T text> as timestamp) is no longer NULL - re-measure before trusting the T form' as failure_kind
    where cast('2026-01-01T10:20:30' as timestamp) is not null
)
select * from text_shape
union all
select * from engine_reads_it
union all
select * from raw_T_is_silent_NULL
