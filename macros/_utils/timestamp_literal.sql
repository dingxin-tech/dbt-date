{%- macro maxcompute_timestamp_literal(value) -%}
    {#-
      Normalise an accepted input into the TIMESTAMP literal shape that MaxCompute reads the
      same way everywhere, and refuse the shapes it does not.

      Every line below was measured on a live three-tier project with the hints this adapter
      sends (dbt/adapters/maxcompute/context.py), on dbt-core 1.11.2 - the two columns are
      the same text used as a `TIMESTAMP '...'` literal and as `cast('...' as timestamp)`:

        input                        literal                     cast
        2026-01-01 10:20:30          10:20:30                    10:20:30
        2026-01-01T10:20:30          10:20:30                    NULL   <- silently
        2026-01-01                   ODPS-0130161 parse error    NULL   <- silently
        2026-01-01 10:20:30.123      10:20:30.123000             10:20:30.123000
        2026-01-01 10:20:30+08:00    10:20:30  (offset IGNORED)  10:20:30
        2026-01-01 10:20:30Z         18:20:30  (CONVERTED +08)   10:20:30
        2026-01-01 10:20:30-05:00    ODPS-0130161 parse error    10:20:30
        2026-01-01 10:20:30+08       ODPS-0130161 parse error    10:20:30
        2026-1-1 1:2:3               01:02:03                    01:02:03

      What that means for this helper:

      * an ISO `T` separator is accepted by a literal but silently NULLs the identical text
        through a cast, so it is normalised to a space rather than passed through;
      * fractional seconds are accepted and kept, so they are kept here too - the previous
        version cut every value at 19 characters and dropped them quietly;
      * a timezone designator is refused: the server ignores a numeric offset, converts `Z`,
        and rejects the other two spellings, so there is no normalisation that preserves the
        instant. Convert to a project-local naive value first;
      * nothing is truncated any more. An input this helper cannot render exactly is a
        compile-time error, not a NULL column three layers away.
    -#}
    {%- if value is string -%}
        {%- set text = value | trim -%}
        {%- set shape = '^(\d{1,4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d+))?)?([Zz]|[+-]\d{1,2}:?\d{2})?$' -%}
        {%- set m = modules.re.match(shape, text) -%}
        {%- if not m -%}
            {{ exceptions.raise_compiler_error(
                "dbt_date could not read the date '" ~ value ~ "'. On MaxCompute pass a date as 'yyyy-mm-dd' "
                ~ "or a timestamp as 'yyyy-mm-dd hh:mm:ss' (optionally with a fractional part). The server "
                ~ "rejects date-only TIMESTAMP literals, silently casts 'yyyy-mm-ddTHH:MM:SS' to NULL, and "
                ~ "does not accept a timezone designator - see macros/_utils/timestamp_literal.sql for the "
                ~ "measured behaviour of each form.")
            }}
        {%- endif -%}
        {%- if m.group(8) -%}
            {{ exceptions.raise_compiler_error(
                "dbt_date refused the timestamp '" ~ value ~ "': MaxCompute does not treat a timezone "
                ~ "designator consistently (measured: '+08:00' is ignored and the wall time is kept, 'Z' is "
                ~ "converted to the project timezone, '-05:00' and '+08' are parse errors). Rendering this "
                ~ "as a naive literal would change the instant without saying so. Convert the value to the "
                ~ "project's local time first, then pass 'yyyy-mm-dd hh:mm:ss'.")
            }}
        {%- endif -%}
        {%- set date_part = "%04d-%02d-%02d" | format(m.group(1) | int, m.group(2) | int, m.group(3) | int) -%}
        {%- if m.group(4) is none -%}
            {{ return(date_part ~ ' 00:00:00') }}
        {%- endif -%}
        {%- set clock = "%02d:%02d:%02d" | format(m.group(4) | int, m.group(5) | int, m.group(6) | int) -%}
        {%- set fraction = m.group(7) -%}
        {%- if fraction -%}
            {{ return(date_part ~ ' ' ~ clock ~ '.' ~ fraction) }}
        {%- endif -%}
        {{ return(date_part ~ ' ' ~ clock) }}
    {%- else -%}
        {#- date / datetime objects. A tz-aware one is refused for the reason above; a plain
           `date` has no tzinfo attribute, so the check has to tolerate an undefined look-up. -#}
        {%- if value.tzinfo is defined and value.tzinfo is not none -%}
            {{ exceptions.raise_compiler_error(
                "dbt_date refused the timezone-aware value '" ~ value ~ "': MaxCompute does not read a "
                ~ "timezone designator in a TIMESTAMP literal the way you would expect (measured: offsets "
                ~ "are ignored, 'Z' is shifted by the project timezone). Convert to the project's local "
                ~ "time before passing it to dbt_date.")
            }}
        {%- endif -%}
        {{ return(value.strftime('%Y-%m-%d %H:%M:%S')) }}
    {%- endif -%}
{%- endmacro -%}
