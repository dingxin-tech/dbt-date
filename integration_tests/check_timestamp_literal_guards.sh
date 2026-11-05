#!/usr/bin/env bash
# Contract check for maxcompute_timestamp_literal(): the shapes the helper must REFUSE.
#
# A dbt data test cannot assert a compile error - the run aborts before any SQL exists - so the
# rejection cases are driven through `dbt compile --select` on a temporary model. Nothing here
# executes against a warehouse: compile renders the macro and writes the SQL, which is exactly
# where a wrong literal would be caught anyway.
#
# Requires: the same env the repo's ci/profiles.yml expects (MC_TEST_PROJECT, MC_TEST_SCHEMA and
# ODPS_* credentials in the environment) and a `dbt` on PATH or in DBT_BIN.
#
# Usage:  ./integration_tests/check_timestamp_literal_guards.sh
# Exit:   0 every rejected shape raised, the canonical shape compiled; 1 otherwise.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_BIN="${DBT_BIN:-dbt}"
MODELS="$HERE/models/maxcompute"
SELECTOR_TARGET="guard_probe"

# value | the message fragment that must appear
CASES="2026-01-01 10:20:30+08:00|refused the timestamp
2026-01-01 10:20:30Z|refused the timestamp
2026-01-01 10:20:30-05:00|refused the timestamp
2026-01-01T10:20:30.123456Z|refused the timestamp
2026-01-01T|could not read the date
2026-01-01 10:20|could not read the date
next tuesday|could not read the date
<empty>|could not read the date"

failures=0
checks=0

write_guard_model() {
    printf '{{ config(tags=["guard"]) }}\nselect %s as rendered\n' \
        "'{{ dbt_date.maxcompute_timestamp_literal(\"$1\") }}'" > "$MODELS/guard_probe.sql"
}

run_compile() {
    (cd "$HERE" && timeout 300 "$DBT_BIN" compile -t maxcompute --no-partial-parse --select guard_probe 2>&1)
}

echo "== refusals: each must be a compile error naming the reason"
while IFS='|' read -r value needle; do
    [ -n "$needle" ] || { echo "  FAIL malformed case row: [$value]"; failures=$((failures + 1)); continue; }
    checks=$((checks + 1))
    # An empty input is a case, not a missing one: it is spelled <empty> in the table because a
    # blank field would be indistinguishable from a broken row, which is how this script used to
    # skip it silently while still counting it.
    [ "$value" = "<empty>" ] && value=""
    write_guard_model "$value"
    out="$(run_compile)"; rc=$?
    if [ $rc -eq 0 ]; then
        echo "  FAIL [$value] compiled; the helper accepted a shape it must refuse"
        failures=$((failures + 1))
    elif ! grep -qF -- "$needle" <<<"$out"; then
        echo "  FAIL [$value] failed for a different reason than '$needle'"
        printf '%s\n' "$out" | grep -E "Compilation Error|Error" | head -2 | sed 's/^/        /'
        failures=$((failures + 1))
    else
        echo "  ok   [$value] refused, message carries: $needle"
    fi
done <<< "$CASES"

echo "== acceptance: the canonical form must compile"
checks=$((checks + 1))
write_guard_model "2026-01-01 10:20:30"
out="$(run_compile)"; rc=$?
if [ $rc -ne 0 ]; then
    echo "  FAIL the canonical 'yyyy-mm-dd hh:mm:ss' value did not compile"
    printf '%s\n' "$out" | tail -5 | sed 's/^/        /'
    failures=$((failures + 1))
else
    echo "  ok   [2026-01-01 10:20:30] compiled"
fi

# The temp model must never survive this script, whatever happens to it.
cleanup() { rm -f "$MODELS/guard_probe.sql"; }
trap cleanup EXIT

echo "TIMESTAMP-LITERAL-GUARDS checks=$checks failures=$failures"
[ "$failures" -eq 0 ] || exit 1
echo "status: PASSED (every refused shape raised for the documented reason)"
