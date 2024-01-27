#!/bin/bash

INPUT=$1
if [ -z "$INPUT" ]; then
    INPUT="measurements.txt"
fi

set -e
# erlc +bin_opt_info -W src/erlang_1brc.erl
erlc -W src/erlang_1brc.erl || exit 1
/bin/time -f "Elapsed time: %e seconds (%E) (%P CPU)" \
    erl \
    -noinput \
    -s erlang_1brc run "$1" \
    -eval "erlang:halt()."
