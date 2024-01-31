#!/bin/bash

INPUT=$1
if [ -z "$INPUT" ]; then
    INPUT="measurements.txt"
fi

set -e
/bin/time -f "Elapsed time: %e seconds (%E) (%P CPU)" \
    erl \
    -noinput \
    -s erlang_1brc main "$1" \
    -eval "erlang:halt()."
