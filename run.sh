#!/bin/bash

INPUT=$1
if [ -z "$INPUT" ]; then
    INPUT="measurements.txt"
fi

erlc src/erlang_1brc.erl
/bin/time -f "Elapsed time: %e seconds (%E)" \
    erl \
    -noinput \
    -s erlang_1brc run "$1" \
    -eval "erlang:halt()."
