#!/bin/bash

erlc src/erlang_1brc.erl
/bin/time -f "Elapsed time: %E" \
    erl \
    +SDio 1 \
    +SDPcpu 50 \
    +sbwt none \
    +sbwtdio none \
    -noinput \
    -s erlang_1brc run "$1" \
    -eval "erlang:halt()."
